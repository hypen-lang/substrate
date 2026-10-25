use substrate_liquid::*;

#[test]
fn projected_native_reads_preserve_values_order_reference_visibility_and_revocation() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "copy",
            1,
            vec![],
            vec![
                Relation::new("copy.Target", vec![Role::new("name", DataType::Text)]),
                Relation::new(
                    "copy.Row",
                    vec![
                        Role::new("target", DataType::Ref("copy.Target".into())),
                        Role::new("n", DataType::Int),
                        Role::new("payload", DataType::Bytes),
                    ],
                ),
            ],
        ),
        Visibility::Private,
    )
    .unwrap();
    let visible = tx.insert("copy.Target", vec!["visible".into()]).unwrap();
    let hidden = tx.insert("copy.Target", vec!["hidden".into()]).unwrap();
    for i in 0..12 {
        tx.insert(
            "copy.Row",
            vec![
                if i % 2 == 0 {
                    visible.into()
                } else {
                    hidden.into()
                },
                i.into(),
                Value::Bytes(vec![42; 65536]),
            ],
        )
        .unwrap();
    }
    tx.grant("reader", "copy.Row", Grant::read_only()).unwrap();
    tx.grant(
        "reader",
        "copy.Target",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "visible".into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("reader").snapshot();
    for columns in [vec!["r.n", "r.id"], vec!["r.payload"], vec![]] {
        let expected = snapshot
            .table("copy.Row", "r")
            .unwrap()
            .project(&columns)
            .unwrap();
        let projected = snapshot.projected_table("copy.Row", "r", &columns).unwrap();
        assert_eq!(expected, projected);
        assert_eq!(projected.rows().len(), 6);
    }
    assert!(
        snapshot
            .projected_table("copy.Row", "r", &["r.n", "r.n"])
            .is_err()
    );
    assert!(
        snapshot
            .projected_table("copy.Row", "r", &["r.missing"])
            .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "copy.Target").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        snapshot.projected_table("copy.Row", "r", &["r.n"]),
        Err(Error::Denied)
    ));
}

#[test]
fn unchanged_bucket_reads_still_conflict_with_payload_updates() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "conflict",
        1,
        vec![],
        vec![
            Relation::new(
                "conflict.Row",
                vec![
                    Role::new("key", DataType::Int),
                    Role::new("value", DataType::Int),
                ],
            )
            .unique(&["key"]),
        ],
    ))
    .unwrap();
    let id = tx.insert("conflict.Row", vec![1.into(), 2.into()]).unwrap();
    tx.commit().unwrap();
    let mut reader = db.admin().transaction();
    assert_eq!(
        reader
            .lookup("conflict.Row", "key", &1.into())
            .unwrap()
            .len(),
        1
    );
    let mut writer = db.admin().transaction();
    writer
        .update("conflict.Row", id, vec![1.into(), 3.into()])
        .unwrap();
    writer.commit().unwrap();
    assert!(matches!(reader.commit(), Err(Error::Conflict)));
    db.admin().snapshot().verify_indexes().unwrap();
}
