use substrate_liquid::*;

#[test]
fn restricted_owned_result_capacity_does_not_reveal_hidden_candidates() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "capacity",
            1,
            vec![],
            vec![Relation::new(
                "capacity.Row",
                vec![
                    Role::new("key", DataType::Int),
                    Role::new("allowed", DataType::Bool),
                ],
            )],
        ),
        Visibility::Private,
    )
    .unwrap();
    tx.grant(
        "reader",
        "capacity.Row",
        Grant {
            filter: RowFilter::Equals {
                role: "allowed".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    for visible in [false, true] {
        if visible {
            let mut tx = db.admin().transaction();
            tx.insert("capacity.Row", vec![1.into(), Value::Bool(true)])
                .unwrap();
            tx.commit().unwrap();
        }
        let before = db.session("reader").snapshot();
        let capacity = before.scan("capacity.Row").unwrap().capacity();
        let bucket_capacity = before
            .lookup("capacity.Row", "key", &1.into())
            .unwrap()
            .capacity();
        let mut tx = db.admin().transaction();
        for _ in 0..128 {
            tx.insert("capacity.Row", vec![1.into(), Value::Bool(false)])
                .unwrap();
        }
        tx.commit().unwrap();
        let after = db.session("reader").snapshot();
        for _ in 0..2 {
            let rows = after.scan("capacity.Row").unwrap();
            assert_eq!(rows.len(), usize::from(visible));
            assert_eq!(rows.capacity(), capacity);
            assert_eq!(
                after
                    .lookup("capacity.Row", "key", &1.into())
                    .unwrap()
                    .capacity(),
                bucket_capacity
            );
        }
    }
}

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

#[test]
fn native_scans_keep_reference_dependencies_even_for_filtered_out_rows() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "scan",
            1,
            vec![],
            vec![
                Relation::new("scan.Target", vec![Role::new("allowed", DataType::Bool)]),
                Relation::new(
                    "scan.Row",
                    vec![
                        Role::new("target", DataType::Ref("scan.Target".into())),
                        Role::new("key", DataType::Int),
                    ],
                ),
            ],
        ),
        Visibility::Private,
    )
    .unwrap();
    let target = tx.insert("scan.Target", vec![Value::Bool(false)]).unwrap();
    tx.insert("scan.Row", vec![target.into(), 7.into()])
        .unwrap();
    tx.grant("reader", "scan.Row", Grant::read_only()).unwrap();
    tx.grant(
        "reader",
        "scan.Target",
        Grant {
            filter: RowFilter::Equals {
                role: "allowed".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let session = db.session("reader");
    // Prime negative snapshot decisions; transactional reads must independently
    // retain the hidden target dependency, for both family and bucket access.
    assert!(session.snapshot().scan("scan.Row").unwrap().is_empty());
    let mut scan = session.transaction();
    let mut lookup = session.transaction();
    assert!(scan.scan("scan.Row").unwrap().is_empty());
    assert!(
        lookup
            .lookup("scan.Row", "key", &7.into())
            .unwrap()
            .is_empty()
    );
    let mut writer = db.admin().transaction();
    writer
        .update("scan.Target", target, vec![Value::Bool(true)])
        .unwrap();
    writer.commit().unwrap();
    assert!(matches!(scan.commit(), Err(Error::Conflict)));
    assert!(matches!(lookup.commit(), Err(Error::Conflict)));
    let snapshot = session.snapshot();
    let rows = snapshot.scan("scan.Row").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows, snapshot.lookup("scan.Row", "key", &7.into()).unwrap());
}
