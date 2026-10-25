use substrate_liquid::{disk_snapshot::*, *};
fn fixture() -> (Database, Vec<Id>) {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "data",
            1,
            vec![],
            vec![
                Relation::new(
                    "data.Record",
                    vec![
                        Role::new("group", DataType::Int),
                        Role::new("text", DataType::Text),
                        Role::new("blob", DataType::Bytes),
                        Role::new("flag", DataType::Bool),
                        Role::new("amount", DataType::Decimal { scale: 2 }),
                        Role::new("at", DataType::Timestamp),
                    ],
                )
                .check(Check::int_range("group", 0, 3)),
            ],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    let ids = (0..25)
        .map(|n| {
            tx.insert(
                "data.Record",
                vec![
                    (n % 4).into(),
                    format!("row 🦀 {n}").as_str().into(),
                    Value::Bytes(vec![n as u8; 32]),
                    Value::Bool(n % 2 == 0),
                    Value::Decimal(-n),
                    Value::Timestamp(n),
                ],
            )
            .unwrap()
        })
        .collect();
    tx.grant(
        "reader",
        "data.Record",
        Grant {
            filter: RowFilter::Equals {
                role: "group".into(),
                value: 1.into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    (db, ids)
}
fn collect(disk: &DiskSnapshot, predicate: Option<(&str, &Value)>, limit: usize) -> Vec<Row> {
    let mut all = vec![];
    let mut after = None;
    loop {
        let page = disk.page("data.Record", predicate, after, limit).unwrap();
        all.extend(page.rows);
        after = page.next;
        if after.is_none() {
            return all;
        }
    }
}
#[test]
fn packed_points_pages_and_indexed_equality_match_native_snapshot() {
    let (db, ids) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let snapshot = db.admin().snapshot();
    let disk = snapshot
        .spill(
            dir.path().join("spill"),
            &["data.Record"],
            SpillOptions::default(),
        )
        .unwrap();
    assert_eq!(disk.stats().unwrap().rows, 25);
    assert_eq!(disk.stats().unwrap().retained_source_row_payload_bytes, 0);
    for id in ids {
        assert_eq!(
            disk.get("data.Record", id).unwrap(),
            snapshot.get("data.Record", id).unwrap()
        );
    }
    assert_eq!(
        collect(&disk, None, 3),
        snapshot.scan("data.Record").unwrap()
    );
    for (role, value) in [
        ("group", Value::Int(2)),
        ("text", "row 🦀 4".into()),
        ("blob", Value::Bytes(vec![5; 32])),
        ("flag", Value::Bool(true)),
        ("amount", Value::Decimal(-4)),
        ("at", Value::Timestamp(5)),
    ] {
        assert_eq!(
            collect(&disk, Some((role, &value)), 2),
            snapshot.lookup("data.Record", role, &value).unwrap()
        );
    }
    disk.verify().unwrap();
}
#[test]
fn materialized_rows_are_scoped_and_revocation_invalidates_every_endpoint() {
    let (db, _) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let snapshot = db.session("reader").snapshot();
    let disk = snapshot
        .spill(
            dir.path().join("spill"),
            &["data.Record"],
            SpillOptions::default(),
        )
        .unwrap();
    let expected = snapshot.scan("data.Record").unwrap();
    assert_eq!(collect(&disk, None, 100), expected);
    assert!(disk.page("secret.Other", None, None, 1).is_err());
    assert!(collect(&disk, Some(("group", &0.into())), 100).is_empty());
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "data.Record").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        disk.page("data.Record", None, None, 1),
        Err(Error::Denied)
    ));
    assert!(matches!(
        disk.get("data.Record", expected[0].id),
        Err(Error::Denied)
    ));
    assert!(disk.stats().is_err());
    assert!(disk.verify().is_err());
}
#[test]
fn source_drop_invalidates_admin_lease_and_reopen_does_not_restore_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let db = Database::open(&path).unwrap();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
        .unwrap();
    let id = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.commit().unwrap();
    let disk = db
        .admin()
        .snapshot()
        .spill(
            dir.path().join("spill"),
            &["core.Person"],
            SpillOptions::default(),
        )
        .unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert!(matches!(disk.get("core.Person", id), Err(Error::Denied)));
    let fresh = reopened
        .admin()
        .snapshot()
        .spill(
            dir.path().join("fresh"),
            &["core.Person"],
            SpillOptions::default(),
        )
        .unwrap();
    assert!(fresh.get("core.Person", id).unwrap().is_some());
}
#[test]
fn historical_rows_remain_pinned_without_retaining_source_state() {
    let (db, ids) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let snapshot = db.admin().snapshot();
    let expected = snapshot.get("data.Record", ids[0]).unwrap();
    let disk = snapshot
        .spill(
            dir.path().join("spill"),
            &["data.Record"],
            SpillOptions::default(),
        )
        .unwrap();
    let revision = disk.stats().unwrap().revision;
    drop(snapshot);
    let mut tx = db.admin().transaction();
    for id in ids {
        tx.delete("data.Record", id).unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        disk.get("data.Record", expected.as_ref().unwrap().id)
            .unwrap(),
        expected
    );
    assert_eq!(
        disk.page("data.Record", None, None, 3).unwrap().revision,
        revision
    );
    assert_eq!(disk.stats().unwrap().rows, 25);
    assert!(
        db.admin()
            .snapshot()
            .scan("data.Record")
            .unwrap()
            .is_empty()
    );
}
#[test]
fn output_bounds_options_and_cleanup_are_enforced_without_overwriting_files() {
    let (db, _) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let snapshot = db.admin().snapshot();
    let path = dir.path().join("spill");
    let options = SpillOptions {
        max_row_bytes: 200,
        max_page_bytes: 200,
        ..Default::default()
    };
    let disk = snapshot.spill(&path, &["data.Record"], options).unwrap();
    assert_eq!(
        disk.page("data.Record", None, None, 1000)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(collect(&disk, None, 1000).len(), 25);
    assert!(snapshot.spill(&path, &["data.Record"], options).is_err());
    disk.verify().unwrap();
    let failed = dir.path().join("too-small");
    assert!(
        snapshot
            .spill(
                &failed,
                &["data.Record"],
                SpillOptions {
                    max_row_bytes: 20,
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert!(!failed.exists());
    for limit in [0, 1001] {
        assert!(disk.page("data.Record", None, None, limit).is_err());
    }
    assert!(
        disk.page(
            "data.Record",
            Some(("group", &Value::Text("2".into()))),
            None,
            1
        )
        .is_err()
    );
    assert!(
        snapshot
            .spill(
                dir.path().join("duplicate"),
                &["data.Record", "data.Record"],
                options
            )
            .is_err()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
#[test]
fn reference_closure_filters_rows_and_hidden_reference_metadata_is_not_spilled() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
        .unwrap();
    tx.install_with_visibility(
        Module::new(
            "links",
            1,
            vec![("core".into(), 1)],
            vec![Relation::new(
                "links.Link",
                vec![Role::new("person", DataType::Ref("core.Person".into()))],
            )],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    let ada = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    let bob = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    let link = tx.insert("links.Link", vec![ada.into()]).unwrap();
    tx.insert("links.Link", vec![bob.into()]).unwrap();
    tx.grant("reader", "links.Link", Grant::read_only())
        .unwrap();
    tx.grant("hidden", "links.Link", Grant::read_only())
        .unwrap();
    tx.grant(
        "reader",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "Ada".into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let dir = tempfile::tempdir().unwrap();
    assert!(
        db.session("hidden")
            .snapshot()
            .spill(
                dir.path().join("hidden"),
                &["links.Link"],
                SpillOptions::default()
            )
            .is_err()
    );
    let disk = db
        .session("reader")
        .snapshot()
        .spill(
            dir.path().join("safe"),
            &["links.Link"],
            SpillOptions::default(),
        )
        .unwrap();
    assert_eq!(
        disk.page("links.Link", None, None, 10).unwrap().rows[0].id,
        link
    );
    assert_eq!(disk.stats().unwrap().rows, 1);
    assert_eq!(
        disk.page("links.Link", Some(("person", &ada.into())), None, 10)
            .unwrap()
            .rows
            .len(),
        1
    );
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(disk.verify().is_err());
}
