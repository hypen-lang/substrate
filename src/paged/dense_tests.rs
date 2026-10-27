//! End-to-end invariants for the production dense physical layout.
use super::*;
use proptest::prelude::*;
use rusqlite::types::Value as SqlValue;

fn id(n: u128) -> Id {
    // Neither insertion order nor application identity is a physical row offset.
    Id::from_u128(n.wrapping_mul(0x9e3779b97f4a7c15_6a09e667f3bcc909))
}

fn values(n: i64, text: String) -> Vec<Value> {
    vec![Value::Int(n), Value::Text(text)]
}

fn fixture(path: &Path, count: u128) -> PagedDatabase {
    let db = PagedDatabase::open(
        path,
        PagedOptions {
            cache_bytes: 1024 * 1024,
            decoded_cache_bytes: 0,
            max_page_rows: 17,
            max_page_bytes: 128 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "dense",
            1,
            vec![],
            vec![
                Relation::new(
                    "dense.Person",
                    vec![
                        Role::new("n", DataType::Int),
                        Role::new("text", DataType::Text),
                    ],
                )
                .unique(&["n"]),
                Relation::new(
                    "dense.Note",
                    vec![
                        Role::new("person", DataType::Ref("dense.Person".into())),
                        Role::new("score", DataType::Int),
                    ],
                ),
            ],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    for n in (1..=count).rev() {
        tx.insert_with_id(
            "dense.Person",
            id(n),
            values(n as i64, format!("person {n}")),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    let meta = snapshot.view.read.open_table(META).unwrap();
    assert_eq!(meta.get("format").unwrap().unwrap().value(), b"LPG3");
    drop(meta);
    drop(snapshot);
    db
}

fn all(snapshot: &PagedSnapshot, name: &str) -> Vec<Row> {
    let mut cursor = None;
    let mut rows = Vec::new();
    loop {
        let page = snapshot.scan_page(name, cursor, 17).unwrap();
        assert!(page.rows.len() <= 17);
        rows.extend(page.rows);
        match page.next {
            None => break,
            Some(next) => {
                assert!(cursor.is_none_or(|old| old < next));
                cursor = Some(next);
            }
        }
    }
    assert!(rows.windows(2).all(|pair| pair[0].id < pair[1].id));
    rows
}

fn physical_family(snapshot: &PagedSnapshot, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let tid = snapshot.view.catalog.state.names[name];
    let prefix = tid.0.as_bytes();
    let end = prefix_end(prefix);
    snapshot
        .view
        .rows
        .physical()
        .range::<&[u8]>((
            std::ops::Bound::Included(prefix.as_slice()),
            end.as_deref()
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
        ))
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

#[test]
fn arbitrary_ids_growth_shrink_delete_and_reopen_preserve_pinned_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path, 180);
    let old = db.admin().snapshot().unwrap();
    let original = all(&old, "dense.Person");
    let mut expected: BTreeMap<_, _> = original.iter().map(|row| (row.id, row.clone())).collect();
    // Cross the block target size, shrink again, and delete neighboring IDs.
    for (n, len) in [(1, 60_000), (2, 8_001), (3, 0), (90, 20_000)] {
        let mut tx = db.admin().transaction().unwrap();
        let vals = values(n as i64 + 1000, "λ".repeat(len / 2));
        tx.update("dense.Person", id(n), vals.clone()).unwrap();
        tx.commit().unwrap();
        expected.insert(
            id(n),
            Row {
                id: id(n),
                values: vals.into_boxed_slice(),
            },
        );
    }
    let mut tx = db.admin().transaction().unwrap();
    tx.update("dense.Person", id(1), values(1001, "small again".into()))
        .unwrap();
    tx.delete("dense.Person", id(2)).unwrap();
    tx.delete("dense.Person", id(179)).unwrap();
    tx.commit().unwrap();
    expected.insert(
        id(1),
        Row {
            id: id(1),
            values: values(1001, "small again".into()).into_boxed_slice(),
        },
    );
    expected.remove(&id(2));
    expected.remove(&id(179));
    assert_eq!(all(&old, "dense.Person"), original);
    assert_eq!(old.verify().unwrap().rows, 180);
    let expected: Vec<_> = expected.into_values().collect();
    assert_eq!(
        all(&db.admin().snapshot().unwrap(), "dense.Person"),
        expected
    );
    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
    drop(old);
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    assert_eq!(all(&snapshot, "dense.Person"), expected);
    assert_eq!(snapshot.verify().unwrap().rows, 178);
    for n in [2, 179] {
        let mut tx = db.admin().transaction().unwrap();
        assert!(
            tx.insert_with_id("dense.Person", id(n), values(n as i64, "reused".into()))
                .is_err()
        );
    }
}

#[test]
fn physical_neighbors_do_not_create_logical_write_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"), 12);
    assert_eq!(
        physical_family(&db.admin().snapshot().unwrap(), "dense.Person").len(),
        1
    );
    // Both writers start before either commit. Publishing the second must merge
    // with the current block rather than overwrite the first writer's row.
    let mut first = db.admin().transaction().unwrap();
    let mut second = db.admin().transaction().unwrap();
    first
        .update("dense.Person", id(1), values(101, "first".into()))
        .unwrap();
    second
        .update("dense.Person", id(2), values(102, "second".into()))
        .unwrap();
    first.commit().unwrap();
    second.commit().unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    assert_eq!(
        snapshot.get("dense.Person", id(1)).unwrap().unwrap().values[0],
        Value::Int(101)
    );
    assert_eq!(
        snapshot.get("dense.Person", id(2)).unwrap().unwrap().values[0],
        Value::Int(102)
    );
    snapshot.verify().unwrap();
    let mut first = db.admin().transaction().unwrap();
    let mut second = db.admin().transaction().unwrap();
    first
        .update("dense.Person", id(3), values(103, "first".into()))
        .unwrap();
    second
        .update("dense.Person", id(3), values(203, "second".into()))
        .unwrap();
    first.commit().unwrap();
    assert!(matches!(second.commit(), Err(Error::Conflict)));
}

#[test]
fn schema_extension_keeps_original_ids_values_and_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"), 75);
    let pinned = db.admin().snapshot().unwrap();
    let before = all(&pinned, "dense.Person");
    let physical_before = physical_family(&pinned, "dense.Person");
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "gaming",
            1,
            vec![("dense".into(), 1)],
            vec![Relation::new(
                "gaming.Profile",
                vec![Role::new("rank", DataType::Int)],
            )],
        ),
        "game",
        Visibility::Private,
    )
    .unwrap();
    for n in 1..=75 {
        tx.extend(
            "dense.Person",
            "gaming.Profile",
            id(n),
            vec![Value::Int(n as i64 * 2)],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let current = db.admin().snapshot().unwrap();
    assert_eq!(all(&pinned, "dense.Person"), before);
    assert_eq!(all(&current, "dense.Person"), before);
    assert_eq!(physical_family(&current, "dense.Person"), physical_before);
    for row in all(&current, "gaming.Profile") {
        assert!(before.iter().any(|person| person.id == row.id));
    }
    assert_eq!(current.verify().unwrap().rows, 150);
    assert!(matches!(
        db.session("game").unwrap().get("dense.Person", id(1)),
        Err(Error::Denied)
    ));
}

#[test]
fn dense_reference_reads_filter_join_aggregate_and_revoke_live() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"), 80);
    let mut tx = db.admin().transaction().unwrap();
    for n in 1..=80 {
        tx.insert_with_id(
            "dense.Note",
            id(1000 + n),
            vec![Value::Ref(id(n)), Value::Int(n as i64 * 10)],
        )
        .unwrap();
    }
    tx.set_visibility("dense.Note", Visibility::Public).unwrap();
    tx.grant(
        "reader",
        "dense.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "n".into(),
                value: Value::Int(37),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(all(&db.anonymous().snapshot().unwrap(), "dense.Note").is_empty());
    let reader = db.session("reader").unwrap().snapshot().unwrap();
    let notes = all(&reader, "dense.Note");
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].id, id(1037));
    let sql = reader.sql().unwrap();
    assert_eq!(
        sql.query("SELECT COUNT(*), SUM(score) FROM dense.Note", &[])
            .unwrap()
            .rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(370)]]
    );
    assert_eq!(sql.query("SELECT p.n, n.score FROM dense.Person p JOIN dense.Note n ON n.person = p.id WHERE p.n = 37", &[]).unwrap().rows,
        vec![vec![SqlValue::Integer(37), SqlValue::Integer(370)]]);
    let mut forbidden = db.session("reader").unwrap().transaction().unwrap();
    assert!(matches!(
        forbidden.insert("dense.Note", vec![Value::Ref(id(37)), Value::Int(0)]),
        Err(Error::Denied)
    ));
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "dense.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        reader.get("dense.Person", id(37)),
        Err(Error::Denied)
    ));
    assert!(sql.query("SELECT COUNT(*) FROM dense.Note", &[]).is_err());
    assert!(
        all(
            &db.session("reader").unwrap().snapshot().unwrap(),
            "dense.Note"
        )
        .is_empty()
    );
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn dense_crash_atomicity_covers_multiple_rows_indexes_references_and_visibility() {
    for point in [
        "paged_after_row_blocks",
        "paged_before_commit",
        "paged_after_persist",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = fixture(&path, 12);
        let previous_revision = db.stats().unwrap().revision;
        drop(db);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "paged::dense_tests::dense_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("SUBSTRATE_DENSE_CRASH_PATH", &path)
            .env("SUBSTRATE_TEST_CRASH_POINT", point)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91));
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let committed = point == "paged_after_persist";
        let snapshot = db.admin().snapshot().unwrap();
        assert_eq!(
            snapshot.revision(),
            previous_revision + u64::from(committed)
        );
        assert_eq!(
            snapshot.get("dense.Person", id(1)).unwrap().unwrap().values[0],
            Value::Int(if committed { 101 } else { 1 })
        );
        assert_eq!(
            snapshot.get("dense.Person", id(2)).unwrap().is_none(),
            committed
        );
        assert_eq!(
            snapshot.get("dense.Person", id(99)).unwrap().is_some(),
            committed
        );
        assert_eq!(
            snapshot.get("dense.Note", id(1001)).unwrap().is_some(),
            committed
        );
        assert_eq!(
            snapshot
                .lookup_page("dense.Person", "n", &Value::Int(101), None, 17)
                .unwrap()
                .rows
                .len(),
            usize::from(committed)
        );
        assert_eq!(db.anonymous().get("dense.Person", id(1)).is_ok(), committed);
        assert_eq!(snapshot.verify().unwrap().rows, 12 + u64::from(committed));
        if committed {
            let mut tx = db.admin().transaction().unwrap();
            assert!(tx.delete("dense.Person", id(1)).is_err());
        }
    }
}

#[test]
#[ignore]
fn dense_crash_child() {
    let Some(path) = std::env::var_os("SUBSTRATE_DENSE_CRASH_PATH") else {
        return;
    };
    let db = PagedDatabase::open(Path::new(&path), Default::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.update("dense.Person", id(1), values(101, "grown".repeat(2000)))
        .unwrap();
    tx.delete("dense.Person", id(2)).unwrap();
    tx.insert_with_id("dense.Person", id(99), values(99, "new".into()))
        .unwrap();
    tx.insert_with_id(
        "dense.Note",
        id(1001),
        vec![Value::Ref(id(1)), Value::Int(101)],
    )
    .unwrap();
    tx.set_visibility("dense.Person", Visibility::Public)
        .unwrap();
    tx.commit().unwrap();
    panic!("crash hook did not run");
}

#[test]
fn oversized_update_aborts_all_staged_block_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"), 12);
    let before = all(&db.admin().snapshot().unwrap(), "dense.Person");
    let revision = db.stats().unwrap().revision;
    let mut tx = db.admin().transaction().unwrap();
    tx.update("dense.Person", id(1), values(101, "staged".into()))
        .unwrap();
    assert!(
        tx.update(
            "dense.Person",
            id(2),
            values(102, "x".repeat(1024 * 1024 + 1))
        )
        .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    assert_eq!(db.stats().unwrap().revision, revision);
    assert_eq!(all(&db.admin().snapshot().unwrap(), "dense.Person"), before);
    db.admin().snapshot().unwrap().verify().unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn dense_mutation_sequences_match_relational_model(
        operations in proptest::collection::vec((1u128..65, 0u8..4, 0usize..6000), 1..25),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = fixture(&path, 40);
        let mut expected: BTreeMap<Id, Row> = all(&db.admin().snapshot().unwrap(), "dense.Person")
            .into_iter().map(|row| (row.id, row)).collect();
        let mut allocated: BTreeSet<Id> = expected.keys().copied().collect();
        for (step, (n, action, length)) in operations.into_iter().enumerate() {
            let pinned = db.admin().snapshot().unwrap();
            let before = all(&pinned, "dense.Person");
            let identity = id(n);
            let vals = values(n as i64, format!("{step}:{}", "z".repeat(length)));
            let mut tx = db.admin().transaction().unwrap();
            match action {
                0 | 1 if expected.contains_key(&identity) => {
                    tx.update("dense.Person", identity, vals.clone()).unwrap();
                    tx.commit().unwrap();
                    expected.insert(identity, Row { id: identity, values: vals.into_boxed_slice() });
                }
                0 | 1 if !allocated.contains(&identity) => {
                    tx.insert_with_id("dense.Person", identity, vals.clone()).unwrap();
                    tx.commit().unwrap();
                    allocated.insert(identity);
                    expected.insert(identity, Row { id: identity, values: vals.into_boxed_slice() });
                }
                2 if expected.contains_key(&identity) => {
                    tx.delete("dense.Person", identity).unwrap();
                    tx.commit().unwrap();
                    expected.remove(&identity);
                }
                3 if expected.contains_key(&identity) => {
                    tx.update("dense.Person", identity, vals).unwrap();
                    drop(tx); // A staged block rewrite must have no durable effects.
                }
                _ => drop(tx),
            }
            prop_assert_eq!(all(&pinned, "dense.Person"), before);
            drop(pinned);
            let snapshot = db.admin().snapshot().unwrap();
            prop_assert_eq!(all(&snapshot, "dense.Person"), expected.values().cloned().collect::<Vec<_>>());
            if step % 4 == 0 {
                prop_assert_eq!(snapshot.verify().unwrap().rows, expected.len() as u64);
                let mut actual = Vec::new();
                snapshot.visit_authorized_range("dense.Person", "n", Some((&Value::Int(10), false)), Some((&Value::Int(50), true)), &mut || Ok(()), &mut |row| {
                    actual.push(row.values[0].clone());
                    Ok(())
                }).unwrap();
                let mut ordered: Vec<_> = expected.values().map(|row| match row.values[0] { Value::Int(n) => n, _ => unreachable!() }).collect();
                ordered.sort_unstable();
                prop_assert_eq!(actual, ordered.iter().filter(|n| **n > 10 && **n <= 50).map(|n| Value::Int(*n)).collect::<Vec<_>>());
                let sql = snapshot.sql().unwrap();
                prop_assert_eq!(sql.query("SELECT n FROM dense.Person ORDER BY n DESC LIMIT 7", &[]).unwrap().rows,
                    ordered.iter().rev().take(7).map(|n| vec![SqlValue::Integer(*n)]).collect::<Vec<_>>());
            }
        }
        drop(db);
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let snapshot = db.admin().snapshot().unwrap();
        prop_assert_eq!(all(&snapshot, "dense.Person"), expected.values().cloned().collect::<Vec<_>>());
        prop_assert_eq!(snapshot.verify().unwrap().rows, expected.len() as u64);
    }
}
