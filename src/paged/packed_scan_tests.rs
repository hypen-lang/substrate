//! Security and failure parity for the fixed-width aggregate source.
use super::*;
use crate::{Module, Relation, Role};

fn fixture(path: &Path) -> PagedDatabase {
    let db = PagedDatabase::open(
        path,
        PagedOptions {
            decoded_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let scalar_roles = || {
        vec![
            Role::new("n", DataType::Int),
            Role::new("flag", DataType::Bool),
            Role::new("decimal", DataType::Decimal { scale: 2 }),
            Role::new("time", DataType::Timestamp),
        ]
    };
    let mut score_roles = vec![Role::new("person", DataType::Ref("p.Person".into()))];
    score_roles.extend(scalar_roles());
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "p",
            1,
            vec![],
            vec![
                Relation::new(
                    "p.Person",
                    vec![
                        Role::new("n", DataType::Int),
                        Role::new("text", DataType::Text),
                    ],
                ),
                Relation::new("p.Scalar", scalar_roles()),
                Relation::new("p.Score", score_roles),
                Relation::new("p.Variable", vec![Role::new("text", DataType::Text)]),
                Relation::new(
                    "p.Pair",
                    vec![
                        Role::new("left", DataType::Ref("p.Person".into())),
                        Role::new("right", DataType::Ref("p.Person".into())),
                    ],
                ),
                Relation::new(
                    "p.Chain",
                    vec![Role::new("score", DataType::Ref("p.Score".into()))],
                ),
                Relation::new(
                    "p.Wide",
                    (0..65)
                        .map(|n| Role::new(&format!("v{n}"), DataType::Bool))
                        .collect(),
                ),
            ],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    for n in 0..130u128 {
        tx.insert_with_id(
            "p.Person",
            Id::from_u128(n + 1),
            vec![
                Value::Int(n as i64),
                Value::Text("target payload".repeat(8)),
            ],
        )
        .unwrap();
    }
    for n in 0..130u128 {
        let values = vec![
            Value::Int(n as i64 - 65),
            Value::Bool(n % 2 == 0),
            Value::Decimal(n as i64 * -17),
            Value::Timestamp(n as i64 * 101),
        ];
        tx.insert_with_id("p.Scalar", Id::from_u128(2000 + n), values.clone())
            .unwrap();
        let mut score = vec![Value::Ref(Id::from_u128((n * 53) % 130 + 1))];
        score.extend(values);
        tx.insert_with_id("p.Score", Id::from_u128(1000 + (n * 37) % 130), score)
            .unwrap();
    }
    tx.insert_with_id(
        "p.Variable",
        Id::from_u128(3000),
        vec![Value::Text("variable".into())],
    )
    .unwrap();
    tx.insert_with_id(
        "p.Pair",
        Id::from_u128(4000),
        vec![Value::Ref(Id::from_u128(1)), Value::Ref(Id::from_u128(2))],
    )
    .unwrap();
    tx.insert_with_id(
        "p.Chain",
        Id::from_u128(5000),
        vec![Value::Ref(Id::from_u128(1000))],
    )
    .unwrap();
    for name in [
        "p.Person",
        "p.Scalar",
        "p.Score",
        "p.Variable",
        "p.Pair",
        "p.Chain",
        "p.Wide",
    ] {
        tx.grant("reader", name, Grant::read_only()).unwrap();
    }
    tx.grant("root_only", "p.Score", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    db
}

fn collect_fixed(snapshot: &PagedSnapshot, name: &str) -> Result<(bool, Vec<Row>, usize)> {
    let def = snapshot.def(name)?;
    let mut rows = Vec::new();
    let mut checks = 0;
    let used = snapshot.visit_fixed_authorized_rows(
        name,
        &mut || {
            checks += 1;
            Ok(())
        },
        &mut |id, row| {
            rows.push(Row {
                id,
                values: (0..def.roles.len())
                    .map(|slot| row.value(slot).unwrap().into_owned())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            });
            Ok(())
        },
    )?;
    Ok((used, rows, checks))
}

fn collect_general(snapshot: &PagedSnapshot, name: &str) -> Result<(Vec<Row>, usize)> {
    let mut rows = Vec::new();
    let mut checks = 0;
    snapshot.visit_authorized_rows(
        name,
        &mut || {
            checks += 1;
            Ok(())
        },
        &mut |row| {
            rows.push(row.clone());
            Ok(())
        },
    )?;
    Ok((rows, checks))
}

#[test]
fn fixed_batches_match_general_rows_and_source_work_across_batch_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    for principal in ["owner", "reader"] {
        let snapshot = db.session(principal).unwrap().snapshot().unwrap();
        for name in ["p.Scalar", "p.Score"] {
            let (used, fixed, fixed_checks) = collect_fixed(&snapshot, name).unwrap();
            let (general, general_checks) = collect_general(&snapshot, name).unwrap();
            assert!(used);
            assert_eq!(fixed.len(), 130);
            assert_eq!(fixed, general);
            assert_eq!(fixed_checks, general_checks);
        }
    }
    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
}

#[test]
fn fixed_scan_declines_unsupported_shapes_before_any_callback() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    for name in ["p.Variable", "p.Pair", "p.Chain", "p.Wide"] {
        let (used, rows, _) = collect_fixed(&snapshot, name).unwrap();
        assert!(!used, "{name}");
        assert!(rows.is_empty());
        let (general, _) = collect_general(&snapshot, name).unwrap();
        assert_eq!(general.len(), usize::from(name != "p.Wide"));
    }
}

#[test]
fn dense_scan_cost_hint_requires_the_actual_compiled_authorized_source() {
    for format in [PagedFormat::V2, PagedFormat::V3] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(super::tests::empty_format(&path, format));
        let db = fixture(&path);
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        for name in ["p.Scalar", "p.Score"] {
            assert_eq!(
                snapshot.has_dense_fixed_scan(name).unwrap(),
                format == PagedFormat::V3
            );
        }
        for name in ["p.Variable", "p.Pair", "p.Chain", "p.Wide"] {
            assert!(!snapshot.has_dense_fixed_scan(name).unwrap());
        }
        drop(snapshot);
        let mut tx = db.admin().transaction().unwrap();
        tx.grant(
            "reader",
            "p.Person",
            Grant {
                filter: RowFilter::Equals {
                    role: "n".into(),
                    value: Value::Int(3),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.grant(
            "reader",
            "p.Scalar",
            Grant {
                filter: RowFilter::Equals {
                    role: "n".into(),
                    value: Value::Int(3),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        assert!(!snapshot.has_dense_fixed_scan("p.Score").unwrap());
        assert!(!snapshot.has_dense_fixed_scan("p.Scalar").unwrap());
    }
}

#[test]
fn fixed_scan_declines_row_filters_and_capability_filters_before_callbacks() {
    use crate::capability::{CapabilityScope, RelationCapability};
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    for filtered in ["p.Score", "p.Person"] {
        let mut tx = db.admin().transaction().unwrap();
        tx.grant("reader", "p.Score", Grant::read_only()).unwrap();
        tx.grant("reader", "p.Person", Grant::read_only()).unwrap();
        tx.grant(
            "reader",
            filtered,
            Grant {
                filter: RowFilter::Equals {
                    role: "n".into(),
                    value: Value::Int(3),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        let (used, rows, _) = collect_fixed(&snapshot, "p.Score").unwrap();
        assert!(!used);
        assert!(rows.is_empty());
        assert_eq!(collect_general(&snapshot, "p.Score").unwrap().0.len(), 1);
    }
    let admin = db.admin().snapshot().unwrap();
    let capability = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![
                    RelationCapability {
                        relation: admin.def("p.Score").unwrap().id,
                        grant: Grant::read_only(),
                    },
                    RelationCapability {
                        relation: admin.def("p.Person").unwrap().id,
                        grant: Grant {
                            filter: RowFilter::Equals {
                                role: "n".into(),
                                value: Value::Int(3),
                            },
                            ..Grant::read_only()
                        },
                    },
                ],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let snapshot = db
        .session("owner")
        .unwrap()
        .with_capability(capability.id)
        .unwrap()
        .snapshot()
        .unwrap();
    assert!(!collect_fixed(&snapshot, "p.Score").unwrap().0);
    assert_eq!(collect_general(&snapshot, "p.Score").unwrap().0.len(), 1);
}

#[test]
fn fixed_scan_denied_references_remain_hidden_and_discovery_denials_fail() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let snapshot = db.session("root_only").unwrap().snapshot().unwrap();
    let (used, fixed, checks) = collect_fixed(&snapshot, "p.Score").unwrap();
    let (general, general_checks) = collect_general(&snapshot, "p.Score").unwrap();
    assert!(used);
    assert!(fixed.is_empty());
    assert_eq!(fixed, general);
    assert_eq!(checks, general_checks);
    assert!(matches!(
        collect_fixed(&snapshot, "p.Person"),
        Err(Error::Denied)
    ));
    assert!(matches!(
        collect_fixed(&db.anonymous().snapshot().unwrap(), "p.Score"),
        Err(Error::Denied)
    ));
}

#[test]
fn fixed_scan_preserves_page_reference_and_row_byte_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(fixture(&path));
    for (options, principal) in [
        (
            PagedOptions {
                max_visibility_rows: 1,
                ..Default::default()
            },
            "reader",
        ),
        (
            PagedOptions {
                max_page_bytes: 40,
                ..Default::default()
            },
            "reader",
        ),
        (
            PagedOptions {
                max_row_bytes: 60,
                ..Default::default()
            },
            "reader",
        ),
        (
            PagedOptions {
                max_page_rows: 1,
                ..Default::default()
            },
            "root_only",
        ),
    ] {
        let db = PagedDatabase::open(&path, options).unwrap();
        let snapshot = db.session(principal).unwrap().snapshot().unwrap();
        let a = collect_fixed(&snapshot, "p.Score");
        let b = collect_general(&snapshot, "p.Score");
        assert!(a.is_err());
        assert!(b.is_err());
        assert_eq!(a.unwrap_err().to_string(), b.unwrap_err().to_string());
    }
}

#[test]
fn fixed_scan_propagates_budget_visitor_errors_and_checks_live_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    let mut checks = 0;
    let mut visited = 0;
    let result = snapshot.visit_fixed_authorized_rows(
        "p.Score",
        &mut || {
            checks += 1;
            if checks == 3 {
                Err(Error::Query("target work exhausted".into()))
            } else {
                Ok(())
            }
        },
        &mut |_, _| {
            visited += 1;
            Ok(())
        },
    );
    assert!(matches!(result, Err(Error::Query(message)) if message == "target work exhausted"));
    assert_eq!(visited, 0);
    let result = snapshot.visit_fixed_authorized_rows("p.Score", &mut || Ok(()), &mut |_, _| {
        Err(Error::Query("visitor error".into()))
    });
    assert!(matches!(result, Err(Error::Query(message)) if message == "visitor error"));
    let mut changed = false;
    let result = snapshot.visit_fixed_authorized_rows("p.Score", &mut || Ok(()), &mut |_, _| {
        if !changed {
            let mut tx = db.admin().transaction()?;
            tx.grant(
                "reader",
                "p.Person",
                Grant {
                    filter: RowFilter::Equals {
                        role: "n".into(),
                        value: Value::Int(3),
                    },
                    ..Grant::read_only()
                },
            )?;
            tx.commit()?;
            changed = true;
        }
        Ok(())
    });
    assert!(matches!(result, Err(Error::Denied)));
    assert!(matches!(
        collect_fixed(&snapshot, "p.Score"),
        Err(Error::Denied)
    ));
}

#[test]
fn fixed_scan_rejects_corruption_even_in_hidden_referenced_rows() {
    for damage in [
        "missing_target",
        "bad_target",
        "bad_bool",
        "zero_revision",
        "future_revision",
        "bad_framing",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // Deliberate PG01 row-table corruption uses the legacy physical layout.
        drop(super::tests::empty_format(&path, PagedFormat::V2));
        let db = fixture(&path);
        let snapshot = db.admin().snapshot().unwrap();
        let target = damage.ends_with("target");
        let tid = snapshot
            .def(if target { "p.Person" } else { "p.Score" })
            .unwrap()
            .id;
        let key = row_key(tid, Id::from_u128(if target { 1 } else { 1000 }));
        drop(snapshot);
        drop(db);
        let raw = redb::Database::create(&path).unwrap();
        let tx = raw.begin_write().unwrap();
        {
            let mut table = tx.open_table(ROWS).unwrap();
            if damage == "missing_target" {
                table.remove(key.as_slice()).unwrap();
            } else {
                let mut bytes = table.get(key.as_slice()).unwrap().unwrap().value().to_vec();
                match damage {
                    "bad_target" => bytes = b"invalid".to_vec(),
                    "bad_bool" => bytes[20 + 16 + 8] = 2,
                    "zero_revision" => bytes[4..12].copy_from_slice(&0u64.to_be_bytes()),
                    "future_revision" => bytes[4..12].copy_from_slice(&u64::MAX.to_be_bytes()),
                    "bad_framing" => bytes.push(0),
                    _ => unreachable!(),
                }
                table.insert(key.as_slice(), bytes.as_slice()).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(raw);
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        for principal in ["reader", "root_only"] {
            let snapshot = db.session(principal).unwrap().snapshot().unwrap();
            assert!(
                matches!(collect_fixed(&snapshot, "p.Score"), Err(Error::Storage(_))),
                "{damage}/{principal}"
            );
            assert!(
                matches!(
                    collect_general(&snapshot, "p.Score"),
                    Err(Error::Storage(_))
                ),
                "{damage}/{principal}"
            );
        }
    }
}

#[test]
fn malformed_batch_shape_does_not_preempt_prior_callback_errors() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    // Deliberate PG01 row-table corruption uses the legacy physical layout.
    drop(super::tests::empty_format(&path, PagedFormat::V2));
    let db = fixture(&path);
    let snapshot = db.admin().snapshot().unwrap();
    let tid = snapshot.def("p.Score").unwrap().id;
    let key = row_key(tid, Id::from_u128(1001));
    drop(snapshot);
    drop(db);
    let raw = redb::Database::create(&path).unwrap();
    let tx = raw.begin_write().unwrap();
    {
        let mut table = tx.open_table(ROWS).unwrap();
        table.insert(key.as_slice(), &[0u8; 8192][..]).unwrap();
    }
    tx.commit().unwrap();
    drop(raw);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    let mut called = 0;
    let result = snapshot.visit_fixed_authorized_rows("p.Score", &mut || Ok(()), &mut |_, _| {
        called += 1;
        Err(Error::Query("prior callback".into()))
    });
    assert!(matches!(result, Err(Error::Query(message)) if message == "prior callback"));
    assert_eq!(called, 1);
    let result =
        snapshot.visit_fixed_authorized_rows("p.Score", &mut || Ok(()), &mut |_, _| Ok(()));
    assert!(matches!(result, Err(Error::Storage(_))));
    // The malformed second candidate still consumes the same source check as
    // the general visitor. An exhausted budget must win before its decoding.
    for limit in 0..=5 {
        let mut fixed_checks = 0;
        let fixed = snapshot
            .visit_fixed_authorized_rows(
                "p.Score",
                &mut || {
                    fixed_checks += 1;
                    if fixed_checks > limit {
                        Err(Error::Query("source budget".into()))
                    } else {
                        Ok(())
                    }
                },
                &mut |_, _| Ok(()),
            )
            .map(|_| ())
            .map_err(|error| error.to_string());
        let mut general_checks = 0;
        let general = snapshot
            .visit_authorized_rows(
                "p.Score",
                &mut || {
                    general_checks += 1;
                    if general_checks > limit {
                        Err(Error::Query("source budget".into()))
                    } else {
                        Ok(())
                    }
                },
                &mut |_| Ok(()),
            )
            .map_err(|error| error.to_string());
        assert_eq!(fixed, general, "source limit {limit}");
        assert_eq!(fixed_checks, general_checks, "source limit {limit}");
    }
}

/// Isolate the source visitor from different executables, query planning and
/// aggregate-representation dispatch. Both paths feed exactly the same fold.
#[test]
#[ignore = "opt-in release same-snapshot scan control; do not overlap timed work"]
fn same_snapshot_general_and_fixed_scan_control() {
    use std::{hint::black_box, time::Instant};
    let rows = std::env::var("SUBSTRATE_PACKED_CONTROL_ROWS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(100_000);
    assert!((100..=1_000_000).contains(&rows));
    let width = std::env::var("SUBSTRATE_PACKED_CONTROL_WIDTH")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(3);
    assert!((3..=64).contains(&width));
    let directory = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        directory.path().join("db"),
        PagedOptions {
            cache_bytes: 8 * 1024 * 1024,
            decoded_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let mut person = Relation::new(
        "core.Person",
        vec![
            Role::new("name", DataType::Text),
            Role::new("number", DataType::Int),
        ],
    )
    .unique(&["number"]);
    person.id = Id::from_u128(0x01000000000000000000000000000001);
    let mut score_roles = vec![
        Role::new("person", DataType::Ref("core.Person".into())),
        Role::new("score", DataType::Int),
        Role::new("bucket", DataType::Int),
    ];
    score_roles.extend((3..width).map(|slot| Role::new(&format!("extra{slot}"), DataType::Int)));
    let mut score = Relation::new("app.Score", score_roles).unique(&["person"]);
    score.id = Id::from_u128(0x02000000000000000000000000000002);
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new("core", 1, vec![], vec![person]),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    tx.install_owned(
        Module::new("app", 1, vec![("core".into(), 1)], vec![score]),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    for name in ["core.Person", "app.Score"] {
        tx.grant("reader", name, Grant::read_only()).unwrap();
    }
    tx.commit().unwrap();
    for start in (0..rows).step_by(128) {
        let mut tx = db.admin().transaction().unwrap();
        for n in start..(start + 128).min(rows) {
            let person = Id::from_u128(n as u128 + 1);
            tx.insert_with_id(
                "core.Person",
                person,
                vec![Value::Text(format!("Person{n:08}")), Value::Int(n as i64)],
            )
            .unwrap();
            let mut score_values = vec![
                Value::Ref(person),
                Value::Int(n as i64 * 10),
                Value::Int((n % 32) as i64),
            ];
            // The extra fields are real stored and indexed values, but are not
            // referenced by the fold. Width three is the original exact fixture.
            score_values.extend((3..width).map(|slot| Value::Int(n as i64 * 100 + slot as i64)));
            tx.insert_with_id(
                "app.Score",
                Id::from_u128((rows + n) as u128 + 1),
                score_values,
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Fold {
        broad: (u64, i128),
        groups: [(u64, i128); 32],
    }
    impl Fold {
        fn add(&mut self, number: i64, bucket: i64, lower: i64) {
            if number >= lower {
                self.broad.0 += 1;
                self.broad.1 = self.broad.1.checked_add(i128::from(number)).unwrap();
            }
            let slot = usize::try_from(bucket).unwrap();
            self.groups[slot].0 += 1;
            self.groups[slot].1 = self.groups[slot].1.checked_add(i128::from(number)).unwrap();
        }
    }
    let lower = (rows / 10) as i64 * 10;
    let mut expected = Fold::default();
    for n in 0..rows {
        expected.add(n as i64 * 10, (n % 32) as i64, lower);
    }
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    // Warm both paths once, then alternate order six times in the same process
    // and snapshot. Cache state is deliberately not claimed to be cold.
    for round in 0..7 {
        for fixed in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let mut fold = Fold::default();
            let mut checks = 0usize;
            let start = Instant::now();
            if fixed {
                assert!(
                    snapshot
                        .visit_fixed_authorized_rows(
                            "app.Score",
                            &mut || {
                                checks += 1;
                                Ok(())
                            },
                            &mut |_, row| {
                                let Some(PackedValue::Int(number)) = row.value(1) else {
                                    panic!("bad number")
                                };
                                let Some(PackedValue::Int(bucket)) = row.value(2) else {
                                    panic!("bad bucket")
                                };
                                fold.add(number, bucket, lower);
                                Ok(())
                            }
                        )
                        .unwrap()
                );
            } else {
                snapshot
                    .visit_authorized_rows(
                        "app.Score",
                        &mut || {
                            checks += 1;
                            Ok(())
                        },
                        &mut |row| {
                            let Value::Int(number) = row.values[1] else {
                                panic!("bad number")
                            };
                            let Value::Int(bucket) = row.values[2] else {
                                panic!("bad bucket")
                            };
                            fold.add(number, bucket, lower);
                            Ok(())
                        },
                    )
                    .unwrap();
            }
            let elapsed = start.elapsed();
            assert_eq!(black_box(&fold), &expected);
            assert_eq!(checks, 2 * rows + 2);
            if round > 0 {
                eprintln!(
                    "PACKED_SCAN_CONTROL {}",
                    serde_json::json!({"round":round,"mode":if fixed {"fixed"}else{"general"},"people":rows,"total_records":2*rows,"width":width,"query_us":elapsed.as_secs_f64()*1e6,"source_checks":checks,"verified":true})
                );
            }
        }
    }
}

struct TestRangeVisitor<F> {
    fields: usize,
    callback: F,
}
impl<F: FnMut(&Row) -> Result<()>> super::packed_range::PackedRangeVisitor for TestRangeVisitor<F> {
    fn owned(&mut self, row: &Row) -> Result<()> {
        (self.callback)(row)
    }
    fn packed(&mut self, id: Id, row: FixedPackedRow<'_, '_>) -> Result<()> {
        (self.callback)(&Row {
            id,
            values: (0..self.fields)
                .map(|slot| row.value(slot).unwrap().into_owned())
                .collect(),
        })
    }
}
type TestRangeBounds<'a> = (Option<(&'a Value, bool)>, Option<(&'a Value, bool)>);

fn packed_range(
    snapshot: &PagedSnapshot,
    name: &str,
    role: &str,
    bounds: TestRangeBounds<'_>,
    check: &mut impl FnMut() -> Result<()>,
    callback: impl FnMut(&Row) -> Result<()>,
) -> Result<bool> {
    snapshot.visit_fixed_authorized_range(
        name,
        role,
        bounds.0,
        bounds.1,
        check,
        &mut TestRangeVisitor {
            fields: snapshot.def(name)?.roles.len(),
            callback,
        },
    )
}

#[test]
fn packed_range_matches_general_bounds_checks_and_singletons() {
    for format in [PagedFormat::V2, PagedFormat::V3] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(super::tests::empty_format(&path, format));
        let db = fixture(&path);
        for principal in ["reader", "root_only"] {
            let snapshot = db.session(principal).unwrap().snapshot().unwrap();
            for (lower, upper) in [
                (None, None),
                (Some((Value::Int(0), true)), None),
                (None, Some((Value::Int(0), false))),
                (Some((Value::Int(-9), false)), Some((Value::Int(15), true))),
                (Some((Value::Int(3), true)), Some((Value::Int(3), true))),
                (Some((Value::Int(3), false)), Some((Value::Int(3), false))),
            ] {
                let bounds = (
                    lower.as_ref().map(|(v, b)| (v, *b)),
                    upper.as_ref().map(|(v, b)| (v, *b)),
                );
                let mut actual = Vec::new();
                let mut actual_checks = 0;
                assert!(
                    packed_range(
                        &snapshot,
                        "p.Score",
                        "n",
                        bounds,
                        &mut || {
                            actual_checks += 1;
                            Ok(())
                        },
                        |row| {
                            actual.push(row.clone());
                            Ok(())
                        }
                    )
                    .unwrap()
                );
                let mut expected = Vec::new();
                let mut expected_checks = 0;
                snapshot
                    .visit_authorized_range(
                        "p.Score",
                        "n",
                        bounds.0,
                        bounds.1,
                        &mut || {
                            expected_checks += 1;
                            Ok(())
                        },
                        &mut |row| {
                            expected.push(row.clone());
                            Ok(())
                        },
                    )
                    .unwrap();
                assert_eq!(actual, expected);
                assert_eq!(actual_checks, expected_checks);
            }
        }
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        for role in ["n", "flag", "decimal", "time"] {
            let mut actual = Vec::new();
            assert!(
                packed_range(
                    &snapshot,
                    "p.Scalar",
                    role,
                    (None, None),
                    &mut || Ok(()),
                    |row| {
                        actual.push(row.clone());
                        Ok(())
                    }
                )
                .unwrap()
            );
            let mut expected = Vec::new();
            snapshot
                .visit_authorized_range("p.Scalar", role, None, None, &mut || Ok(()), &mut |row| {
                    expected.push(row.clone());
                    Ok(())
                })
                .unwrap();
            assert_eq!(actual, expected);
        }
        // Multi-candidate packed reads do not populate the decoded cache.
        assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
    }
}

#[test]
fn packed_range_declines_unsupported_schemas_and_attenuated_policies() {
    use crate::capability::{CapabilityScope, RelationCapability};
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    for (name, role) in [
        ("p.Variable", "text"),
        ("p.Pair", "left"),
        ("p.Chain", "score"),
        ("p.Wide", "v0"),
    ] {
        assert!(
            !packed_range(
                &snapshot,
                name,
                role,
                (None, None),
                &mut || panic!("fallback charged source"),
                |_| panic!("fallback delivered row")
            )
            .unwrap()
        );
    }
    for filtered in ["p.Score", "p.Person"] {
        let mut tx = db.admin().transaction().unwrap();
        tx.grant("reader", "p.Score", Grant::read_only()).unwrap();
        tx.grant("reader", "p.Person", Grant::read_only()).unwrap();
        tx.grant(
            "reader",
            filtered,
            Grant {
                filter: RowFilter::Equals {
                    role: "n".into(),
                    value: Value::Int(3),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        assert!(
            !packed_range(
                &snapshot,
                "p.Score",
                "n",
                (None, None),
                &mut || panic!("fallback charged"),
                |_| panic!("fallback delivered")
            )
            .unwrap()
        );
    }
    let admin = db.admin().snapshot().unwrap();
    let capability = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![
                    RelationCapability {
                        relation: admin.def("p.Score").unwrap().id,
                        grant: Grant::read_only(),
                    },
                    RelationCapability {
                        relation: admin.def("p.Person").unwrap().id,
                        grant: Grant {
                            filter: RowFilter::Equals {
                                role: "n".into(),
                                value: Value::Int(3),
                            },
                            ..Grant::read_only()
                        },
                    },
                ],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let snapshot = db
        .session("owner")
        .unwrap()
        .with_capability(capability.id)
        .unwrap()
        .snapshot()
        .unwrap();
    assert!(
        !packed_range(
            &snapshot,
            "p.Score",
            "n",
            (None, None),
            &mut || panic!("scope fallback charged"),
            |_| panic!("scope bypassed")
        )
        .unwrap()
    );
    assert!(matches!(
        packed_range(
            &db.anonymous().snapshot().unwrap(),
            "p.Score",
            "n",
            (None, None),
            &mut || Ok(()),
            |_| Ok(())
        ),
        Err(Error::Denied)
    ));
}

#[test]
fn packed_range_preserves_budget_errors_and_live_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path);
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    for limit in 0..10 {
        let mut actual_checks = 0;
        let actual = packed_range(
            &snapshot,
            "p.Score",
            "n",
            (None, None),
            &mut || {
                actual_checks += 1;
                if actual_checks > limit {
                    Err(Error::Query("budget".into()))
                } else {
                    Ok(())
                }
            },
            |_| Ok(()),
        )
        .map(|_| ())
        .map_err(|e| e.to_string());
        let mut expected_checks = 0;
        let expected = snapshot
            .visit_authorized_range(
                "p.Score",
                "n",
                None,
                None,
                &mut || {
                    expected_checks += 1;
                    if expected_checks > limit {
                        Err(Error::Query("budget".into()))
                    } else {
                        Ok(())
                    }
                },
                &mut |_| Ok(()),
            )
            .map_err(|e| e.to_string());
        assert_eq!(actual, expected);
        assert_eq!(actual_checks, expected_checks);
    }
    let mut changed = false;
    let result = packed_range(
        &snapshot,
        "p.Score",
        "n",
        (None, None),
        &mut || Ok(()),
        |_| {
            if !changed {
                let mut tx = db.admin().transaction()?;
                tx.grant(
                    "reader",
                    "p.Person",
                    Grant {
                        filter: RowFilter::Equals {
                            role: "n".into(),
                            value: Value::Int(3),
                        },
                        ..Grant::read_only()
                    },
                )?;
                tx.commit()?;
                changed = true;
            }
            Ok(())
        },
    );
    assert!(matches!(result, Err(Error::Denied)));
    drop(snapshot);
    drop(db);
    for (options, principal) in [
        (
            PagedOptions {
                max_visibility_rows: 1,
                ..Default::default()
            },
            "owner",
        ),
        (
            PagedOptions {
                max_page_bytes: 40,
                ..Default::default()
            },
            "owner",
        ),
        (
            PagedOptions {
                max_row_bytes: 60,
                ..Default::default()
            },
            "owner",
        ),
        (
            PagedOptions {
                max_page_rows: 1,
                ..Default::default()
            },
            "root_only",
        ),
    ] {
        let db = PagedDatabase::open(&path, options).unwrap();
        let snapshot = db.session(principal).unwrap().snapshot().unwrap();
        let actual = packed_range(
            &snapshot,
            "p.Score",
            "n",
            (None, None),
            &mut || Ok(()),
            |_| Ok(()),
        )
        .unwrap_err();
        let expected = snapshot
            .visit_authorized_range("p.Score", "n", None, None, &mut || Ok(()), &mut |_| Ok(()))
            .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
    }
}

#[test]
fn packed_range_checks_index_and_unprojected_target_bytes_before_delivery() {
    for format in [PagedFormat::V2, PagedFormat::V3] {
        for ordinal in [1usize, 2, 80] {
            for damage in [
                "index",
                "root_bool",
                "target_utf8",
                "target_truncated",
                "missing_target",
            ] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("db");
                drop(super::tests::empty_format(&path, format));
                let db = fixture(&path);
                let snapshot = db.admin().snapshot().unwrap();
                // Exercise owned lookahead, the first borrowed entry, and an
                // entry beyond the first index leaf despite permuted root IDs.
                let target = damage.contains("target");
                let def = snapshot
                    .def(if target { "p.Person" } else { "p.Score" })
                    .unwrap();
                let id = Id::from_u128(if target {
                    (ordinal as u128 * 53) % 130 + 1
                } else {
                    1000 + (ordinal as u128 * 37) % 130
                });
                let row = snapshot.raw(def.id, id).unwrap().unwrap();
                let mut bytes = encode_row(&def, &row, snapshot.revision()).unwrap();
                match damage {
                    "index" => bytes[36..44].copy_from_slice(&999i64.to_be_bytes()),
                    "root_bool" => bytes[44] = 2,
                    "target_utf8" => bytes[32] = 0xff,
                    "target_truncated" => {
                        bytes.pop();
                    }
                    "missing_target" => {}
                    _ => unreachable!(),
                }
                let write = db.inner.db.begin_write().unwrap();
                {
                    let mut writer = RowWriter::new(
                        write.open_table(ROWS).unwrap(),
                        format,
                        db.inner.options.max_row_bytes,
                    );
                    if damage == "missing_target" {
                        writer.remove(&row_key(def.id, id)).unwrap();
                    } else {
                        writer.insert(&row_key(def.id, id), bytes).unwrap();
                    }
                    writer.finish().unwrap();
                }
                write.commit().unwrap();
                drop(snapshot);
                drop(db);
                let db = PagedDatabase::open(&path, Default::default()).unwrap();
                let snapshot = db.session("reader").unwrap().snapshot().unwrap();
                let mut delivered = 0;
                let fail_at = if ordinal > 2 { 3 } else { 1 };
                let error = packed_range(
                    &snapshot,
                    "p.Score",
                    "n",
                    (None, None),
                    &mut || Ok(()),
                    |_| {
                        delivered += 1;
                        if delivered == fail_at {
                            Err(Error::Query("earlier callback".into()))
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap_err();
                assert!(matches!(error,Error::Query(message) if message == "earlier callback"));
                assert_eq!(delivered, fail_at);
                for principal in ["reader", "root_only"] {
                    let snapshot = db.session(principal).unwrap().snapshot().unwrap();
                    let error = packed_range(
                        &snapshot,
                        "p.Score",
                        "n",
                        (None, None),
                        &mut || Ok(()),
                        |_| Ok(()),
                    )
                    .unwrap_err();
                    assert!(
                        matches!(error, Error::Storage(_)),
                        "{format:?}/{damage}/{principal}"
                    );
                }
                for limit in (0..8).chain([
                    3 * ordinal + 1,
                    3 * ordinal + 2,
                    3 * ordinal + 3,
                    usize::MAX,
                ]) {
                    let mut actual_checks = 0;
                    let actual = packed_range(
                        &snapshot,
                        "p.Score",
                        "n",
                        (None, None),
                        &mut || {
                            actual_checks += 1;
                            if actual_checks > limit {
                                Err(Error::Query("budget".into()))
                            } else {
                                Ok(())
                            }
                        },
                        |_| Ok(()),
                    )
                    .map(|_| ())
                    .map_err(|e| e.to_string());
                    let mut expected_checks = 0;
                    let expected = snapshot
                        .visit_authorized_range(
                            "p.Score",
                            "n",
                            None,
                            None,
                            &mut || {
                                expected_checks += 1;
                                if expected_checks > limit {
                                    Err(Error::Query("budget".into()))
                                } else {
                                    Ok(())
                                }
                            },
                            &mut |_| Ok(()),
                        )
                        .map_err(|e| e.to_string());
                    assert_eq!(actual, expected, "{damage} limit{limit}");
                    assert_eq!(actual_checks, expected_checks);
                }
            }
        }
    }
}

#[test]
fn packed_range_unique_index_reversals_and_extended_type_fallbacks() {
    for format in [PagedFormat::V2, PagedFormat::V3] {
        let dir = tempfile::tempdir().unwrap();
        let db = super::tests::empty_format(&dir.path().join("db"), format);
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "r",
            1,
            vec![],
            vec![
                Relation::new("r.Fixed", vec![Role::new("n", DataType::Int)]).unique(&["n"]),
                Relation::new("r.Nullable", vec![Role::new("n", DataType::Int.nullable())]),
                Relation::new("r.Float", vec![Role::new("n", DataType::Float)]),
            ],
        ))
        .unwrap();
        for n in 0..128u128 {
            tx.insert_with_id(
                "r.Fixed",
                Id::from_u128(if n < 32 { n + 1 } else { 300 - n }),
                vec![Value::Int(n as i64)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot().unwrap();
        for name in ["r.Nullable", "r.Float"] {
            assert!(
                !packed_range(
                    &snapshot,
                    name,
                    "n",
                    (None, None),
                    &mut || panic!("fallback charged"),
                    |_| panic!("fallback row")
                )
                .unwrap()
            );
        }
        let mut actual = Vec::new();
        assert!(
            packed_range(
                &snapshot,
                "r.Fixed",
                "n",
                (None, None),
                &mut || Ok(()),
                |row| {
                    actual.push(row.clone());
                    Ok(())
                }
            )
            .unwrap()
        );
        let mut expected = Vec::new();
        snapshot
            .visit_authorized_range("r.Fixed", "n", None, None, &mut || Ok(()), &mut |row| {
                expected.push(row.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 128);
        for bound in [Value::Int(i64::MIN), Value::Int(3), Value::Int(i64::MAX)] {
            for inclusive in [false, true] {
                let mut actual = Vec::new();
                packed_range(
                    &snapshot,
                    "r.Fixed",
                    "n",
                    (Some((&bound, inclusive)), Some((&bound, true))),
                    &mut || Ok(()),
                    |row| {
                        actual.push(row.clone());
                        Ok(())
                    },
                )
                .unwrap();
                let mut expected = Vec::new();
                snapshot
                    .visit_authorized_range(
                        "r.Fixed",
                        "n",
                        Some((&bound, inclusive)),
                        Some((&bound, true)),
                        &mut || Ok(()),
                        &mut |row| {
                            expected.push(row.clone());
                            Ok(())
                        },
                    )
                    .unwrap();
                assert_eq!(actual, expected);
            }
        }
    }
}

#[test]
fn packed_range_borrowed_unique_values_preserve_error_and_callback_order() {
    for format in [PagedFormat::V2, PagedFormat::V3] {
        // The first two entries retain owned lookahead. Corrupt a borrowed
        // entry both immediately after lookahead and beyond the first leaf.
        for damaged in [2usize, 100] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let db = super::tests::empty_format(&path, format);
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "u",
                1,
                vec![],
                vec![Relation::new("u.Fixed", vec![Role::new("n", DataType::Int)]).unique(&["n"])],
            ))
            .unwrap();
            for n in 0..130u128 {
                tx.insert_with_id("u.Fixed", Id::from_u128(n + 1), vec![Value::Int(n as i64)])
                    .unwrap();
            }
            tx.commit().unwrap();
            let tid = db.admin().snapshot().unwrap().def("u.Fixed").unwrap().id;
            let write = db.inner.db.begin_write().unwrap();
            write
                .open_table(UNIQUE)
                .unwrap()
                .insert(
                    unique_key(tid, 0, &[Value::Int(damaged as i64)]).as_slice(),
                    b"invalid occurrence id".as_slice(),
                )
                .unwrap();
            write.commit().unwrap();
            drop(db);
            let db = PagedDatabase::open(&path, Default::default()).unwrap();
            let snapshot = db.admin().snapshot().unwrap();
            for (limit, fail_at) in [
                (usize::MAX, usize::MAX),
                (damaged + 1, usize::MAX),
                (usize::MAX, damaged),
            ] {
                let run = |packed: bool| {
                    let mut checks = 0;
                    let mut delivered = Vec::new();
                    let mut check = || {
                        checks += 1;
                        if checks > limit {
                            Err(Error::Query("budget before invalid identity".into()))
                        } else {
                            Ok(())
                        }
                    };
                    let mut visit = |row: &Row| {
                        delivered.push(row.clone());
                        if delivered.len() == fail_at {
                            Err(Error::Query("earlier callback".into()))
                        } else {
                            Ok(())
                        }
                    };
                    let result = if packed {
                        packed_range(
                            &snapshot,
                            "u.Fixed",
                            "n",
                            (None, None),
                            &mut check,
                            &mut visit,
                        )
                        .map(|_| ())
                    } else {
                        snapshot.visit_authorized_range(
                            "u.Fixed", "n", None, None, &mut check, &mut visit,
                        )
                    };
                    (result.unwrap_err().to_string(), checks, delivered)
                };
                assert_eq!(
                    run(true),
                    run(false),
                    "{format:?}/{damaged}/{limit}/{fail_at}"
                );
            }
        }
    }
}

#[test]
fn aggregate_routes_half_selectivity_to_packed_index_and_broad_to_scan() {
    for format in [PagedFormat::V2, PagedFormat::V3] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(super::tests::empty_format(&path, format));
        let db = fixture(&path);
        let (snapshot, metrics) = db
            .with_read_metrics(db.session("reader").unwrap().snapshot().unwrap())
            .unwrap();
        let sql = snapshot.sql().unwrap();
        for (bound, candidates, root_rows) in [(0, 65, 65), (-52, 0, 130)] {
            let before = metrics.snapshot();
            let result = sql
                .query(
                    &format!(
                        "SELECT flag,COUNT(*),SUM(n) FROM p.Score WHERE n>={bound} GROUP BY flag"
                    ),
                    &[],
                )
                .unwrap();
            assert_eq!(result.rows.len(), 2);
            let counts = metrics.snapshot().delta_since(before);
            assert_eq!(counts.index_candidates, candidates);
            assert_eq!(counts.root_scan_rows, root_rows);
            assert_eq!(counts.decoded_rows, 0);
        }
    }
}
