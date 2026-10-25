use rusqlite::types::Value as S;
use substrate_liquid::*;

fn fixture(rows: usize) -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "indexed",
            1,
            vec![],
            vec![Relation::new(
                "indexed.Item",
                vec![
                    Role::new("n", DataType::Int),
                    Role::new("tag", DataType::Text),
                    Role::new("enabled", DataType::Bool),
                    Role::new("payload", DataType::Bytes),
                ],
            )],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    for n in 0..rows {
        tx.insert_with_id(
            "indexed.Item",
            Id::from_u128(n as u128 + 1),
            vec![
                Value::Int(n as i64),
                Value::Text(format!("tag{}", n % 7)),
                Value::Bool(n % 2 == 0),
                Value::Bytes(vec![n as u8; 8]),
            ],
        )
        .unwrap();
    }
    tx.grant(
        "even",
        "indexed.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "enabled".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db
}
fn explain(sql: &SqlSnapshot, query: &str, params: &[S], analyze: bool) -> String {
    sql.query(
        &format!("EXPLAIN {}{query}", if analyze { "ANALYZE " } else { "" }),
        params,
    )
    .unwrap()
    .rows
    .into_iter()
    .flatten()
    .filter_map(|value| match value {
        S::Text(value) => Some(value),
        _ => None,
    })
    .collect::<Vec<_>>()
    .join("\n")
}

#[test]
fn identity_and_role_indexes_are_used_and_only_candidates_are_read() {
    let db = fixture(2500);
    let sql = db.admin().snapshot().sql().unwrap();
    let id = Id::from_u128(2001).to_string();
    let point = explain(
        &sql,
        "SELECT n FROM indexed.Item WHERE id=?1",
        &[S::Text(id.clone())],
        true,
    );
    assert!(point.contains("access=identity"), "{point}");
    assert!(point.contains("source_rows=1"), "{point}");
    assert_eq!(
        sql.query("SELECT n FROM indexed.Item WHERE id=?1", &[S::Text(id)])
            .unwrap()
            .rows,
        vec![vec![S::Integer(2000)]]
    );
    let equality = explain(&sql, "SELECT tag FROM indexed.Item WHERE n=2000", &[], true);
    assert!(equality.contains("access=role_equality"), "{equality}");
    assert!(equality.contains("source_rows=1"), "{equality}");
    for query in [
        "SELECT n FROM indexed.Item WHERE n>=1000 AND n<1005",
        "SELECT n FROM indexed.Item WHERE n<1005 AND n>=1000",
        "SELECT n FROM indexed.Item WHERE n>=999 AND n>999 AND n<=1005 AND n<1005",
    ] {
        let range = explain(&sql, query, &[], true);
        assert!(range.contains("access=numeric_range"), "{range}");
        assert!(range.contains("source_rows=5"), "{range}");
    }
}

#[test]
fn ranges_residual_predicates_limits_and_paging_match_sqlite() {
    let db = fixture(2500);
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let old = snapshot.sqlite_compat().unwrap();
    for query in [
        "SELECT n FROM indexed.Item WHERE n>=990 AND n<2010 ORDER BY n",
        "SELECT n FROM indexed.Item WHERE 2010>n AND 990<=n ORDER BY n DESC LIMIT 17",
        "SELECT n FROM indexed.Item WHERE tag='tag3' AND n>1000 ORDER BY n LIMIT 9",
        "SELECT n FROM indexed.Item WHERE n<0 AND n>10",
        "SELECT n FROM indexed.Item WHERE n>2498 OR n<3 ORDER BY n",
        "SELECT n FROM indexed.Item WHERE n>=3 AND n % 7=2 ORDER BY n LIMIT 5",
        "SELECT n FROM indexed.Item WHERE n>2.5 ORDER BY n LIMIT 3",
        "SELECT n FROM indexed.Item WHERE enabled=2",
        "SELECT n FROM indexed.Item WHERE n=NULL",
        "SELECT n FROM indexed.Item WHERE tag='missing' LIMIT 1",
        "SELECT count(*) FROM indexed.Item",
        "SELECT n FROM indexed.Item LIMIT 0",
    ] {
        assert_eq!(
            sql.query(query, &[]).unwrap().rows,
            old.query(query, &[]).unwrap().rows,
            "{query}"
        );
    }
}

#[test]
fn pushdown_keeps_row_filters_and_live_revocation() {
    let db = fixture(100);
    let sql = db.session("even").snapshot().sql().unwrap();
    assert!(
        sql.query(
            "SELECT n FROM indexed.Item WHERE id=?1",
            &[S::Text(Id::from_u128(2).to_string())]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert!(
        sql.query("SELECT n FROM indexed.Item WHERE n=3", &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        sql.query(
            "SELECT n FROM indexed.Item WHERE n>=95 ORDER BY n LIMIT 2",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(96)], vec![S::Integer(98)]]
    );
    assert_eq!(
        sql.query("SELECT n FROM indexed.Item LIMIT 1", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
    let mut prepared = sql
        .prepare("SELECT n FROM indexed.Item WHERE n=?1")
        .unwrap();
    assert_eq!(
        prepared.query(&[S::Integer(10)]).unwrap().rows,
        vec![vec![S::Integer(10)]]
    );
    let mut revoke = db.admin().transaction();
    revoke.revoke("even", "indexed.Item").unwrap();
    revoke.commit().unwrap();
    assert!(matches!(
        prepared.query(&[S::Integer(10)]),
        Err(Error::Denied)
    ));
}

#[test]
fn projection_does_not_convert_unused_large_values_and_execution_errors_are_recoverable() {
    let db = fixture(2);
    let mut tx = db.admin().transaction();
    tx.insert_with_id(
        "indexed.Item",
        Id::from_u128(10000),
        vec![
            Value::Int(999),
            Value::Text("large".into()),
            Value::Bool(true),
            Value::Bytes(vec![7; 2 * 1024 * 1024]),
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    let sql = db.admin().snapshot().sql().unwrap();
    assert_eq!(
        sql.query("SELECT count(*) FROM indexed.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(3)]]
    );
    assert_eq!(
        sql.query("SELECT n FROM indexed.Item WHERE tag='large'", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(999)]]
    );
    assert!(
        sql.query("SELECT payload FROM indexed.Item WHERE tag='large'", &[])
            .is_err()
    );
    // Invalid full-scan cells must never become successful cached batches.
    for _ in 0..2 {
        assert!(sql.query("SELECT payload FROM indexed.Item", &[]).is_err());
    }

    assert_eq!(
        sql.query("SELECT n FROM indexed.Item WHERE n=1", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(1)]]
    );
}

#[test]
fn noncanonical_uuid_text_cannot_be_treated_as_native_identity_equality() {
    let db = fixture(12);
    let sql = db.admin().snapshot().sql().unwrap();
    let id = Id::from_u128(11).to_string();
    assert_ne!(id, id.to_uppercase());
    for parameter in [id.to_uppercase(), id.replace('-', ""), "not-a-uuid".into()] {
        assert!(
            sql.query(
                "SELECT n FROM indexed.Item WHERE id=?1",
                &[S::Text(parameter)]
            )
            .unwrap()
            .rows
            .is_empty()
        );
    }
}

#[test]
fn repeated_parameter_binding_and_missing_identity_do_not_reuse_index_candidates() {
    let db = fixture(2500);
    let sql = db.admin().snapshot().sql().unwrap();
    let mut query = sql
        .prepare("SELECT n FROM indexed.Item WHERE n=?1")
        .unwrap();
    for value in [1, 2499, -1, 20, 1] {
        let expected = if value >= 0 {
            vec![vec![S::Integer(value)]]
        } else {
            vec![]
        };
        assert_eq!(query.query(&[S::Integer(value)]).unwrap().rows, expected);
    }
    assert!(
        sql.query(
            "SELECT n FROM indexed.Item WHERE id=?1",
            &[S::Text(Id::from_u128(99999).to_string())]
        )
        .unwrap()
        .rows
        .is_empty()
    );
}

#[test]
fn reference_equality_indexes_preserve_reference_visibility_and_text_semantics() {
    let db = fixture(20);
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "links",
            1,
            vec![("indexed".into(), 1)],
            vec![Relation::new(
                "links.Link",
                vec![Role::new("target", DataType::Ref("indexed.Item".into()))],
            )],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.insert("links.Link", vec![Value::Ref(Id::from_u128(11))])
        .unwrap();
    tx.insert("links.Link", vec![Value::Ref(Id::from_u128(12))])
        .unwrap();
    tx.grant("even", "links.Link", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    let sql = db.session("even").snapshot().sql().unwrap();
    let visible = Id::from_u128(11).to_string();
    let mut direct = sql
        .prepare("SELECT target FROM links.Link WHERE target=?1")
        .unwrap();
    assert_eq!(
        direct.query(&[S::Text(visible.clone())]).unwrap().rows,
        vec![vec![S::Text(visible.clone())]]
    );
    assert!(
        direct
            .query(&[S::Text(Id::from_u128(12).to_string())])
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        sql.query(
            "SELECT count(*) FROM links.Link WHERE target=?1",
            &[S::Text(visible.clone())]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(1)]]
    );
    assert_eq!(
        sql.query(
            "SELECT count(*) FROM links.Link WHERE target=?1",
            &[S::Text(Id::from_u128(12).to_string())]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(0)]]
    );
    assert_eq!(
        sql.query(
            "SELECT count(*) FROM links.Link WHERE target=?1",
            &[S::Text(visible.to_uppercase())]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(0)]]
    );
    let plan = explain(
        &sql,
        "SELECT target FROM links.Link WHERE target=?1",
        &[S::Text(visible)],
        true,
    );
    assert!(plan.contains("access=role_equality"), "{plan}");
    assert!(plan.contains("source_rows=1"), "{plan}");
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn indexed_range_conjunctions_match_reference(lower in -10i64..75, upper in -10i64..75, limit in 0usize..20, reverse in proptest::bool::ANY) {
        let db = fixture(64);
        let sql = db.session("even").snapshot().sql().unwrap();
        let order = if reverse { "DESC" } else { "ASC" };
        let rows = sql.query(&format!("SELECT n FROM indexed.Item WHERE n>=?1 AND n<?2 ORDER BY n {order} LIMIT {limit}"), &[S::Integer(lower), S::Integer(upper)]).unwrap().rows;
        let mut expected = (0..64).filter(|n| n % 2 == 0 && *n >= lower && *n < upper).collect::<Vec<_>>();
        if reverse { expected.reverse(); }
        expected.truncate(limit);
        proptest::prop_assert_eq!(rows, expected.into_iter().map(|n| vec![S::Integer(n)]).collect::<Vec<_>>());
    }
}

#[test]
fn full_scan_cache_reuses_authorized_pages_and_isolates_projections() {
    let db = fixture(2500);
    let sql = db.admin().snapshot().sql().unwrap();
    let first = explain(&sql, "SELECT sum(n) FROM indexed.Item", &[], true);
    assert!(first.contains("cache_hits=0"), "{first}");
    let second = explain(&sql, "SELECT sum(n) FROM indexed.Item", &[], true);
    assert!(second.contains("cache_hits=3"), "{second}");
    assert_eq!(
        sql.query("SELECT sum(n) FROM indexed.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(3_123_750)]]
    );
    let other = explain(&sql, "SELECT count(tag) FROM indexed.Item", &[], true);
    assert!(other.contains("cache_hits=0"), "{other}");
    let other_again = explain(&sql, "SELECT count(tag) FROM indexed.Item", &[], true);
    assert!(other_again.contains("cache_hits=3"), "{other_again}");
    let selected = explain(&sql, "SELECT n FROM indexed.Item WHERE n=123", &[], true);
    assert!(selected.contains("access=role_equality"), "{selected}");
    assert!(selected.contains("cache_hits=0"), "{selected}");
    assert!(selected.contains("source_rows=1"), "{selected}");
}

#[test]
fn cached_pages_remain_pinned_and_revocation_denies_warm_scans() {
    let db = fixture(10);
    let admin = db.admin().snapshot().sql().unwrap();
    let restricted = db.session("even").snapshot().sql().unwrap();
    assert_eq!(
        admin
            .query("SELECT sum(n) FROM indexed.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(45)]]
    );
    assert_eq!(
        restricted
            .query("SELECT sum(n) FROM indexed.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(20)]]
    );
    let mut tx = db.admin().transaction();
    tx.update(
        "indexed.Item",
        Id::from_u128(1),
        vec![
            Value::Int(100),
            Value::Text("changed".into()),
            Value::Bool(true),
            Value::Bytes(vec![]),
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        admin
            .query("SELECT sum(n) FROM indexed.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(45)]]
    );
    assert_eq!(
        db.admin()
            .snapshot()
            .sql()
            .unwrap()
            .query("SELECT sum(n) FROM indexed.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(145)]]
    );
    let mut revoke = db.admin().transaction();
    revoke.revoke("even", "indexed.Item").unwrap();
    revoke.commit().unwrap();
    assert!(matches!(
        restricted.query("SELECT sum(n) FROM indexed.Item", &[]),
        Err(Error::Denied)
    ));
}

#[test]
fn concurrent_full_scans_share_pages_without_sharing_execution_state() {
    let db = fixture(2000);
    let sql = db.admin().snapshot().sql().unwrap();
    std::thread::scope(|scope| {
        let tasks = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    sql.query("SELECT sum(n) FROM indexed.Item", &[])
                        .unwrap()
                        .rows
                })
            })
            .collect::<Vec<_>>();
        for task in tasks {
            assert_eq!(task.join().unwrap(), vec![vec![S::Integer(1_999_000)]]);
        }
    });
    let cached = explain(&sql, "SELECT sum(n) FROM indexed.Item", &[], true);
    assert!(cached.contains("cache_hits=2"), "{cached}");
}

#[test]
fn typed_predicates_and_coercion_fallback_match_sqlite() {
    let db = fixture(20);
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let old = snapshot.sqlite_compat().unwrap();
    for (predicate, params) in [
        ("enabled = ?1", vec![S::Integer(1)]),
        ("enabled > ?1", vec![S::Integer(0)]),
        ("enabled = ?1", vec![S::Integer(2)]),
        ("n > ?1", vec![S::Real(5.5)]),
        ("CAST(n AS DOUBLE) < ?1", vec![S::Real(8.5)]),
        ("payload = ?1", vec![S::Blob(vec![7; 8])]),
        ("tag = ?1", vec![S::Text("tag1".into())]),
        ("n = ?1", vec![S::Null]),
    ] {
        let query = format!("SELECT id,n FROM indexed.Item WHERE {predicate} ORDER BY id");
        assert_eq!(
            sql.query(&query, &params).unwrap(),
            old.query(&query, &params).unwrap(),
            "{predicate}"
        );
    }
}
