use rusqlite::types::Value as S;
use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};
fn fixture() -> (tempfile::TempDir, PagedDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        dir.path().join("db"),
        PagedOptions {
            max_page_rows: 3,
            decoded_cache_bytes: 512,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "core",
            1,
            vec![],
            vec![Relation::new(
                "core.Person",
                vec![
                    Role::new("name", DataType::Text),
                    Role::new("age", DataType::Int),
                ],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    tx.install_owned(
        Module::new(
            "app",
            1,
            vec![("core".into(), 1)],
            vec![Relation::new(
                "app.Link",
                vec![
                    Role::new("person", DataType::Ref("core.Person".into())),
                    Role::new("score", DataType::Int),
                ],
            )],
        ),
        "owner",
        Visibility::Public,
    )
    .unwrap();
    for n in 1..=20 {
        tx.insert_with_id(
            "core.Person",
            Id::from_u128(n),
            vec![Value::Text(format!("P{n:02}")), Value::Int(n as i64)],
        )
        .unwrap();
        tx.insert_with_id(
            "app.Link",
            Id::from_u128(100 + n),
            vec![Value::Ref(Id::from_u128(n)), Value::Int(n as i64 * 10)],
        )
        .unwrap();
    }
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    (dir, db)
}
#[test]
fn paged_sql_streaming_join_aggregate_and_index_probes() {
    let (_dir, db) = fixture();
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(
        sql.query("SELECT COUNT(*), SUM(age) FROM core.Person", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(20), S::Integer(210)]]
    );
    assert_eq!(sql.query("SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON l.person=p.id WHERE p.age=7", &[]).unwrap().rows, vec![vec![S::Text("P07".into()), S::Integer(70)]]);
    let mut query = sql
        .prepare("SELECT name FROM core.Person WHERE age=?1")
        .unwrap();
    for n in [7, 3, 20, 1] {
        assert_eq!(
            query.query(&[S::Integer(n)]).unwrap().rows,
            vec![vec![S::Text(format!("P{n:02}"))]]
        );
    }
    assert!(query.query(&[]).is_err());
    let plan = sql
        .query("EXPLAIN SELECT name FROM core.Person WHERE age=7", &[])
        .unwrap();
    assert!(format!("{plan:?}").contains("role_equality"));
    let plan = sql
        .query(
            &format!(
                "EXPLAIN SELECT name FROM core.Person WHERE id='{}'",
                Id::from_u128(7)
            ),
            &[],
        )
        .unwrap();
    assert!(format!("{plan:?}").contains("access=identity"));
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}
#[test]
fn paged_sql_hidden_references_and_live_revocation() {
    let (_dir, db) = fixture();
    let anonymous = db.anonymous().snapshot().unwrap().sql().unwrap();
    assert!(anonymous.query("SELECT * FROM core.Person", &[]).is_err());
    // Even the public reference-bearing family must not reveal private metadata/data.
    assert!(anonymous.query("SELECT * FROM app.Link", &[]).is_err());
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut prepared = sql.prepare("SELECT COUNT(*) FROM app.Link").unwrap();
    assert_eq!(
        prepared.query(&[]).unwrap().rows,
        vec![vec![S::Integer(20)]]
    );
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(prepared.query(&[]), Err(Error::Denied)));
    assert!(matches!(sql.query("SELECT 1", &[]), Err(Error::Denied)));
}
#[test]
fn paged_sql_snapshot_isolation_limits_and_readonly() {
    let (_dir, db) = fixture();
    let mut sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    sql.view("old_people", "SELECT name,age FROM core.Person")
        .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.update(
        "core.Person",
        Id::from_u128(1),
        vec!["Changed".into(), Value::Int(99)],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        sql.query("SELECT name FROM old_people WHERE age=1", &[])
            .unwrap()
            .rows,
        vec![vec![S::Text("P01".into())]]
    );
    for statement in [
        "DELETE FROM core.Person",
        "CREATE TABLE x(a INT)",
        "SELECT 1; SELECT 2",
        "COPY core.Person TO '/tmp/leak'",
        "SELECT * FROM read_csv('/etc/passwd')",
    ] {
        assert!(sql.query(statement, &[]).is_err(), "{statement}");
    }
    assert!(
        sql.query_with_limits(
            "SELECT * FROM core.Person",
            &[],
            QueryLimits {
                max_rows: 2,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        sql.query_with_limits(
            "SELECT name FROM core.Person LIMIT 1",
            &[],
            QueryLimits {
                max_bytes: 1,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(sql.query("SELECT 9223372036854775807 + 1", &[]).is_err());
    let selected = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql_selected(&["core.Person"])
        .unwrap();
    assert!(selected.query("SELECT * FROM app.Link", &[]).is_err());
}
#[test]
fn paged_sql_equality_semantics_empty_projection_and_windows() {
    let (_dir, db) = fixture();
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let id = Id::from_u128(7).to_string();
    assert_eq!(
        sql.query(
            "SELECT COUNT(*) FROM core.Person WHERE id=?1",
            &[S::Text(id.clone())]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(1)]]
    );
    assert_eq!(
        sql.query(
            "SELECT COUNT(*) FROM core.Person WHERE id=?1",
            &[S::Text(id.replace('-', ""))]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(0)]]
    );
    assert_eq!(sql.query("SELECT age,ROW_NUMBER() OVER(ORDER BY age DESC) AS n FROM core.Person ORDER BY age DESC LIMIT 2", &[]).unwrap().rows, vec![vec![S::Integer(20), S::Integer(1)],vec![S::Integer(19), S::Integer(2)]]);
    assert_eq!(
        sql.query(
            "SELECT name FROM core.Person WHERE age=7 AND name='missing' LIMIT 1",
            &[]
        )
        .unwrap()
        .rows
        .len(),
        0
    );
}
#[test]
fn paged_sql_row_filters_apply_to_join_and_aggregate() {
    let (_dir, db) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    tx.grant(
        "limited",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "age".into(),
                value: Value::Int(7),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let sql = db
        .session("limited")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(
        sql.query("SELECT COUNT(*) FROM app.Link", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(1)]]
    );
    assert_eq!(
        sql.query(
            "SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON p.id=l.person",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Text("P07".into()), S::Integer(70)]]
    );
    assert!(
        sql.query(
            "SELECT name FROM core.Person WHERE age=?1",
            &[S::Integer(8)]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert_eq!(
        sql.query(
            "SELECT name FROM core.Person WHERE age=?1",
            &[S::Integer(7)]
        )
        .unwrap()
        .rows,
        vec![vec![S::Text("P07".into())]]
    );
}
#[test]
fn reviewed_pushdown_preserves_boolean_logic_limits_and_alias_projection() {
    let (_dir, db) = fixture();
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let cases = [
        (
            "SELECT age FROM core.Person WHERE age=3 OR age=7 ORDER BY age",
            vec![3, 7],
        ),
        (
            "SELECT age FROM core.Person WHERE (age=3 OR age=7) AND name='P07' LIMIT 1",
            vec![7],
        ),
        ("SELECT age FROM core.Person WHERE age=3 AND age=7", vec![]),
        (
            "SELECT age FROM core.Person WHERE age=3 AND name='missing' LIMIT 1",
            vec![],
        ),
        (
            "SELECT age FROM core.Person WHERE age=3 OR name='P07' ORDER BY age LIMIT 1",
            vec![3],
        ),
        (
            "SELECT age FROM core.Person WHERE NOT(age=3) ORDER BY age LIMIT 2",
            vec![1, 2],
        ),
    ];
    for (query, expected) in cases {
        assert_eq!(
            sql.query(query, &[]).unwrap().rows,
            expected
                .into_iter()
                .map(|n| vec![S::Integer(n)])
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
    let mut prepared=sql.prepare("SELECT y, x AS renamed FROM (SELECT name AS x, age AS y FROM core.Person) q WHERE y=?1").unwrap();
    for value in [S::Integer(7), S::Integer(2)] {
        let n = match value {
            S::Integer(n) => n,
            _ => unreachable!(),
        };
        assert_eq!(
            prepared.query(&[value]).unwrap().rows,
            vec![vec![S::Integer(n), S::Text(format!("P{n:02}"))]]
        );
    }
    assert!(prepared.query(&[S::Null]).unwrap().rows.is_empty());
    assert!(prepared.query(&[S::Integer(7), S::Integer(2)]).is_err());
}
#[test]
fn reviewed_direct_and_fallback_bounds_and_selected_schema_fences() {
    let (_dir, db) = fixture();
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    let sql = snapshot.sql_selected(&["core.Person"]).unwrap();
    assert!(snapshot.sql_selected(&["missing.Hidden"]).is_err());
    for query in [
        "SELECT * FROM app.Link",
        "SELECT p.name FROM core.Person p JOIN app.Link l ON p.id=l.person",
    ] {
        assert!(sql.query(query, &[]).is_err());
    }
    let mut prepared = sql
        .prepare("SELECT name,age FROM core.Person WHERE age=?1")
        .unwrap();
    assert!(
        prepared
            .query_with_limits(
                &[S::Integer(7)],
                QueryLimits {
                    vm_steps: 1,
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert!(
        prepared
            .query_with_limits(
                &[S::Integer(7)],
                QueryLimits {
                    max_bytes: 10,
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert!(
        prepared
            .query_with_limits(
                &[S::Integer(7)],
                QueryLimits {
                    timeout_ms: 0,
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert!(
        sql.query(
            "SELECT name FROM core.Person WHERE name=?1",
            &[S::Text("x".repeat(1024 * 1024 + 1))]
        )
        .is_err()
    );
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "core.Person").unwrap();
    tx.commit().unwrap();
    // Fast empty-result bindings must not bypass live revocation either.
    assert!(matches!(prepared.query(&[S::Null]), Err(Error::Denied)));
}

#[test]
fn indexed_joins_rebind_preserve_duplicates_filters_and_budgets() {
    let (_dir, db) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    for n in 0..7 {
        tx.insert_with_id(
            "app.Link",
            Id::from_u128(1000 + n),
            vec![Value::Ref(Id::from_u128(7)), Value::Int(70)],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut prepared = sql.prepare("SELECT p.name AS person,l.score FROM app.Link l JOIN core.Person p ON l.person=p.id WHERE p.age=?1 AND l.score=?2").unwrap();
    for n in [7, 3, 8, 7] {
        let result = prepared
            .query(&[S::Integer(n), S::Integer(n * 10)])
            .unwrap();
        assert_eq!(result.columns, vec!["person", "score"]);
        assert_eq!(
            result.rows,
            vec![vec![S::Text(format!("P{n:02}")), S::Integer(n * 10)]; if n == 7 { 8 } else { 1 }]
        );
    }
    assert!(
        prepared
            .query(&[S::Integer(7), S::Integer(99)])
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        prepared
            .query(&[S::Null, S::Integer(70)])
            .unwrap()
            .rows
            .is_empty()
    );
    // Float binding requires DataFusion coercion, not a stale typed index key.
    assert_eq!(
        prepared
            .query(&[S::Real(3.0), S::Integer(30)])
            .unwrap()
            .rows,
        vec![vec![S::Text("P03".into()), S::Integer(30)]]
    );
    for limits in [
        QueryLimits {
            max_rows: 7,
            ..Default::default()
        },
        QueryLimits {
            max_bytes: 20,
            ..Default::default()
        },
        QueryLimits {
            vm_steps: 1,
            ..Default::default()
        },
    ] {
        assert!(
            prepared
                .query_with_limits(&[S::Integer(7), S::Integer(70)], limits)
                .is_err()
        );
    }
    // Repeated aliases create independent source occurrences; 8x8 matches,
    // including duplicate output values, must survive index probing.
    let rows = sql.query("SELECT a.score,b.score FROM core.Person p JOIN app.Link a ON a.person=p.id JOIN app.Link b ON b.person=p.id WHERE p.age=7", &[]).unwrap().rows;
    assert_eq!(rows, vec![vec![S::Integer(70), S::Integer(70)]; 64]);
    // OR, outer joins, sorting/limits and non-equality residuals stay on DF.
    assert_eq!(sql.query("SELECT p.name,l.score FROM core.Person p LEFT JOIN app.Link l ON l.person=p.id WHERE p.age=7 AND l.score>60 ORDER BY l.id LIMIT 2", &[]).unwrap().rows.len(),2);
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        prepared.query(&[S::Null, S::Integer(70)]),
        Err(Error::Denied)
    ));
}

#[test]
fn indexed_join_probes_keep_reference_closure_and_row_permissions() {
    let (_dir, db) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    tx.grant(
        "limited",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "age".into(),
                value: Value::Int(7),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let sql = db
        .session("limited")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    for statement in [
        "SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON l.person=p.id WHERE p.age=?1",
        "SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON l.person=p.id WHERE l.score=?1",
    ] {
        let mut q = sql.prepare(statement).unwrap();
        let factor = if statement.contains("WHERE l.") {
            10
        } else {
            1
        };
        assert_eq!(
            q.query(&[S::Integer(7 * factor)]).unwrap().rows,
            vec![vec![S::Text("P07".into()), S::Integer(70)]]
        );
        assert!(q.query(&[S::Integer(8 * factor)]).unwrap().rows.is_empty());
    }
    let selected = db
        .session("limited")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql_selected(&["core.Person"])
        .unwrap();
    assert!(
        selected
            .prepare(
                "SELECT p.name FROM core.Person p JOIN app.Link l ON p.id=l.person WHERE p.age=7"
            )
            .is_err()
    );
}

#[test]
fn indexed_join_results_match_sqlite_across_aliases_and_bindings() {
    let (_dir, db) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    for n in 0..7 {
        tx.insert_with_id(
            "app.Link",
            Id::from_u128(1000 + n),
            vec![Value::Ref(Id::from_u128(7)), Value::Int(70)],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch("ATTACH DATABASE ':memory:' AS core; ATTACH DATABASE ':memory:' AS app; CREATE TABLE core.Person(id TEXT,name TEXT,age INTEGER); CREATE TABLE app.Link(id TEXT,person TEXT,score INTEGER);").unwrap();
    for n in 1..=20 {
        sqlite
            .execute(
                "INSERT INTO core.Person VALUES(?1,?2,?3)",
                rusqlite::params![Id::from_u128(n).to_string(), format!("P{n:02}"), n as i64],
            )
            .unwrap();
        sqlite
            .execute(
                "INSERT INTO app.Link VALUES(?1,?2,?3)",
                rusqlite::params![
                    Id::from_u128(100 + n).to_string(),
                    Id::from_u128(n).to_string(),
                    n as i64 * 10
                ],
            )
            .unwrap();
    }
    for n in 0..7 {
        sqlite
            .execute(
                "INSERT INTO app.Link VALUES(?1,?2,70)",
                rusqlite::params![
                    Id::from_u128(1000 + n).to_string(),
                    Id::from_u128(7).to_string()
                ],
            )
            .unwrap();
    }
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    for statement in [
        "SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON p.id=l.person WHERE p.age=?1",
        "SELECT p.name,l.score FROM app.Link l JOIN core.Person p ON p.id=l.person WHERE l.score=?1",
        "SELECT a.score,b.score FROM core.Person p JOIN app.Link a ON a.person=p.id JOIN app.Link b ON b.person=p.id WHERE p.age=?1 AND a.score=b.score",
        "SELECT a.score,c.score FROM core.Person p JOIN app.Link a ON a.person=p.id JOIN app.Link b ON b.person=p.id JOIN app.Link c ON c.person=p.id WHERE p.age=?1 AND a.score=b.score AND b.score=c.score",
        "SELECT p.name,q.name FROM core.Person p JOIN core.Person q ON p.age=q.age WHERE p.age=?1",
        "SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON p.id=l.person WHERE p.age=?1 AND p.age=l.score",
    ] {
        let mut paged = sql.prepare(statement).unwrap();
        let mut reference = sqlite.prepare(statement).unwrap();
        for binding in [
            S::Integer(7),
            S::Integer(70),
            S::Integer(99),
            S::Null,
            S::Real(7.0),
        ] {
            let mut expected = reference
                .query_map([&binding], |row| {
                    Ok(vec![row.get::<_, S>(0)?, row.get::<_, S>(1)?])
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            let mut actual = paged.query(&[binding]).unwrap().rows;
            expected.sort_by_key(|row| format!("{row:?}"));
            actual.sort_by_key(|row| format!("{row:?}"));
            assert_eq!(actual, expected, "{statement}");
        }
    }
    let mut uuid_join = sql.prepare("SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON p.id=l.person WHERE p.id=?1").unwrap();
    for binding in [
        S::Text("invalid uuid".into()),
        S::Text(format!("{{{}}}", Id::from_u128(7))),
    ] {
        assert!(uuid_join.query(&[binding]).unwrap().rows.is_empty());
    }
}

#[test]
fn indexed_join_unsupported_binding_types_match_datafusion_fallback() {
    let (_dir, db) = fixture();
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let statement =
        "SELECT p.name,l.score FROM core.Person p JOIN app.Link l ON p.id=l.person WHERE p.age=?1";
    let mut indexed = sql.prepare(statement).unwrap();
    // OFFSET is deliberately outside the direct compiler's accepted shape.
    let mut fallback = sql.prepare(&format!("{statement} OFFSET 0")).unwrap();
    for value in [
        S::Text("7".into()),
        S::Text("invalid integer".into()),
        S::Blob(vec![7]),
        S::Real(7.5),
    ] {
        match (
            indexed.query(std::slice::from_ref(&value)),
            fallback.query(&[value]),
        ) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
            (Err(_), Err(_)) => {}
            (actual, expected) => {
                panic!("binding fallback diverged: {actual:?} versus {expected:?}")
            }
        }
    }
}

fn aggregate_fixture(values: &[i64]) -> (tempfile::TempDir, PagedDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        dir.path().join("aggregates"),
        PagedOptions {
            max_page_rows: 3,
            decoded_cache_bytes: 512,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "agg",
            1,
            vec![],
            vec![Relation::new(
                "agg.Item",
                vec![
                    Role::new("n", DataType::Int),
                    Role::new("amount", DataType::Decimal { scale: 2 }),
                    Role::new("time", DataType::Timestamp),
                    Role::new("flag", DataType::Bool),
                    Role::new("label", DataType::Text),
                ],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    for (i, &n) in values.iter().enumerate() {
        tx.insert_with_id(
            "agg.Item",
            Id::from_u128(i as u128 + 1),
            vec![
                Value::Int(n),
                Value::Decimal(n),
                Value::Timestamp(n),
                Value::Bool(i % 2 == 0),
                Value::Text(format!("item-{i}")),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    (dir, db)
}

#[test]
fn scalar_aggregates_match_sqlite_for_empty_typed_and_fallback_inputs() {
    for values in [
        vec![],
        vec![0],
        (0..137).map(|n| (n * 19 % 31) - 15).collect(),
    ] {
        let (_dir, db) = aggregate_fixture(&values);
        let sql = db
            .session("owner")
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        sqlite.execute_batch("ATTACH DATABASE ':memory:' AS agg; CREATE TABLE agg.Item(n INTEGER, amount INTEGER, time INTEGER, flag INTEGER, label TEXT);").unwrap();
        for (i, &n) in values.iter().enumerate() {
            sqlite
                .execute(
                    "INSERT INTO agg.Item VALUES(?1,?1,?1,?2,?3)",
                    rusqlite::params![n, i % 2 == 0, format!("item-{i}")],
                )
                .unwrap();
        }
        for statement in [
            "SELECT COUNT(*),COUNT(n),COUNT(label),COUNT(1),SUM(n),MIN(n),MAX(n) FROM agg.Item",
            "SELECT MAX(n) AS largest,SUM(n) AS total,COUNT(*) AS count,SUM(n) AS repeated FROM agg.Item",
            "SELECT COUNT(*),SUM(amount),SUM(time),SUM(flag),MIN(amount),MAX(time) FROM agg.Item",
            "SELECT SUM(n),COUNT(*) FROM agg.Item WHERE n>=0",
            "SELECT COUNT(DISTINCT n),AVG(n) FROM agg.Item",
            "SELECT COUNT(NULL),SUM(CAST(NULL AS BIGINT)),COUNT(CASE WHEN n>0 THEN n END),SUM(CASE WHEN n>0 THEN n END) FROM agg.Item",
            "SELECT SUM(n) FILTER (WHERE n>0),COUNT(*) FILTER (WHERE n<0) FROM agg.Item",
            "SELECT flag,COUNT(*),SUM(n) FROM agg.Item GROUP BY flag ORDER BY flag",
            "SELECT COUNT(*),SUM(n) FROM (SELECT n FROM agg.Item LIMIT 4) small",
        ] {
            let mut reference = sqlite.prepare(statement).unwrap();
            let columns = reference.column_count();
            let expected = reference
                .query_map([], |row| {
                    (0..columns)
                        .map(|i| row.get::<_, S>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                sql.query(statement, &[]).unwrap().rows,
                expected,
                "{statement}; values={values:?}"
            );
        }
        assert!(
            sql.query("SELECT SUM(DISTINCT n) FROM agg.Item", &[])
                .is_err()
        );
        // The base relations are non-nullable; nullable expressions above must
        // remain on the fallback path rather than counting every physical row.
        assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
    }
}

#[test]
fn scalar_sum_checks_final_overflow_without_rejecting_cancellation() {
    for (values, expected) in [
        (vec![i64::MAX, 1, -1], Some(i64::MAX)),
        (vec![i64::MIN, -1, 1], Some(i64::MIN)),
        (vec![i64::MAX, 1], None),
        (vec![i64::MIN, -1], None),
    ] {
        let (_dir, db) = aggregate_fixture(&values);
        let sql = db
            .session("owner")
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        for column in ["n", "amount", "time"] {
            let result = sql.query(&format!("SELECT SUM({column}) FROM agg.Item"), &[]);
            match expected {
                Some(value) => assert_eq!(result.unwrap().rows, vec![vec![S::Integer(value)]]),
                None => assert!(
                    result.is_err(),
                    "overflow accepted for {column}: {values:?}"
                ),
            }
        }
    }
}

#[test]
fn scalar_aggregate_snapshot_and_resource_budgets_remain_effective() {
    let (_dir, db) = fixture();
    let held = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let statement = "SELECT COUNT(*) AS count,SUM(score) AS total FROM app.Link";
    let mut query = held.prepare(statement).unwrap();
    let expected = vec![vec![S::Integer(20), S::Integer(2100)]];
    assert_eq!(query.query(&[]).unwrap().rows, expected);
    for limits in [
        QueryLimits {
            vm_steps: 1,
            ..Default::default()
        },
        QueryLimits {
            max_bytes: 1,
            ..Default::default()
        },
        QueryLimits {
            timeout_ms: 0,
            ..Default::default()
        },
        QueryLimits {
            max_rows: 0,
            ..Default::default()
        },
    ] {
        assert!(query.query_with_limits(&[], limits).is_err());
    }
    assert!(query.query(&[S::Integer(1)]).is_err());
    let mut tx = db.admin().transaction().unwrap();
    tx.update(
        "app.Link",
        Id::from_u128(101),
        vec![Value::Ref(Id::from_u128(1)), Value::Int(500)],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(query.query(&[]).unwrap().rows, expected);
    assert_eq!(
        db.session("reader")
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap()
            .query(statement, &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(20), S::Integer(2590)]]
    );
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}

#[test]
fn scalar_aggregates_enforce_reference_filters_visibility_and_capability_scope() {
    use substrate_liquid::capability::{CapabilityScope, RelationCapability};
    let (_dir, db) = fixture();
    let schemas = db.admin().snapshot().unwrap().schemas().unwrap();
    let person = schemas.iter().find(|r| r.name == "core.Person").unwrap().id;
    let link = schemas.iter().find(|r| r.name == "app.Link").unwrap().id;
    let filter = Grant {
        filter: RowFilter::Equals {
            role: "age".into(),
            value: Value::Int(7),
        },
        ..Grant::read_only()
    };
    let narrow = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: person,
                    grant: filter.clone(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let sql = db
        .session("owner")
        .unwrap()
        .with_capability(narrow.id)
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(
        sql.query("SELECT COUNT(*),SUM(age) FROM core.Person", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(1), S::Integer(7)]]
    );
    // Public visibility cannot expand a credential's explicit family scope.
    assert!(
        sql.query("SELECT COUNT(*),SUM(score) FROM app.Link", &[])
            .is_err()
    );
    let both = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![
                    RelationCapability {
                        relation: person,
                        grant: filter,
                    },
                    RelationCapability {
                        relation: link,
                        grant: Grant::read_only(),
                    },
                ],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let held = db
        .session("owner")
        .unwrap()
        .with_capability(both.id)
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut aggregate = held
        .prepare("SELECT COUNT(*),SUM(score),MIN(score),MAX(score) FROM app.Link")
        .unwrap();
    assert_eq!(
        aggregate.query(&[]).unwrap().rows,
        vec![vec![
            S::Integer(1),
            S::Integer(70),
            S::Integer(70),
            S::Integer(70)
        ]]
    );
    db.revoke_capability(both.id).unwrap();
    assert!(matches!(aggregate.query(&[]), Err(Error::Denied)));
    let public = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut aggregate = public
        .prepare("SELECT COUNT(*),SUM(score) FROM app.Link")
        .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.set_visibility("app.Link", Visibility::Private).unwrap();
    tx.commit().unwrap();
    assert!(matches!(aggregate.query(&[]), Err(Error::Denied)));
    let anonymous = db.anonymous().snapshot().unwrap().sql().unwrap();
    assert!(
        anonymous
            .query("SELECT COUNT(*),SUM(score) FROM app.Link", &[])
            .is_err()
    );
}

#[test]
fn scalar_aggregates_return_sql_empty_values_for_fully_filtered_references() {
    let (_dir, db) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    tx.grant(
        "nobody",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "age".into(),
                value: Value::Int(99),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let sql = db
        .session("nobody")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    for statement in [
        "SELECT COUNT(*),SUM(age),MIN(age),MAX(age) FROM core.Person",
        "SELECT COUNT(*),SUM(score),MIN(score),MAX(score) FROM app.Link",
    ] {
        assert_eq!(
            sql.query(statement, &[]).unwrap().rows,
            vec![vec![S::Integer(0), S::Null, S::Null, S::Null]]
        );
    }
}

#[test]
fn scalar_aggregate_retained_plan_fails_after_credential_rotation() {
    use substrate_liquid::capability::{CapabilityScope, RelationCapability};
    let (_dir, db) = fixture();
    let schemas = db.admin().snapshot().unwrap().schemas().unwrap();
    let person = schemas.iter().find(|r| r.name == "core.Person").unwrap().id;
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 120;
    let capability = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: person,
                    grant: Grant::read_only(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let credential = db.issue_credential("owner", capability.id, end).unwrap();
    let sql = db
        .authenticate(&credential.token)
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut query = sql
        .prepare("SELECT COUNT(*),SUM(age) FROM core.Person")
        .unwrap();
    assert_eq!(
        query.query(&[]).unwrap().rows,
        vec![vec![S::Integer(20), S::Integer(210)]]
    );
    let replacement = db.rotate_credential(credential.id, end).unwrap();
    assert!(matches!(query.query(&[]), Err(Error::Denied)));
    assert_eq!(
        db.authenticate(&replacement.token)
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap()
            .query("SELECT COUNT(*),SUM(age) FROM core.Person", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(20), S::Integer(210)]]
    );
}

#[test]
fn scalar_aggregates_require_every_transitive_reference_to_be_visible() {
    let (_dir, db) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "tail",
            1,
            vec![("app".into(), 1)],
            vec![Relation::new(
                "tail.Pair",
                vec![
                    Role::new("left", DataType::Ref("app.Link".into())),
                    Role::new("right", DataType::Ref("app.Link".into())),
                    Role::new("n", DataType::Int),
                ],
            )],
        ),
        "owner",
        Visibility::Public,
    )
    .unwrap();
    tx.insert(
        "tail.Pair",
        vec![
            Value::Ref(Id::from_u128(107)),
            Value::Ref(Id::from_u128(107)),
            Value::Int(5),
        ],
    )
    .unwrap();
    tx.insert(
        "tail.Pair",
        vec![
            Value::Ref(Id::from_u128(107)),
            Value::Ref(Id::from_u128(108)),
            Value::Int(900),
        ],
    )
    .unwrap();
    tx.grant(
        "limited",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "age".into(),
                value: Value::Int(7),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let sql = db
        .session("limited")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(
        sql.query("SELECT COUNT(*),SUM(n) FROM tail.Pair", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(1), S::Integer(5)]]
    );
    let full = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(
        full.query("SELECT COUNT(*),SUM(n) FROM tail.Pair", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(2), S::Integer(905)]]
    );
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}

#[test]
fn scalar_aggregate_null_output_bytes_and_pruned_overflow_match_fallback() {
    let (_dir, empty) = aggregate_fixture(&[]);
    let sql = empty
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut query = sql.prepare("SELECT SUM(n) AS s FROM agg.Item").unwrap();
    assert!(
        query
            .query_with_limits(
                &[],
                QueryLimits {
                    max_bytes: 8,
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert_eq!(
        query
            .query_with_limits(
                &[],
                QueryLimits {
                    max_bytes: 9,
                    ..Default::default()
                }
            )
            .unwrap()
            .rows,
        vec![vec![S::Null]]
    );
    let (_dir, overflow) = aggregate_fixture(&[i64::MAX, 1]);
    let sql = overflow
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    // The unselected SUM may be removed by DataFusion; eagerly computing it in
    // a shortcut would raise a new error that changes this query's meaning.
    assert_eq!(
        sql.query(
            "SELECT n FROM (SELECT COUNT(*) AS n,SUM(n) AS unused FROM agg.Item) q",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(2)]]
    );
}

fn sqlite_aggregate_reference(values: &[i64]) -> rusqlite::Connection {
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch("ATTACH DATABASE ':memory:' AS agg; CREATE TABLE agg.Item(n INTEGER NOT NULL, amount INTEGER NOT NULL, time INTEGER NOT NULL, flag INTEGER NOT NULL, label TEXT NOT NULL);").unwrap();
    for (i, &n) in values.iter().enumerate() {
        sqlite
            .execute(
                "INSERT INTO agg.Item VALUES(?1,?1,?1,?2,?3)",
                rusqlite::params![n, i % 2 == 0, format!("item-{i}")],
            )
            .unwrap();
    }
    sqlite
}

#[test]
fn filtered_and_grouped_aggregates_match_sqlite_and_rebind_parameters() {
    for values in [
        vec![],
        vec![0],
        (0..137).map(|n| (n * 19 % 31) - 15).collect(),
    ] {
        let (_dir, db) = aggregate_fixture(&values);
        let sql = db
            .session("owner")
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        let sqlite = sqlite_aggregate_reference(&values);
        for statement in [
            "SELECT COUNT(*),SUM(n),MIN(n),MAX(n) FROM agg.Item WHERE n=?1",
            "SELECT COUNT(*),SUM(n),MIN(n),MAX(n) FROM agg.Item WHERE n>=?1 AND n<?2",
            "SELECT flag,COUNT(*),SUM(n),MIN(amount),MAX(time) FROM agg.Item WHERE n>=?1 AND n<?2 GROUP BY flag ORDER BY flag",
            "SELECT n,flag,COUNT(*),SUM(amount) FROM agg.Item WHERE n>=?1 AND n<?2 GROUP BY n,flag ORDER BY n DESC,flag",
            "SELECT flag AS bucket,SUM(n) AS total,COUNT(*) AS count,SUM(n) AS repeated FROM agg.Item WHERE n>=?1 AND n<?2 GROUP BY flag ORDER BY bucket DESC",
            "SELECT label,COUNT(*),MIN(n) FROM agg.Item WHERE n>=?1 AND n<?2 GROUP BY label ORDER BY label",
            "SELECT flag,COUNT(*) FILTER (WHERE n>0),SUM(n) FROM agg.Item WHERE n>=?1 AND n<?2 GROUP BY flag HAVING COUNT(*)>1 ORDER BY flag",
            "SELECT k,COUNT(*),SUM(v) FROM (SELECT flag AS k,n AS v FROM agg.Item WHERE n>=?1 AND n<?2) selected GROUP BY k ORDER BY k",
        ] {
            let mut prepared = sql.prepare(statement).unwrap();
            let mut reference = sqlite.prepare(statement).unwrap();
            let count = reference.column_count();
            for (low, high) in [(-100, 100), (-8, 7), (30, 40), (0, 0)] {
                let parameters = if statement.contains("?2") {
                    vec![S::Integer(low), S::Integer(high)]
                } else {
                    vec![S::Integer(low)]
                };
                let expected = reference
                    .query_map(rusqlite::params_from_iter(&parameters), |row| {
                        (0..count)
                            .map(|i| row.get::<_, S>(i))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                assert_eq!(
                    prepared.query(&parameters).unwrap().rows,
                    expected,
                    "{statement}, {parameters:?}"
                );
            }
        }
        assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
    }
}

#[test]
fn grouped_aggregate_binding_coercions_and_nulls_match_fallback() {
    let (_dir, db) = aggregate_fixture(&[-3, 0, 1, 1, 2, 7]);
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    for statement in [
        "SELECT COUNT(*),SUM(n) FROM agg.Item WHERE n=?1",
        "SELECT flag,COUNT(*),SUM(n) FROM agg.Item WHERE n>=?1 GROUP BY flag ORDER BY flag",
        "SELECT label,COUNT(*),SUM(n) FROM agg.Item WHERE label=?1 GROUP BY label ORDER BY label",
    ] {
        let mut query = sql.prepare(statement).unwrap();
        let mut fallback = sql.prepare(&format!("{statement} OFFSET 0")).unwrap();
        for value in [
            S::Null,
            S::Integer(1),
            S::Real(1.0),
            S::Real(1.5),
            S::Text("1".into()),
            S::Text("bad".into()),
            S::Blob(vec![1]),
        ] {
            match (
                query.query(std::slice::from_ref(&value)),
                fallback.query(std::slice::from_ref(&value)),
            ) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected, "{statement}, {value:?}")
                }
                (Err(_), Err(_)) => {}
                pair => panic!("fallback mismatch {statement}, {value:?}: {pair:?}"),
            }
        }
    }
}

#[test]
fn grouped_aggregate_budgets_bound_group_cardinality_and_text_state() {
    let values = (0..256).collect::<Vec<_>>();
    let (_dir, db) = aggregate_fixture(&values);
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    for statement in [
        "SELECT n,COUNT(*),SUM(amount) FROM agg.Item GROUP BY n",
        "SELECT label,COUNT(*),SUM(n) FROM agg.Item WHERE n>=0 GROUP BY label",
    ] {
        let mut query = sql.prepare(statement).unwrap();
        for limits in [
            QueryLimits {
                max_rows: 1,
                ..Default::default()
            },
            QueryLimits {
                max_bytes: 128,
                ..Default::default()
            },
            QueryLimits {
                vm_steps: 1,
                ..Default::default()
            },
        ] {
            assert!(query.query_with_limits(&[], limits).is_err(), "{statement}");
        }
        assert_eq!(query.query(&[]).unwrap().rows.len(), 256);
    }
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}

#[test]
fn grouped_aggregate_final_overflow_and_cancellation_are_per_group() {
    for (values, overflows) in [
        (vec![i64::MAX, 0, 1], true),
        (vec![i64::MAX, 0, 1, 0, -1], false),
        (vec![i64::MIN, 0, -1, 0, 1], false),
        (vec![i64::MIN, 0, -1], true),
    ] {
        let (_dir, db) = aggregate_fixture(&values);
        let sql = db
            .session("owner")
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        let statement =
            "SELECT flag,COUNT(*),SUM(n) FROM agg.Item WHERE n>=?1 GROUP BY flag ORDER BY flag";
        let result = sql.query(statement, &[S::Integer(i64::MIN)]);
        if overflows {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result.unwrap().rows,
                vec![
                    vec![S::Integer(0), S::Integer(2), S::Integer(0)],
                    vec![S::Integer(1), S::Integer(3), S::Integer(values[0])]
                ]
            );
        }
    }
}

#[test]
fn filtered_grouped_aggregates_preserve_scoped_reference_visibility_and_revocation() {
    use substrate_liquid::capability::{CapabilityScope, RelationCapability};
    let (_dir, db) = fixture();
    let schemas = db.admin().snapshot().unwrap().schemas().unwrap();
    let person = schemas.iter().find(|r| r.name == "core.Person").unwrap().id;
    let link = schemas.iter().find(|r| r.name == "app.Link").unwrap().id;
    let cap = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![
                    RelationCapability {
                        relation: person,
                        grant: Grant {
                            filter: RowFilter::Equals {
                                role: "age".into(),
                                value: Value::Int(7),
                            },
                            ..Grant::read_only()
                        },
                    },
                    RelationCapability {
                        relation: link,
                        grant: Grant::read_only(),
                    },
                ],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let sql = db
        .session("owner")
        .unwrap()
        .with_capability(cap.id)
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let mut filtered = sql
        .prepare("SELECT COUNT(*),SUM(score) FROM app.Link WHERE score>=?1")
        .unwrap();
    let mut grouped=sql.prepare("SELECT score,COUNT(*),SUM(score) FROM app.Link WHERE score>=?1 GROUP BY score ORDER BY score").unwrap();
    assert_eq!(
        filtered.query(&[S::Integer(0)]).unwrap().rows,
        vec![vec![S::Integer(1), S::Integer(70)]]
    );
    assert_eq!(
        grouped.query(&[S::Integer(0)]).unwrap().rows,
        vec![vec![S::Integer(70), S::Integer(1), S::Integer(70)]]
    );
    assert_eq!(
        filtered.query(&[S::Integer(100)]).unwrap().rows,
        vec![vec![S::Integer(0), S::Null]]
    );
    assert!(grouped.query(&[S::Integer(100)]).unwrap().rows.is_empty());
    db.revoke_capability(cap.id).unwrap();
    assert!(matches!(
        filtered.query(&[S::Integer(0)]),
        Err(Error::Denied)
    ));
    assert!(matches!(
        grouped.query(&[S::Integer(0)]),
        Err(Error::Denied)
    ));
    let anonymous = db.anonymous().snapshot().unwrap().sql().unwrap();
    assert!(
        anonymous
            .query("SELECT score,COUNT(*) FROM app.Link GROUP BY score", &[])
            .is_err()
    );
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}

#[test]
fn filtered_grouped_aggregates_hold_their_data_snapshot() {
    let (_dir, db) = aggregate_fixture(&[1, 2, 1]);
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let statement = "SELECT n,COUNT(*),SUM(n) FROM agg.Item WHERE n>=?1 GROUP BY n ORDER BY n";
    let mut prepared = sql.prepare(statement).unwrap();
    let before = vec![
        vec![S::Integer(1), S::Integer(2), S::Integer(2)],
        vec![S::Integer(2), S::Integer(1), S::Integer(2)],
    ];
    assert_eq!(prepared.query(&[S::Integer(0)]).unwrap().rows, before);
    let mut tx = db.admin().transaction().unwrap();
    tx.update(
        "agg.Item",
        Id::from_u128(1),
        vec![
            Value::Int(4),
            Value::Decimal(4),
            Value::Timestamp(4),
            Value::Bool(true),
            Value::Text("item-0".into()),
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(prepared.query(&[S::Integer(0)]).unwrap().rows, before);
    let current = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(
        current.query(statement, &[S::Integer(0)]).unwrap().rows,
        vec![
            vec![S::Integer(1), S::Integer(1), S::Integer(1)],
            vec![S::Integer(2), S::Integer(1), S::Integer(2)],
            vec![S::Integer(4), S::Integer(1), S::Integer(4)]
        ]
    );
}

#[test]
fn aggregate_filter_keeps_source_groups_and_orders_filtered_nulls() {
    let values = vec![-7, 2, 0, 4, 7, 2];
    let (_dir, db) = aggregate_fixture(&values);
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let sqlite = sqlite_aggregate_reference(&values);
    for statement in [
        "SELECT flag,COUNT(*) FILTER (WHERE n>?1),SUM(n) FILTER (WHERE n<?2) FROM agg.Item GROUP BY flag ORDER BY flag",
        "SELECT flag,COUNT(*) FILTER (WHERE n>?1),SUM(n) FILTER (WHERE n>?1) AS total FROM agg.Item WHERE n<?2 GROUP BY flag ORDER BY total ASC NULLS FIRST,flag",
        "SELECT flag,SUM(n) FILTER (WHERE n>?1) AS total FROM agg.Item WHERE n<?2 GROUP BY flag ORDER BY total DESC NULLS LAST,flag",
        "SELECT COUNT(*) FILTER (WHERE n>?1),SUM(n) FILTER (WHERE n<?2),MIN(n) FILTER (WHERE n>?1),MAX(n) FILTER (WHERE n<?2) FROM agg.Item",
    ] {
        let mut query = sql.prepare(statement).unwrap();
        let mut reference = sqlite.prepare(statement).unwrap();
        let columns = reference.column_count();
        for params in [
            vec![S::Integer(0), S::Integer(100)],
            vec![S::Integer(100), S::Integer(100)],
            vec![S::Integer(5), S::Integer(100)],
            vec![S::Null, S::Integer(100)],
        ] {
            let expected = reference
                .query_map(rusqlite::params_from_iter(&params), |row| {
                    (0..columns)
                        .map(|i| row.get::<_, S>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                query.query(&params).unwrap().rows,
                expected,
                "{statement}, {params:?}"
            );
        }
    }
}

#[test]
fn aggregate_filter_does_not_accumulate_excluded_overflowing_values() {
    let (_dir, db) = aggregate_fixture(&[i64::MAX, 0, 1]);
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    assert_eq!(sql.query("SELECT flag,COUNT(*),SUM(n) FILTER (WHERE n<=0) FROM agg.Item GROUP BY flag ORDER BY flag",&[]).unwrap().rows,vec![
        vec![S::Integer(0),S::Integer(1),S::Integer(0)],
        vec![S::Integer(1),S::Integer(2),S::Null],
    ]);
    assert_eq!(
        sql.query("SELECT SUM(n) FILTER (WHERE n<=0) FROM agg.Item", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(0)]]
    );
    assert!(
        sql.query(
            "SELECT flag,SUM(n) FILTER (WHERE n>=0) FROM agg.Item GROUP BY flag",
            &[]
        )
        .is_err()
    );
}

#[test]
fn grouped_scans_keep_extreme_sparse_integer_keys_distinct() {
    // Exercise signed-key conversion and sparse-key fallbacks without SUM
    // overflow obscuring a grouping error.
    let values = vec![
        i64::MIN,
        i64::MAX,
        0,
        -1,
        1,
        4096,
        -4096,
        i64::MIN,
        0,
        i64::MAX,
        1 << 40,
        -(1 << 40),
        4096,
        -1,
        0,
        64,
        128,
        -64,
        0,
        128,
        64,
        -64,
    ];
    let (_dir, db) = aggregate_fixture(&values);
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    let sqlite = sqlite_aggregate_reference(&values);
    for statement in [
        "SELECT n,COUNT(*),MIN(n),MAX(n) FROM agg.Item GROUP BY n ORDER BY n",
        "SELECT n,flag,COUNT(*) FROM agg.Item WHERE n>=?1 GROUP BY n,flag ORDER BY n DESC,flag",
        "SELECT n,COUNT(*) FILTER (WHERE n>?1),MIN(n) FILTER (WHERE n>?1) FROM agg.Item GROUP BY n ORDER BY n",
    ] {
        let params = if statement.contains("?1") {
            vec![S::Integer(i64::MIN)]
        } else {
            vec![]
        };
        let mut reference = sqlite.prepare(statement).unwrap();
        let columns = reference.column_count();
        let expected = reference
            .query_map(rusqlite::params_from_iter(&params), |row| {
                (0..columns)
                    .map(|i| row.get::<_, S>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let mut query = sql.prepare(statement).unwrap();
        assert!(
            query
                .query_with_limits(
                    &params,
                    QueryLimits {
                        max_rows: 1,
                        ..Default::default()
                    }
                )
                .is_err()
        );
        assert_eq!(query.query(&params).unwrap().rows, expected, "{statement}");
        assert_eq!(
            query.query(&params).unwrap().rows,
            expected,
            "repeated {statement}"
        );
    }
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}

#[test]
fn grouped_scans_retain_variable_keys_across_reused_row_buffers() {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        dir.path().join("variable-keys"),
        PagedOptions {
            max_page_rows: 3,
            decoded_cache_bytes: 512,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "keys",
            1,
            vec![],
            vec![Relation::new(
                "keys.Item",
                vec![
                    Role::new("label", DataType::Text),
                    Role::new("payload", DataType::Bytes),
                    Role::new("n", DataType::Int),
                ],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch("ATTACH DATABASE ':memory:' AS keys; CREATE TABLE keys.Item(label TEXT NOT NULL,payload BLOB NOT NULL,n INTEGER NOT NULL);").unwrap();
    let labels = [
        "".to_owned(),
        "a".into(),
        "a\0".into(),
        "a\0b".into(),
        "é".into(),
        "🦀".into(),
        "z".repeat(4096),
    ];
    let payloads = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![255],
        vec![255, 0],
        vec![128; 4096],
    ];
    for n in 0..140 {
        let label = &labels[(n * 5) % labels.len()];
        let payload = &payloads[(n * 3 + n / 7) % payloads.len()];
        let value = n as i64 % 9 - 4;
        tx.insert_with_id(
            "keys.Item",
            Id::from_u128(n as u128 + 1),
            vec![
                Value::Text(label.clone()),
                Value::Bytes(payload.clone()),
                Value::Int(value),
            ],
        )
        .unwrap();
        sqlite
            .execute(
                "INSERT INTO keys.Item VALUES(?1,?2,?3)",
                rusqlite::params![label, payload, value],
            )
            .unwrap();
    }
    tx.commit().unwrap();
    let sql = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    for statement in [
        "SELECT label,COUNT(*),SUM(n) FROM keys.Item GROUP BY label ORDER BY label",
        "SELECT payload,COUNT(*),MIN(n),MAX(n) FROM keys.Item GROUP BY payload ORDER BY payload",
        "SELECT label,payload,COUNT(*),SUM(n) FROM keys.Item WHERE n>=?1 GROUP BY label,payload ORDER BY label,payload",
        "SELECT label,COUNT(*) FILTER (WHERE n>?1),SUM(n) FILTER (WHERE n>?1) FROM keys.Item GROUP BY label ORDER BY label",
    ] {
        let mut query = sql.prepare(statement).unwrap();
        let mut reference = sqlite.prepare(statement).unwrap();
        let columns = reference.column_count();
        for threshold in [-10, 0, 10] {
            let params = if statement.contains("?1") {
                vec![S::Integer(threshold)]
            } else {
                vec![]
            };
            let expected = reference
                .query_map(rusqlite::params_from_iter(&params), |row| {
                    (0..columns)
                        .map(|i| row.get::<_, S>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                query.query(&params).unwrap().rows,
                expected,
                "{statement}, {threshold}"
            );
        }
    }
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
}
