mod common;
use common::*;
use rusqlite::types::Value as S;
use substrate_liquid::*;

#[test]
fn datafusion_repeated_sorted_limits_and_parameter_rebinding_are_stable() {
    let (db, person) = fixture();
    let mut tx = db.admin().transaction();
    for i in 0..100 {
        workout(&mut tx, person, i);
    }
    tx.commit().unwrap();
    let sql = db.admin().snapshot().sql().unwrap();
    let mut prepared = sql.prepare("SELECT durationMinutes FROM fitness.Workout WHERE durationMinutes >= ?1 ORDER BY durationMinutes DESC LIMIT 5").unwrap();
    for lower in [0, 80, 0, 95, 0] {
        let rows = prepared.query(&[S::Integer(lower)]).unwrap().rows;
        assert_eq!(
            rows,
            (95..100)
                .rev()
                .map(|i| vec![S::Integer(i)])
                .collect::<Vec<_>>()
        );
    }
    assert!(prepared.query(&[]).is_err());
    assert!(prepared.query(&[S::Integer(0), S::Integer(1)]).is_err());
    assert_eq!(
        sql.query("SELECT '?1' AS literal, $1 AS bound", &[S::Integer(7)])
            .unwrap()
            .rows,
        vec![vec![S::Text("?1".into()), S::Integer(7)]]
    );
}

#[test]
fn datafusion_filters_rows_and_relationships_before_aggregates() {
    let (db, person) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, person, 45);
    workout(&mut tx, person, 900);
    tx.grant("limited", "core.Person", Grant::read_only())
        .unwrap();
    tx.grant("limited", "fitness.Performed", Grant::read_only())
        .unwrap();
    tx.grant(
        "limited",
        "fitness.Workout",
        Grant {
            filter: RowFilter::Equals {
                role: "durationMinutes".into(),
                value: 45.into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let sql = db.session("limited").snapshot().sql().unwrap();
    assert_eq!(sql.query("SELECT count(*),sum(w.durationMinutes) FROM fitness.Performed f JOIN fitness.Workout w ON f.workout=w.id", &[]).unwrap().rows, vec![vec![S::Integer(1),S::Integer(45)]]);
    assert!(sql.query("SELECT * FROM crm.Deal", &[]).is_err());
    assert!(
        sql.query("SELECT * FROM information_schema.tables", &[])
            .is_err()
    );
}

#[test]
fn datafusion_retained_handles_fail_after_revocation_and_host_drop() {
    let (db, _) = fixture();
    let sql = db.session("fitness").snapshot().sql().unwrap();
    let mut prepared = sql.prepare("SELECT count(*) FROM core.Person").unwrap();
    let mut tx = db.admin().transaction();
    tx.revoke("fitness", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(prepared.query(&[]), Err(Error::Denied)));
    let admin = db.admin().snapshot().sql().unwrap();
    drop(db);
    assert!(matches!(admin.query("SELECT 1", &[]), Err(Error::Denied)));
}

#[test]
fn datafusion_rejects_mutations_external_access_and_bad_statements() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    for query in [
        "DELETE FROM core.Person",
        "CREATE TABLE x AS SELECT * FROM core.Person",
        "CREATE EXTERNAL TABLE x STORED AS CSV LOCATION '/tmp/private'",
        "COPY core.Person TO '/tmp/leak'",
        "SET datafusion.execution.target_partitions=1000",
        "SELECT * FROM read_csv('/tmp/private')",
        "SELECT * FROM parquet_scan('/tmp/private')",
        "SELECT * FROM generate_series(1,100000000)",
        "SELECT 1; SELECT 2",
        "",
        "SELECT * FROM information_schema.tables",
        "SHOW TABLES",
    ] {
        assert!(sql.query(query, &[]).is_err(), "{query}");
    }
    assert_eq!(
        sql.query("SELECT 1", &[]).unwrap().rows,
        vec![vec![S::Integer(1)]]
    );
}

#[test]
fn datafusion_output_budgets_fail_without_partial_results_or_poisoning() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    for (query, limits) in [
        (
            "SELECT * FROM core.Person",
            QueryLimits {
                max_bytes: 1,
                ..QueryLimits::default()
            },
        ),
        (
            "SELECT 1 UNION ALL SELECT 2",
            QueryLimits {
                max_rows: 1,
                ..QueryLimits::default()
            },
        ),
        (
            "SELECT 1,2",
            QueryLimits {
                vm_steps: 1,
                ..QueryLimits::default()
            },
        ),
        (
            "SELECT 1",
            QueryLimits {
                timeout_ms: 0,
                ..QueryLimits::default()
            },
        ),
    ] {
        assert!(
            sql.query_with_limits(query, &[], limits).is_err(),
            "{query}"
        );
    }
    assert_eq!(
        sql.query("SELECT 1", &[]).unwrap().rows,
        vec![vec![S::Integer(1)]]
    );
}

#[test]
fn datafusion_exact_types_nulls_and_overflow_are_explicit() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "typed",
        1,
        vec![],
        vec![Relation::new(
            "typed.All",
            vec![
                Role::new("n", DataType::Int),
                Role::new("blob", DataType::Bytes),
                Role::new("text", DataType::Text),
            ],
        )],
    ))
    .unwrap();
    for n in [i64::MIN, i64::MAX] {
        tx.insert(
            "typed.All",
            vec![
                n.into(),
                Value::Bytes(vec![0, 255]),
                "quote' unicode 🦀".into(),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let sql = db.admin().snapshot().sql().unwrap();
    let rows = sql
        .query("SELECT n,blob,text FROM typed.All ORDER BY n", &[])
        .unwrap()
        .rows;
    assert_eq!(
        rows[0],
        vec![
            S::Integer(i64::MIN),
            S::Blob(vec![0, 255]),
            S::Text("quote' unicode 🦀".into())
        ]
    );
    assert_eq!(rows[1][0], S::Integer(i64::MAX));
    assert_eq!(
        sql.query("SELECT NULL", &[]).unwrap().rows,
        vec![vec![S::Null]]
    );
    assert!(
        sql.query("SELECT CAST(18446744073709551615 AS BIGINT UNSIGNED)", &[])
            .is_err()
    );
}

#[test]
fn datafusion_views_and_selected_schemas_remain_restricted() {
    let (db, _) = fixture();
    let mut sql = db
        .session("fitness")
        .snapshot()
        .sql_selected(&["core.Person"])
        .unwrap();
    sql.view("names", "SELECT name FROM core.Person").unwrap();
    assert_eq!(
        sql.query("SELECT name FROM names", &[]).unwrap().rows,
        vec![vec![S::Text("Alice".into())]]
    );
    assert!(sql.view("bad", "DELETE FROM core.Person").is_err());
    assert!(sql.view("names", "SELECT 1").is_err());
    assert!(sql.query("SELECT * FROM fitness.Workout", &[]).is_err());
}

#[test]
fn synchronous_datafusion_api_works_inside_a_tokio_runtime() {
    let (db, _) = fixture();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let sql = db.admin().snapshot().sql().unwrap();
            assert_eq!(
                sql.query("SELECT count(*) FROM core.Person", &[])
                    .unwrap()
                    .rows,
                vec![vec![S::Integer(1)]]
            );
        });
}

#[test]
fn datafusion_credential_rotation_invalidates_cached_batches() {
    use substrate_liquid::capability::{CapabilityScope, RelationCapability};
    let (db, _) = fixture();
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let cap = db
        .issue_capability_until(
            Id::new(),
            "fitness",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                    grant: Grant::read_only(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
            Some(deadline),
        )
        .unwrap();
    let credential = db.issue_credential("fitness", cap.id, deadline).unwrap();
    let sql = db
        .authenticate(&credential.token)
        .unwrap()
        .snapshot()
        .sql()
        .unwrap();
    let mut prepared = sql.prepare("SELECT name FROM core.Person").unwrap();
    assert_eq!(prepared.query(&[]).unwrap().rows.len(), 1);
    let replacement = db.rotate_credential(credential.id, deadline).unwrap();
    assert!(matches!(prepared.query(&[]), Err(Error::Denied)));
    assert_eq!(
        db.authenticate(&replacement.token)
            .unwrap()
            .snapshot()
            .sql()
            .unwrap()
            .query("SELECT name FROM core.Person", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn datafusion_parameter_tokenization_preserves_literals_and_comments() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    assert_eq!(
        sql.query(
            "SELECT '?1 $2' AS a, ?1 AS b /* ?99 */ -- ?88\n",
            &[S::Integer(4)]
        )
        .unwrap()
        .rows,
        vec![vec![S::Text("?1 $2".into()), S::Integer(4)]]
    );
    for query in [
        "SELECT ?0",
        "SELECT ?1025",
        "SELECT $name",
        "SELECT ?9999999999999999999999999999999",
    ] {
        assert!(sql.query(query, &[]).is_err(), "{query}");
    }
    assert!(sql.query("SELECT CAST(1 AS DECIMAL(10,2))", &[]).is_err());
}

#[test]
fn datafusion_null_order_and_outer_join_results_are_explicit() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    assert_eq!(
        sql.query("SELECT x FROM (VALUES (1),(NULL),(2)) t(x) ORDER BY x", &[])
            .unwrap()
            .rows,
        vec![vec![S::Null], vec![S::Integer(1)], vec![S::Integer(2)]]
    );
    assert_eq!(
        sql.query(
            "SELECT x FROM (VALUES (1),(NULL),(2)) t(x) ORDER BY x DESC",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(2)], vec![S::Integer(1)], vec![S::Null]]
    );
    assert_eq!(
        sql.query(
            "SELECT x FROM (VALUES (1),(NULL),(2)) t(x) ORDER BY x NULLS LAST",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(1)], vec![S::Integer(2)], vec![S::Null]]
    );
    assert_eq!(
        sql.query(
            "SELECT p.name,w.durationMinutes FROM core.Person p LEFT JOIN fitness.Workout w ON 1=0",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Text("Alice".into()), S::Null]]
    );
}

#[test]
fn datafusion_rejects_expanding_functions_and_oversized_source_cells() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    for query in [
        "SELECT repeat('x',1000000000)",
        "SELECT concat('a','b')",
        "SELECT overlay('ab' placing 'ab' from 1 for 0)",
        "SELECT overlay('ab','ab',1,0)",
        "SELECT 'a'||'b'",
        "SELECT string_agg(name,',') FROM core.Person",
    ] {
        assert!(sql.query(query, &[]).is_err(), "{query}");
    }
    let mut tx = db.admin().transaction();
    tx.insert(
        "core.Person",
        vec![Value::Text("x".repeat(1024 * 1024 + 1))],
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(db.admin().snapshot().sql().is_err());
    // Native reads keep their existing behavior and can access this large value.
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 2);
}

#[test]
fn datafusion_deadline_failure_does_not_poison_reusable_snapshot() {
    let (db, person) = fixture();
    let mut tx = db.admin().transaction();
    for n in 0..500 {
        workout(&mut tx, person, n);
    }
    tx.commit().unwrap();
    let sql = db.admin().snapshot().sql().unwrap();
    let result=sql.query_with_limits("SELECT SUM(a.durationMinutes+b.durationMinutes+c.durationMinutes) FROM fitness.Workout a CROSS JOIN fitness.Workout b CROSS JOIN fitness.Workout c",&[],QueryLimits{timeout_ms:1,..QueryLimits::default()});
    assert!(result.is_err());
    assert_eq!(
        sql.query("SELECT count(*) FROM fitness.Workout", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(500)]]
    );
}

#[test]
fn datafusion_snapshot_pins_rows_across_native_updates() {
    let (db, person) = fixture();
    let old = db.admin().snapshot().sql().unwrap();
    let mut tx = db.admin().transaction();
    tx.update("core.Person", person, vec!["New name".into()])
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        old.query("SELECT name FROM core.Person", &[]).unwrap().rows,
        vec![vec![S::Text("Alice".into())]]
    );
    assert_eq!(
        db.admin()
            .snapshot()
            .sql()
            .unwrap()
            .query("SELECT name FROM core.Person", &[])
            .unwrap()
            .rows,
        vec![vec![S::Text("New name".into())]]
    );
}

#[test]
fn datafusion_integer_overflow_is_an_error_before_and_after_optimization() {
    let (db, person) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, person, i64::MAX);
    workout(&mut tx, person, 1);
    tx.commit().unwrap();
    let sql = db.admin().snapshot().sql().unwrap();
    for query in [
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775808 - 1",
        "SELECT 9223372036854775807 * 2",
        "SELECT -(-9223372036854775808)",
        "SELECT CAST(2147483647 AS INT) + CAST(1 AS INT)",
        "SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS INT)",
        "SELECT durationMinutes + 1 FROM fitness.Workout",
        "SELECT durationMinutes + CAST(1 AS INT) FROM fitness.Workout",
        "SELECT durationMinutes FROM fitness.Workout WHERE durationMinutes + 1 < 0",
        "SELECT 1 WHERE 9223372036854775807 + 1 < 0",
        "SELECT SUM(durationMinutes) FROM fitness.Workout",
        "SELECT SUM(durationMinutes) OVER () FROM fitness.Workout",
        "SELECT CAST('NaN' AS DOUBLE)",
        "SELECT CAST('Infinity' AS DOUBLE)",
    ] {
        assert!(
            sql.query(query, &[]).is_err(),
            "must fail without wrapping: {query}"
        );
    }
    let mut prepared = sql.prepare("SELECT ?1 + ?2 AS result").unwrap();
    assert!(
        prepared
            .query(&[S::Integer(i64::MAX), S::Integer(1)])
            .is_err()
    );
    assert_eq!(
        prepared
            .query(&[S::Integer(40), S::Integer(2)])
            .unwrap()
            .rows,
        vec![vec![S::Integer(42)]]
    );
    assert_eq!(
        sql.query(
            "SELECT CAST(20 AS INT) + CAST(22 AS INT), 6 * 7, 85 / 2, 43 % 41",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![
            S::Integer(42),
            S::Integer(42),
            S::Integer(42),
            S::Integer(2)
        ]]
    );
}

#[test]
fn datafusion_checked_sum_handles_nulls_cancellation_groups_and_sliding_windows() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    let grouped = "SELECT g,SUM(x) FROM (VALUES (1,9223372036854775807),(1,1),(1,-1),(2,NULL)) t(g,x) GROUP BY g ORDER BY g";
    let mut prepared = sql.prepare(grouped).unwrap();
    for _ in 0..3 {
        assert_eq!(
            prepared.query(&[]).unwrap().rows,
            vec![
                vec![S::Integer(1), S::Integer(i64::MAX)],
                vec![S::Integer(2), S::Null]
            ]
        );
    }
    assert_eq!(
        sql.query(
            "SELECT SUM(x) FROM (VALUES (CAST(NULL AS BIGINT))) t(x)",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Null]]
    );
    assert_eq!(
        sql.query(
            "SELECT SUM(x) FROM (VALUES (CAST(1 AS INT)),(CAST(2 AS INT))) t(x)",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(3)]]
    );
    assert_eq!(sql.query("SELECT i,SUM(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1,1),(2,NULL),(3,NULL),(4,4),(5,-4)) t(i,x) ORDER BY i", &[]).unwrap().rows,
        vec![vec![S::Integer(1),S::Integer(1)],vec![S::Integer(2),S::Integer(1)],vec![S::Integer(3),S::Null],vec![S::Integer(4),S::Integer(4)],vec![S::Integer(5),S::Integer(0)]]);
}
