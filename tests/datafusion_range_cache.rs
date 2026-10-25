use rusqlite::types::Value as S;
use substrate_liquid::*;

fn fixture() -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "ranges",
        1,
        vec![],
        vec![Relation::new(
            "ranges.Item",
            vec![Role::new("n", DataType::Int), Role::new("m", DataType::Int)],
        )],
    ))
    .unwrap();
    // Large equal-value runs cross page boundaries; the cursor needs both value and ID.
    for i in 0..2500 {
        tx.insert_with_id(
            "ranges.Item",
            Id::from_u128(i + 1),
            vec![Value::Int((i / 1200) as i64), Value::Int(i as i64)],
        )
        .unwrap();
    }
    tx.grant(
        "zero",
        "ranges.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "n".into(),
                value: Value::Int(0),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db
}
fn explain(sql: &SqlSnapshot, query: &str, params: &[S]) -> String {
    sql.query(&format!("EXPLAIN ANALYZE {query}"), params)
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .filter_map(|v| match v {
            S::Text(s) => Some(s),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn warm_ranges_keep_bindings_projection_and_tied_cursor_pages_independent() {
    let db = fixture();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let query = "SELECT id,n,m FROM ranges.Item WHERE n>=?1 AND n<=?2 ORDER BY n,id";
    let cold = explain(&sql, query, &[S::Integer(0), S::Integer(2)]);
    assert!(cold.contains("access=numeric_range"), "{cold}");
    assert!(cold.contains("cache_hits=0"), "{cold}");
    let warm = explain(&sql, query, &[S::Integer(0), S::Integer(2)]);
    assert!(warm.contains("cache_hits=3"), "{warm}");
    for (lower, upper) in [(0, 2), (1, 2), (0, 0), (2, 1), (0, 2)] {
        let params = [S::Integer(lower), S::Integer(upper)];
        assert_eq!(
            sql.query(query, &params).unwrap().rows,
            sqlite.query(query, &params).unwrap().rows
        );
    }
    // Project a different set, then reorder equal-typed projected columns.
    for query in [
        "SELECT m,n FROM ranges.Item WHERE n>=0 AND n<=2 ORDER BY n,m",
        "SELECT n,m FROM ranges.Item WHERE n>=0 AND n<=2 ORDER BY n,m",
        "SELECT count(*) FROM ranges.Item WHERE n>0 AND n<2",
        "SELECT id,n FROM ranges.Item WHERE n>=0 AND m>2400 ORDER BY n,id",
        "SELECT id,n FROM ranges.Item WHERE n>=0 AND m>1200 ORDER BY n,id",
        "SELECT id,n FROM ranges.Item WHERE n>=0 AND m>2400 ORDER BY n,id",
    ] {
        for _ in 0..2 {
            assert_eq!(
                sql.query(query, &[]).unwrap().rows,
                sqlite.query(query, &[]).unwrap().rows,
                "{query}"
            );
        }
    }
}

#[test]
fn cached_ranges_preserve_residual_filters_limits_and_window_results() {
    let db = fixture();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    for query in [
        "SELECT m FROM ranges.Item WHERE n>=0 AND m%701=3 ORDER BY m LIMIT 3",
        "SELECT m FROM ranges.Item WHERE n>=0 AND (m<3 OR m>2496) ORDER BY m LIMIT 4",
        "SELECT m FROM ranges.Item WHERE n>=0 ORDER BY n,m DESC LIMIT 7",
        "SELECT n,m,row_number() OVER(PARTITION BY n ORDER BY m DESC) AS rn FROM ranges.Item WHERE n>=1 ORDER BY n,m",
    ] {
        for _ in 0..2 {
            assert_eq!(
                sql.query(query, &[]).unwrap().rows,
                sqlite.query(query, &[]).unwrap().rows,
                "{query}"
            );
        }
    }
    let limited = "SELECT m FROM ranges.Item WHERE n>=0 LIMIT 7";
    for _ in 0..2 {
        let plan = explain(&sql, limited, &[]);
        assert!(plan.contains("limit=Some(7)"), "{plan}");
        assert!(plan.contains("cache_hits=0"), "{plan}");
        assert_eq!(sql.query(limited, &[]).unwrap().rows.len(), 7);
    }
}

#[test]
fn range_pages_remain_scoped_to_principal_revision_and_live_grants() {
    let db = fixture();
    let admin = db.admin().snapshot().sql().unwrap();
    let restricted = db.session("zero").snapshot().sql().unwrap();
    let query = "SELECT count(*) FROM ranges.Item WHERE n>=0";
    for _ in 0..2 {
        assert_eq!(
            admin.query(query, &[]).unwrap().rows,
            vec![vec![S::Integer(2500)]]
        );
        assert_eq!(
            restricted.query(query, &[]).unwrap().rows,
            vec![vec![S::Integer(1200)]]
        );
    }
    let mut tx = db.admin().transaction();
    tx.insert("ranges.Item", vec![Value::Int(0), Value::Int(3000)])
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        admin.query(query, &[]).unwrap().rows,
        vec![vec![S::Integer(2500)]]
    );
    assert_eq!(
        db.admin()
            .snapshot()
            .sql()
            .unwrap()
            .query(query, &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(2501)]]
    );
    let mut tx = db.admin().transaction();
    tx.revoke("zero", "ranges.Item").unwrap();
    tx.commit().unwrap();
    assert!(
        restricted
            .query(query, &[])
            .unwrap_err()
            .to_string()
            .contains("access denied")
    );
}
