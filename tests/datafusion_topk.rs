use rusqlite::types::Value as S;
use substrate_liquid::*;

fn fixture() -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "topk",
            1,
            vec![],
            vec![Relation::new(
                "topk.Item",
                vec![
                    Role::new("n", DataType::Int),
                    Role::new("m", DataType::Int),
                    Role::new("visible", DataType::Bool),
                ],
            )],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    for i in 0..5000 {
        tx.insert_with_id(
            "topk.Item",
            Id::from_u128(i + 1),
            vec![
                Value::Int(((i * 37) % 251) as i64 - 125),
                Value::Int(i as i64),
                Value::Bool(i % 2 == 0),
            ],
        )
        .unwrap();
    }
    tx.grant(
        "reader",
        "topk.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "visible".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db
}
fn analyze(sql: &SqlSnapshot, query: &str) -> String {
    sql.query(&format!("EXPLAIN ANALYZE {query}"), &[])
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .filter_map(|v| if let S::Text(s) = v { Some(s) } else { None })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn ordered_range_topk_stops_at_fetch_and_offset_without_sort() {
    let db = fixture();
    for snapshot in [db.admin().snapshot(), db.session("reader").snapshot()] {
        let sql = snapshot.sql().unwrap();
        let sqlite = snapshot.sqlite_compat().unwrap();
        for (query, expected) in [
            (
                "SELECT id,n FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY n,id LIMIT 100",
                100,
            ),
            (
                "SELECT id,n FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY n,id LIMIT 7 OFFSET 103",
                110,
            ),
            (
                "SELECT n FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY n,id LIMIT 100",
                100,
            ),
            (
                "SELECT id,n FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY n DESC,id DESC LIMIT 100",
                100,
            ),
            (
                "SELECT id,n FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY n DESC,id DESC LIMIT 7 OFFSET 103",
                110,
            ),
            (
                "SELECT n FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY n DESC NULLS FIRST LIMIT 100",
                100,
            ),
            (
                "SELECT n AS value,id AS occurrence FROM topk.Item WHERE n>=-90 AND n<90 ORDER BY value ASC NULLS LAST,occurrence ASC NULLS LAST LIMIT 100",
                100,
            ),
        ] {
            assert_eq!(
                sql.query(query, &[]).unwrap().rows,
                sqlite.query(query, &[]).unwrap().rows
            );
            let plan = analyze(&sql, query);
            assert!(!plan.contains("SortExec"), "{plan}");
            assert!(plan.contains(&format!("source_rows={expected}")), "{plan}");
        }
    }
}

#[test]
fn ordering_claims_respect_projection_direction_and_filter_barriers() {
    let db = fixture();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    for query in [
        "SELECT id FROM topk.Item WHERE n>=-90 ORDER BY id LIMIT 100",
        "SELECT n,id FROM topk.Item WHERE n>=-90 ORDER BY n DESC,id DESC LIMIT 100",
        "SELECT n,id FROM topk.Item WHERE n>=-90 ORDER BY n DESC,id DESC LIMIT 2001",
        "SELECT n,id FROM topk.Item WHERE n>=-90 ORDER BY n DESC,id DESC",
        "SELECT n,id FROM topk.Item WHERE n>=-90 ORDER BY n DESC,id ASC LIMIT 100",
        "SELECT n,id FROM topk.Item WHERE n>=-90 ORDER BY n ASC,id DESC LIMIT 100",
        "SELECT n,id FROM topk.Item WHERE n>=-90 AND m%701=3 ORDER BY n,id LIMIT 3",
        "SELECT n,id FROM topk.Item WHERE n>=-90 AND m>4500 ORDER BY n,id LIMIT 17 OFFSET 9",
        "SELECT n,id FROM topk.Item WHERE n>=-90 AND (m<3 OR m>4996) ORDER BY n,id LIMIT 4",
        "SELECT n,id FROM topk.Item WHERE n>90 AND n<90 ORDER BY n,id LIMIT 7",
        "SELECT n,id FROM topk.Item WHERE n>=-90 ORDER BY n,id LIMIT 0",
        "SELECT id,n FROM (SELECT id,n FROM topk.Item WHERE n>=-90 ORDER BY n,id LIMIT 17) q ORDER BY n DESC,id DESC LIMIT 3",
        "SELECT m,n,row_number() OVER(PARTITION BY n ORDER BY m DESC) AS rn FROM topk.Item WHERE n>=120 ORDER BY n,m LIMIT 100",
    ] {
        for _ in 0..2 {
            assert_eq!(
                sql.query(query, &[]).unwrap().rows,
                sqlite.query(query, &[]).unwrap().rows,
                "{query}"
            );
        }
    }
    // IDs in a range traverse value buckets, so removing n from the projection
    // must not falsely claim global ID order.
    let plan = analyze(
        &sql,
        "SELECT id FROM topk.Item WHERE n>=-90 ORDER BY id LIMIT 100",
    );
    assert!(plan.contains("SortExec"), "{plan}");
    let plan = analyze(
        &sql,
        "SELECT n,id FROM topk.Item WHERE n>=-90 AND m%701=3 ORDER BY n,id LIMIT 3",
    );
    assert!(!plan.contains("limit=Some(3)"), "{plan}");
}

#[test]
fn ordered_fetch_handles_new_bindings_and_live_revocation() {
    let db = fixture();
    let snapshot = db.session("reader").snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let query = "SELECT id,n FROM topk.Item WHERE n>=?1 AND n<?2 ORDER BY n,id LIMIT 20";
    let mut prepared = sql.prepare(query).unwrap();
    for bounds in [[-100, 0], [0, 100], [-10, 10], [0, 100], [3, 3]] {
        let params = bounds.map(S::Integer);
        assert_eq!(
            prepared.query(&params).unwrap().rows,
            sqlite.query(query, &params).unwrap().rows
        );
    }
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "topk.Item").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        prepared.query(&[S::Integer(-100), S::Integer(100)]),
        Err(Error::Denied)
    ));
}
