//! Legacy SQLite dialect/index adapter regressions; default read SQL uses DataFusion.
use rusqlite::{Connection, types::Value as SqlValue};
use substrate_liquid::*;
fn fixture() -> (Database, Connection) {
    let db = Database::memory();
    let sqlite = Connection::open_in_memory().unwrap();
    sqlite.execute_batch("CREATE TABLE Item(id TEXT PRIMARY KEY,n INTEGER NOT NULL, visible INTEGER NOT NULL);CREATE INDEX by_n ON Item(n);").unwrap();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "bench",
            1,
            vec![],
            vec![Relation::new(
                "bench.Item",
                vec![
                    Role::new("n", DataType::Int),
                    Role::new("visible", DataType::Bool),
                ],
            )],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    for i in 0..405 {
        let n = match i {
            403 => i64::MIN,
            404 => i64::MAX,
            _ => (i % 101) as i64 - 50,
        };
        let visible = i % 3 == 0;
        let id = Id::from_u128(i + 1);
        tx.insert_with_id("bench.Item", id, vec![n.into(), Value::Bool(visible)])
            .unwrap();
        sqlite
            .execute(
                "INSERT INTO Item VALUES(?1,?2,?3)",
                rusqlite::params![id.to_string(), n, visible],
            )
            .unwrap();
    }
    tx.grant(
        "reader",
        "bench.Item",
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
    (db, sqlite)
}
fn sqlite_query(db: &Connection, sql: &str, params: &[SqlValue]) -> Vec<Vec<SqlValue>> {
    let mut statement = db.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            (0..columns).map(|c| row.get(c)).collect()
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}
fn plan(sql: &SqliteSnapshot, query: &str) -> String {
    format!(
        "{:?}",
        sql.query(&format!("EXPLAIN QUERY PLAN {query}"), &[])
            .unwrap()
            .rows
    )
}
#[test]
fn identity_selection_ignores_predicate_order_and_broad_buckets() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sqlite_compat().unwrap();
    for clause in [
        "i.visible=o.visible AND i.id=o.id",
        "i.id=o.id AND i.visible=o.visible",
    ] {
        let query = format!(
            "SELECT count(*) FROM bench.Item o WHERE EXISTS(SELECT 1 FROM bench.Item i WHERE {clause})"
        );
        assert!(plan(&sql, &query).contains("identity_lookup"));
        assert_eq!(
            sql.query(&query, &[]).unwrap().rows,
            vec![vec![SqlValue::Integer(405)]]
        );
    }
}
#[test]
fn numeric_ranges_match_sqlite_with_coercions_boundaries_duplicates_and_limits() {
    let (db, sqlite) = fixture();
    let sql = db.admin().snapshot().sqlite_compat().unwrap();
    let bounds = [
        (SqlValue::Integer(-20), SqlValue::Integer(20)),
        (SqlValue::Integer(i64::MIN), SqlValue::Integer(i64::MAX)),
        (SqlValue::Real(-20.25), SqlValue::Real(20.75)),
        (SqlValue::Text("-20".into()), SqlValue::Text("20.5".into())),
        (SqlValue::Text("not a number".into()), SqlValue::Integer(20)),
        (SqlValue::Null, SqlValue::Integer(20)),
        (SqlValue::Blob(vec![1]), SqlValue::Integer(20)),
        (SqlValue::Integer(10), SqlValue::Integer(-10)),
        (SqlValue::Integer(1), SqlValue::Integer(1)),
    ];
    for (low, high) in bounds {
        for (a, b) in [(">=", "<="), (">", "<"), (">=", "<"), (">", "<=")] {
            for order in ["ASC", "DESC"] {
                for limit in [1, 63, 64, 65, 130, 1000] {
                    let query = format!(
                        "SELECT n FROM bench.Item WHERE n{a}?1 AND n{b}?2 ORDER BY n {order} LIMIT {limit}"
                    );
                    let params = [low.clone(), high.clone()];
                    let expected =
                        sqlite_query(&sqlite, &query.replace("bench.Item", "Item"), &params);
                    assert_eq!(
                        sql.query(&query, &params).unwrap().rows,
                        expected,
                        "{query} {params:?}"
                    );
                }
            }
        }
    }
}
#[test]
fn range_ordering_is_reported_only_when_supported() {
    let (db, _) = fixture();
    let sql = db.admin().snapshot().sqlite_compat().unwrap();
    let indexed = plan(
        &sql,
        "SELECT n FROM bench.Item WHERE n>=0 AND n<20 ORDER BY n DESC LIMIT 100",
    );
    assert!(indexed.contains("role_index_range"), "{indexed}");
    assert!(!indexed.contains("TEMP B-TREE"), "{indexed}");
    let tie_order = plan(
        &sql,
        "SELECT id,n FROM bench.Item WHERE n>=0 ORDER BY n DESC,id ASC",
    );
    assert!(tie_order.contains("TEMP B-TREE"), "{tie_order}");
    let id_order = plan(&sql, "SELECT id FROM bench.Item ORDER BY id COLLATE NOCASE");
    assert!(!id_order.contains("role_index_range"));
}
#[test]
fn ordered_limit_filters_authorization_before_counting_rows_and_honors_revocation() {
    let (db, sqlite) = fixture();
    let snapshot = db.session("reader").snapshot();
    let sql = snapshot.sqlite_compat().unwrap();
    for order in ["ASC", "DESC"] {
        let query = format!("SELECT n FROM bench.Item WHERE n>=-30 ORDER BY n {order} LIMIT 100");
        let expected = sqlite_query(
            &sqlite,
            &format!("SELECT n FROM Item WHERE visible=1 AND n>=-30 ORDER BY n {order} LIMIT 100"),
            &[],
        );
        assert_eq!(sql.query(&query, &[]).unwrap().rows, expected);
    }
    let mut prepared = sql
        .prepare("SELECT n FROM bench.Item WHERE n>0 ORDER BY n LIMIT 1")
        .unwrap();
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "bench.Item").unwrap();
    revoke.commit().unwrap();
    assert!(prepared.query(&[]).is_err());
    assert!(snapshot.page("bench.Item", None, None, 10).is_err());
}
#[test]
fn mutable_range_scans_keep_phantom_dependencies() {
    let (db, _) = fixture();
    let mut sql = db.admin().sql_transaction().unwrap();
    assert_eq!(
        sql.query("SELECT n FROM bench.Item WHERE n>100 ORDER BY n", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
    let mut other = db.admin().transaction();
    other
        .insert("bench.Item", vec![200.into(), Value::Bool(true)])
        .unwrap();
    other.commit().unwrap();
    assert!(matches!(sql.commit(), Err(Error::Conflict)));
}
#[test]
fn explicit_sql_selection_does_not_register_or_expose_other_families() {
    let (db, _) = fixture();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "other",
            1,
            vec![],
            vec![Relation::new("other.Secret", vec![])],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("reader").snapshot();
    let sql = snapshot.sql_selected(&["bench.Item"]).unwrap();
    assert!(sql.query("SELECT count(*) FROM bench.Item", &[]).is_ok());
    assert!(sql.query("SELECT * FROM other.Secret", &[]).is_err());
    assert!(snapshot.sql_selected(&["other.Secret"]).is_err());
    assert_eq!(
        db.admin()
            .snapshot()
            .sql_selected(&[])
            .unwrap()
            .query("SELECT 1", &[])
            .unwrap()
            .rows,
        vec![vec![SqlValue::Integer(1)]]
    );
}
