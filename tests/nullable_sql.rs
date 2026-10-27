use rusqlite::types::Value as S;
use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};

fn module() -> Module {
    Module::new(
        "nullable",
        1,
        vec![],
        vec![
            Relation::new(
                "nullable.Item",
                vec![
                    Role::new("key", DataType::Int.nullable()),
                    Role::new("score", DataType::Float.nullable()),
                    Role::new("note", DataType::Text.nullable()),
                    Role::new("parent", DataType::Ref("nullable.Item".into()).nullable()),
                ],
            )
            .unique(&["key"]),
        ],
    )
}
fn rows() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Null,
            Value::try_from(1.5).unwrap(),
            Value::Null,
            Value::Null,
        ],
        vec![
            Value::Null,
            Value::Null,
            "two".into(),
            Id::from_u128(1).into(),
        ],
        vec![
            1i64.into(),
            Value::try_from(-2.5).unwrap(),
            "three".into(),
            Value::Null,
        ],
        vec![
            2i64.into(),
            Value::try_from(3.0).unwrap(),
            Value::Null,
            Id::from_u128(1).into(),
        ],
    ]
}
fn check(query: impl Fn(&str) -> Result<SqlTable>) {
    assert_eq!(
        query("SELECT COUNT(*),COUNT(score),SUM(score),MIN(score),MAX(score) FROM nullable.Item")
            .unwrap()
            .rows,
        vec![vec![
            S::Integer(4),
            S::Integer(3),
            S::Real(2.0),
            S::Real(-2.5),
            S::Real(3.0)
        ]]
    );
    for (sql, count) in [
        ("SELECT COUNT(*) FROM nullable.Item WHERE key IS NULL", 2),
        ("SELECT COUNT(*) FROM nullable.Item WHERE key>0", 2),
        ("SELECT COUNT(*) FROM nullable.Item WHERE score>0", 2),
        ("SELECT COUNT(*) FROM nullable.Item WHERE score=NULL", 0),
        (
            "SELECT COUNT(*) FROM nullable.Item a JOIN nullable.Item b ON a.key=b.key",
            2,
        ),
        ("SELECT COUNT(*) FROM nullable.Item WHERE parent IS NULL", 2),
    ] {
        assert_eq!(
            query(sql).unwrap().rows,
            vec![vec![S::Integer(count)]],
            "{sql}"
        );
    }
    assert_eq!(query("SELECT a.key FROM nullable.Item a WHERE EXISTS (SELECT 1 FROM nullable.Item b WHERE b.key=a.key) ORDER BY a.id LIMIT 100").unwrap().rows,
        vec![vec![S::Integer(1)], vec![S::Integer(2)]]);
    assert_eq!(
        query("SELECT score FROM nullable.Item ORDER BY score NULLS FIRST")
            .unwrap()
            .rows,
        vec![
            vec![S::Null],
            vec![S::Real(-2.5)],
            vec![S::Real(1.5)],
            vec![S::Real(3.0)]
        ]
    );
    assert_eq!(
        query("SELECT note,COUNT(*) FROM nullable.Item GROUP BY note ORDER BY note NULLS FIRST")
            .unwrap()
            .rows,
        vec![
            vec![S::Null, S::Integer(2)],
            vec![S::Text("three".into()), S::Integer(1)],
            vec![S::Text("two".into()), S::Integer(1)]
        ]
    );
}
#[test]
fn resident_nullable_float_sql_matches_sqlite_and_preserves_live_authority() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(module()).unwrap();
    for (n, row) in rows().into_iter().enumerate() {
        tx.insert_with_id("nullable.Item", Id::from_u128(n as u128 + 1), row)
            .unwrap();
    }
    tx.grant("reader", "nullable.Item", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("reader").snapshot();
    let sql = snapshot.sql().unwrap();
    check(|q| sql.query(q, &[]));
    let sqlite = snapshot.sqlite_compat().unwrap();
    check(|q| sqlite.query(q, &[]));
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "nullable.Item").unwrap();
    tx.commit().unwrap();
    assert!(sql.query("SELECT * FROM nullable.Item", &[]).is_err());
}
#[test]
fn paged_nullable_float_roundtrips_indexes_sql_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = PagedDatabase::open(&path, PagedOptions::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(module(), "owner", Visibility::Private)
            .unwrap();
        for (n, row) in rows().into_iter().enumerate() {
            tx.insert_with_id("nullable.Item", Id::from_u128(n as u128 + 1), row)
                .unwrap();
        }
        tx.grant("reader", "nullable.Item", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        db.admin().snapshot().unwrap().verify().unwrap();
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        let sql = snapshot.sql().unwrap();
        check(|q| sql.query(q, &[]));
        assert_eq!(
            snapshot
                .lookup_page("nullable.Item", "key", &Value::Null, None, 10)
                .unwrap()
                .rows
                .len(),
            2
        );
        let mut tx = db.admin().transaction().unwrap();
        assert!(
            tx.insert(
                "nullable.Item",
                vec![1i64.into(), Value::Null, Value::Null, Value::Null]
            )
            .is_err()
        );
    }
    let db = PagedDatabase::open(&path, PagedOptions::default()).unwrap();
    let sql = db
        .session("reader")
        .unwrap()
        .snapshot()
        .unwrap()
        .sql()
        .unwrap();
    check(|q| sql.query(q, &[]));
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "nullable.Item").unwrap();
    tx.commit().unwrap();
    assert!(sql.query("SELECT * FROM nullable.Item", &[]).is_err());
}
