use rusqlite::types::Value as S;
use substrate_liquid::*;

fn fixture() -> (Database, Id) {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    let person = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.commit().unwrap();
    (db, person)
}

#[test]
fn sql_transaction_reads_staged_writes_through_datafusion() {
    let (db, person) = fixture();
    let mut tx = db.admin().sql_transaction().unwrap();
    tx.execute(
        "UPDATE core.Person SET name=?1 WHERE id=?2",
        &[S::Text("Grace".into()), S::Text(person.to_string())],
    )
    .unwrap();
    assert_eq!(
        tx.query("SELECT name FROM core.Person", &[]).unwrap().rows,
        vec![vec![S::Text("Grace".into())]]
    );
    let plan = tx
        .query("EXPLAIN SELECT name FROM core.Person", &[])
        .unwrap();
    assert!(format!("{plan:?}").contains("LiquidScanExec"));
    tx.commit().unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .get("core.Person", person)
            .unwrap()
            .unwrap()
            .values,
        vec!["Grace".into()].into()
    );
}

#[test]
fn sql_transaction_batch_reads_track_phantoms_and_fail_closed() {
    let (db, _) = fixture();
    let mut reader = db.admin().sql_transaction().unwrap();
    assert_eq!(
        reader
            .query("SELECT count(*) FROM core.Person", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(1)]]
    );
    let mut other = db.admin().transaction();
    other.insert("core.Person", vec!["Grace".into()]).unwrap();
    other.commit().unwrap();
    assert!(matches!(reader.commit(), Err(Error::Conflict)));
    let mut failed = db.admin().sql_transaction().unwrap();
    failed
        .execute("UPDATE core.Person SET name='staged'", &[])
        .unwrap();
    assert!(failed.query("SELECT * FROM missing.Table", &[]).is_err());
    assert!(matches!(failed.commit(), Err(Error::Aborted)));
    assert_eq!(
        db.admin()
            .snapshot()
            .lookup("core.Person", "name", &"staged".into())
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn default_sql_and_explicit_compatibility_do_not_change_native_queries() {
    let (db, person) = fixture();
    let snapshot = db.admin().snapshot();
    let native = snapshot
        .table("core.Person", "p")
        .unwrap()
        .project(&["p.name"])
        .unwrap();
    let sql = snapshot.sql().unwrap();
    let compat = snapshot.sqlite_compat().unwrap();
    assert_eq!(
        sql.query("SELECT name FROM core.Person", &[]).unwrap(),
        compat.query("SELECT name FROM core.Person", &[]).unwrap()
    );
    assert_eq!(native.rows(), &[vec![Value::Text("Ada".into())]]);
    assert_eq!(
        snapshot
            .lookup("core.Person", "name", &"Ada".into())
            .unwrap()[0]
            .id,
        person
    );
    assert!(
        sql.query(
            "SELECT name FROM core.Person WHERE name=?1",
            &[S::Text("Ada'; DELETE FROM core.Person; --".into())]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert_eq!(snapshot.scan("core.Person").unwrap().len(), 1);
}
