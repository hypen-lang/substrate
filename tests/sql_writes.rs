use rusqlite::types::Value as S;
use substrate_liquid::*;
fn fixture() -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.install(demo::fitness()).unwrap();
    tx.commit().unwrap();
    db
}
#[test]
fn sql_dml_is_transactional_and_reads_its_writes() {
    let db = fixture();
    let id = Id::new();
    let mut tx = db.admin().sql_transaction().unwrap();
    tx.execute(
        "INSERT INTO core.Person (name,id) VALUES (?1,?2)",
        &[S::Text("Ada".into()), S::Text(id.to_string())],
    )
    .unwrap();
    assert_eq!(
        tx.query("SELECT name FROM core.Person", &[]).unwrap().rows,
        vec![vec![S::Text("Ada".into())]]
    );
    assert!(
        db.admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .is_empty()
    );
    tx.execute(
        "UPDATE core.Person SET name='Grace' WHERE id=?1",
        &[S::Text(id.to_string())],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .get("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        Value::from("Grace")
    );
    let mut tx = db.admin().sql_transaction().unwrap();
    tx.execute(
        "DELETE FROM core.Person WHERE id=?1",
        &[S::Text(id.to_string())],
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(
        db.admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .is_empty()
    );
}
#[test]
fn sql_error_or_drop_rolls_back_all_previous_statements() {
    let db = fixture();
    for commit in [true, false] {
        let mut tx = db.admin().sql_transaction().unwrap();
        tx.execute(
            "INSERT INTO core.Person(id,name) VALUES (?1,'x')",
            &[S::Text(Id::new().to_string())],
        )
        .unwrap();
        if commit {
            assert!(
                tx.execute(
                    "INSERT INTO fitness.Performed(id,athlete,workout) VALUES (?1,?2,?3)",
                    &[
                        S::Text(Id::new().to_string()),
                        S::Text(Id::new().to_string()),
                        S::Text(Id::new().to_string())
                    ]
                )
                .is_err()
            );
            assert!(matches!(tx.commit(), Err(Error::Aborted)));
        }
    }
    assert!(
        db.admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .is_empty()
    );
}
#[test]
fn sql_reads_track_conflicts_and_identity_changes_are_rejected() {
    let db = fixture();
    let mut a = db.admin().sql_transaction().unwrap();
    a.query("SELECT * FROM core.Person", &[]).unwrap();
    let mut other = db.admin().transaction();
    let id = other.insert("core.Person", vec!["Ada".into()]).unwrap();
    other.commit().unwrap();
    assert!(matches!(a.commit(), Err(Error::Conflict)));
    let mut tx = db.admin().sql_transaction().unwrap();
    assert!(
        tx.execute(
            "UPDATE core.Person SET id=?1 WHERE id=?2",
            &[S::Text(Id::new().to_string()), S::Text(id.to_string())]
        )
        .is_err()
    );
    assert!(tx.commit().is_err());
}
#[test]
fn sql_mutations_cannot_escape_authorization_or_run_ddl() {
    let db = fixture();
    let mut tx = db.admin().transaction();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("reader").sql_transaction().unwrap();
    assert!(
        tx.execute(
            "INSERT INTO core.Person(id,name) VALUES (?1,'x')",
            &[S::Text(Id::new().to_string())]
        )
        .is_err()
    );
    assert!(tx.commit().is_err());
    for sql in [
        "PRAGMA query_only=OFF",
        "ATTACH DATABASE ':memory:' AS other",
        "CREATE TABLE t(x)",
        "BEGIN",
        "DELETE FROM core.Person; SELECT 1",
    ] {
        let mut tx = db.admin().sql_transaction().unwrap();
        assert!(tx.execute(sql, &[]).is_err(), "{sql}");
        assert!(tx.commit().is_err());
    }
}

#[test]
fn write_only_sql_insert_and_row_filter_updates_use_kernel_permissions() {
    let db = fixture();
    let mut tx = db.admin().transaction();
    tx.grant(
        "writer",
        "core.Person",
        Grant {
            read: false,
            ..Grant::read_write()
        },
    )
    .unwrap();
    tx.grant(
        "scoped",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "allowed".into(),
            },
            ..Grant::read_write()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let id = Id::new();
    let mut tx = db.session("writer").sql_transaction().unwrap();
    tx.execute(
        "INSERT INTO core.Person(id,name) VALUES (?1,'allowed')",
        &[S::Text(id.to_string())],
    )
    .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("writer").sql_transaction().unwrap();
    assert!(tx.query("SELECT * FROM core.Person", &[]).is_err());
    assert!(tx.commit().is_err());
    let mut tx = db.session("scoped").sql_transaction().unwrap();
    assert!(
        tx.execute(
            "UPDATE core.Person SET name='forbidden' WHERE id=?1",
            &[S::Text(id.to_string())]
        )
        .is_err()
    );
    assert!(tx.commit().is_err());
    assert_eq!(
        db.admin()
            .snapshot()
            .get("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        Value::from("allowed")
    );
}
#[test]
fn sql_delete_respects_incoming_references_and_releases_them_atomically() {
    let db = fixture();
    let mut tx = db.admin().transaction();
    let p = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    let w = tx.insert("fitness.Workout", vec![1.into()]).unwrap();
    let link = tx
        .insert("fitness.Performed", vec![p.into(), w.into()])
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().sql_transaction().unwrap();
    assert!(
        tx.execute(
            "DELETE FROM core.Person WHERE id=?1",
            &[S::Text(p.to_string())]
        )
        .is_err()
    );
    assert!(tx.commit().is_err());
    let mut tx = db.admin().sql_transaction().unwrap();
    tx.execute(
        "DELETE FROM fitness.Performed WHERE id=?1",
        &[S::Text(link.to_string())],
    )
    .unwrap();
    tx.execute(
        "DELETE FROM core.Person WHERE id=?1",
        &[S::Text(p.to_string())],
    )
    .unwrap();
    tx.commit().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
    assert!(
        db.admin()
            .snapshot()
            .get("core.Person", p)
            .unwrap()
            .is_none()
    );
}
#[test]
fn sqlite_value_allocation_budget_resets_after_errors() {
    let db = fixture();
    let sql = db.admin().snapshot().sqlite_compat().unwrap();
    let limits = QueryLimits {
        max_rows: 10,
        max_bytes: 128,
        vm_steps: 10000,
        timeout_ms: 5_000,
    };
    assert!(
        sql.query_with_limits("SELECT zeroblob(1000000)", &[], limits)
            .is_err()
    );
    assert_eq!(
        sql.query("SELECT length(zeroblob(256))", &[]).unwrap().rows,
        vec![vec![S::Integer(256)]]
    );
}
