mod common;
use common::*;
use rusqlite::types::Value as S;
use substrate_liquid::*;
#[test]
fn sqlite_compat_join_and_index_pushdown_match_native() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, p, 45);
    deal(&mut tx, p, 125050);
    tx.commit().unwrap();
    let sql = db.admin().snapshot().sqlite_compat().unwrap();
    assert_eq!(
        sql.query(demo::CROSS_APP_SQL, &[]).unwrap().rows,
        vec![vec![
            S::Text("Alice".into()),
            S::Integer(45),
            S::Integer(125050)
        ]]
    );
    let plan = sql
        .query(
            "EXPLAIN QUERY PLAN SELECT * FROM fitness.Performed WHERE athlete=?1",
            &[S::Text(p.to_string())],
        )
        .unwrap();
    assert!(format!("{plan:?}").contains("role_index_eq"));
    assert_eq!(
        sql.query(
            "SELECT durationMinutes FROM fitness.Workout WHERE durationMinutes=?1",
            &[S::Real(45.0)]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(45)]]
    );
    assert_eq!(
        sql.query(
            "SELECT name FROM core.Person WHERE name COLLATE NOCASE='alice'",
            &[]
        )
        .unwrap()
        .rows
        .len(),
        1
    );
    assert!(
        sql.query("SELECT name FROM core.Person WHERE name='alice'", &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        sql.query(
            "SELECT name FROM core.Person WHERE id=?1",
            &[S::Text(p.to_string())]
        )
        .unwrap()
        .rows
        .len(),
        1
    );
}
#[test]
fn default_deny_app_isolation_and_read_only_are_enforced() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, p, 45);
    deal(&mut tx, p, 10000);
    tx.commit().unwrap();
    assert!(
        db.session("unknown")
            .snapshot()
            .scan("core.Person")
            .is_err()
    );
    assert!(db.session("fitness").snapshot().scan("crm.Deal").is_err());
    let sql = db.session("fitness").snapshot().sql().unwrap();
    assert!(sql.query(demo::CROSS_APP_SQL, &[]).is_err());
    assert!(sql.query("SELECT * FROM crm.Deal", &[]).is_err());
    for query in [
        "DELETE FROM fitness.Workout",
        "PRAGMA query_only=OFF",
        "ATTACH DATABASE ':memory:' AS theft",
        "CREATE TABLE leak(x)",
        "SELECT 1; SELECT 2",
        "SELECT load_extension('anything')",
    ] {
        assert!(sql.query(query, &[]).is_err(), "{query}");
    }
    let mut tx = db.admin().transaction();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("reader").transaction();
    assert!(tx.update("core.Person", p, vec!["hijack".into()]).is_err());
    let mut tx = db.session("reader").transaction();
    assert!(tx.grant("reader", "crm.Deal", Grant::read_write()).is_err());
    let mut tx = db.session("fitness").transaction();
    assert!(tx.install(Module::new("crm", 2, vec![], vec![])).is_err());
}
#[test]
fn row_policies_apply_before_joins_aggregates_and_both_sides_of_update() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, p, 45);
    let (hidden, _) = workout(&mut tx, p, 900);
    tx.grant("limited", "core.Person", Grant::read_only())
        .unwrap();
    tx.grant("limited", "fitness.Performed", Grant::read_only())
        .unwrap();
    tx.grant(
        "limited",
        "fitness.Workout",
        Grant {
            read: true,
            write: true,
            reference: true,
            filter: RowFilter::Equals {
                role: "durationMinutes".into(),
                value: 45.into(),
            },
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("limited").snapshot();
    let visible = snapshot.scan("fitness.Workout").unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(snapshot.scan("fitness.Performed").unwrap().len(), 1); // hidden participant closes visibility
    let mut sql = snapshot.sql().unwrap();
    assert_eq!(
        sql.query(
            "SELECT count(*),sum(durationMinutes) FROM fitness.Workout",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(1), S::Integer(45)]]
    );
    assert_eq!(
        sql.query(
            "SELECT count(*) FROM fitness.Performed f JOIN fitness.Workout w ON f.workout=w.id",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![S::Integer(1)]]
    );
    sql.view(
        "summary",
        "SELECT sum(durationMinutes) AS minutes FROM fitness.Workout",
    )
    .unwrap();
    assert_eq!(
        sql.query("SELECT minutes FROM summary", &[]).unwrap().rows,
        vec![vec![S::Integer(45)]]
    );
    assert!(sql.query("UPDATE summary SET minutes=100", &[]).is_err());
    assert!(
        sql.view("bad", "SELECT 1; DROP TABLE fitness.Workout")
            .is_err()
    );
    let mut tx = db.session("limited").transaction();
    assert!(
        tx.update("fitness.Workout", visible[0].id, vec![900.into()])
            .is_err()
    );
    let mut tx = db.session("limited").transaction();
    assert!(
        tx.update("fitness.Workout", hidden, vec![45.into()])
            .is_err()
    );
}
#[test]
fn reference_only_can_attach_but_cannot_read_hidden_participants() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    tx.grant("blind", "core.Person", Grant::reference_only())
        .unwrap();
    tx.grant("blind", "fitness.Workout", Grant::read_write())
        .unwrap();
    tx.grant("blind", "fitness.Performed", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("blind").transaction();
    workout(&mut tx, p, 10);
    tx.commit().unwrap();
    let s = db.session("blind").snapshot();
    assert!(s.scan("core.Person").is_err());
    assert!(s.scan("fitness.Performed").unwrap().is_empty());
    assert_eq!(s.scan("fitness.Workout").unwrap().len(), 1);
}
#[test]
fn revocation_invalidates_inflight_writes_and_retained_snapshots() {
    let (db, p) = fixture();
    let old = db.session("fitness").snapshot();
    let mut writing = db.session("fitness").transaction();
    workout(&mut writing, p, 10);
    let mut revoking = db.admin().transaction();
    revoking.revoke("fitness", "core.Person").unwrap();
    revoking.commit().unwrap();
    assert!(matches!(writing.commit(), Err(Error::Conflict)));
    assert!(matches!(old.scan("core.Person"), Err(Error::Denied)));
    assert!(
        db.session("fitness")
            .snapshot()
            .scan("core.Person")
            .is_err()
    );
}
#[test]
fn visibility_dependency_changes_invalidate_transaction_reads() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, p, 10);
    tx.commit().unwrap();
    let mut a = db.session("fitness").transaction();
    assert_eq!(a.scan("fitness.Performed").unwrap().len(), 1);
    let mut b = db.admin().transaction();
    b.update("core.Person", p, vec!["changed".into()]).unwrap();
    b.commit().unwrap();
    assert!(matches!(a.commit(), Err(Error::Conflict)));
}
#[test]
fn sql_nulls_bags_scalars_and_snapshot_semantics() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    workout(&mut tx, p, 10);
    workout(&mut tx, p, 10);
    tx.commit().unwrap();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    assert_eq!(
        sql.query(
            "SELECT durationMinutes FROM fitness.Workout ORDER BY durationMinutes LIMIT 2",
            &[]
        )
        .unwrap()
        .rows
        .len(),
        2
    );
    assert_eq!(
        sql.query("SELECT DISTINCT durationMinutes FROM fitness.Workout", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        sql.query(
            "SELECT * FROM fitness.Workout WHERE durationMinutes=NULL",
            &[]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert_eq!(sql.query("SELECT d.amount FROM core.Person p LEFT JOIN crm.CustomerDeal c ON c.customer=p.id LEFT JOIN crm.Deal d ON d.id=c.deal",&[]).unwrap().rows,vec![vec![S::Null]]);
    let mut tx = db.admin().transaction();
    workout(&mut tx, p, 90);
    tx.commit().unwrap();
    assert_eq!(
        sql.query("SELECT count(*) FROM fitness.Workout", &[])
            .unwrap()
            .rows,
        vec![vec![S::Integer(2)]]
    );
}
