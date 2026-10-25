mod common;
use common::*;
use substrate_liquid::*;
#[test]
fn reopen_restores_catalog_ids_rows_grants_and_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("liquid.redb");
    let (person, stats, schema) = {
        let db = Database::open(&path).unwrap();
        let p = seed(&db);
        let mut tx = db.session("fitness").transaction();
        let (w, _) = workout(&mut tx, p, 45);
        tx.commit().unwrap();
        let mut tx = db.session("fitness").transaction();
        tx.update("fitness.Workout", w, vec![60.into()]).unwrap();
        tx.commit().unwrap();
        let mut tx = db.admin().transaction();
        let doomed = tx.insert("crm.Deal", vec![Value::Decimal(1)]).unwrap();
        tx.commit().unwrap();
        let mut tx = db.admin().transaction();
        tx.delete("crm.Deal", doomed).unwrap();
        tx.commit().unwrap();
        (
            p,
            db.admin().snapshot().stats().unwrap(),
            db.admin().snapshot().schema("core.Person").unwrap(),
        )
    };
    let db = Database::open(&path).unwrap();
    let s = db.admin().snapshot();
    assert_eq!(stats, s.stats().unwrap());
    assert_eq!(schema, s.schema("core.Person").unwrap());
    assert!(s.get("core.Person", person).unwrap().is_some());
    s.verify_indexes().unwrap();
    assert_eq!(
        db.session("fitness")
            .snapshot()
            .lookup("fitness.Workout", "durationMinutes", &60.into())
            .unwrap()
            .len(),
        1
    );
    assert!(db.session("fitness").snapshot().scan("crm.Deal").is_err());
}
#[test]
fn recovery_child() {
    let Some(path) = std::env::var_os("SUBSTRATE_RECOVERY_CHILD_PATH") else {
        return;
    };
    let db = Database::open(path).unwrap();
    seed(&db);
    let mut uncommitted = db.admin().transaction();
    uncommitted
        .insert("core.Person", vec!["must disappear".into()])
        .unwrap();
    // Exit without running destructors, leaving the storage handle open.
    std::process::exit(0);
}
#[test]
fn committed_state_survives_abrupt_process_exit() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("abrupt.redb");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recovery_child", "--nocapture"])
        .env("SUBSTRATE_RECOVERY_CHILD_PATH", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let db = Database::open(path).unwrap();
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 1);
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn durable_file_excludes_other_writers() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("locked.redb");
    let _db = Database::open(&path).unwrap();
    assert!(Database::open(&path).is_err());
}
