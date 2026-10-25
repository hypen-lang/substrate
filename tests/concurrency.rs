mod common;
use common::*;
use std::sync::{Arc, Barrier};
use substrate_liquid::*;
#[test]
fn independent_threads_share_person_without_write_conflict() {
    let (db, p) = fixture();
    let barrier = Arc::new(Barrier::new(2));
    let a = db.session("fitness");
    let b = db.session("crm");
    let barrier2 = barrier.clone();
    let a = std::thread::spawn(move || {
        let mut tx = a.transaction();
        workout(&mut tx, p, 45);
        barrier2.wait();
        tx.commit()
    });
    let b = std::thread::spawn(move || {
        let mut tx = b.transaction();
        deal(&mut tx, p, 100);
        barrier.wait();
        tx.commit()
    });
    a.join().unwrap().unwrap();
    b.join().unwrap().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn distinct_occurrences_in_same_family_do_not_conflict() {
    let (db, p) = fixture();
    let mut a = db.session("fitness").transaction();
    let mut b = db.session("fitness").transaction();
    workout(&mut a, p, 20);
    workout(&mut b, p, 30);
    a.commit().unwrap();
    b.commit().unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .scan("fitness.Performed")
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn conflicting_updates_and_unique_keys_serialize() {
    let (db, p) = fixture();
    let mut a = db.admin().transaction();
    let mut b = db.admin().transaction();
    a.update("core.Person", p, vec!["A".into()]).unwrap();
    b.update("core.Person", p, vec!["B".into()]).unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(Error::Conflict)));
    let mut a = db.session("fitness").transaction();
    let mut b = db.session("fitness").transaction();
    a.insert("fitness.TrainingGoal", vec![p.into(), "A".into()])
        .unwrap();
    b.insert("fitness.TrainingGoal", vec![p.into(), "B".into()])
        .unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(Error::Conflict)));
}
#[test]
fn empty_index_predicates_and_scans_detect_phantoms() {
    let (db, p) = fixture();
    let mut a = db.admin().transaction();
    assert!(
        a.lookup("fitness.Workout", "durationMinutes", &45.into())
            .unwrap()
            .is_empty()
    );
    let mut b = db.admin().transaction();
    workout(&mut b, p, 45);
    b.commit().unwrap();
    a.insert("core.Person", vec!["predicate-dependent write".into()])
        .unwrap();
    assert!(matches!(a.commit(), Err(Error::Conflict)));
    let mut a = db.admin().transaction();
    a.scan("crm.Deal").unwrap();
    let mut b = db.admin().transaction();
    deal(&mut b, p, 1);
    b.commit().unwrap();
    assert!(matches!(a.commit(), Err(Error::Conflict)));
}
#[test]
fn write_skew_is_rejected() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    let q = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.commit().unwrap();
    let mut a = db.admin().transaction();
    let mut b = db.admin().transaction();
    a.get("core.Person", q).unwrap();
    b.get("core.Person", p).unwrap();
    a.update("core.Person", p, vec!["offline".into()]).unwrap();
    b.update("core.Person", q, vec!["offline".into()]).unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(Error::Conflict)));
}
#[test]
fn delete_vs_reference_race_is_safe_in_both_commit_orders() {
    for deletion_first in [true, false] {
        let (db, p) = fixture();
        let mut deleting = db.admin().transaction();
        let mut referencing = db.session("fitness").transaction();
        deleting.delete("core.Person", p).unwrap();
        workout(&mut referencing, p, 45);
        if deletion_first {
            deleting.commit().unwrap();
            assert!(matches!(referencing.commit(), Err(Error::Conflict)));
        } else {
            referencing.commit().unwrap();
            assert!(matches!(deleting.commit(), Err(Error::Conflict)));
        }
        db.admin().snapshot().verify_indexes().unwrap();
    }
}
#[test]
fn concurrent_module_installs_retry_without_partial_catalog() {
    let db = Database::memory();
    let mut a = db.admin().transaction();
    let mut b = db.admin().transaction();
    a.install_with_visibility(
        Module::new("a", 1, vec![], vec![Relation::new("a.R", vec![])]),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    b.install_with_visibility(
        Module::new("b", 1, vec![], vec![Relation::new("b.R", vec![])]),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(Error::Conflict)));
    assert!(db.admin().snapshot().schema("b.R").is_err());
}
#[test]
fn allocated_ids_cannot_collide_with_concurrent_schema_ids() {
    let (db, _) = fixture();
    let mut data = db.admin().transaction();
    let id = data.insert("core.Person", vec!["new".into()]).unwrap();
    let mut schema = db.admin().transaction();
    let mut def = Relation::new("extra.R", vec![]);
    def.id = id;
    schema
        .install_with_visibility(
            Module::new("extra", 1, vec![], vec![def]),
            substrate_liquid::Visibility::Private,
        )
        .unwrap();
    data.commit().unwrap();
    assert!(matches!(schema.commit(), Err(Error::Conflict)));
}
#[test]
fn missing_schema_reads_are_predicates_too() {
    let (db, p) = fixture();
    let mut a = db.admin().transaction();
    assert!(a.scan("new.R").is_err());
    let mut b = db.admin().transaction();
    b.get("core.Person", p).unwrap();
    b.install_with_visibility(
        Module::new("new", 1, vec![], vec![Relation::new("new.R", vec![])]),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    b.commit().unwrap();
    a.update("core.Person", p, vec!["changed".into()]).unwrap();
    assert!(matches!(a.commit(), Err(Error::Conflict)));
}
