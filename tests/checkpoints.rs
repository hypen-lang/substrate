mod common;
use common::*;
use substrate_liquid::*;
fn manual() -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: None,
    }
}
fn policy(commits: u64, bytes: u64) -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: Some(CheckpointPolicy {
            max_log_commits: commits,
            max_log_bytes: bytes,
        }),
    }
}

#[test]
fn checkpoint_and_tail_recover_the_same_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (person, expected, schemas, revision) = {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let p = seed(&db);
        let mut tx = db.session("fitness").transaction();
        let (w, _) = workout(&mut tx, p, 45);
        tx.commit().unwrap();
        let before = db.admin().snapshot();
        let stats = before.stats().unwrap();
        let cp = db.checkpoint().unwrap();
        assert_eq!(cp.revision, stats.revision);
        assert_eq!(cp.rows, stats.rows as u64);
        assert!(cp.pruned_commits > 0);
        assert_eq!(db.storage_info().unwrap().log_commits, 0);
        let mut tx = db.session("fitness").transaction();
        tx.update("fitness.Workout", w, vec![60.into()]).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            before.scan("fitness.Workout").unwrap()[0].values[0],
            Value::Int(45)
        );
        let current = db.admin().snapshot();
        let rows = current
            .relations()
            .iter()
            .map(|r| (r.name.clone(), current.scan(&r.name).unwrap()))
            .collect::<Vec<_>>();
        (p, rows, current.relations(), current.revision())
    };
    let db = Database::open_with_options(&path, manual()).unwrap();
    let recovery = db.recovery_info().unwrap();
    assert_eq!(recovery.replayed_commits, 1);
    assert_eq!(recovery.recovered_revision, revision);
    assert_eq!(recovery.checkpoint_revision, revision - 1);
    assert_eq!(schemas, db.admin().snapshot().relations());
    for (name, rows) in expected {
        assert_eq!(db.admin().snapshot().scan(&name).unwrap(), rows);
    }
    assert_eq!(
        db.session("fitness")
            .snapshot()
            .get("core.Person", person)
            .unwrap()
            .unwrap()
            .values[0],
        Value::from("Alice")
    );
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn empty_checkpoint_and_repeated_checkpoint_are_valid() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = Database::open(&path).unwrap();
        for _ in 0..3 {
            assert_eq!(db.checkpoint().unwrap().revision, 0);
        }
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
    seed(&db);
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn automatic_commit_threshold_is_strict_and_reopen_preserves_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let options = policy(3, u64::MAX);
    let person = {
        let db = Database::open_with_options(&path, options).unwrap();
        seed(&db)
    };
    for i in 0..12 {
        let db = Database::open_with_options(&path, options).unwrap();
        assert!(db.recovery_info().unwrap().replayed_commits < 3);
        let mut tx = db.admin().transaction();
        tx.update("core.Person", person, vec![format!("v{i}").as_str().into()])
            .unwrap();
        tx.commit().unwrap();
        assert!(db.storage_info().unwrap().log_commits < 3);
    }
}
#[test]
fn oversized_commit_goes_directly_to_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let revision = {
        let db = Database::open_with_options(&path, policy(1000, 128)).unwrap();
        let p = seed(&db);
        let mut tx = db.admin().transaction();
        tx.update("core.Person", p, vec![Value::Text("x".repeat(8192))])
            .unwrap();
        let r = tx.commit().unwrap();
        assert_eq!(db.storage_info().unwrap().log_bytes, 0);
        assert_eq!(db.storage_info().unwrap().checkpoint_revision, r);
        r
    };
    let db = Database::open(&path).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
    assert_eq!(db.admin().snapshot().revision(), revision);
}
#[test]
fn lower_budget_on_open_compacts_preexisting_tail_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let p = seed(&db);
        for _ in 0..6 {
            let mut tx = db.admin().transaction();
            tx.update("core.Person", p, vec!["changed".into()]).unwrap();
            tx.commit().unwrap();
        }
    }
    {
        let db = Database::open_with_options(&path, policy(2, 10000)).unwrap();
        assert_eq!(db.recovery_info().unwrap().replayed_commits, 9);
        assert_eq!(db.storage_info().unwrap().log_commits, 0);
    }
    let db = Database::open_with_options(&path, policy(2, 10000)).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
}
#[test]
fn checkpoint_does_not_reset_live_conflict_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("db")).unwrap();
    let p = seed(&db);
    let mut stale = db.admin().transaction();
    stale.get("core.Person", p).unwrap();
    let mut fresh = db.admin().transaction();
    fresh
        .update("core.Person", p, vec!["changed".into()])
        .unwrap();
    fresh.commit().unwrap();
    db.checkpoint().unwrap();
    assert!(matches!(stale.commit(), Err(Error::Conflict)));
    let mut a = db.session("fitness").transaction();
    let mut b = db.session("crm").transaction();
    workout(&mut a, p, 30);
    deal(&mut b, p, 100);
    db.checkpoint().unwrap();
    a.commit().unwrap();
    db.checkpoint().unwrap();
    b.commit().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn checkpoint_preserves_retired_ids_memberships_and_revocations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let retired = {
        let db = Database::open(&path).unwrap();
        let p = seed(&db);
        let mut tx = db.admin().transaction();
        let retired = tx.insert("core.Person", vec!["retired".into()]).unwrap();
        tx.delete("core.Person", retired).unwrap();
        tx.install_with_visibility(
            Module::new(
                "member",
                1,
                vec![],
                vec![Relation::new("member.Contact", vec![])],
            ),
            substrate_liquid::Visibility::Private,
        )
        .unwrap();
        tx.add_membership("core.Person", p, "member.Contact", vec![])
            .unwrap();
        tx.revoke("fitness", "core.Person").unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
        retired
    };
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.admin().snapshot().scan("member.Contact").unwrap().len(),
        1
    );
    assert!(
        db.session("fitness")
            .snapshot()
            .scan("core.Person")
            .is_err()
    );
    let mut def = Relation::new("reuse.R", vec![]);
    def.id = retired;
    let mut tx = db.admin().transaction();
    assert!(
        tx.install_with_visibility(
            Module::new("reuse", 1, vec![], vec![def]),
            substrate_liquid::Visibility::Private
        )
        .is_err()
    );
}
#[test]
fn indexes_and_restrict_deletion_still_work_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (p, goal) = {
        let db = Database::open(&path).unwrap();
        let p = seed(&db);
        let mut tx = db.session("fitness").transaction();
        let goal = tx
            .insert("fitness.TrainingGoal", vec![p.into(), "run".into()])
            .unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
        (p, goal)
    };
    let db = Database::open(&path).unwrap();
    let mut tx = db.admin().transaction();
    assert!(tx.delete("core.Person", p).is_err());
    let mut tx = db.admin().transaction();
    assert!(
        tx.insert("fitness.TrainingGoal", vec![p.into(), "duplicate".into()])
            .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.delete("fitness.TrainingGoal", goal).unwrap();
    tx.delete("core.Person", p).unwrap();
    tx.commit().unwrap();
    db.checkpoint().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn invalid_policy_is_rejected_without_creating_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    for options in [policy(0, 1), policy(1, 0)] {
        assert!(Database::open_with_options(&path, options).is_err());
        assert!(!path.exists());
    }
    assert!(Database::memory().checkpoint().is_err());
}
#[test]
fn checkpoint_can_overlap_threaded_writes_without_losing_them() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(dir.path().join("db"), policy(5, 100000)).unwrap();
    let p = seed(&db);
    let maintenance = db.clone();
    let worker = db.session("fitness");
    let a = std::thread::spawn(move || {
        for _ in 0..10 {
            maintenance.checkpoint().unwrap();
        }
    });
    let b = std::thread::spawn(move || {
        for n in 0..20 {
            let mut tx = worker.transaction();
            workout(&mut tx, p, n);
            tx.commit().unwrap();
        }
    });
    a.join().unwrap();
    b.join().unwrap();
    assert_eq!(
        db.admin().snapshot().scan("fitness.Workout").unwrap().len(),
        20
    );
    db.admin().snapshot().verify_indexes().unwrap();
}

#[test]
fn checkpoint_key_order_can_precede_import_and_reference_targets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (base, edge) = {
        let db = Database::open(&path).unwrap();
        let mut tx = db.admin().transaction();
        let mut def = Relation::new("z.Base", vec![Role::new("n", DataType::Int)]);
        def.id = Id(uuid::Uuid::from_u128(u128::MAX));
        tx.install_with_visibility(
            Module::new("z", 1, vec![], vec![def]),
            substrate_liquid::Visibility::Private,
        )
        .unwrap();
        let mut def = Relation::new(
            "a.Edge",
            vec![Role::new("base", DataType::Ref("z.Base".into()))],
        );
        def.id = Id(uuid::Uuid::from_u128(1));
        tx.install_with_visibility(
            Module::new("a", 1, vec![("z".into(), 1)], vec![def]),
            substrate_liquid::Visibility::Private,
        )
        .unwrap();
        let base = tx.insert("z.Base", vec![42.into()]).unwrap();
        let edge = tx.insert("a.Edge", vec![base.into()]).unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
        (base, edge)
    };
    let db = Database::open(&path).unwrap();
    let s = db.admin().snapshot();
    assert_eq!(
        s.lookup("a.Edge", "base", &base.into()).unwrap()[0].id,
        edge
    );
    assert_eq!(
        s.get("z.Base", base).unwrap().unwrap().values[0],
        Value::Int(42)
    );
    s.verify_indexes().unwrap();
}
