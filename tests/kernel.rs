mod common;
use common::*;
use substrate_liquid::*;

#[test]
fn additive_install_preserves_rows_schema_indexes_and_snapshot() {
    let db = Database::memory();
    let admin = db.admin();
    let mut tx = admin.transaction();
    tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
        .unwrap();
    let p = tx.insert("core.Person", vec!["Alice".into()]).unwrap();
    tx.commit().unwrap();
    let before = admin.snapshot();
    let schema = before.schema("core.Person").unwrap();
    let mut tx = admin.transaction();
    tx.install_with_visibility(demo::fitness(), substrate_liquid::Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    let after = admin.snapshot();
    assert_eq!(
        before.scan("core.Person").unwrap(),
        after.scan("core.Person").unwrap()
    );
    assert_eq!(schema, after.schema("core.Person").unwrap());
    assert_eq!(
        before.get("core.Person", p).unwrap(),
        after.get("core.Person", p).unwrap()
    );
    assert_eq!(
        before
            .lookup("core.Person", "name", &"Alice".into())
            .unwrap(),
        after
            .lookup("core.Person", "name", &"Alice".into())
            .unwrap()
    );
    // Three new family policies, each with three indexed metadata roles.
    assert_eq!(
        after.stats().unwrap().role_index_entries,
        before.stats().unwrap().role_index_entries + 9
    );
    assert!(before.schema("fitness.Workout").is_err());
    after.verify_indexes().unwrap();
}
#[test]
fn two_apps_native_join_and_index_lookup() {
    let (db, p) = fixture();
    let mut a = db.session("fitness").transaction();
    workout(&mut a, p, 45);
    a.commit().unwrap();
    let mut b = db.session("crm").transaction();
    deal(&mut b, p, 125050);
    b.commit().unwrap();
    let s = db.admin().snapshot();
    assert_eq!(
        s.lookup("fitness.Performed", "athlete", &p.into())
            .unwrap()
            .len(),
        1
    );
    let table = s
        .table("core.Person", "p")
        .unwrap()
        .join(
            &s.table("fitness.Performed", "f").unwrap(),
            "p.id",
            "f.athlete",
        )
        .unwrap()
        .join(
            &s.table("fitness.Workout", "w").unwrap(),
            "f.workout",
            "w.id",
        )
        .unwrap()
        .join(
            &s.table("crm.CustomerDeal", "c").unwrap(),
            "p.id",
            "c.customer",
        )
        .unwrap()
        .join(&s.table("crm.Deal", "d").unwrap(), "c.deal", "d.id")
        .unwrap()
        .project(&["p.name", "w.durationMinutes", "d.amount"])
        .unwrap();
    assert_eq!(
        table.rows(),
        &[vec!["Alice".into(), 45.into(), Value::Decimal(125050)]]
    );
}
#[test]
fn rejected_mutation_aborts_whole_transaction_and_schema() {
    let (db, _) = fixture();
    let before = db.admin().snapshot();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "extra",
            1,
            vec![],
            vec![Relation::new("extra.Empty", vec![])],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    assert!(
        tx.insert(
            "fitness.Performed",
            vec![Id::new().into(), Id::new().into()]
        )
        .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    let after = db.admin().snapshot();
    assert_eq!(before.stats().unwrap(), after.stats().unwrap());
    assert!(after.schema("extra.Empty").is_err());
}
#[test]
fn imports_versions_redefinitions_and_case_collisions_are_rejected() {
    let (db, _) = fixture();
    for module in [
        Module::new(
            "bad",
            1,
            vec![],
            vec![Relation::new(
                "bad.Ref",
                vec![Role::new("p", DataType::Ref("core.Person".into()))],
            )],
        ),
        Module::new(
            "core",
            2,
            vec![],
            vec![Relation::new("core.Person", vec![])],
        ),
        Module::new("CORE", 1, vec![], vec![]),
        Module::new("core", 3, vec![], vec![]),
        Module::new(
            "bad",
            1,
            vec![],
            vec![Relation::new(
                "bad.R",
                vec![Role::new("a", DataType::Int), Role::new("A", DataType::Int)],
            )],
        ),
    ] {
        let mut tx = db.admin().transaction();
        assert!(
            tx.install_with_visibility(module, substrate_liquid::Visibility::Private)
                .is_err()
        );
        assert!(tx.commit().is_err());
    }
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new("core", 2, vec![], vec![Relation::new("core.New", vec![])]),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction();
    assert!(
        tx.install_with_visibility(
            Module::new(
                "other",
                1,
                vec![("core".into(), 1)],
                vec![Relation::new(
                    "other.Ref",
                    vec![Role::new("p", DataType::Ref("core.New".into()))]
                )]
            ),
            substrate_liquid::Visibility::Private
        )
        .is_err()
    );
}
#[test]
fn typed_nary_occurrences_can_themselves_be_referenced() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "org",
            1,
            vec![("core".into(), 1)],
            vec![
                Relation::new(
                    "org.Employment",
                    vec![
                        Role::new("employee", DataType::Ref("core.Person".into())),
                        Role::new("employer", DataType::Ref("core.Person".into())),
                        Role::new("title", DataType::Text),
                        Role::new("started", DataType::Timestamp),
                    ],
                ),
                Relation::new(
                    "org.Audit",
                    vec![
                        Role::new("event", DataType::Ref("org.Employment".into())),
                        Role::new("data", DataType::Bytes),
                    ],
                ),
            ],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    let event = tx
        .insert(
            "org.Employment",
            vec![
                p.into(),
                p.into(),
                "Engineer".into(),
                Value::Timestamp(1234),
            ],
        )
        .unwrap();
    tx.insert("org.Audit", vec![event.into(), Value::Bytes(vec![0, 255])])
        .unwrap();
    tx.commit().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
    let mut tx = db.admin().transaction();
    assert!(tx.delete("org.Employment", event).is_err());
    let mut tx = db.admin().transaction();
    assert!(
        tx.insert("org.Audit", vec![p.into(), Value::Bytes(vec![])])
            .is_err()
    );
    let mut tx = db.admin().transaction();
    assert!(tx.insert("core.Person", vec![42.into()]).is_err());
}
#[test]
fn identity_has_explicit_multiple_memberships() {
    let (db, p) = fixture();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "contacts",
            1,
            vec![],
            vec![Relation::new("contacts.Contact", vec![])],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.add_membership("core.Person", p, "contacts.Contact", vec![])
        .unwrap();
    tx.commit().unwrap();
    let s = db.admin().snapshot();
    assert_eq!(s.scan("contacts.Contact").unwrap()[0].id, p);
    assert!(s.get("core.Person", p).unwrap().is_some());
    let mut tx = db.admin().transaction();
    tx.delete("contacts.Contact", p).unwrap();
    tx.commit().unwrap();
    assert!(
        db.admin()
            .snapshot()
            .get("core.Person", p)
            .unwrap()
            .is_some()
    );
}
#[test]
fn unique_functional_property_and_index_updates_are_atomic() {
    let (db, p) = fixture();
    let mut tx = db.session("fitness").transaction();
    let id = tx
        .insert("fitness.TrainingGoal", vec![p.into(), "run".into()])
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("fitness").transaction();
    assert!(
        tx.insert("fitness.TrainingGoal", vec![p.into(), "walk".into()])
            .is_err()
    );
    let mut tx = db.session("fitness").transaction();
    tx.update("fitness.TrainingGoal", id, vec![p.into(), "walk".into()])
        .unwrap();
    tx.commit().unwrap();
    let s = db.admin().snapshot();
    assert!(
        s.lookup("fitness.TrainingGoal", "value", &"run".into())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        s.lookup("fitness.TrainingGoal", "value", &"walk".into())
            .unwrap()
            .len(),
        1
    );
    let mut tx = db.session("fitness").transaction();
    tx.delete("fitness.TrainingGoal", id).unwrap();
    tx.commit().unwrap();
    assert!(
        db.admin()
            .snapshot()
            .lookup("fitness.TrainingGoal", "person", &p.into())
            .unwrap()
            .is_empty()
    );
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn snapshot_is_pinned_and_drop_rolls_back() {
    let (db, p) = fixture();
    let old = db.admin().snapshot();
    let mut tx = db.admin().transaction();
    tx.update("core.Person", p, vec!["Bob".into()]).unwrap();
    assert_eq!(
        tx.get("core.Person", p).unwrap().unwrap().values[0],
        Value::from("Bob")
    );
    tx.commit().unwrap();
    assert_eq!(
        old.get("core.Person", p).unwrap().unwrap().values[0],
        Value::from("Alice")
    );
    {
        let mut tx = db.admin().transaction();
        tx.insert("core.Person", vec!["discard".into()]).unwrap();
    }
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 1);
}
