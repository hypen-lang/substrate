use substrate_liquid::disk_snapshot::SpillOptions;
use substrate_liquid::*;

fn types() -> Module {
    Module::new(
        "typed",
        1,
        vec![],
        vec![
            Relation::new(
                "typed.Reading",
                vec![
                    Role::new("sensor", DataType::Text.nullable()),
                    Role::new("value", DataType::Float.nullable()),
                    Role::new("quality", DataType::Int.nullable()),
                ],
            )
            .unique(&["sensor"])
            .check(Check::int_range("quality", 0, 100)),
        ],
    )
}
fn references(cascade: bool, deferred: bool) -> Module {
    let mut role = Role::new("target", DataType::Ref("refs.Target".into()));
    if cascade {
        role = role.on_delete(OnDelete::Cascade);
    }
    if deferred {
        role = role.deferrable();
    }
    Module::new(
        "refs",
        1,
        vec![],
        vec![
            Relation::new("refs.Target", vec![Role::new("name", DataType::Text)]),
            Relation::new("refs.Link", vec![role]),
        ],
    )
}
fn install(db: &Database, module: Module) {
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(module, Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
}
#[test]
fn finite_float_canonical_zero_total_order_and_json() {
    use std::collections::{BTreeSet, HashSet};
    for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(FiniteFloat::new(n).is_err());
    }
    let values: Vec<_> = [-2.5, -0.0, 0.0, f64::MIN_POSITIVE, 5.0]
        .into_iter()
        .map(|x| FiniteFloat::new(x).unwrap())
        .collect();
    assert_eq!(values[1], values[2]);
    assert_eq!(values[1].get().to_bits(), 0);
    assert_eq!(values.iter().copied().collect::<HashSet<_>>().len(), 4);
    assert_eq!(values.iter().copied().collect::<BTreeSet<_>>().len(), 4);
    for value in values {
        assert_eq!(
            value,
            serde_json::from_str::<FiniteFloat>(&serde_json::to_string(&value).unwrap()).unwrap()
        );
    }
    assert!(serde_json::from_str::<FiniteFloat>("null").is_err());
}
#[test]
fn nullable_unique_check_and_index_updates_are_consistent() {
    let db = Database::memory();
    install(&db, types());
    let mut tx = db.admin().transaction();
    let a = tx
        .insert("typed.Reading", vec![Value::Null, Value::Null, Value::Null])
        .unwrap();
    let b = tx
        .insert(
            "typed.Reading",
            vec![Value::Null, Value::try_from(1.25).unwrap(), Value::Int(100)],
        )
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .lookup("typed.Reading", "sensor", &Value::Null)
            .unwrap()
            .len(),
        2
    );
    let mut tx = db.admin().transaction();
    tx.update(
        "typed.Reading",
        a,
        vec!["s".into(), Value::try_from(-0.0).unwrap(), 1.into()],
    )
    .unwrap();
    tx.commit().unwrap();
    let mut failed = db.admin().transaction();
    assert!(
        failed
            .update(
                "typed.Reading",
                b,
                vec!["s".into(), Value::Null, Value::Null]
            )
            .is_err()
    );
    assert!(matches!(failed.commit(), Err(Error::Aborted)));
    let mut failed = db.admin().transaction();
    assert!(
        failed
            .update(
                "typed.Reading",
                a,
                vec!["s".into(), Value::Null, 101.into()]
            )
            .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.update(
        "typed.Reading",
        a,
        vec![Value::Null, Value::Null, Value::Null],
    )
    .unwrap();
    tx.update(
        "typed.Reading",
        b,
        vec!["s".into(), Value::Null, Value::Null],
    )
    .unwrap();
    tx.commit().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .lookup("typed.Reading", "sensor", &Value::Null)
            .unwrap()[0]
            .id,
        a
    );
}
#[test]
fn nullable_float_log_checkpoint_and_spill_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("db");
    let id;
    {
        let db = Database::open(&file).unwrap();
        install(&db, types());
        let mut tx = db.admin().transaction();
        id = tx
            .insert(
                "typed.Reading",
                vec![Value::Null, Value::try_from(-1.5).unwrap(), Value::Null],
            )
            .unwrap();
        tx.commit().unwrap();
    }
    {
        let db = Database::open(&file).unwrap();
        db.admin().snapshot().verify_indexes().unwrap();
        let snapshot = db.admin().snapshot();
        let disk = snapshot
            .spill(
                dir.path().join("spill"),
                &["typed.Reading"],
                SpillOptions::default(),
            )
            .unwrap();
        assert_eq!(
            disk.get("typed.Reading", id).unwrap(),
            snapshot.get("typed.Reading", id).unwrap()
        );
        db.checkpoint().unwrap();
    }
    let db = Database::open(&file).unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .get("typed.Reading", id)
            .unwrap()
            .unwrap()
            .values[1],
        Value::try_from(-1.5).unwrap()
    );
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn deferral_is_explicit_and_final_references_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("db");
    {
        let db = Database::open(&file).unwrap();
        install(&db, references(false, true));
        let missing = Id::new();
        let mut immediate = db.admin().transaction();
        assert!(immediate.insert("refs.Link", vec![missing.into()]).is_err());
        let mut invalid = db.admin().transaction();
        invalid.defer_constraints().unwrap();
        invalid.insert("refs.Link", vec![missing.into()]).unwrap();
        assert!(invalid.commit().is_err());
        assert!(db.admin().snapshot().scan("refs.Link").unwrap().is_empty());
        let mut tx = db.admin().transaction();
        tx.defer_constraints().unwrap();
        tx.insert("refs.Link", vec![missing.into()]).unwrap();
        tx.insert_with_id("refs.Target", missing, vec!["later".into()])
            .unwrap();
        tx.commit().unwrap();
    }
    let db = Database::open(&file).unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
    assert_eq!(db.admin().snapshot().scan("refs.Link").unwrap().len(), 1);
}
#[test]
fn deferral_cannot_weaken_immediate_roles_or_target_permissions() {
    let db = Database::memory();
    install(&db, references(false, false));
    let mut tx = db.admin().transaction();
    tx.defer_constraints().unwrap();
    assert!(tx.insert("refs.Link", vec![Id::new().into()]).is_err());
    let db = Database::memory();
    install(&db, references(false, true));
    let mut tx = db.admin().transaction();
    tx.grant("app", "refs.Link", Grant::read_write()).unwrap();
    tx.grant(
        "app",
        "refs.Target",
        Grant {
            read: false,
            write: true,
            reference: true,
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "allowed".into(),
            },
        },
    )
    .unwrap();
    let forbidden = tx.insert("refs.Target", vec!["private".into()]).unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("app").transaction();
    tx.defer_constraints().unwrap();
    tx.insert("refs.Link", vec![forbidden.into()]).unwrap();
    assert!(matches!(tx.commit(), Err(Error::Denied)));
    assert!(db.admin().snapshot().scan("refs.Link").unwrap().is_empty());
}
#[test]
fn cascade_requires_each_rows_write_permission_and_keeps_old_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = Database::open(&path).unwrap();
        install(&db, references(true, false));
        let mut tx = db.admin().transaction();
        let p = tx.insert("refs.Target", vec!["p".into()]).unwrap();
        let link = tx.insert("refs.Link", vec![p.into()]).unwrap();
        tx.grant("app", "refs.Target", Grant::read_write()).unwrap();
        tx.commit().unwrap();
        let before = db.admin().snapshot();
        let mut failed = db.session("app").transaction();
        assert!(matches!(
            failed.delete("refs.Target", p),
            Err(Error::Denied)
        ));
        assert!(failed.commit().is_err());
        assert!(
            db.admin()
                .snapshot()
                .get("refs.Target", p)
                .unwrap()
                .is_some()
        );
        let mut tx = db.admin().transaction();
        tx.delete("refs.Target", p).unwrap();
        tx.commit().unwrap();
        assert!(
            db.admin()
                .snapshot()
                .get("refs.Link", link)
                .unwrap()
                .is_none()
        );
        assert!(before.get("refs.Link", link).unwrap().is_some());
    }
    let db = Database::open(&path).unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
    assert!(db.admin().snapshot().scan("refs.Link").unwrap().is_empty());
}
#[test]
fn concurrent_incoming_edge_conflicts_with_cascade() {
    let db = Database::memory();
    install(&db, references(true, false));
    let mut tx = db.admin().transaction();
    let p = tx.insert("refs.Target", vec!["p".into()]).unwrap();
    tx.commit().unwrap();
    let mut delete = db.admin().transaction();
    delete.delete("refs.Target", p).unwrap();
    let mut insert = db.admin().transaction();
    insert.insert("refs.Link", vec![p.into()]).unwrap();
    insert.commit().unwrap();
    assert!(matches!(delete.commit(), Err(Error::Conflict)));
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn cascade_cycles_and_deferred_delete_repair_recover() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("db");
    let a = Id::new();
    let b = Id::new();
    {
        let db = Database::open(&file).unwrap();
        install(
            &db,
            Module::new(
                "cycle",
                1,
                vec![],
                vec![Relation::new(
                    "cycle.Node",
                    vec![
                        Role::new("next", DataType::Ref("cycle.Node".into()))
                            .on_delete(OnDelete::Cascade)
                            .deferrable(),
                    ],
                )],
            ),
        );
        let mut tx = db.admin().transaction();
        tx.defer_constraints().unwrap();
        tx.insert_with_id("cycle.Node", a, vec![b.into()]).unwrap();
        tx.insert_with_id("cycle.Node", b, vec![a.into()]).unwrap();
        tx.commit().unwrap();
    }
    {
        let db = Database::open(&file).unwrap();
        let mut tx = db.admin().transaction();
        tx.delete("cycle.Node", a).unwrap();
        tx.commit().unwrap();
    }
    let db = Database::open(&file).unwrap();
    assert!(db.admin().snapshot().scan("cycle.Node").unwrap().is_empty());
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn new_schema_flags_are_conservative_and_generated_types_are_precise() {
    let old = Role::new("value", DataType::Int);
    assert_eq!(
        serde_json::to_string(&old).unwrap(),
        r#"{"name":"value","data_type":"Int"}"#
    );
    for kind in [DataType::Int, DataType::Int.nullable()] {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        assert!(
            tx.install(Module::new(
                "bad",
                1,
                vec![],
                vec![Relation::new(
                    "bad.R",
                    vec![Role::new("v", kind).deferrable()]
                )]
            ))
            .is_err()
        );
    }
    let manifest = SchemaManifest {
        format: 2,
        modules: vec![types()],
    };
    let generated = manifest.generate().unwrap();
    assert!(
        generated
            .rust
            .contains("Option<substrate_liquid::FiniteFloat>")
    );
    assert!(generated.typescript.contains("number | null"));
    assert!(generated.typescript.contains("nullable: true"));
}

#[test]
fn deferred_delete_requires_repair_and_reference_target_changes_conflict() {
    let db = Database::memory();
    install(&db, references(false, true));
    let mut tx = db.admin().transaction();
    let p = tx.insert("refs.Target", vec!["p".into()]).unwrap();
    let link = tx.insert("refs.Link", vec![p.into()]).unwrap();
    tx.commit().unwrap();
    let mut invalid = db.admin().transaction();
    invalid.defer_constraints().unwrap();
    invalid.delete("refs.Target", p).unwrap();
    assert!(invalid.commit().is_err());
    assert!(
        db.admin()
            .snapshot()
            .get("refs.Target", p)
            .unwrap()
            .is_some()
    );
    let mut deferred = db.admin().transaction();
    deferred.defer_constraints().unwrap();
    deferred.insert("refs.Link", vec![p.into()]).unwrap();
    let mut other = db.admin().transaction();
    other
        .update("refs.Target", p, vec!["changed".into()])
        .unwrap();
    other.commit().unwrap();
    assert!(matches!(deferred.commit(), Err(Error::Conflict)));
    let mut repaired = db.admin().transaction();
    repaired.defer_constraints().unwrap();
    repaired.delete("refs.Target", p).unwrap();
    repaired.delete("refs.Link", link).unwrap();
    repaired.commit().unwrap();
    db.admin().snapshot().verify_indexes().unwrap();
}
