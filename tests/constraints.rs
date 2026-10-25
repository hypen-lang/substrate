use proptest::prelude::*;
use substrate_liquid::*;

fn record() -> Relation {
    Relation::new(
        "app.Profile",
        vec![
            Role::new("age", DataType::Int),
            Role::new("name", DataType::Text),
            Role::new("photo", DataType::Bytes),
        ],
    )
    .check(Check::int_range("age", 0, 150))
    .check(Check::byte_length("name", 1, 8))
    .check(Check::byte_length("photo", 0, 4))
}
fn install(db: &Database) {
    let mut tx = db.admin().transaction();
    tx.install(Module::new("app", 1, vec![], vec![record()]))
        .unwrap();
    tx.commit().unwrap();
}
fn values(age: i64, name: &str, bytes: usize) -> Vec<Value> {
    vec![age.into(), name.into(), Value::Bytes(vec![0; bytes])]
}

#[test]
fn inclusive_checks_cover_inserts_updates_named_dynamic_and_sql_writes() {
    let db = Database::memory();
    install(&db);
    let mut tx = db.admin().transaction();
    let id = tx.insert("app.Profile", values(150, "🦀🦀", 4)).unwrap();
    tx.commit().unwrap();
    for bad in [
        values(-1, "Ada", 0),
        values(151, "Ada", 0),
        values(1, "", 0),
        values(1, "🦀🦀x", 0),
        values(1, "Ada", 5),
    ] {
        let mut tx = db.admin().transaction();
        assert!(matches!(
            tx.insert("app.Profile", bad.clone()),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(tx.commit(), Err(Error::Aborted)));
        let mut tx = db.admin().transaction();
        assert!(matches!(
            tx.update("app.Profile", id, bad),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(tx.commit(), Err(Error::Aborted)));
    }
    let descriptor = db.admin().snapshot().describe("app.Profile").unwrap();
    assert_eq!(descriptor.relation().checks.len(), 3);
    let mut tx = db.admin().transaction();
    assert!(
        tx.update_dynamic(&descriptor, id, [("age".into(), 151.into())].into())
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    let mut sql = db.admin().sql_transaction().unwrap();
    assert!(sql.execute("UPDATE app.Profile SET age = -1", &[]).is_err());
    assert!(sql.commit().is_err());
    assert_eq!(
        db.admin()
            .snapshot()
            .get("app.Profile", id)
            .unwrap()
            .unwrap()
            .values
            .as_ref(),
        values(150, "🦀🦀", 4)
    );
}

#[test]
fn invalid_declarations_fail_before_install_and_poison_transaction() {
    for check in [
        Check::int_range("missing", 0, 1),
        Check::int_range("age", 5, 4),
        Check::int_range("name", 0, 1),
        Check::byte_length("age", 0, 1),
        Check::byte_length("name", 2, 1),
    ] {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        assert!(matches!(
            tx.install(Module::new("app", 1, vec![], vec![record().check(check)])),
            Err(Error::Schema(_))
        ));
        assert!(matches!(tx.commit(), Err(Error::Aborted)));
        assert!(db.admin().snapshot().schema("app.Profile").is_err());
    }
}

#[test]
fn durable_redo_and_incremental_checkpoint_preserve_compiled_checks() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checks.redb");
        let db = Database::open(&path).unwrap();
        install(&db);
        let mut tx = db.admin().transaction();
        tx.insert("app.Profile", values(0, "Ada", 0)).unwrap();
        tx.commit().unwrap();
        if checkpoint {
            db.checkpoint().unwrap();
        }
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(db.admin().snapshot().scan("app.Profile").unwrap().len(), 1);
        assert_eq!(
            db.admin()
                .snapshot()
                .schema("app.Profile")
                .unwrap()
                .checks
                .len(),
            3
        );
        let mut tx = db.admin().transaction();
        assert!(tx.insert("app.Profile", values(151, "Ada", 0)).is_err());
        assert!(tx.commit().is_err());
    }
}

#[test]
fn manifest_v2_is_conservative_and_old_contracts_remain_stable() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    let person = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.commit().unwrap();
    let old = db.admin().snapshot().manifest().unwrap();
    assert_eq!(old.format, 1);
    let old_hash = old.modules[0].relations[0].fingerprint();
    let old_row = db.admin().snapshot().get("core.Person", person).unwrap();
    let extension = Relation::new(
        "app.Age",
        vec![
            Role::new("person", DataType::Ref("core.Person".into())),
            Role::new("age", DataType::Int),
        ],
    )
    .unique(&["person"])
    .check(Check::int_range("age", 0, 150));
    let mut next = old.clone();
    next.format = 2;
    next.modules.push(Module::new(
        "app",
        1,
        vec![("core".into(), 1)],
        vec![extension],
    ));
    let json = next.to_json().unwrap();
    assert_eq!(
        SchemaManifest::lock(&json, Some(&old)).unwrap(),
        next.canonical().unwrap()
    );
    assert_eq!(
        SchemaManifest::from_json(&json).unwrap(),
        next.canonical().unwrap()
    );
    assert!(
        SchemaManifest {
            format: 1,
            modules: next.modules.clone()
        }
        .validate()
        .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.install_manifest(&next).unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.admin().snapshot().get("core.Person", person).unwrap(),
        old_row
    );
    assert_eq!(
        db.admin()
            .snapshot()
            .schema("core.Person")
            .unwrap()
            .fingerprint(),
        old_hash
    );
    assert_eq!(db.admin().snapshot().manifest().unwrap().format, 2);
    assert_eq!(
        next.select(&[("core".into(), 1)])
            .unwrap()
            .fingerprint()
            .unwrap(),
        old.fingerprint().unwrap()
    );
    let mut changed = next.clone();
    changed
        .modules
        .iter_mut()
        .find(|m| m.name == "app")
        .unwrap()
        .relations[0]
        .checks[0] = Check::int_range("age", 0, 200);
    assert!(SchemaManifest::lock(&changed.to_json().unwrap(), Some(&next)).is_err());
    assert!(db.admin().snapshot().plan_manifest(&changed).is_err());
    let code = next.generate().unwrap();
    assert!(
        code.typescript
            .contains("kind: \"IntRange\", min: 0n, max: 150n")
    );
}

#[test]
fn constraints_do_not_bypass_authorization_or_referential_integrity() {
    let db = Database::memory();
    install(&db);
    assert!(matches!(
        db.session("stranger").snapshot().schema("app.Profile"),
        Err(Error::Denied)
    ));
    let mut tx = db.session("stranger").transaction();
    assert!(matches!(
        tx.insert("app.Profile", values(1, "Ada", 0)),
        Err(Error::Denied)
    ));
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "extension",
        1,
        vec![("app".into(), 1)],
        vec![
            Relation::new(
                "extension.Score",
                vec![
                    Role::new("owner", DataType::Ref("app.Profile".into())),
                    Role::new("score", DataType::Int),
                ],
            )
            .check(Check::int_range("score", 0, 10)),
        ],
    ))
    .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction();
    assert!(matches!(
        tx.insert("extension.Score", vec![Id::new().into(), 5.into()]),
        Err(Error::Constraint(_))
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn typed_execution_agrees_with_reference_checks(age in -20i64..170, text in ".{0,12}", bytes in 0usize..8) {
        let db = Database::memory(); install(&db);
        let expected = (0..=150).contains(&age) && (1..=8).contains(&text.len()) && bytes <= 4;
        let mut tx = db.admin().transaction();
        let result = tx.insert("app.Profile", values(age, &text, bytes));
        prop_assert_eq!(result.is_ok(), expected);
        prop_assert_eq!(tx.commit().is_ok(), expected);
        prop_assert_eq!(db.admin().snapshot().scan("app.Profile").unwrap().len(), usize::from(expected));
    }
}
