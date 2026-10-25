use substrate_liquid::*;

fn main() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let path = directory.path().join("constraints.redb");
    let db = Database::open(&path)?;
    let mut tx = db.admin().transaction();
    tx.install(demo::core())?;
    let person = tx.insert("core.Person", vec!["Ada".into()])?;
    tx.commit()?;
    let original = db.admin().snapshot().get("core.Person", person)?;

    // An independently owned relation carries its own checks and layout.
    let age = Relation::new(
        "health.Age",
        vec![
            Role::new("person", DataType::Ref("core.Person".into())),
            Role::new("years", DataType::Int),
            Role::new("note", DataType::Text),
        ],
    )
    .unique(&["person"])
    .check(Check::int_range("years", 0, 150))
    .check(Check::byte_length("note", 0, 64));
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "health",
        1,
        vec![("core".into(), 1)],
        vec![age],
    ))?;
    tx.grant(
        "health",
        "core.Person",
        Grant {
            reference: true,
            ..Grant::read_only()
        },
    )?;
    tx.grant("health", "health.Age", Grant::read_write())?;
    tx.commit()?;

    let descriptor = db.session("health").snapshot().describe("health.Age")?;
    let mut tx = db.session("health").transaction();
    tx.insert_dynamic(
        &descriptor,
        [
            ("person".into(), person.into()),
            ("years".into(), 36.into()),
            ("note".into(), "confirmed".into()),
        ]
        .into(),
    )?;
    tx.commit()?;
    let mut invalid = db.session("health").transaction();
    assert!(
        invalid
            .insert(
                "health.Age",
                vec![person.into(), 151.into(), "invalid".into()]
            )
            .is_err()
    );
    assert!(matches!(invalid.commit(), Err(Error::Aborted)));
    db.checkpoint()?;
    drop(descriptor);
    drop(db);

    let db = Database::open(&path)?;
    assert_eq!(db.admin().snapshot().get("core.Person", person)?, original);
    let records = db.session("health").snapshot().scan("health.Age")?;
    assert_eq!(records.len(), 1);
    println!("Ada's identity and original record survived the extension unchanged.");
    println!(
        "Validated age 36 persisted; invalid age 151 aborted. Checks survived checkpoint/reopen."
    );
    println!(
        "Exported schema manifest format: {}",
        db.admin().snapshot().manifest()?.format
    );
    Ok(())
}
