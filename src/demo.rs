//! Reusable example schema declarations. Runtime validation is authoritative.
use crate::*;
pub fn core() -> Module {
    Module::new(
        "core",
        1,
        vec![],
        vec![Relation::new(
            "core.Person",
            vec![Role::new("name", DataType::Text)],
        )],
    )
}
pub fn fitness() -> Module {
    Module::new(
        "fitness",
        1,
        vec![("core".into(), 1)],
        vec![
            Relation::new(
                "fitness.Workout",
                vec![Role::new("durationMinutes", DataType::Int)],
            ),
            Relation::new(
                "fitness.Performed",
                vec![
                    Role::new("athlete", DataType::Ref("core.Person".into())),
                    Role::new("workout", DataType::Ref("fitness.Workout".into())),
                ],
            ),
            Relation::new(
                "fitness.TrainingGoal",
                vec![
                    Role::new("person", DataType::Ref("core.Person".into())),
                    Role::new("value", DataType::Text),
                ],
            )
            .unique(&["person"]),
        ],
    )
}
pub fn crm() -> Module {
    Module::new(
        "crm",
        1,
        vec![("core".into(), 1)],
        vec![
            Relation::new(
                "crm.Deal",
                vec![Role::new("amount", DataType::Decimal { scale: 2 })],
            ),
            Relation::new(
                "crm.CustomerDeal",
                vec![
                    Role::new("customer", DataType::Ref("core.Person".into())),
                    Role::new("deal", DataType::Ref("crm.Deal".into())),
                ],
            ),
        ],
    )
}
pub const CROSS_APP_SQL: &str = "SELECT p.name, w.durationMinutes, d.amount
FROM core.Person p
JOIN fitness.Performed f ON f.athlete = p.id
JOIN fitness.Workout w ON w.id = f.workout
JOIN crm.CustomerDeal c ON c.customer = p.id
JOIN crm.Deal d ON d.id = c.deal";

/// Execute the founding cross-application workflow. Requires an empty database.
pub fn run(db: &Database) -> Result<()> {
    let admin = db.admin();
    if admin.snapshot().stats()?.relations != 0 {
        return Err(Error::Query("demo needs a fresh database".into()));
    }
    let mut tx = admin.transaction();
    tx.install(core())?;
    let person = tx.insert("core.Person", vec!["Alice".into()])?;
    for app in ["fitness", "crm"] {
        tx.grant_schema(app, app)?;
        tx.grant(
            app,
            "core.Person",
            Grant {
                reference: true,
                ..Grant::read_only()
            },
        )?;
    }
    tx.commit()?;
    let baseline = admin.snapshot();
    println!("Person: {person}; core revision {}", baseline.revision());
    let fitness = db.session("fitness");
    let crm = db.session("crm");
    let mut tx = fitness.transaction();
    tx.install(self::fitness())?;
    tx.commit()?;
    assert_eq!(
        baseline.scan("core.Person")?,
        admin.snapshot().scan("core.Person")?
    );
    let mut tx = fitness.transaction();
    let workout = tx.insert("fitness.Workout", vec![45.into()])?;
    tx.insert("fitness.Performed", vec![person.into(), workout.into()])?;
    tx.commit()?;
    let before_crm = admin.snapshot();
    let mut tx = crm.transaction();
    tx.install(self::crm())?;
    tx.commit()?;
    assert_eq!(
        before_crm.scan("fitness.Performed")?,
        admin.snapshot().scan("fitness.Performed")?
    );
    assert_eq!(
        baseline.scan("core.Person")?,
        admin.snapshot().scan("core.Person")?
    );
    let mut tx = crm.transaction();
    let deal = tx.insert("crm.Deal", vec![Value::Decimal(125050)])?;
    tx.insert("crm.CustomerDeal", vec![person.into(), deal.into()])?;
    tx.commit()?;
    let snapshot = admin.snapshot();
    println!("Additive installs preserved existing records and identity.");
    println!(
        "Cross-app query: {:?}",
        snapshot.sql()?.query(CROSS_APP_SQL, &[])?.rows
    );
    println!("Deal amount is exact minor units: 125050 = 1250.50 at scale 2.");
    let links = fitness
        .snapshot()
        .lookup("fitness.Performed", "athlete", &person.into())?;
    println!("Indexed relationship lookup: {} occurrence(s)", links.len());
    assert!(fitness.snapshot().scan("crm.Deal").is_err());
    println!("Fitness cannot read CRM data.");
    let mut invalid = fitness.transaction();
    invalid.insert("fitness.Workout", vec![99.into()])?;
    assert!(
        invalid
            .insert("fitness.Performed", vec![Id::new().into(), workout.into()])
            .is_err()
    );
    assert!(matches!(invalid.commit(), Err(Error::Aborted)));
    assert_eq!(snapshot.stats()?, admin.snapshot().stats()?);
    println!("Invalid reference rolled back the entire transaction.");
    let mut a = fitness.transaction();
    let mut b = crm.transaction();
    a.insert(
        "fitness.TrainingGoal",
        vec![person.into(), "Run 5 km".into()],
    )?;
    b.insert("crm.CustomerDeal", vec![person.into(), deal.into()])?;
    a.commit()?;
    b.commit()?;
    println!("Independent writes from the same starting snapshot both committed.");
    assert!(snapshot.scan("fitness.TrainingGoal")?.is_empty());
    println!("Retained snapshot still sees its original data.");
    admin.snapshot().verify_indexes()?;
    println!("Storage statistics: {:?}", admin.snapshot().stats()?);
    // Equivalent ordinary SQLite projection, for a small correctness baseline.
    let conn = rusqlite::Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE Person(id TEXT PRIMARY KEY,name TEXT); CREATE TABLE Workout(id TEXT PRIMARY KEY,durationMinutes INTEGER); CREATE TABLE Performed(athlete TEXT,workout TEXT); CREATE TABLE Deal(id TEXT PRIMARY KEY,amount INTEGER); CREATE TABLE CustomerDeal(customer TEXT,deal TEXT);")?;
    conn.execute(
        "INSERT INTO Person VALUES (?1,'Alice')",
        [person.to_string()],
    )?;
    conn.execute("INSERT INTO Workout VALUES (?1,45)", [workout.to_string()])?;
    conn.execute(
        "INSERT INTO Performed VALUES (?1,?2)",
        [person.to_string(), workout.to_string()],
    )?;
    conn.execute("INSERT INTO Deal VALUES (?1,125050)", [deal.to_string()])?;
    conn.execute(
        "INSERT INTO CustomerDeal VALUES (?1,?2)",
        [person.to_string(), deal.to_string()],
    )?;
    let result=conn.query_row("SELECT p.name,w.durationMinutes,d.amount FROM Person p JOIN Performed f ON f.athlete=p.id JOIN Workout w ON w.id=f.workout JOIN CustomerDeal c ON c.customer=p.id JOIN Deal d ON d.id=c.deal",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?)))?;
    assert_eq!(result, ("Alice".into(), 45, 125050));
    println!("Equivalent SQLite query agrees: {result:?}");
    Ok(())
}
