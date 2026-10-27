//! cargo run --example nullable_references
use substrate_liquid::*;
fn main() -> Result<()> {
    let db = Database::memory();
    let person = Relation::new("app.Person", vec![Role::new("name", DataType::Text)]);
    let reading = Relation::new(
        "app.Reading",
        vec![
            Role::new("person", DataType::Ref("app.Person".into()))
                .on_delete(OnDelete::Cascade)
                .deferrable(),
            Role::new("score", DataType::Float.nullable()),
        ],
    );
    let mut install = db.admin().transaction();
    install.install(Module::new("app", 1, vec![], vec![person, reading]))?;
    install.commit()?;
    let id = Id::new();
    let mut tx = db.admin().transaction();
    tx.defer_constraints()?;
    tx.insert("app.Reading", vec![id.into(), Value::try_from(1.25)?])?;
    tx.insert("app.Reading", vec![id.into(), Value::Null])?;
    tx.insert_with_id("app.Person", id, vec!["Ada".into()])?;
    tx.commit()?;
    println!(
        "{} readings share one stable person identity",
        db.admin().snapshot().scan("app.Reading")?.len()
    );
    let mut tx = db.admin().transaction();
    tx.delete("app.Person", id)?;
    tx.commit()?;
    assert!(db.admin().snapshot().scan("app.Reading")?.is_empty());
    println!("authorized cascade removed both dependent readings atomically");
    Ok(())
}
