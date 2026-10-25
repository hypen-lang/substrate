//! Run with `cargo run --example dx [new-or-existing.redb]`.
use substrate_liquid::*;
#[allow(dead_code)]
#[path = "dx/generated/schema.rs"]
mod generated;
use generated::{
    core::Person,
    fitness::{Performed, Workout},
};
fn main() -> Result<()> {
    let db = if let Some(path) = std::env::args().nth(1) {
        Database::open(path)?
    } else {
        Database::memory()
    };
    let manifest = SchemaManifest::from_json(include_str!("dx/manifest.json"))?;
    db.admin().transact(RetryPolicy::default(), |tx| {
        tx.install_manifest_with_visibility(&manifest, Visibility::Private)
    })?;
    let person = db.admin().transact(RetryPolicy::default(), |tx| {
        let person = tx.insert_record(Person { name: "Ada".into() })?;
        let workout = tx.insert_record(Workout {
            durationMinutes: 45,
        })?;
        tx.insert_record(Performed {
            athlete: person,
            workout,
        })?;
        Ok(person)
    })?;
    let mut sql = db.admin().sql_transaction()?;
    sql.execute(
        "UPDATE core.Person SET name=?1 WHERE id=?2",
        &[
            rusqlite::types::Value::Text("Ada Lovelace".into()),
            rusqlite::types::Value::Text(person.raw().to_string()),
        ],
    )?;
    sql.commit()?;
    let result=db.admin().snapshot().sql()?.query("SELECT p.name,w.durationMinutes FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout",&[])?;
    println!("{:?}", result.rows);
    assert_eq!(
        db.admin().snapshot().get_record(person)?.unwrap().name,
        "Ada Lovelace"
    );
    Ok(())
}
