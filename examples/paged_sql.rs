//! Run with `cargo run --release --example paged_sql`.
//! Application rows remain in paged B-trees throughout SQL execution.
use rusqlite::types::Value as S;
use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};
fn main() -> Result<()> {
    let dir = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let database = PagedDatabase::open(dir.path().join("app.redb"), PagedOptions::default())?;
    let mut tx = database.admin().transaction()?;
    tx.install_owned(demo::core(), "host", Visibility::Private)?;
    tx.install_owned(demo::fitness(), "host", Visibility::Private)?;
    let person = tx.insert("core.Person", vec!["Ada".into()])?;
    for minutes in [20, 45, 60] {
        let workout = tx.insert("fitness.Workout", vec![minutes.into()])?;
        tx.insert("fitness.Performed", vec![person.into(), workout.into()])?;
    }
    for relation in ["core.Person", "fitness.Workout", "fitness.Performed"] {
        tx.grant("reporter", relation, Grant::read_only())?;
    }
    tx.commit()?;
    let sql = database.session("reporter")?.snapshot()?.sql()?;
    let mut point = sql.prepare("SELECT name FROM core.Person WHERE id=?1")?;
    assert_eq!(
        point.query(&[S::Text(person.to_string())])?.rows,
        vec![vec![S::Text("Ada".into())]]
    );
    let result = sql.query("SELECT p.name,SUM(w.durationMinutes) AS total FROM core.Person p JOIN fitness.Performed f ON p.id=f.athlete JOIN fitness.Workout w ON f.workout=w.id WHERE w.durationMinutes>=?1 GROUP BY p.name", &[S::Integer(30)])?;
    assert_eq!(
        result.rows,
        vec![vec![S::Text("Ada".into()), S::Integer(105)]]
    );
    println!("Paged DataFusion join and aggregate: {:?}", result.rows);
    let mut tx = database.admin().transaction()?;
    tx.revoke("reporter", "core.Person")?;
    tx.commit()?;
    assert!(matches!(
        point.query(&[S::Text(person.to_string())]),
        Err(Error::Denied)
    ));
    println!("Retained SQL handle correctly denied after live revocation.");
    Ok(())
}
