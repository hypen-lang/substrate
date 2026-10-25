use rusqlite::types::Value as S;
use substrate_liquid::*;

fn main() -> Result<()> {
    let database = Database::memory();
    let mut transaction = database.admin().transaction();
    transaction.install(demo::core())?;
    transaction.install(demo::fitness())?;
    let person = transaction.insert("core.Person", vec!["Ada".into()])?;
    for minutes in [20, 45, 60] {
        let workout = transaction.insert("fitness.Workout", vec![minutes.into()])?;
        transaction.insert("fitness.Performed", vec![person.into(), workout.into()])?;
    }
    for relation in ["core.Person", "fitness.Workout", "fitness.Performed"] {
        transaction.grant("reporter", relation, Grant::read_only())?;
    }
    transaction.commit()?;
    let snapshot = database.session("reporter").snapshot();
    let sql = snapshot.sql_selected(&["core.Person", "fitness.Workout", "fitness.Performed"])?;
    let mut query = sql.prepare("SELECT p.name,SUM(w.durationMinutes) AS total FROM core.Person p JOIN fitness.Performed f ON p.id=f.athlete JOIN fitness.Workout w ON f.workout=w.id WHERE w.durationMinutes>=?1 GROUP BY p.name")?;
    let result = query.query(&[S::Integer(30)])?;
    assert_eq!(
        result.rows,
        vec![vec![S::Text("Ada".into()), S::Integer(105)]]
    );
    println!("DataFusion aggregate: {:?}", result.rows);
    assert_eq!(
        snapshot.lookup("core.Person", "name", &"Ada".into())?[0].id,
        person
    );
    let mut revoke = database.admin().transaction();
    revoke.revoke("reporter", "fitness.Workout")?;
    revoke.commit()?;
    assert!(matches!(query.query(&[S::Integer(30)]), Err(Error::Denied)));
    println!("Native lookup preserved; retained SQL handle denied after revocation.");
    Ok(())
}
