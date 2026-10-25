//! Reproducible warm-cache microbenchmarks. No performance targets are assertions.
use rusqlite::{Connection, params, types::Value as S};
use std::{hint::black_box, time::Instant};
use substrate_liquid::*;

#[derive(Clone)]
struct Fixture {
    person: Id,
    workout: Id,
    performed: Id,
    deal: Id,
    link: Id,
    minutes: i64,
    amount: i64,
}
fn populate(db: &Database, n: usize) -> Vec<Fixture> {
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.install(demo::fitness()).unwrap();
    tx.install(demo::crm()).unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction();
    let mut fixtures = vec![];
    for i in 0..n {
        let person = tx
            .insert("core.Person", vec![format!("Person {i}").as_str().into()])
            .unwrap();
        let minutes = (i % 120) as i64;
        let amount = (i as i64 + 1) * 100;
        let workout = tx.insert("fitness.Workout", vec![minutes.into()]).unwrap();
        let performed = tx
            .insert("fitness.Performed", vec![person.into(), workout.into()])
            .unwrap();
        let deal = tx.insert("crm.Deal", vec![Value::Decimal(amount)]).unwrap();
        let link = tx
            .insert("crm.CustomerDeal", vec![person.into(), deal.into()])
            .unwrap();
        fixtures.push(Fixture {
            person,
            workout,
            performed,
            deal,
            link,
            minutes,
            amount,
        });
    }
    tx.commit().unwrap();
    fixtures
}
fn sqlite(conn: &mut Connection, fixtures: &[Fixture]) {
    conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;
        CREATE TABLE Person(id BLOB PRIMARY KEY,name TEXT NOT NULL);
        CREATE TABLE Workout(id BLOB PRIMARY KEY,durationMinutes INTEGER NOT NULL);
        CREATE TABLE Performed(id BLOB PRIMARY KEY,athlete BLOB NOT NULL REFERENCES Person(id),workout BLOB NOT NULL REFERENCES Workout(id));
        CREATE TABLE Deal(id BLOB PRIMARY KEY,amount INTEGER NOT NULL);
        CREATE TABLE CustomerDeal(id BLOB PRIMARY KEY,customer BLOB NOT NULL REFERENCES Person(id),deal BLOB NOT NULL REFERENCES Deal(id));
        CREATE INDEX person_name ON Person(name); CREATE INDEX workout_duration ON Workout(durationMinutes);
        CREATE INDEX performed_athlete ON Performed(athlete); CREATE INDEX performed_workout ON Performed(workout);
        CREATE INDEX deal_amount ON Deal(amount); CREATE INDEX customer_person ON CustomerDeal(customer); CREATE INDEX customer_deal ON CustomerDeal(deal);").unwrap();
    let tx = conn.transaction().unwrap();
    {
        let mut person = tx.prepare("INSERT INTO Person VALUES (?1,?2)").unwrap();
        let mut workout = tx.prepare("INSERT INTO Workout VALUES (?1,?2)").unwrap();
        let mut performed = tx
            .prepare("INSERT INTO Performed VALUES (?1,?2,?3)")
            .unwrap();
        let mut deal = tx.prepare("INSERT INTO Deal VALUES (?1,?2)").unwrap();
        let mut linked = tx
            .prepare("INSERT INTO CustomerDeal VALUES (?1,?2,?3)")
            .unwrap();
        for (i, f) in fixtures.iter().enumerate() {
            person
                .execute(params![
                    f.person.0.as_bytes().as_slice(),
                    format!("Person {i}")
                ])
                .unwrap();
            workout
                .execute(params![f.workout.0.as_bytes().as_slice(), f.minutes])
                .unwrap();
            performed
                .execute(params![
                    f.performed.0.as_bytes().as_slice(),
                    f.person.0.as_bytes().as_slice(),
                    f.workout.0.as_bytes().as_slice()
                ])
                .unwrap();
            deal.execute(params![f.deal.0.as_bytes().as_slice(), f.amount])
                .unwrap();
            linked
                .execute(params![
                    f.link.0.as_bytes().as_slice(),
                    f.person.0.as_bytes().as_slice(),
                    f.deal.0.as_bytes().as_slice()
                ])
                .unwrap();
        }
    }
    tx.commit().unwrap();
}
fn measure(label: &str, iterations: usize, mut f: impl FnMut(usize)) {
    for i in 0..iterations {
        f(i);
    }
    let mut times = vec![];
    for _ in 0..7 {
        let start = Instant::now();
        for i in 0..iterations {
            f(i);
        }
        times.push(start.elapsed().as_secs_f64() * 1e6 / iterations as f64);
    }
    times.sort_by(f64::total_cmp);
    println!(
        "{label},median_us_per_op={:.3},min={:.3},max={:.3}",
        times[3], times[0], times[6]
    );
}
fn install_bench(db: &Database, label: &str) {
    let mut version = 0;
    measure(label, 20, |_| {
        version += 1;
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "extensions",
            version,
            vec![],
            vec![Relation::new(
                &format!("extensions.R{version}"),
                vec![Role::new("n", DataType::Int)],
            )],
        ))
        .unwrap();
        black_box(tx.commit().unwrap());
    });
}
fn main() {
    let n = std::env::var("SUBSTRATE_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000usize);
    assert!(n > 0);
    println!(
        "dataset_people={n},occurrences={},samples=7,lookup_iterations=1000,warm_cache=true",
        n * 5
    );
    let db = Database::memory();
    let fixtures = populate(&db, n);
    let snapshot = db.admin().snapshot();
    let mut conn = Connection::open_in_memory().unwrap();
    sqlite(&mut conn, &fixtures);
    let mut lookup = conn
        .prepare("SELECT id,durationMinutes FROM Workout WHERE durationMinutes=?1")
        .unwrap();
    for value in 0..120 {
        let mut expected = lookup
            .query_map([value], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let mut actual = snapshot
            .lookup("fitness.Workout", "durationMinutes", &Value::Int(value))
            .unwrap()
            .into_iter()
            .map(|r| {
                (
                    r.id.0.as_bytes().to_vec(),
                    match r.values[0] {
                        Value::Int(v) => v,
                        _ => unreachable!(),
                    },
                )
            })
            .collect::<Vec<_>>();
        expected.sort();
        actual.sort();
        assert_eq!(expected, actual);
    }
    measure("liquid_native_index", 1000, |i| {
        black_box(
            snapshot
                .lookup(
                    "fitness.Workout",
                    "durationMinutes",
                    &Value::Int((i % 120) as i64),
                )
                .unwrap(),
        );
    });
    measure("sqlite_prepared_index", 1000, |i| {
        black_box(
            lookup
                .query_map([(i % 120) as i64], |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
        );
    });
    let liquid_sql = snapshot.sql().unwrap();
    let sql = "SELECT w.durationMinutes,d.amount FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout JOIN crm.CustomerDeal c ON c.customer=p.id JOIN crm.Deal d ON d.id=c.deal WHERE p.id=?1";
    let baseline = "SELECT w.durationMinutes,d.amount FROM Person p JOIN Performed f ON f.athlete=p.id JOIN Workout w ON w.id=f.workout JOIN CustomerDeal c ON c.customer=p.id JOIN Deal d ON d.id=c.deal WHERE p.id=?1";
    let mut prepared = conn.prepare(baseline).unwrap();
    let ids = fixtures
        .iter()
        .map(|f| f.person.to_string())
        .collect::<Vec<_>>();
    for (i, f) in fixtures.iter().take(20).enumerate() {
        assert_eq!(
            liquid_sql
                .query(sql, &[S::Text(ids[i].clone())])
                .unwrap()
                .rows,
            vec![vec![S::Integer(f.minutes), S::Integer(f.amount)]]
        );
    }
    measure("liquid_sql_cross_app_point_join", 200, |i| {
        black_box(
            liquid_sql
                .query(sql, &[S::Text(ids[i % n].clone())])
                .unwrap(),
        );
    });
    let mut prepared_liquid = liquid_sql.prepare(sql).unwrap();
    measure("liquid_prepared_cross_app_point_join", 200, |i| {
        black_box(
            prepared_liquid
                .query(&[S::Text(ids[i % n].clone())])
                .unwrap(),
        );
    });
    measure("sqlite_prepared_cross_app_point_join", 200, |i| {
        black_box(
            prepared
                .query_row([fixtures[i % n].person.0.as_bytes().as_slice()], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
                })
                .unwrap(),
        );
    });
    // Both memory backends; commit cost includes metadata validation and publication.
    install_bench(&Database::memory(), "install_empty_database");
    install_bench(&db, "install_populated_database");
    let temp = tempfile::tempdir().unwrap();
    let liquid_path = temp.path().join("liquid.redb");
    let sqlite_path = temp.path().join("baseline.sqlite");
    {
        let durable = Database::open(&liquid_path).unwrap();
        populate(&durable, n);
    }
    {
        let mut durable = Connection::open(&sqlite_path).unwrap();
        sqlite(&mut durable, &fixtures);
    }
    println!(
        "liquid_redo_file_bytes={},sqlite_current_tables_and_indexes_bytes={}",
        std::fs::metadata(&liquid_path).unwrap().len(),
        std::fs::metadata(&sqlite_path).unwrap().len()
    );
    println!(
        "File sizes cover different representations: Liquid replays packed redo and rebuilds indexes; SQLite persists its current indexes. No storage-efficiency claim is implied."
    );
    measure("liquid_durable_reopen", 3, |_| {
        black_box(Database::open(&liquid_path).unwrap());
    });
}
