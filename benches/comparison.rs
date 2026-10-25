//! Shared deterministic fixture, verified outputs, and prepared read workloads.
//! PostgreSQL is optional and uses psql's client timing, never process startup.
use rusqlite::{Connection, types::Value as S};
use serde_json::{Value as J, json};
use sha2::{Digest, Sha256};
use std::{
    hint::black_box,
    io::Write,
    process::{Command, Stdio},
    time::Instant,
};
use substrate_liquid::*;

const SAMPLES: usize = 7;
const ITERATIONS: usize = 10;
type Rows = Vec<Vec<String>>;

fn person(i: usize) -> Id {
    Id::from_u128(i as u128 + 1)
}
fn occurrence(family: u128, i: usize, child: usize) -> Id {
    Id::from_u128(family * 1_000_000 + (i * 2 + child) as u128 + 1)
}
fn fixture(n: usize) -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.install(demo::fitness()).unwrap();
    tx.install(demo::crm()).unwrap();
    for i in 0..n {
        let p = person(i);
        tx.insert_with_id(
            "core.Person",
            p,
            vec![format!("Person{i:06}").as_str().into()],
        )
        .unwrap();
        for j in 0..2 {
            let w = occurrence(1, i, j);
            let f = occurrence(2, i, j);
            let d = occurrence(3, i, j);
            let c = occurrence(4, i, j);
            tx.insert_with_id(
                "fitness.Workout",
                w,
                vec![(((i * 7 + j * 19) % 120) as i64).into()],
            )
            .unwrap();
            tx.insert_with_id("fitness.Performed", f, vec![p.into(), w.into()])
                .unwrap();
            tx.insert_with_id(
                "crm.Deal",
                d,
                vec![Value::Decimal(((i + 1) * 100 + j * 17) as i64)],
            )
            .unwrap();
            tx.insert_with_id("crm.CustomerDeal", c, vec![p.into(), d.into()])
                .unwrap();
        }
    }
    for relation in [
        "core.Person",
        "fitness.Workout",
        "fitness.Performed",
        "crm.Deal",
        "crm.CustomerDeal",
    ] {
        tx.grant("reader", relation, Grant::read_only()).unwrap();
    }
    tx.commit().unwrap();
    db
}
fn ddl(postgres: bool) -> String {
    let id = if postgres { "UUID" } else { "TEXT" };
    let prefix = if postgres {
        "CREATE SCHEMA core; CREATE SCHEMA fitness; CREATE SCHEMA crm;"
    } else {
        "PRAGMA foreign_keys=ON;"
    };
    let sql = format!(
        r#"{prefix}
CREATE TABLE core."Person"(id {id} PRIMARY KEY,name TEXT NOT NULL);
CREATE TABLE fitness."Workout"(id {id} PRIMARY KEY,"durationMinutes" BIGINT NOT NULL);
CREATE TABLE fitness."Performed"(id {id} PRIMARY KEY,athlete {id} NOT NULL REFERENCES core."Person"(id),workout {id} NOT NULL REFERENCES fitness."Workout"(id));
CREATE TABLE crm."Deal"(id {id} PRIMARY KEY,amount BIGINT NOT NULL);
CREATE TABLE crm."CustomerDeal"(id {id} PRIMARY KEY,customer {id} NOT NULL REFERENCES core."Person"(id),deal {id} NOT NULL REFERENCES crm."Deal"(id));
CREATE INDEX person_name ON core."Person"(name);
CREATE INDEX workout_duration ON fitness."Workout"("durationMinutes");
CREATE INDEX performed_athlete ON fitness."Performed"(athlete);
CREATE INDEX performed_workout ON fitness."Performed"(workout);
CREATE INDEX deal_amount ON crm."Deal"(amount);
CREATE INDEX customer_person ON crm."CustomerDeal"(customer);
CREATE INDEX customer_deal ON crm."CustomerDeal"(deal);
"#
    );
    if postgres { sql } else { flatten(&sql) }
}
fn inserts(n: usize) -> String {
    let mut s = String::from("BEGIN;\n");
    for i in 0..n {
        let p = person(i);
        s.push_str(&format!(
            "INSERT INTO core.\"Person\" VALUES ('{p}','Person{i:06}');\n"
        ));
        for j in 0..2 {
            let w = occurrence(1, i, j);
            let f = occurrence(2, i, j);
            let d = occurrence(3, i, j);
            let c = occurrence(4, i, j);
            let minutes = (i * 7 + j * 19) % 120;
            let amount = (i + 1) * 100 + j * 17;
            s.push_str(&format!("INSERT INTO fitness.\"Workout\" VALUES ('{w}',{minutes});\nINSERT INTO fitness.\"Performed\" VALUES ('{f}','{p}','{w}');\nINSERT INTO crm.\"Deal\" VALUES ('{d}',{amount});\nINSERT INTO crm.\"CustomerDeal\" VALUES ('{c}','{p}','{d}');\n"));
        }
    }
    s.push_str("COMMIT;\nANALYZE;\n");
    s
}
fn flatten(sql: &str) -> String {
    sql.replace("core.\"Person\"", "\"Person\"")
        .replace("fitness.\"Workout\"", "\"Workout\"")
        .replace("fitness.\"Performed\"", "\"Performed\"")
        .replace("crm.\"Deal\"", "\"Deal\"")
        .replace("crm.\"CustomerDeal\"", "\"CustomerDeal\"")
}
fn workloads(n: usize) -> Vec<(&'static str, String)> {
    let id = person(n / 2);
    vec![
        ("01_primary_key",format!(r#"SELECT id,name FROM core."Person" WHERE id='{id}'"#)),
        ("02_indexed_equality",r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes"=45 ORDER BY id"#.into()),
        ("03_range_top_100",r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes">=40 AND "durationMinutes"<60 ORDER BY "durationMinutes",id LIMIT 100"#.into()),
        ("04_relationship_lookup",format!(r#"SELECT f.id,w.id,w."durationMinutes" FROM fitness."Performed" f JOIN fitness."Workout" w ON w.id=f.workout WHERE f.athlete='{id}' ORDER BY f.id"#)),
        ("05_cross_app_five_way",format!(r#"SELECT p.id,f.id,w."durationMinutes",c.id,d.amount FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout JOIN crm."CustomerDeal" c ON c.customer=p.id JOIN crm."Deal" d ON d.id=c.deal WHERE p.id='{id}' ORDER BY f.id,c.id"#)),
        ("06_cross_app_group_top_25",r#"SELECT p.id,COUNT(*),SUM(w."durationMinutes"),SUM(d.amount) FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout JOIN crm."CustomerDeal" c ON c.customer=p.id JOIN crm."Deal" d ON d.id=c.deal GROUP BY p.id ORDER BY SUM(d.amount) DESC,p.id LIMIT 25"#.into()),
        ("07_correlated_exists",r#"SELECT p.id,p.name FROM core."Person" p WHERE EXISTS (SELECT 1 FROM fitness."Performed" f JOIN fitness."Workout" w ON w.id=f.workout WHERE f.athlete=p.id AND w."durationMinutes">110) ORDER BY p.id LIMIT 100"#.into()),
        ("08_filtered_nested_top_1",r#"SELECT person_id,link_id,workout_id,minutes FROM (SELECT p.id AS person_id,f.id AS link_id,w.id AS workout_id,w."durationMinutes" AS minutes,ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY w."durationMinutes" DESC,f.id) AS rn FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout WHERE w."durationMinutes">=30) ranked WHERE rn<=1 ORDER BY person_id LIMIT 100"#.into()),
    ]
}
fn canonical(rows: Vec<Vec<S>>) -> Rows {
    rows.into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| match v {
                    S::Text(s) => s,
                    S::Integer(n) => n.to_string(),
                    _ => panic!("unexpected benchmark scalar: {v:?}"),
                })
                .collect()
        })
        .collect()
}
fn sqlite_rows(statement: &mut rusqlite::Statement<'_>) -> Rows {
    let count = statement.column_count();
    canonical(
        statement
            .query_map([], |r| (0..count).map(|c| r.get(c)).collect())
            .unwrap()
            .collect::<rusqlite::Result<Vec<Vec<S>>>>()
            .unwrap(),
    )
}
fn measure(mut f: impl FnMut()) -> J {
    f();
    let mut means = vec![];
    for _ in 0..SAMPLES {
        let t = Instant::now();
        for _ in 0..ITERATIONS {
            f();
        }
        means.push(t.elapsed().as_secs_f64() * 1e6 / ITERATIONS as f64);
    }
    stats(means)
}
fn stats(mut means: Vec<f64>) -> J {
    means.sort_by(f64::total_cmp);
    json!({"median_us":means[means.len()/2],"min_us":means[0],"max_us":means[means.len()-1]})
}
fn psql(dsn: &str, sql: &str) -> String {
    let mut child = Command::new("psql")
        .args([
            "-X",
            "-w",
            "-qAt",
            "-F",
            "\t",
            "-v",
            "ON_ERROR_STOP=1",
            "--dbname",
            dsn,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("psql is required when SUBSTRATE_BENCH_PG_DSN is set");
    // Write concurrently: a large fixture or result must not deadlock full pipes.
    let mut stdin = child.stdin.take().unwrap();
    let sql = sql.to_owned();
    let writer = std::thread::spawn(move || stdin.write_all(sql.as_bytes()).unwrap());
    let output = child.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(
        output.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
fn pg_rows(dsn: &str, sql: &str) -> Rows {
    psql(dsn, &format!("BEGIN READ ONLY;\n{sql};\nROLLBACK;\n"))
        .lines()
        .map(|l| l.split('\t').map(str::to_owned).collect())
        .collect()
}
fn pg_measure(dsn: &str, sql: &str) -> J {
    let mut script = format!(
        "BEGIN READ ONLY;\nPREPARE measured AS {sql};\n\\o /dev/null\nEXECUTE measured;\n\\timing on\n"
    );
    for _ in 0..SAMPLES * ITERATIONS {
        script.push_str("EXECUTE measured;\n");
    }
    script.push_str("\\timing off\nROLLBACK;\n");
    let output = psql(dsn, &script);
    let times = output
        .lines()
        .filter_map(|l| l.strip_prefix("Time: "))
        .map(|s| s.split_whitespace().next().unwrap().parse::<f64>().unwrap() * 1000.0)
        .collect::<Vec<_>>();
    assert_eq!(
        times.len(),
        SAMPLES * ITERATIONS,
        "unexpected psql timing format: {output}"
    );
    stats(
        times
            .chunks(ITERATIONS)
            .map(|t| t.iter().sum::<f64>() / ITERATIONS as f64)
            .collect(),
    )
}
fn main() {
    let n = std::env::var("SUBSTRATE_BENCH_ROWS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(2000);
    assert!((10..=100_000).contains(&n), "choose 10..100000 people");
    let pg = std::env::var("SUBSTRATE_BENCH_PG_DSN").ok();
    let start = Instant::now();
    let db = fixture(n);
    let substrate_setup = start.elapsed().as_secs_f64() * 1000.;
    let sql = db.admin().snapshot().sql().unwrap();
    let authorized = db.session("reader").snapshot().sql().unwrap();
    let conn = Connection::open_in_memory().unwrap();
    let start = Instant::now();
    conn.execute_batch(&ddl(false)).unwrap();
    conn.execute_batch(&flatten(&inserts(n))).unwrap();
    let sqlite_setup = start.elapsed().as_secs_f64() * 1000.;
    let postgres_setup = pg.as_ref().map(|dsn| {
        let t = Instant::now();
        psql(dsn, &format!("{}{}", ddl(true), inserts(n)));
        t.elapsed().as_secs_f64() * 1000.
    });
    let mut measurements = vec![];
    for (name, query) in workloads(n) {
        let mut ordinary = conn.prepare(&flatten(&query)).unwrap();
        let mut liquid = sql.prepare(&query).unwrap();
        let mut restricted = authorized.prepare(&query).unwrap();
        let expected = sqlite_rows(&mut ordinary);
        assert_eq!(
            canonical(liquid.query(&[]).unwrap().rows),
            expected,
            "Substrate admin: {name}"
        );
        assert_eq!(
            canonical(restricted.query(&[]).unwrap().rows),
            expected,
            "Substrate reader: {name}"
        );
        if let Some(dsn) = &pg {
            assert_eq!(pg_rows(dsn, &query), expected, "PostgreSQL: {name}");
        }
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&expected).unwrap())
        );
        let sqlite = measure(|| {
            black_box(sqlite_rows(&mut ordinary));
        });
        let substrate_admin = measure(|| {
            black_box(canonical(liquid.query(&[]).unwrap().rows));
        });
        let substrate_reader = measure(|| {
            black_box(canonical(restricted.query(&[]).unwrap().rows));
        });
        let postgres = pg.as_ref().map(|dsn| pg_measure(dsn, &query));
        let explain = format!("EXPLAIN QUERY PLAN {query}");
        let sqlite_plan = sqlite_rows(&mut conn.prepare(&flatten(&explain)).unwrap());
        let substrate_plan = canonical(sql.query(&explain, &[]).unwrap().rows);
        let postgres_plan = pg.as_ref().map(|dsn| {
            serde_json::from_str::<J>(&psql(dsn, &format!("EXPLAIN (FORMAT JSON) {query};")))
                .unwrap()
        });
        measurements.push(json!({"name":name,"sql":query,"rows":expected.len(),"sha256":digest,"sqlite":sqlite,"substrate_admin":substrate_admin,"substrate_reader":substrate_reader,"postgres_psql":postgres,"plans":{"sqlite":sqlite_plan,"substrate":substrate_plan,"postgres":postgres_plan}}));
        eprintln!("verified and measured {name}");
    }
    let postgres_version = pg
        .as_ref()
        .map(|dsn| psql(dsn, "SELECT version();").trim().to_owned());
    println!("{}",serde_json::to_string_pretty(&json!({"people":n,"occurrences":n*9,"children_per_app_per_person":2,"samples":SAMPLES,"iterations":ITERATIONS,"warm_cache":true,"prepared":true,"environment":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"sqlite":rusqlite::version(),"substrate":env!("CARGO_PKG_VERSION"),"postgres":postgres_version},"setup_ms":{"substrate":substrate_setup,"sqlite":sqlite_setup,"postgres":postgres_setup},"postgres_measured":pg.is_some(),"postgres_timing":"psql client elapsed incl local IPC/protocol, excluding process startup; not comparable engine-only latency","measurements":measurements})).unwrap());
}
