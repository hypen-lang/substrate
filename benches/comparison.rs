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
// Prepared fixture loading avoids benchmarking a giant SQL script parser.
// Fixture setup remains excluded from all read measurements.
fn load_sqlite(conn: &mut Connection, n: usize) {
    let tx = conn.transaction().unwrap();
    {
        let mut person_insert = tx.prepare("INSERT INTO Person VALUES (?1,?2)").unwrap();
        let mut workout = tx.prepare("INSERT INTO Workout VALUES (?1,?2)").unwrap();
        let mut performed = tx
            .prepare("INSERT INTO Performed VALUES (?1,?2,?3)")
            .unwrap();
        let mut deal = tx.prepare("INSERT INTO Deal VALUES (?1,?2)").unwrap();
        let mut customer = tx
            .prepare("INSERT INTO CustomerDeal VALUES (?1,?2,?3)")
            .unwrap();
        for i in 0..n {
            let p = person(i).to_string();
            person_insert
                .execute(rusqlite::params![p, format!("Person{i:06}")])
                .unwrap();
            for j in 0..2 {
                let w = occurrence(1, i, j).to_string();
                let f = occurrence(2, i, j).to_string();
                let d = occurrence(3, i, j).to_string();
                let c = occurrence(4, i, j).to_string();
                workout
                    .execute(rusqlite::params![w, ((i * 7 + j * 19) % 120) as i64])
                    .unwrap();
                performed.execute(rusqlite::params![f, p, w]).unwrap();
                deal.execute(rusqlite::params![d, ((i + 1) * 100 + j * 17) as i64])
                    .unwrap();
                customer.execute(rusqlite::params![c, p, d]).unwrap();
            }
        }
    }
    tx.commit().unwrap();
    conn.execute_batch("ANALYZE").unwrap();
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
        ("09_full_scan_aggregate",r#"SELECT COUNT(*),SUM("durationMinutes"),MIN("durationMinutes"),MAX("durationMinutes") FROM fitness."Workout""#.into()),
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
    sqlite_rows_with(statement, &[])
}
fn sqlite_rows_with(statement: &mut rusqlite::Statement<'_>, params: &[S]) -> Rows {
    let count = statement.column_count();
    canonical(
        statement
            .query_map(rusqlite::params_from_iter(params), |r| {
                (0..count).map(|c| r.get(c)).collect()
            })
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
    let samples = means.clone();
    means.sort_by(f64::total_cmp);
    json!({"median_us":means[means.len()/2],"min_us":means[0],"max_us":means[means.len()-1],"samples_us":samples})
}
fn timed<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed().as_secs_f64() * 1000.)
}
fn locked_version(package: &str) -> &'static str {
    let name = format!("name = \"{package}\"");
    include_str!("../Cargo.lock")
        .split("[[package]]")
        .find(|block| block.lines().any(|line| line == name))
        .and_then(|block| {
            block
                .lines()
                .find_map(|line| line.strip_prefix("version = \""))
        })
        .and_then(|version| version.strip_suffix('"'))
        .expect("benchmark dependency must be in Cargo.lock")
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
fn measure_rebinding(
    label: &str,
    cases: &[Vec<S>],
    expected: &[Rows],
    mut query: impl FnMut(&[S]) -> Rows,
) -> J {
    for (params, expected) in cases.iter().zip(expected) {
        assert_eq!(&query(params), expected, "{label}: before rebinding");
    }
    let mut cursor = 0;
    let timing = measure(|| {
        black_box(query(&cases[cursor % cases.len()]));
        cursor += 1;
    });
    for (params, expected) in cases.iter().zip(expected) {
        assert_eq!(&query(params), expected, "{label}: after rebinding");
    }
    timing
}
fn parameter_workloads(
    n: usize,
    conn: &Connection,
    admin: &SqlSnapshot,
    reader: &SqlSnapshot,
    compat: &SqliteSnapshot,
    compat_reader: &SqliteSnapshot,
) -> Vec<J> {
    let cases = [
        (
            "id_same_value",
            r#"SELECT id,name FROM core."Person" WHERE id=?1"#,
            vec![vec![S::Text(person(n / 2).to_string())]],
        ),
        (
            "id_16_values",
            r#"SELECT id,name FROM core."Person" WHERE id=?1"#,
            (0..16)
                .map(|i| vec![S::Text(person((i * 97) % n).to_string())])
                .collect::<Vec<_>>(),
        ),
        (
            "numeric_equality_16_values",
            r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes"=?1 ORDER BY id"#,
            (0..16).map(|i| vec![S::Integer(i * 7)]).collect(),
        ),
        (
            "numeric_range_16_values",
            r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes">=?1 AND "durationMinutes"<?2 ORDER BY "durationMinutes",id LIMIT 10"#,
            (0..16)
                .map(|i| vec![S::Integer(i * 7), S::Integer(i * 7 + 5)])
                .collect(),
        ),
    ];
    cases.into_iter().map(|(name, sql, parameters)| {
        let query = flatten(sql);
        let (mut sqlite, sqlite_prepare_ms) = timed(|| conn.prepare(&query).unwrap());
        let (mut liquid, admin_prepare_ms) = timed(|| admin.prepare(sql).unwrap());
        let (mut authorized, reader_prepare_ms) = timed(|| reader.prepare(sql).unwrap());
        let (mut old, compat_prepare_ms) = timed(|| compat.prepare(sql).unwrap());
        let (mut old_reader, compat_reader_prepare_ms) = timed(|| compat_reader.prepare(sql).unwrap());
        let expected = parameters.iter().map(|params| sqlite_rows_with(&mut sqlite, params)).collect::<Vec<_>>();
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&expected).unwrap()));
        let ordinary = measure_rebinding(name, &parameters, &expected, |params| sqlite_rows_with(&mut sqlite, params));
        let substrate_admin = measure_rebinding(name, &parameters, &expected, |params| canonical(liquid.query(params).unwrap().rows));
        let substrate_reader = measure_rebinding(name, &parameters, &expected, |params| canonical(authorized.query(params).unwrap().rows));
        let sqlite_compat_admin = measure_rebinding(name, &parameters, &expected, |params| canonical(old.query(params).unwrap().rows));
        let sqlite_compat_reader = measure_rebinding(name, &parameters, &expected, |params| canonical(old_reader.query(params).unwrap().rows));
        json!({"name":name,"sql":sql,"distinct_parameter_sets":parameters.len(),"sha256":digest,"repeat_verified":true,"prepare_ms":{"sqlite":sqlite_prepare_ms,"substrate_admin":admin_prepare_ms,"substrate_reader":reader_prepare_ms,"sqlite_compat_admin":compat_prepare_ms,"sqlite_compat_reader":compat_reader_prepare_ms},"sqlite":ordinary,"substrate_admin":substrate_admin,"substrate_reader":substrate_reader,"sqlite_compat_admin":sqlite_compat_admin,"sqlite_compat_reader":sqlite_compat_reader})
    }).collect()
}
fn projection_workload() -> J {
    const ROWS: usize = 256;
    const PAYLOAD_BYTES: usize = 16 * 1024;
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "bench",
        1,
        vec![],
        vec![Relation::new(
            "bench.Payload",
            vec![
                Role::new("n", DataType::Int),
                Role::new("payload", DataType::Text),
                Role::new("visible", DataType::Bool),
            ],
        )],
    ))
    .unwrap();
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE Payload(id TEXT PRIMARY KEY,n INTEGER NOT NULL,payload TEXT NOT NULL,visible INTEGER NOT NULL);CREATE INDEX payload_n ON Payload(n);CREATE INDEX payload_visible ON Payload(visible,n);").unwrap();
    {
        let sqlite_tx = conn.transaction().unwrap();
        let mut insert = sqlite_tx
            .prepare("INSERT INTO Payload VALUES(?1,?2,?3,?4)")
            .unwrap();
        for i in 0..ROWS {
            let id = Id::from_u128(i as u128 + 1);
            let payload = "x".repeat(PAYLOAD_BYTES);
            tx.insert_with_id(
                "bench.Payload",
                id,
                vec![
                    Value::Int(i as i64),
                    Value::Text(payload.clone()),
                    Value::Bool(i % 2 == 0),
                ],
            )
            .unwrap();
            insert
                .execute(rusqlite::params![
                    id.to_string(),
                    i as i64,
                    payload,
                    i % 2 == 0
                ])
                .unwrap();
        }
        drop(insert);
        sqlite_tx.commit().unwrap();
    }
    tx.grant(
        "reader",
        "bench.Payload",
        Grant {
            filter: RowFilter::Equals {
                role: "visible".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let (admin, admin_catalog_ms) = timed(|| db.admin().snapshot().sql().unwrap());
    let (reader, reader_catalog_ms) = timed(|| db.session("reader").snapshot().sql().unwrap());
    let (compat, compat_catalog_ms) = timed(|| db.admin().snapshot().sqlite_compat().unwrap());
    let (compat_reader, compat_reader_catalog_ms) =
        timed(|| db.session("reader").snapshot().sqlite_compat().unwrap());
    let mut timings = Vec::new();
    for (label, query) in [
        (
            "project_without_payload",
            "SELECT n FROM bench.Payload ORDER BY n",
        ),
        (
            "count_without_payload",
            "SELECT COUNT(*) FROM bench.Payload",
        ),
    ] {
        let plain = query.replace("bench.Payload", "Payload");
        let filtered = plain.replace("Payload", "Payload WHERE visible=1");
        let mut ordinary = conn.prepare(&plain).unwrap();
        let mut ordinary_reader = conn.prepare(&filtered).unwrap();
        let mut liquid = admin.prepare(query).unwrap();
        let mut authorized = reader.prepare(query).unwrap();
        let mut old = compat.prepare(query).unwrap();
        let mut old_reader = compat_reader.prepare(query).unwrap();
        let expected = vec![sqlite_rows(&mut ordinary)];
        let expected_reader = vec![sqlite_rows(&mut ordinary_reader)];
        let params = vec![vec![]];
        let sqlite = measure_rebinding(label, &params, &expected, |p| {
            sqlite_rows_with(&mut ordinary, p)
        });
        let sqlite_filtered = measure_rebinding(label, &params, &expected_reader, |p| {
            sqlite_rows_with(&mut ordinary_reader, p)
        });
        let substrate_admin = measure_rebinding(label, &params, &expected, |p| {
            canonical(liquid.query(p).unwrap().rows)
        });
        let substrate_reader = measure_rebinding(label, &params, &expected_reader, |p| {
            canonical(authorized.query(p).unwrap().rows)
        });
        let sqlite_compat_admin = measure_rebinding(label, &params, &expected, |p| {
            canonical(old.query(p).unwrap().rows)
        });
        let sqlite_compat_reader = measure_rebinding(label, &params, &expected_reader, |p| {
            canonical(old_reader.query(p).unwrap().rows)
        });
        timings.push(json!({"name":label,"repeat_verified":true,"admin_rows":expected[0].len(),"reader_rows":expected_reader[0].len(),"admin_sha256":format!("{:x}", Sha256::digest(serde_json::to_vec(&expected).unwrap())),"reader_sha256":format!("{:x}", Sha256::digest(serde_json::to_vec(&expected_reader).unwrap())),"sqlite":sqlite,"sqlite_filtered":sqlite_filtered,"substrate_admin":substrate_admin,"substrate_reader":substrate_reader,"sqlite_compat_admin":sqlite_compat_admin,"sqlite_compat_reader":sqlite_compat_reader}));
    }
    json!({"fixture_rows":ROWS,"unselected_payload_bytes_per_row":PAYLOAD_BYTES,"row_policy":"reader sees visible=true rows only; direct SQLite reader query has equivalent WHERE","catalog_ms":{"substrate_admin":admin_catalog_ms,"substrate_reader":reader_catalog_ms,"sqlite_compat_admin":compat_catalog_ms,"sqlite_compat_reader":compat_reader_catalog_ms},"measurements":timings})
}

// A new SQL handle gives each execution an unprimed authorized Arrow-page cache.
// The native database and CPU/OS caches are already warm; this is not cold IO.
fn fresh_handles(db: &Database, sql: &str, expected: &Rows) -> J {
    let mut result = serde_json::Map::new();
    for principal in ["admin", "reader"] {
        let session = if principal == "admin" {
            db.admin()
        } else {
            db.session("reader")
        };
        let (handle, catalog_ms) = timed(|| session.snapshot().sql().unwrap());
        let (mut prepared, prepare_ms) = timed(|| handle.prepare(sql).unwrap());
        let (rows, execution_ms) = timed(|| canonical(prepared.query(&[]).unwrap().rows));
        assert_eq!(&rows, expected, "fresh DataFusion {principal}: {sql}");
        result.insert(
            format!("substrate_{principal}"),
            json!({"catalog_ms":catalog_ms,"prepare_ms":prepare_ms,"execution_ms":execution_ms}),
        );
        let (handle, catalog_ms) = timed(|| session.snapshot().sqlite_compat().unwrap());
        let (mut prepared, prepare_ms) = timed(|| handle.prepare(sql).unwrap());
        let (rows, execution_ms) = timed(|| canonical(prepared.query(&[]).unwrap().rows));
        assert_eq!(&rows, expected, "fresh compatibility {principal}: {sql}");
        result.insert(
            format!("sqlite_compat_{principal}"),
            json!({"catalog_ms":catalog_ms,"prepare_ms":prepare_ms,"execution_ms":execution_ms}),
        );
    }
    J::Object(result)
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
    let (sql, admin_catalog_ms) = timed(|| db.admin().snapshot().sql().unwrap());
    let (authorized, reader_catalog_ms) = timed(|| db.session("reader").snapshot().sql().unwrap());
    let (compat, compat_admin_catalog_ms) =
        timed(|| db.admin().snapshot().sqlite_compat().unwrap());
    let (compat_reader, compat_reader_catalog_ms) =
        timed(|| db.session("reader").snapshot().sqlite_compat().unwrap());
    let mut conn = Connection::open_in_memory().unwrap();
    let start = Instant::now();
    conn.execute_batch(&ddl(false)).unwrap();
    load_sqlite(&mut conn, n);
    let sqlite_setup = start.elapsed().as_secs_f64() * 1000.;
    let postgres_setup = pg.as_ref().map(|dsn| {
        let t = Instant::now();
        psql(dsn, &format!("{}{}", ddl(true), inserts(n)));
        t.elapsed().as_secs_f64() * 1000.
    });
    let mut measurements = vec![];
    for (name, query) in workloads(n) {
        let flattened = flatten(&query);
        let (mut ordinary, sqlite_prepare_ms) = timed(|| conn.prepare(&flattened).unwrap());
        let (mut liquid, admin_prepare_ms) = timed(|| sql.prepare(&query).unwrap());
        let (mut restricted, reader_prepare_ms) = timed(|| authorized.prepare(&query).unwrap());
        let (mut old, compat_admin_prepare_ms) = timed(|| compat.prepare(&query).unwrap());
        let (mut old_reader, compat_reader_prepare_ms) =
            timed(|| compat_reader.prepare(&query).unwrap());
        let (expected, sqlite_first_ms) = timed(|| sqlite_rows(&mut ordinary));
        let (old_rows, compat_admin_first_ms) = timed(|| canonical(old.query(&[]).unwrap().rows));
        let (old_reader_rows, compat_reader_first_ms) =
            timed(|| canonical(old_reader.query(&[]).unwrap().rows));
        let (liquid_rows, admin_first_ms) = timed(|| canonical(liquid.query(&[]).unwrap().rows));
        let (reader_rows, reader_first_ms) =
            timed(|| canonical(restricted.query(&[]).unwrap().rows));
        assert_eq!(old_rows, expected, "compat admin: {name}");
        assert_eq!(old_reader_rows, expected, "compat reader: {name}");
        assert_eq!(liquid_rows, expected, "Substrate admin: {name}");
        assert_eq!(reader_rows, expected, "Substrate reader: {name}");
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
        let sqlite_compat_admin = measure(|| {
            black_box(canonical(old.query(&[]).unwrap().rows));
        });
        let sqlite_compat_reader = measure(|| {
            black_box(canonical(old_reader.query(&[]).unwrap().rows));
        });
        assert_eq!(
            canonical(old.query(&[]).unwrap().rows),
            expected,
            "compat admin repeat: {name}"
        );
        assert_eq!(
            canonical(old_reader.query(&[]).unwrap().rows),
            expected,
            "compat reader repeat: {name}"
        );
        // Repeated physical execution must not retain LIMIT/dynamic-filter state.
        assert_eq!(
            sqlite_rows(&mut ordinary),
            expected,
            "SQLite repeat: {name}"
        );
        assert_eq!(
            canonical(liquid.query(&[]).unwrap().rows),
            expected,
            "Substrate admin repeat: {name}"
        );
        assert_eq!(
            canonical(restricted.query(&[]).unwrap().rows),
            expected,
            "Substrate reader repeat: {name}"
        );
        let postgres = pg.as_ref().map(|dsn| pg_measure(dsn, &query));
        let sqlite_plan = sqlite_rows(
            &mut conn
                .prepare(&flatten(&format!("EXPLAIN QUERY PLAN {query}")))
                .unwrap(),
        );
        let substrate_plan = canonical(sql.query(&format!("EXPLAIN {query}"), &[]).unwrap().rows);
        let postgres_plan = pg.as_ref().map(|dsn| {
            serde_json::from_str::<J>(&psql(dsn, &format!("EXPLAIN (FORMAT JSON) {query};")))
                .unwrap()
        });
        let fresh_handle = fresh_handles(&db, &query, &expected);
        measurements.push(json!({"name":name,"sql":query,"rows":expected.len(),"sha256":digest,"repeat_verified":true,"prepare_ms":{"sqlite":sqlite_prepare_ms,"substrate_admin":admin_prepare_ms,"substrate_reader":reader_prepare_ms,"sqlite_compat_admin":compat_admin_prepare_ms,"sqlite_compat_reader":compat_reader_prepare_ms},"fresh_handle":fresh_handle,"first_execution_ms":{"sqlite":sqlite_first_ms,"substrate_admin":admin_first_ms,"substrate_reader":reader_first_ms,"sqlite_compat_admin":compat_admin_first_ms,"sqlite_compat_reader":compat_reader_first_ms},"sqlite_compat_admin":sqlite_compat_admin,"sqlite_compat_reader":sqlite_compat_reader,"sqlite":sqlite,"substrate_admin":substrate_admin,"substrate_reader":substrate_reader,"postgres_psql":postgres,"plans":{"sqlite":sqlite_plan,"substrate":substrate_plan,"postgres":postgres_plan}}));
        eprintln!("verified and measured {name}");
    }
    let rebinding = parameter_workloads(n, &conn, &sql, &authorized, &compat, &compat_reader);
    let projection = projection_workload();
    let postgres_version = pg
        .as_ref()
        .map(|dsn| psql(dsn, "SELECT version();").trim().to_owned());
    println!("{}",serde_json::to_string_pretty(&json!({"people":n,"occurrences":n*9,"children_per_app_per_person":2,"samples":SAMPLES,"iterations":ITERATIONS,"warm_cache":true,"prepared":true,"environment":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"sqlite":rusqlite::version(),"substrate":env!("CARGO_PKG_VERSION"),"sql_engine":"DataFusion","datafusion_facade":datafusion::DATAFUSION_VERSION,"datafusion_execution":locked_version("datafusion-physical-plan"),"imbl":locked_version("imbl"),"postgres":postgres_version},"setup_ms":{"substrate":substrate_setup,"substrate_admin_sql_catalog":admin_catalog_ms,"substrate_reader_sql_catalog":reader_catalog_ms,"sqlite_compat_admin_catalog":compat_admin_catalog_ms,"sqlite_compat_reader_catalog":compat_reader_catalog_ms,"sqlite":sqlite_setup,"postgres":postgres_setup},"postgres_measured":pg.is_some(),"postgres_timing":"psql client elapsed incl local IPC/protocol, excluding process startup; not comparable engine-only latency","measurements":measurements,"parameter_rebinding":rebinding,"projection":projection})).unwrap());
}
