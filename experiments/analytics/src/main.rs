//! Isolated analytical bridge experiment. Fixture/query ladder copied from benches/comparison.rs.
use rusqlite::{Connection, types::Value as S};
use serde_json::{Value as J, json};
use sha2::{Digest, Sha256};
use std::{hint::black_box, time::Instant};
use substrate_liquid::*;

const SAMPLES: usize = 5;
const ITERATIONS: usize = 3;
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
    let mut queries = vec![
        ("01_primary_key",format!(r#"SELECT id,name FROM core."Person" WHERE id='{id}'"#)),
        ("02_indexed_equality",r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes"=45 ORDER BY id"#.into()),
        ("03_range_top_100",r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes">=40 AND "durationMinutes"<60 ORDER BY "durationMinutes",id LIMIT 100"#.into()),
        ("04_relationship_lookup",format!(r#"SELECT f.id,w.id,w."durationMinutes" FROM fitness."Performed" f JOIN fitness."Workout" w ON w.id=f.workout WHERE f.athlete='{id}' ORDER BY f.id"#)),
        ("05_cross_app_five_way",format!(r#"SELECT p.id,f.id,w."durationMinutes",c.id,d.amount FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout JOIN crm."CustomerDeal" c ON c.customer=p.id JOIN crm."Deal" d ON d.id=c.deal WHERE p.id='{id}' ORDER BY f.id,c.id"#)),
        ("06_cross_app_group_top_25",r#"SELECT p.id,COUNT(*),SUM(w."durationMinutes"),SUM(d.amount) FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout JOIN crm."CustomerDeal" c ON c.customer=p.id JOIN crm."Deal" d ON d.id=c.deal GROUP BY p.id ORDER BY SUM(d.amount) DESC,p.id LIMIT 25"#.into()),
        ("07_correlated_exists",r#"SELECT p.id,p.name FROM core."Person" p WHERE EXISTS (SELECT 1 FROM fitness."Performed" f JOIN fitness."Workout" w ON w.id=f.workout WHERE f.athlete=p.id AND w."durationMinutes">110) ORDER BY p.id LIMIT 100"#.into()),
        ("09_full_scan_aggregate",r#"SELECT COUNT(*),SUM("durationMinutes"),MIN("durationMinutes"),MAX("durationMinutes") FROM fitness."Workout""#.into()),
        ("08_filtered_nested_top_1",r#"SELECT person_id,link_id,workout_id,minutes FROM (SELECT p.id AS person_id,f.id AS link_id,w.id AS workout_id,w."durationMinutes" AS minutes,ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY w."durationMinutes" DESC,f.id) AS rn FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout WHERE w."durationMinutes">=30) ranked WHERE rn<=1 ORDER BY person_id LIMIT 100"#.into()),
    ];
    queries.sort_by_key(|(name, _)| *name);
    queries
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
    let samples = means.clone();
    means.sort_by(f64::total_cmp);
    json!({"median_us":means[means.len()/2],"min_us":means[0],"max_us":means[means.len()-1],"samples_us":samples})
}

fn exported(snapshot: &Snapshot) -> Vec<J> {
    let mut tables = Vec::new();
    for name in [
        "core.Person",
        "fitness.Workout",
        "fitness.Performed",
        "crm.Deal",
        "crm.CustomerDeal",
    ] {
        let def = snapshot.schema(name).unwrap();
        let fields: Vec<J> = std::iter::once(json!({"name":"id","type":"utf8"}))
            .chain(def.roles.iter().map(|role| json!({"name":role.name,"type": match role.data_type { DataType::Int | DataType::Decimal{..} => "int64", DataType::Text | DataType::Ref(_) => "utf8", _ => panic!("unsupported fixture type") }}))).collect();
        let mut columns: Vec<Vec<J>> = vec![vec![]; fields.len()];
        let mut after = None;
        loop {
            let page = snapshot.page(name, None, after, 256).unwrap();
            for row in page.rows {
                columns[0].push(json!(row.id.to_string()));
                for (i, value) in row.values.iter().enumerate() {
                    columns[i + 1].push(match value {
                        Value::Int(n) | Value::Decimal(n) => json!(n),
                        Value::Text(s) => json!(s),
                        Value::Ref(id) => json!(id.to_string()),
                        _ => panic!("unsupported fixture value"),
                    });
                }
            }
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        tables.push(json!({"name":name,"fields":fields,"columns":columns}));
    }
    tables
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let n = args.get(1).expect("people count").parse::<usize>().unwrap();
    assert!((10..=20_000).contains(&n));
    let started = Instant::now();
    let db = fixture(n);
    let fixture_ms = started.elapsed().as_secs_f64() * 1000.;
    let reader = db.session("reader");
    let snapshot = reader.snapshot();
    let started = Instant::now();
    let tables = exported(&snapshot);
    let export_ms = started.elapsed().as_secs_f64() * 1000.;
    let started = Instant::now();
    let serialized_tables = serde_json::to_vec(&tables).unwrap();
    let serialize_ms = started.elapsed().as_secs_f64() * 1000.;
    let sqlite = Connection::open_in_memory().unwrap();
    sqlite.execute_batch(&ddl(false)).unwrap();
    sqlite.execute_batch(&flatten(&inserts(n))).unwrap();
    let started = Instant::now();
    let sql = snapshot.sql().unwrap();
    let sql_setup_ms = started.elapsed().as_secs_f64() * 1000.;
    let mut measurements = vec![];
    for (name, text) in workloads(n) {
        let mut ordinary = sqlite.prepare(&flatten(&text)).unwrap();
        let expected = sqlite_rows(&mut ordinary);
        let mut liquid = sql.prepare(&text).unwrap();
        let actual = canonical(liquid.query(&[]).unwrap().rows);
        assert_eq!(actual, expected, "{name}");
        let sqlite_us = measure(|| {
            black_box(sqlite_rows(&mut ordinary));
        });
        let substrate_us = measure(|| {
            black_box(canonical(liquid.query(&[]).unwrap().rows));
        });
        measurements.push(json!({"name":name,"sql":text,"expected":expected,"sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&expected).unwrap())),"sqlite":sqlite_us,"substrate_reader":substrate_us}));
    }
    // The bridge is a trusted-host export, never a long-lived app authorization token.
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    assert!(snapshot.page("core.Person", None, None, 1).is_err());
    let report = json!({"sqlite_version":rusqlite::version(),"people":n,"occurrences":n*9,"samples":SAMPLES,"iterations":ITERATIONS,"fixture_ms":fixture_ms,"authorized_export_ms":export_ms,"serialize_columns_ms":serialize_ms,"serialized_columns_bytes":serialized_tables.len(),"substrate_sql_setup_ms":sql_setup_ms,"tables":tables,"measurements":measurements,"source_revocation_checked":true});
    std::fs::write(
        args.get(2).expect("output path"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn export_preserves_source_row_filter() {
        let db = fixture(10);
        let mut tx = db.admin().transaction();
        tx.grant(
            "reader",
            "core.Person",
            Grant {
                filter: RowFilter::Equals {
                    role: "name".into(),
                    value: "Person000003".into(),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let tables = exported(&db.session("reader").snapshot());
        assert_eq!(tables[0]["columns"][0], json!([person(3).to_string()]));
        assert_eq!(tables[0]["columns"][1], json!(["Person000003"]));
    }
    #[test]
    fn exports_authorized_typed_columns_and_preserves_identity_and_integer_units() {
        let db = fixture(10);
        let s = db.session("reader").snapshot();
        let tables = exported(&s);
        assert_eq!(tables.len(), 5);
        assert_eq!(tables[0]["columns"][0].as_array().unwrap().len(), 10);
        assert_eq!(tables[0]["columns"][0][0], person(0).to_string());
        assert_eq!(tables[3]["columns"][1][0], 100);
        assert!(
            db.session("stranger")
                .snapshot()
                .schema("core.Person")
                .is_err()
        );
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
        assert!(s.page("core.Person", None, None, 1).is_err());
    }
}
