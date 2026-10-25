//! Additive durable schema control while an existing snapshot reader runs.
//! SQLite raw DDL is a storage reference, not an equivalent authorization workflow.
use rusqlite::{Connection, params, types::Value as S};
use serde_json::{Value as J, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Instant,
};
use substrate_liquid::{schema_control::*, *};
const INSTALLS: u64 = 8; // First install warms the path; seven fresh installs measured.
fn micros(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e6
}
fn stats(values: impl Iterator<Item = f64>) -> J {
    let mut v: Vec<_> = values.collect();
    v.sort_by(f64::total_cmp);
    json!({"median_us":v[v.len()/2],"min_us":v[0],"max_us":v[v.len()-1]})
}
fn digest(rows: &[Vec<String>]) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(rows).unwrap()))
}
fn id(i: usize) -> Id {
    Id::from_u128(i as u128 + 1)
}
fn module(version: u64) -> Module {
    Module::new(
        "agent",
        version,
        vec![("core".into(), 1)],
        vec![
            Relation::new(
                &format!("agent.Extension{version}"),
                vec![
                    Role::new("person", DataType::Ref("core.Person".into())),
                    Role::new("value", DataType::Int),
                ],
            )
            .unique(&["person"]),
        ],
    )
}
fn expected(n: usize) -> Vec<Vec<String>> {
    (0..n)
        .map(|i| vec![id(i).to_string(), format!("Person{i:06}")])
        .collect()
}
fn liquid_rows(db: &Database) -> Vec<Vec<String>> {
    db.session("agent")
        .snapshot()
        .sql()
        .unwrap()
        .query("SELECT id,name FROM core.Person ORDER BY id", &[])
        .unwrap()
        .rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| match v {
                    S::Text(t) => t,
                    _ => panic!("unexpected scalar"),
                })
                .collect()
        })
        .collect()
}
fn substrate(n: usize, path: &std::path::Path) -> J {
    let setup = Instant::now();
    let db = Database::open(path).unwrap();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    for i in 0..n {
        tx.insert_with_id(
            "core.Person",
            id(i),
            vec![format!("Person{i:06}").as_str().into()],
        )
        .unwrap();
    }
    tx.grant(
        "agent",
        "core.Person",
        Grant {
            reference: true,
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db.configure_namespace(NamespacePolicy {
        namespace: "agent".into(),
        owner: "agent".into(),
        auto_approve: false,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
    let setup_us = micros(setup);
    let baseline = expected(n);
    assert_eq!(liquid_rows(&db), baseline);
    let pinned = db.session("agent").snapshot();
    let reader_snapshot = pinned.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = stop.clone();
    let (ready, wait) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let sql = reader_snapshot.sql().unwrap();
        let mut q = sql
            .prepare("SELECT name FROM core.Person WHERE id=?1")
            .unwrap();
        let parameters = [S::Text(id(0).to_string())];
        assert_eq!(
            q.query(&parameters).unwrap().rows,
            vec![vec![S::Text("Person000000".into())]]
        );
        ready.send(()).unwrap();
        let mut count = 0;
        let mut max_us = 0f64;
        while !reader_stop.load(Ordering::Acquire) {
            let t = Instant::now();
            assert_eq!(
                q.query(&parameters).unwrap().rows,
                vec![vec![S::Text("Person000000".into())]]
            );
            max_us = max_us.max(micros(t));
            count += 1;
        }
        assert!(reader_snapshot.schema("agent.Extension1").is_err());
        json!({"successful_queries":count,"max_observed_query_us":max_us})
    });
    wait.recv().unwrap();
    let session = db.session("agent");
    let mut samples = vec![];
    for version in 1..=INSTALLS {
        let declaration = module(version);
        let name = declaration.relations[0].name.clone();
        let t = Instant::now();
        let proposal = session.propose_schema(Id::new(), declaration).unwrap();
        let propose_us = micros(t);
        assert_eq!(proposal.status, ProposalStatus::Pending);
        assert_eq!(proposal.plan.existing_record_rewrites, 0);
        let t = Instant::now();
        db.decide_schema_proposal(proposal.id, &proposal.plan.digest, true)
            .unwrap();
        let approve_us = micros(t);
        let t = Instant::now();
        let receipt = session
            .apply_schema_proposal(proposal.id, &proposal.plan.digest)
            .unwrap();
        let apply_us = micros(t);
        assert_eq!(receipt.status, ProposalStatus::Installed);
        let t = Instant::now();
        let descriptor = session.snapshot().describe(&name).unwrap();
        let describe_us = micros(t);
        let t = Instant::now();
        let occurrence = session
            .transact(RetryPolicy::default(), |tx| {
                tx.insert_dynamic(
                    &descriptor,
                    [
                        ("person".into(), id(0).into()),
                        ("value".into(), Value::Int(version as i64)),
                    ]
                    .into(),
                )
            })
            .unwrap();
        let first_write_us = micros(t);
        let t = Instant::now();
        let row = session
            .snapshot()
            .get_dynamic(&descriptor, occurrence)
            .unwrap()
            .unwrap();
        let first_read_us = micros(t);
        assert_eq!(row["person"], Value::Ref(id(0)));
        assert_eq!(row["value"], Value::Int(version as i64));
        assert!(pinned.schema(&name).is_err());
        samples.push(json!({"version":version,"warmup":version==1,"propose_us":propose_us,"approve_us":approve_us,"apply_us":apply_us,"pipeline_us":propose_us+approve_us+apply_us,"describe_us":describe_us,"first_write_us":first_write_us,"first_read_us":first_read_us}));
    }
    stop.store(true, Ordering::Release);
    let reader = reader.join().unwrap();
    assert!(reader["successful_queries"].as_u64().unwrap() > 0);
    assert_eq!(liquid_rows(&db), baseline);
    assert_eq!(
        pinned.scan("core.Person").unwrap(),
        session.snapshot().scan("core.Person").unwrap()
    );
    db.admin().snapshot().verify_indexes().unwrap();
    let info = db.storage_info().unwrap();
    let summary: serde_json::Map<_, _> = [
        "propose_us",
        "approve_us",
        "apply_us",
        "pipeline_us",
        "describe_us",
        "first_write_us",
        "first_read_us",
    ]
    .into_iter()
    .map(|field| {
        (
            field.to_string(),
            stats(samples.iter().skip(1).map(|s| s[field].as_f64().unwrap())),
        )
    })
    .collect();
    json!({"setup_us":setup_us,"summary":summary,"samples":samples,"reader":reader,"original_rows_sha256":digest(&baseline),"original_rows_unchanged":true,"existing_record_rewrites_claimed":0,"log_commits_after":info.log_commits,"checkpoint_revision_after":info.checkpoint_revision})
}
fn sqlite_rows(connection: &Connection) -> Vec<Vec<String>> {
    connection
        .prepare("SELECT id,name FROM Person ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok(vec![r.get(0)?, r.get(1)?]))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}
fn sqlite(n: usize, path: &std::path::Path) -> J {
    let t = Instant::now();
    let mut connection = Connection::open(path).unwrap();
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; CREATE TABLE Person(id TEXT PRIMARY KEY,name TEXT NOT NULL); CREATE INDEX person_name ON Person(name);").unwrap();
    let tx = connection.transaction().unwrap();
    for i in 0..n {
        tx.execute(
            "INSERT INTO Person VALUES (?1,?2)",
            params![id(i).to_string(), format!("Person{i:06}")],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let setup_us = micros(t);
    let baseline = expected(n);
    assert_eq!(sqlite_rows(&connection), baseline);
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = stop.clone();
    let reader_path = path.to_owned();
    let (ready, wait) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let c = Connection::open(reader_path).unwrap();
        c.execute_batch("BEGIN;").unwrap();
        let mut query = c.prepare("SELECT name FROM Person WHERE id=?1").unwrap();
        let person = id(0).to_string();
        assert_eq!(
            query
                .query_row([&person], |r| r.get::<_, String>(0))
                .unwrap(),
            "Person000000"
        );
        ready.send(()).unwrap();
        let mut count = 0;
        let mut max_us = 0f64;
        while !reader_stop.load(Ordering::Acquire) {
            let t = Instant::now();
            assert_eq!(
                query
                    .query_row([&person], |r| r.get::<_, String>(0))
                    .unwrap(),
                "Person000000"
            );
            max_us = max_us.max(micros(t));
            count += 1;
        }
        assert_eq!(
            c.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'Extension%'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        c.execute_batch("ROLLBACK;").unwrap();
        json!({"successful_queries":count,"max_observed_query_us":max_us})
    });
    wait.recv().unwrap();
    let mut samples = vec![];
    for version in 1..=INSTALLS {
        let table = format!("Extension{version}");
        let t = Instant::now();
        connection.execute_batch(&format!("BEGIN; CREATE TABLE {table}(id TEXT PRIMARY KEY,person TEXT NOT NULL UNIQUE REFERENCES Person(id),value INTEGER NOT NULL); CREATE INDEX value_{version} ON {table}(value); COMMIT;")).unwrap();
        let ddl_us = micros(t);
        let t = Instant::now();
        let fields = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let describe_us = micros(t);
        assert_eq!(fields, vec!["id", "person", "value"]);
        let occurrence = Id::new().to_string();
        let t = Instant::now();
        connection
            .execute(
                &format!("INSERT INTO {table} VALUES (?1,?2,?3)"),
                params![occurrence, id(0).to_string(), version as i64],
            )
            .unwrap();
        let first_write_us = micros(t);
        let t = Instant::now();
        let actual = connection
            .query_row(
                &format!("SELECT person,value FROM {table} WHERE id=?1"),
                [&occurrence],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .unwrap();
        let first_read_us = micros(t);
        assert_eq!(actual, (id(0).to_string(), version as i64));
        samples.push(json!({"version":version,"warmup":version==1,"ddl_us":ddl_us,"describe_us":describe_us,"first_write_us":first_write_us,"first_read_us":first_read_us}));
    }
    stop.store(true, Ordering::Release);
    let reader = reader.join().unwrap();
    assert!(reader["successful_queries"].as_u64().unwrap() > 0);
    assert_eq!(sqlite_rows(&connection), baseline);
    let summary: serde_json::Map<_, _> =
        ["ddl_us", "describe_us", "first_write_us", "first_read_us"]
            .into_iter()
            .map(|field| {
                (
                    field.to_string(),
                    stats(samples.iter().skip(1).map(|s| s[field].as_f64().unwrap())),
                )
            })
            .collect();
    json!({"setup_us":setup_us,"summary":summary,"samples":samples,"reader":reader,"original_rows_sha256":digest(&baseline),"original_rows_unchanged":true})
}
fn main() {
    let temp = tempfile::tempdir().unwrap();
    let mut measurements = vec![];
    for people in [10, 2000] {
        let sub = substrate(people, &temp.path().join(format!("liquid-{people}.redb")));
        let sqlite = sqlite(people, &temp.path().join(format!("sqlite-{people}.db")));
        assert_eq!(sub["original_rows_sha256"], sqlite["original_rows_sha256"]);
        measurements.push(json!({"existing_people":people,"substrate":sub,"sqlite":sqlite}));
    }
    println!("{}",serde_json::to_string_pretty(&json!({"environment":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"sqlite":rusqlite::version()},"installs_per_fixture":INSTALLS,"measured_installs":INSTALLS-1,"warmup_installs":1,"concurrent_pinned_readers":1,"durable":true,"postgres_measured":false,"note":"Substrate full authorization workflow (three commits) vs raw SQLite WAL DDL (one commit); not equivalent security operations or storage implementations","measurements":measurements})).unwrap());
}
