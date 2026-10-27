//! Durable scale probe; run with experiments/scale/run.py. Never use production files.
use rusqlite::{Connection, types::Value as S};
use serde_json::{Value as J, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    sync::{Arc, Barrier, Mutex},
    time::{Duration, Instant},
};
use substrate_liquid::*;
#[path = "../experiments/scale/queries.rs"]
mod queries;
type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const FAMILIES: [&str; 5] = [
    "core.Person",
    "fitness.Workout",
    "fitness.Performed",
    "crm.Deal",
    "crm.CustomerDeal",
];
fn person(i: usize) -> Id {
    Id::from_u128(i as u128 + 1)
}
fn child(family: u128, i: usize, j: usize) -> Id {
    Id::from_u128((family << 64) | (i as u128 * 2 + j as u128 + 1))
}
fn emit(value: J) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
fn usage() -> libc::rusage {
    unsafe {
        let mut u = std::mem::zeroed();
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, &mut u), 0);
        u
    }
}
fn seconds(t: libc::timeval) -> f64 {
    t.tv_sec as f64 + t.tv_usec as f64 / 1e6
}
fn resource(before: &libc::rusage, wall: f64) -> J {
    let u = usage();
    let user = seconds(u.ru_utime) - seconds(before.ru_utime);
    let system = seconds(u.ru_stime) - seconds(before.ru_stime);
    let rss = u.ru_maxrss as u64 * if cfg!(target_os = "macos") { 1 } else { 1024 };
    json!({"wall_seconds":wall,"user_cpu_seconds":user,"system_cpu_seconds":system,"cpu_core_equivalents":(user+system)/wall.max(1e-9),"process_lifetime_peak_rss_bytes":rss,"block_input_operations":u.ru_inblock-before.ru_inblock,"block_output_operations":u.ru_oublock-before.ru_oublock,"voluntary_context_switches":u.ru_nvcsw-before.ru_nvcsw,"involuntary_context_switches":u.ru_nivcsw-before.ru_nivcsw,"major_faults":u.ru_majflt-before.ru_majflt})
}
fn sizes(path: &Path) -> J {
    use std::os::unix::fs::MetadataExt;
    let mut logical = 0;
    let mut allocated = 0;
    for suffix in ["", "-wal", "-shm"] {
        if let Ok(m) = std::fs::metadata(format!("{}{suffix}", path.display())) {
            logical += m.len();
            allocated += m.blocks() * 512;
        }
    }
    json!({"file_bytes":logical,"allocated_bytes":allocated})
}
fn sqlite(path: &Path) -> rusqlite::Result<Connection> {
    let c = Connection::open(path)?;
    c.busy_timeout(Duration::from_millis(250))?;
    c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA cache_size=-8192;")?;
    Ok(c)
}
fn flatten(s: &str) -> String {
    let mut s = s.to_owned();
    for name in FAMILIES {
        let (schema, table) = name.split_once('.').unwrap();
        s = s.replace(&format!("{schema}.\"{table}\""), &format!("\"{table}\""));
    }
    s
}
fn ddl() -> &'static str {
    r#"
PRAGMA journal_mode=WAL;
CREATE TABLE Person(id BLOB PRIMARY KEY,name TEXT NOT NULL);
CREATE TABLE Workout(id BLOB PRIMARY KEY,durationMinutes INTEGER NOT NULL);
CREATE TABLE Performed(id BLOB PRIMARY KEY,athlete BLOB NOT NULL REFERENCES Person(id),workout BLOB NOT NULL REFERENCES Workout(id));
CREATE TABLE Deal(id BLOB PRIMARY KEY,amount INTEGER NOT NULL);
CREATE TABLE CustomerDeal(id BLOB PRIMARY KEY,customer BLOB NOT NULL REFERENCES Person(id),deal BLOB NOT NULL REFERENCES Deal(id));
CREATE INDEX person_name ON Person(name);CREATE INDEX workout_duration ON Workout(durationMinutes);
CREATE INDEX performed_athlete ON Performed(athlete);CREATE INDEX performed_workout ON Performed(workout);
CREATE INDEX deal_amount ON Deal(amount);CREATE INDEX customer_person ON CustomerDeal(customer);CREATE INDEX customer_deal ON CustomerDeal(deal);
"#
}
fn records(i: usize, complete: bool) -> Vec<(usize, Id, Vec<Value>)> {
    let p = person(i);
    let mut rows = vec![(0, p, vec![Value::Text(format!("Person{i:06}"))])];
    if complete {
        for j in 0..2 {
            let w = child(1, i, j);
            let d = child(3, i, j);
            rows.push((1, w, vec![Value::Int(((i * 7 + j * 19) % 120) as i64)]));
            rows.push((2, child(2, i, j), vec![Value::Ref(p), Value::Ref(w)]));
            rows.push((3, d, vec![Value::Decimal(((i + 1) * 100 + j * 17) as i64)]));
            rows.push((4, child(4, i, j), vec![Value::Ref(p), Value::Ref(d)]));
        }
    }
    rows
}
fn value(v: Value) -> S {
    match v {
        Value::Ref(id) => S::Blob(id.0.as_bytes().to_vec()),
        Value::Text(t) => S::Text(t),
        Value::Int(n) | Value::Decimal(n) => S::Integer(n),
        _ => unreachable!(),
    }
}
fn env_u64(name: &str, default: u64) -> AnyResult<u64> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}
fn open_database(path: &Path) -> AnyResult<Database> {
    let default = CheckpointPolicy::default();
    let policy = CheckpointPolicy {
        max_log_commits: env_u64("SCALE_CHECKPOINT_COMMITS", default.max_log_commits)?,
        max_log_bytes: env_u64("SCALE_CHECKPOINT_BYTES", default.max_log_bytes)?,
    };
    let mut groups = GroupCommitOptions::default();
    groups.max_group_transactions =
        env_u64("SCALE_GROUP_SIZE", groups.max_group_transactions as u64)?.try_into()?;
    groups.collection_window = Duration::from_micros(env_u64(
        "SCALE_COLLECTION_US",
        groups.collection_window.as_micros().try_into()?,
    )?);
    let storage = StorageOptions {
        cache_bytes: env_u64(
            "SCALE_CACHE_BYTES",
            StorageOptions::default().cache_bytes as u64,
        )?
        .try_into()?,
    };
    emit(
        json!({"event":"configuration","cache_bytes":storage.cache_bytes,"checkpoint_commits":policy.max_log_commits,"checkpoint_bytes":policy.max_log_bytes,"group_size":groups.max_group_transactions,"collection_us":groups.collection_window.as_micros()}),
    );
    Ok(Database::open_with_storage_options(
        path,
        DatabaseOptions {
            checkpoint_policy: Some(policy),
        },
        groups,
        storage,
    )?)
}
fn numeric_delta<T: serde::Serialize>(after: &T, before: &T) -> J {
    let after = serde_json::to_value(after).expect("diagnostics serialize");
    let before = serde_json::to_value(before).expect("diagnostics serialize");
    J::Object(
        after
            .as_object()
            .expect("diagnostic object")
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    json!(
                        value
                            .as_u64()
                            .unwrap()
                            .saturating_sub(before[key].as_u64().unwrap())
                    ),
                )
            })
            .collect(),
    )
}
fn load(backend: &str, path: &Path, rows: usize) -> AnyResult<()> {
    if path.exists() {
        return Err("refusing to overwrite database".into());
    }
    let start = Instant::now();
    let cpu = usage();
    let people = rows.div_ceil(9);
    let full = rows / 9;
    let db = if backend == "substrate" {
        Some(open_database(path)?)
    } else {
        None
    };
    let mut sql = if backend == "sqlite" {
        Some(sqlite(path)?)
    } else {
        None
    };
    if let Some(db) = &db {
        let mut t = db.admin().transaction();
        for m in [demo::core(), demo::fitness(), demo::crm()] {
            t.install_with_visibility(m, Visibility::Private)?;
        }
        for r in FAMILIES {
            t.grant("bench", r, Grant::read_write())?;
        }
        t.commit()?;
    }
    if let Some(c) = &sql {
        c.execute_batch(ddl())?;
    }
    let catalog_rows = db
        .as_ref()
        .map(|d| d.admin().snapshot().stats().map(|s| s.rows))
        .transpose()?
        .unwrap_or(0);
    let mut loaded = 0;
    let mut payload = 0u64;
    for begin in (0..people).step_by(1000) {
        let mut tx = db.as_ref().map(|db| db.admin().transaction());
        let st = sql.as_mut().map(|c| c.transaction()).transpose()?;
        for i in begin..(begin + 1000).min(people) {
            for (family, id, values) in records(i, i < full) {
                payload += 16
                    + values
                        .iter()
                        .map(|v| match v {
                            Value::Text(t) => t.len() as u64,
                            Value::Ref(_) => 16,
                            _ => 8,
                        })
                        .sum::<u64>();
                if let Some(t) = &mut tx {
                    t.insert_with_id(FAMILIES[family], id, values)?;
                } else {
                    let mut params = vec![S::Blob(id.0.as_bytes().to_vec())];
                    params.extend(values.into_iter().map(value));
                    let table = FAMILIES[family].split_once('.').unwrap().1;
                    let stmt = format!(
                        "INSERT INTO {table} VALUES ({})",
                        vec!["?"; params.len()].join(",")
                    );
                    st.as_ref()
                        .unwrap()
                        .prepare_cached(&stmt)?
                        .execute(rusqlite::params_from_iter(params))?;
                }
                loaded += 1;
            }
        }
        if let Some(t) = tx {
            t.commit()?;
        }
        if let Some(t) = st {
            t.commit()?;
        }
        if begin % 10000 == 0 || begin + 1000 >= people {
            emit(
                json!({"event":"load_progress","backend":backend,"target_rows":rows,"loaded_rows":loaded,"elapsed_seconds":start.elapsed().as_secs_f64(),"resource":resource(&cpu,start.elapsed().as_secs_f64()),"size":sizes(path)}),
            );
        }
    }
    let ingest = start.elapsed().as_secs_f64();
    let maintenance = Instant::now();
    if let Some(db) = &db {
        db.checkpoint()?;
        assert_eq!(db.admin().snapshot().stats()?.rows, rows + catalog_rows);
    }
    if let Some(c) = &sql {
        c.execute_batch("ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);")?;
    }
    let total = start.elapsed().as_secs_f64();
    emit(
        json!({"event":"load","backend":backend,"rows":rows,"people":people,"workouts":full*2,"batch_people":1000,"ingest_seconds":ingest,"rows_per_second":rows as f64/ingest,"maintenance_seconds":maintenance.elapsed().as_secs_f64(),"logical_payload_bytes":payload,"logical_payload_bytes_per_second":payload as f64/ingest,"resource":resource(&cpu,total),"size":sizes(path)}),
    );
    Ok(())
}
fn canonical(rows: Vec<Vec<S>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| match v {
                    S::Text(s) => s,
                    S::Integer(n) => n.to_string(),
                    S::Blob(b) => uuid::Uuid::from_slice(&b).unwrap().to_string(),
                    other => panic!("unexpected {other:?}"),
                })
                .collect()
        })
        .collect()
}
fn query_sqlite(stmt: &mut rusqlite::Statement<'_>) -> rusqlite::Result<Vec<Vec<S>>> {
    let width = stmt.column_count();
    stmt.query_map([], |row| (0..width).map(|i| row.get(i)).collect())?
        .collect()
}
#[derive(Clone)]
struct Histogram {
    buckets: Vec<u64>,
    count: u64,
    total: f64,
    min: f64,
    max: f64,
}
impl Histogram {
    fn new() -> Self {
        Self {
            buckets: vec![0; 2048],
            count: 0,
            total: 0.,
            min: f64::INFINITY,
            max: 0.,
        }
    }
    fn add(&mut self, seconds: f64) {
        let ns = (seconds * 1e9).max(1.);
        let bucket = (ns.ln() / 1.02f64.ln()).floor() as usize;
        self.buckets[bucket.min(2047)] += 1;
        self.count += 1;
        self.total += seconds;
        self.min = self.min.min(seconds);
        self.max = self.max.max(seconds);
    }
    fn merge(&mut self, h: &Self) {
        for (a, b) in self.buckets.iter_mut().zip(&h.buckets) {
            *a += b;
        }
        self.count += h.count;
        self.total += h.total;
        self.min = self.min.min(h.min);
        self.max = self.max.max(h.max);
    }
    fn q(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let threshold = (self.count as f64 * q).ceil() as u64;
        let mut count = 0;
        for (i, n) in self.buckets.iter().enumerate() {
            count += n;
            if count >= threshold {
                return Some(1.02f64.powi(i as i32 + 1) / 1000.);
            }
        }
        None
    }
    fn json(&self) -> J {
        json!({"count":self.count,"p50_us":self.q(0.5),"p95_us":self.q(0.95),"p99_us":self.q(0.99),"mean_us":if self.count>0 {Some(self.total*1e6/self.count as f64)}else{None},"max_us":self.max*1e6,"percentile_relative_error_bound":0.02})
    }
}
fn query_bench(db: Option<&Database>, conn: Option<&Connection>, rows: usize) -> AnyResult<()> {
    let people = rows.div_ceil(9);
    let backend = if db.is_some() { "substrate" } else { "sqlite" };
    let setup = Instant::now();
    let catalog = db
        .map(|d| d.session("bench").snapshot().sql())
        .transpose()?;
    emit(
        json!({"event":"catalog","backend":backend,"rows":rows,"seconds":setup.elapsed().as_secs_f64()}),
    );
    for (name, query) in queries::workloads(people) {
        emit(json!({"event":"phase","phase":"sql","backend":backend,"rows":rows,"query":name}));
        let cpu = usage();
        let start = Instant::now();
        let result = (|| -> AnyResult<()> {
            let prep = Instant::now();
            let mut sub = catalog.as_ref().map(|c| c.prepare(&query)).transpose()?;
            let sq = flatten(&query).replace(
                &format!("'{}'", person(people / 2)),
                &format!("X'{}'", person(people / 2).0.simple()),
            );
            let mut sqlite = conn.map(|c| c.prepare(&sq)).transpose()?;
            let preparation = prep.elapsed().as_secs_f64();
            let mut run = || -> AnyResult<Vec<Vec<String>>> {
                Ok(canonical(if let Some(q) = &mut sub {
                    q.query(&[])?.rows
                } else {
                    query_sqlite(sqlite.as_mut().unwrap())?
                }))
            };
            let first = Instant::now();
            let expected = run()?;
            let first_us = first.elapsed().as_secs_f64() * 1e6;
            let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&expected)?));
            let mut h = Histogram::new();
            for _ in 0..7 {
                let t = Instant::now();
                let result = run()?;
                h.add(t.elapsed().as_secs_f64());
                assert_eq!(result, expected);
            }
            emit(
                json!({"event":"sql","backend":backend,"rows":rows,"query":name,"prepare_seconds":preparation,"first_us":first_us,"result_rows":expected.len(),"sha256":digest,"latency":h.json(),"resource":resource(&cpu,start.elapsed().as_secs_f64())}),
            );
            Ok(())
        })();
        if let Err(error) = result {
            emit(
                json!({"event":"sql_error","backend":backend,"rows":rows,"query":name,"error":error.to_string(),"resource":resource(&cpu,start.elapsed().as_secs_f64())}),
            );
        }
    }
    Ok(())
}
fn random(seed: &mut u64, n: usize) -> usize {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    (*seed % n as u64) as usize
}
struct Worker {
    write: bool,
    ok: Histogram,
    failed: Histogram,
    errors: BTreeMap<String, u64>,
    logical_bytes: u64,
}
fn concurrent(
    db: Option<&Database>,
    path: &Path,
    rows: usize,
    writers: usize,
    readers: usize,
    duration: f64,
) -> AnyResult<u64> {
    let backend = if db.is_some() { "substrate" } else { "sqlite" };
    let point_api = std::env::var("SCALE_POINT_API").unwrap_or_else(|_| "snapshot".into());
    if !matches!(point_api.as_str(), "snapshot" | "session") {
        return Err("SCALE_POINT_API must be snapshot or session".into());
    }
    let use_session = point_api == "session";
    emit(
        json!({"event":"phase","phase":"concurrent","backend":backend,"rows":rows,"writers":writers,"readers":readers}),
    );
    let n = writers + readers;
    let ready = Arc::new(Barrier::new(n + 1));
    let go = Arc::new(Barrier::new(n + 1));
    let deadline = Arc::new(Mutex::new(Instant::now()));
    let mut threads = vec![];
    let before_groups = db.map(Database::group_commit_stats);
    let before_storage = db.map(Database::storage_info).transpose()?;
    for worker in 0..n {
        let db = db.cloned();
        let path = path.to_owned();
        let ready = ready.clone();
        let go = go.clone();
        let deadline = deadline.clone();
        threads.push(std::thread::spawn(move || -> AnyResult<Worker> {
            let setup=if db.is_none(){sqlite(&path).map(Some)}else{Ok(None)};let session=db.as_ref().map(|d|d.session("bench"));
            let mut out=Worker{write:worker<writers,ok:Histogram::new(),failed:Histogram::new(),errors:BTreeMap::new(),logical_bytes:0};
            let mut seed=worker as u64+1234567;
            ready.wait();go.wait();let mut c=setup?;let deadline=*deadline.lock().unwrap();
            while Instant::now()<deadline {
                let t=Instant::now();let result=(||->AnyResult<()>{
                    if out.write {let index=random(&mut seed,(rows/9)*2);let id=child(1,index/2,index%2);
                        if let Some(s)=&session {let mut tx=s.transaction();let row=tx.get("fitness.Workout",id)?.ok_or("missing workout")?;let Value::Int(v)=row.values[0] else{return Err("wrong type".into());};tx.update("fitness.Workout",id,vec![Value::Int(v+1)])?;tx.commit()?;
                        } else {let c=c.as_mut().unwrap();let tx=c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;assert_eq!(tx.prepare_cached("UPDATE Workout SET durationMinutes=durationMinutes+1 WHERE id=?")?.execute([id.0.as_bytes().as_slice()])?,1);tx.commit()?;}
                        out.logical_bytes+=24;
                    } else {let index=random(&mut seed,rows.div_ceil(9));let id=person(index);
                        let name=if let Some(s)=&session {let row=if use_session {s.get("core.Person",id)?} else {s.snapshot().get("core.Person",id)?}.ok_or("missing person")?;let Value::Text(name)=row.values[0].clone() else{return Err("wrong type".into());};name}
                        else {c.as_ref().unwrap().prepare_cached("SELECT id,name FROM Person WHERE id=?")?.query_row([id.0.as_bytes().as_slice()],|row|{let found:Vec<u8>=row.get(0)?;assert_eq!(found,id.0.as_bytes());row.get::<_,String>(1)})?};
                        if name!=format!("Person{index:06}"){return Err("wrong person".into());}out.logical_bytes+=16+name.len() as u64;
                    } Ok(())})();
                let elapsed=t.elapsed().as_secs_f64();match result {Ok(())=>out.ok.add(elapsed),Err(e)=>{out.failed.add(elapsed);*out.errors.entry(e.to_string()).or_default()+=1;}}
            } Ok(out)
        }));
    }
    ready.wait();
    let cpu = usage();
    let start = Instant::now();
    *deadline.lock().unwrap() = start + Duration::from_secs_f64(duration);
    go.wait();
    let mut wh = Histogram::new();
    let mut rh = Histogram::new();
    let mut failures = Histogram::new();
    let mut errors = BTreeMap::<String, u64>::new();
    let mut per_worker = vec![];
    let mut logical_bytes = 0;
    for t in threads {
        let w = t.join().map_err(|_| "worker panic")??;
        logical_bytes += w.logical_bytes;
        if w.write {
            wh.merge(&w.ok)
        } else {
            rh.merge(&w.ok)
        };
        failures.merge(&w.failed);
        for (e, n) in w.errors {
            *errors.entry(e).or_default() += n;
        }
        per_worker.push(json!({"writer":w.write,"succeeded":w.ok.count,"failed":w.failed.count}));
    }
    let wall = start.elapsed().as_secs_f64();
    let groups=db.zip(before_groups).map(|(d,b)|{let a=d.group_commit_stats();json!({"durable_groups":a.durable_groups-b.durable_groups,"committed_writes":a.committed_writes-b.committed_writes,"largest_group_lifetime":a.largest_group,"writer_wait_ns":a.writer_wait_ns-b.writer_wait_ns,"validation_apply_ns":a.validation_apply_ns-b.validation_apply_ns,"persistence_ns":a.persistence_ns-b.persistence_ns,"publication_ns":a.publication_ns-b.publication_ns})});
    let persistence = db
        .zip(before_storage)
        .map(|(d, b)| {
            d.storage_info()
                .map(|a| numeric_delta(&a.persistence, &b.persistence))
        })
        .transpose()?;
    emit(
        json!({"event":"concurrent","backend":backend,"point_api":point_api,"rows":rows,"writers":writers,"readers":readers,"requested_seconds":duration,"write_tps":wh.count as f64/wall,"read_ops_per_second":rh.count as f64/wall,"logical_app_bytes_per_second":logical_bytes as f64/wall,"write_latency":wh.json(),"read_latency":rh.json(),"failed_latency":failures.json(),"errors":errors,"per_worker":per_worker,"group_commit":groups,"persistence":persistence,"resource":resource(&cpu,wall),"size":sizes(path)}),
    );
    Ok(wh.count)
}
fn total(db: Option<&Database>, conn: Option<&Connection>) -> AnyResult<i64> {
    if let Some(d) = db {
        let t = d.admin().snapshot().sql()?.query(
            "SELECT SUM(\"durationMinutes\") FROM fitness.\"Workout\"",
            &[],
        )?;
        if let S::Integer(n) = t.rows[0][0] {
            Ok(n)
        } else {
            Err("bad sum".into())
        }
    } else {
        Ok(conn
            .unwrap()
            .query_row("SELECT SUM(durationMinutes) FROM Workout", [], |r| r.get(0))?)
    }
}
fn main() -> AnyResult<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 5 {
        return Err("usage: scale_benchmark load|run|recover|crash substrate|sqlite PATH ROWS [SECONDS] [CLIENTS comma list]".into());
    }
    let mode = &args[1];
    let backend = &args[2];
    let path = Path::new(&args[3]);
    let rows: usize = args[4].parse()?;
    if !["substrate", "sqlite"].contains(&backend.as_str()) || rows < 100 || rows % 9 != 1 {
        return Err("backend invalid or rows must be >=100 and congruent to 1 mod 9".into());
    }
    if mode == "spec" {
        emit(json!(
            queries::workloads(rows.div_ceil(9))
                .into_iter()
                .map(|(name, sql)| json!({"name":name,"sql":sql}))
                .collect::<Vec<_>>()
        ));
        return Ok(());
    }
    if mode == "load" {
        return load(backend, path, rows);
    }
    if !path.is_file() {
        return Err("database missing".into());
    }
    let cpu = usage();
    let start = Instant::now();
    let db = if backend == "substrate" {
        Some(open_database(path)?)
    } else {
        None
    };
    let c = if backend == "sqlite" {
        Some(sqlite(path)?)
    } else {
        None
    };
    // Include first usable read in recovery timing; SQLite opens lazily.
    let expected = format!("Person{:06}", rows.div_ceil(9) / 2);
    if let Some(d) = &db {
        assert_eq!(
            d.session("bench")
                .snapshot()
                .get("core.Person", person(rows.div_ceil(9) / 2))?
                .unwrap()
                .values[0],
            Value::Text(expected.clone())
        );
    }
    if let Some(c) = &c {
        assert_eq!(
            c.query_row(
                "SELECT name FROM Person WHERE id=?",
                [person(rows.div_ceil(9) / 2).0.as_bytes().as_slice()],
                |r| r.get::<_, String>(0)
            )?,
            expected
        );
    }
    emit(
        json!({"event":"open","mode":mode,"backend":backend,"rows":rows,"resource":resource(&cpu,start.elapsed().as_secs_f64()),"size":sizes(path)}),
    );
    if mode == "crash" {
        if let Some(d) = &db {
            let mut t = d.admin().transaction();
            t.update(
                "fitness.Workout",
                child(1, 0, 0),
                vec![Value::Int(987654321)],
            )?;
            t.commit()?;
        } else {
            c.as_ref().unwrap().execute(
                "UPDATE Workout SET durationMinutes=987654321 WHERE id=?",
                [child(1, 0, 0).0.as_bytes().as_slice()],
            )?;
        }
        emit(json!({"event":"crash_commit_acknowledged","backend":backend,"rows":rows}));
        // Intentionally bypass all destructors: runner records status, then reopens.
        unsafe { libc::_exit(73) }
    }
    if mode == "recover" {
        let v = if let Some(d) = &db {
            match d
                .admin()
                .snapshot()
                .get("fitness.Workout", child(1, 0, 0))?
                .unwrap()
                .values[0]
            {
                Value::Int(n) => n,
                _ => return Err("wrong type".into()),
            }
        } else {
            c.as_ref().unwrap().query_row(
                "SELECT durationMinutes FROM Workout WHERE id=?",
                [child(1, 0, 0).0.as_bytes().as_slice()],
                |r| r.get(0),
            )?
        };
        assert_eq!(v, 987654321);
        emit(json!({"event":"recovery_verified","backend":backend,"rows":rows,"sentinel":v}));
        return Ok(());
    }
    if mode != "run" && mode != "run-held" {
        return Err("unknown mode".into());
    }
    let held = mode == "run-held";
    if held && let Some(c) = &c {
        c.execute_batch("BEGIN DEFERRED;")?;
    }
    emit(
        json!({"event":"query_mode","backend":backend,"rows":rows,"held_read_transaction":held || db.is_some()}),
    );
    let queries = query_bench(db.as_ref(), c.as_ref(), rows);
    if held && let Some(c) = &c {
        c.execute_batch("ROLLBACK;")?;
    }
    queries?;
    let before = total(db.as_ref(), c.as_ref())?;
    let seconds = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(2.0);
    let clients = args
        .get(6)
        .map(String::as_str)
        .unwrap_or("1,2,4,8,16,32,64,128")
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<Vec<usize>, _>>()?;
    if clients.iter().any(|n| *n == 0 || *n > 256) || !(0.05..=60.).contains(&seconds) {
        return Err("invalid concurrency/duration".into());
    }
    let modes = args
        .get(7)
        .map(String::as_str)
        .unwrap_or("read,write,mixed")
        .split(',')
        .collect::<Vec<_>>();
    if modes
        .iter()
        .any(|m| !["read", "write", "mixed"].contains(m))
    {
        return Err("invalid workload mode".into());
    }
    let mut commits = 0;
    for mode in modes {
        for &n in &clients {
            let (writers, readers) = match mode {
                "read" => (0, n),
                "write" => (n, 0),
                _ => (n, 4),
            };
            commits += concurrent(db.as_ref(), path, rows, writers, readers, seconds)?;
        }
    }
    assert_eq!(total(db.as_ref(), c.as_ref())? - before, commits as i64);
    emit(
        json!({"event":"writes_verified","backend":backend,"rows":rows,"successful_commits":commits}),
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn globally_unique_fixture_ids() {
        for i in [0, 499999, 500000, 1111111] {
            assert_ne!(person(i), child(1, i, 0));
            for f in 1..=4 {
                assert_ne!(child(f, i, 0), child(f, i, 1));
                assert_ne!(child(f, i, 0), child(f + 1, i, 0));
            }
        }
    }
    #[test]
    fn exact_fixture_size() {
        for n in [100usize, 1000, 10000] {
            let actual = (0..n.div_ceil(9))
                .map(|i| records(i, i < n / 9).len())
                .sum::<usize>();
            assert_eq!(actual, n);
        }
    }
    #[test]
    fn histogram_bounds() {
        let mut h = Histogram::new();
        for n in 1..=100 {
            h.add(n as f64 / 1e6);
        }
        let q = h.q(0.99).unwrap();
        assert!((99.0..=100.98).contains(&q));
        assert_eq!(h.count, 100);
    }
}
