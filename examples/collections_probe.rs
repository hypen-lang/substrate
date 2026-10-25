//! Engine-level collections comparison. Run only through the isolated experiment.
use sha2::{Digest, Sha256};
use std::{
    hint::black_box,
    sync::{Arc, Barrier},
    time::Instant,
};
use substrate_liquid::*;

const FAMILY: &str = "probe.Record";
fn identity(n: usize) -> Id {
    Id::from_u128(100_000 + n as u128)
}
fn values(n: usize, changed: bool) -> Vec<Value> {
    vec![
        Value::Int(n as i64),
        Value::Int((n % 100) as i64),
        Value::Bytes(vec![u8::from(changed); 256]),
    ]
}
fn fixture(size: usize) -> Result<Database> {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    let mut relation = Relation::new(
        FAMILY,
        vec![
            Role::new("number", DataType::Int),
            Role::new("bucket", DataType::Int),
            Role::new("payload", DataType::Bytes),
        ],
    )
    .unique(&["number"]);
    relation.id = Id::from_u128(1);
    tx.install(Module::new("probe", 1, vec![], vec![relation]))?;
    for n in 0..size {
        tx.insert_with_id(FAMILY, identity(n), values(n, false))?;
    }
    tx.commit()?;
    Ok(db)
}
fn verify(db: &Database, count: usize) -> Result<String> {
    let snapshot = db.admin().snapshot();
    snapshot.verify_indexes()?;
    let rows = snapshot.scan(FAMILY)?;
    assert_eq!(rows.len(), count);
    let bytes = serde_json::to_vec(&rows).unwrap();
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn record(
    backend: &str,
    size: usize,
    scenario: &str,
    operations: usize,
    start: Instant,
    digest: Option<String>,
) {
    let elapsed_ns = start.elapsed().as_nanos();
    println!(
        "{}",
        serde_json::json!({
            "backend":backend, "rows":size, "scenario":scenario, "operations":operations,
            "elapsed_ns":elapsed_ns, "ns_per_operation":elapsed_ns as f64 / operations as f64, "digest":digest,
        })
    );
}
fn timed<T>(work: impl FnOnce() -> T) -> (T, std::time::Duration) {
    let start = Instant::now();
    let result = work();
    (result, start.elapsed())
}
fn run(backend: &str, size: usize) -> Result<()> {
    // Setup is separate from execution. Every workload below checks final indexes.
    let (db, duration) = timed(|| fixture(size));
    let db = db?;
    println!(
        "{}",
        serde_json::json!({"backend":backend,"rows":size,"scenario":"bulk_fixture","operations":size,"elapsed_ns":duration.as_nanos(),"ns_per_operation":duration.as_nanos() as f64 / size as f64,"digest":verify(&db,size)?})
    );

    let start = Instant::now();
    for _ in 0..20_000 {
        black_box(db.admin().snapshot());
    }
    record(backend, size, "snapshot_capture", 20_000, start, None);
    let snapshot = db.admin().snapshot();
    let start = Instant::now();
    for _ in 0..20_000 {
        black_box(snapshot.clone());
    }
    record(backend, size, "snapshot_clone", 20_000, start, None);
    let start = Instant::now();
    for n in 0..10_000 {
        black_box(snapshot.get(FAMILY, identity(n % size))?.unwrap());
    }
    record(backend, size, "point_read", 10_000, start, None);
    let start = Instant::now();
    for n in 0..500 {
        black_box(snapshot.lookup(FAMILY, "bucket", &Value::Int(n % 100))?);
    }
    record(backend, size, "indexed_equality", 500, start, None);
    let start = Instant::now();
    for _ in 0..10 {
        black_box(snapshot.scan(FAMILY)?);
    }
    record(backend, size, "full_scan", 10, start, None);
    drop(snapshot);
    drop(db);

    for retain in [false, true] {
        let db = fixture(size)?;
        let original = retain.then(|| db.admin().snapshot());
        let mut retained = Vec::new();
        let start = Instant::now();
        for n in 0..512 {
            let row = n % size;
            let mut tx = db.admin().transaction();
            tx.update(FAMILY, identity(row), values(row, true))?;
            tx.commit()?;
            if retain && n % 32 == 0 {
                retained.push(db.admin().snapshot());
            }
        }
        let elapsed = start.elapsed();
        if let Some(original) = original {
            assert_eq!(
                original.get(FAMILY, identity(0))?.unwrap().values[2],
                Value::Bytes(vec![0; 256])
            );
            for old in retained {
                old.verify_indexes()?;
            }
        }
        let digest = verify(&db, size)?;
        println!(
            "{}",
            serde_json::json!({"backend":backend,"rows":size,"scenario":if retain {"updates_retained_snapshots"} else {"updates_unretained"},"operations":512,"elapsed_ns":elapsed.as_nanos(),"ns_per_operation":elapsed.as_nanos() as f64 / 512.0,"digest":digest})
        );
    }
    for batch in [1, 32] {
        let db = fixture(size)?;
        let start = Instant::now();
        for begin in (0..512).step_by(batch) {
            let mut tx = db.admin().transaction();
            for offset in begin..begin + batch {
                let n = size + offset;
                tx.insert_with_id(FAMILY, identity(n), values(n, false))?;
            }
            tx.commit()?;
        }
        let elapsed = start.elapsed();
        let digest = verify(&db, size + 512)?;
        println!(
            "{}",
            serde_json::json!({"backend":backend,"rows":size,"scenario":format!("insert_batch_{batch}"),"operations":512,"elapsed_ns":elapsed.as_nanos(),"ns_per_operation":elapsed.as_nanos() as f64 /512.0,"digest":digest})
        );
    }
    let db = fixture(size)?;
    let gate = Arc::new(Barrier::new(9));
    let start = Instant::now();
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let db = &db;
            let gate = gate.clone();
            scope.spawn(move || {
                gate.wait();
                for offset in 0..64 {
                    let n = size + worker * 64 + offset;
                    let mut tx = db.admin().transaction();
                    tx.insert_with_id(FAMILY, identity(n), values(n, false))
                        .unwrap();
                    tx.commit().unwrap();
                }
            });
        }
        gate.wait();
    });
    let elapsed = start.elapsed();
    let digest = verify(&db, size + 512)?;
    println!(
        "{}",
        serde_json::json!({"backend":backend,"rows":size,"scenario":"independent_writers_8","operations":512,"elapsed_ns":elapsed.as_nanos(),"ns_per_operation":elapsed.as_nanos() as f64/512.0,"digest":digest})
    );

    // Deterministic OCC/isolation parity independent of timings.
    let mut first = db.admin().transaction();
    let mut second = db.admin().transaction();
    first.update(FAMILY, identity(0), values(0, true))?;
    second.update(FAMILY, identity(0), values(0, false))?;
    first.commit()?;
    assert!(matches!(second.commit(), Err(Error::Conflict)));
    verify(&db, size + 512)?;
    Ok(())
}
fn main() -> Result<()> {
    let backend = std::env::args().nth(1).expect("backend label");
    for size in [1_000, 10_000] {
        run(&backend, size)?;
    }
    Ok(())
}
