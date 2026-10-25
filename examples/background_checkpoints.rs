//! cargo run --release --offline --example background_checkpoints
//! Compare host pauses for explicit maintenance with coalesced background requests.
use std::time::{Duration, Instant};
use substrate_liquid::*;

fn percentile(values: &mut [f64], q: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * q) as usize]
}
fn run(background: bool) -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Database::open(&path)?;
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "sample",
        1,
        vec![],
        vec![Relation::new(
            "sample.Record",
            vec![
                Role::new("n", DataType::Int),
                Role::new("payload", DataType::Bytes),
            ],
        )],
    ))?;
    tx.commit()?;
    let worker = background
        .then(|| {
            db.start_checkpointer(CheckpointerOptions {
                interval: Duration::from_millis(2),
                early_policy: CheckpointPolicy {
                    max_log_commits: u64::MAX,
                    max_log_bytes: u64::MAX,
                },
            })
        })
        .transpose()?;
    let mut pauses = Vec::new();
    let mut commits = Vec::new();
    let mut ticket = 0;
    let start = Instant::now();
    for n in 0..256 {
        let mut tx = db.admin().transaction();
        tx.insert(
            "sample.Record",
            vec![Value::Int(n), Value::Bytes(vec![42; 4096])],
        )?;
        let before = Instant::now();
        tx.commit()?;
        commits.push(before.elapsed().as_secs_f64() * 1e6);
        if n % 16 == 15 {
            let before = Instant::now();
            if let Some(worker) = &worker {
                ticket = worker.request()?;
            } else {
                db.checkpoint()?;
            }
            pauses.push(before.elapsed().as_secs_f64() * 1e6);
        }
    }
    let stats = if let Some(worker) = worker {
        let receipt = worker.wait(ticket, Duration::from_secs(30))?;
        assert_eq!(receipt.revision, 257);
        Some(worker.stop())
    } else {
        None
    };
    let elapsed = start.elapsed().as_secs_f64();
    assert_eq!(db.storage_info()?.log_commits, 0);
    drop(db);
    let reopened = Database::open(&path)?;
    assert_eq!(
        reopened.admin().snapshot().scan("sample.Record")?.len(),
        256
    );
    assert_eq!(reopened.recovery_info()?.replayed_commits, 0);
    println!(
        "{}",
        serde_json::json!({
            "mode": if background { "background" } else { "foreground" },
            "writes": 256, "requests": 16, "payload_bytes": 4096,
            "elapsed_seconds": elapsed, "writes_per_second": 256.0 / elapsed,
            "commit_p99_us": percentile(&mut commits, 0.99),
            "maintenance_call_p50_us": percentile(&mut pauses, 0.5),
            "maintenance_call_p99_us": percentile(&mut pauses, 0.99),
            "background_stats": stats,
        })
    );
    Ok(())
}
fn main() -> Result<()> {
    for _ in 0..3 {
        run(false)?;
        run(true)?;
    }
    Ok(())
}
