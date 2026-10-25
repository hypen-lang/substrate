//! Durable group commit example and reproducible concurrency benchmark.
//! cargo run --release --example group_commit -- 64
//! Optional second argument: "conflicts" or "checkpoint".
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use substrate_liquid::*;

fn percentile(samples: &mut [f64], quantile: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[((samples.len().saturating_sub(1) as f64 * quantile) as usize)
        .min(samples.len().saturating_sub(1))]
}
fn run(
    writers: usize,
    per_writer: usize,
    group: usize,
    window_us: u64,
    scenario: &str,
) -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = scenario == "checkpoint";
    let options = DatabaseOptions {
        checkpoint_policy: if checkpoint {
            Some(CheckpointPolicy {
                max_log_commits: 32,
                max_log_bytes: 8 * 1024 * 1024,
            })
        } else {
            None
        },
    };
    let db = Database::open_with_commit_options(
        directory.path().join("db"),
        options,
        GroupCommitOptions {
            max_group_transactions: group,
            collection_window: Duration::from_micros(window_us),
            ..Default::default()
        },
    )?;
    let mut tx = db.admin().transaction();
    tx.install(demo::core())?;
    let person = tx.insert("core.Person", vec!["shared".into()])?;
    tx.commit()?;
    let before = db.group_commit_stats();
    let barrier = Arc::new(Barrier::new(writers + 1));
    let done = Arc::new(AtomicBool::new(false));
    let mut samples = Vec::new();
    let mut conflicts = 0;
    let started = Instant::now();
    let (elapsed, mut reader_samples) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut samples = Vec::new();
            while !done.load(Ordering::Acquire) {
                let start = Instant::now();
                db.admin().snapshot().get("core.Person", person).unwrap();
                samples.push(start.elapsed().as_secs_f64() * 1e6);
                std::thread::sleep(Duration::from_micros(50));
            }
            samples
        });
        let workers: Vec<_> = (0..writers)
            .map(|_| {
                let db = &db;
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let mut samples = Vec::new();
                    let mut conflicts = 0;
                    for _ in 0..per_writer {
                        let mut tx = db.admin().transaction();
                        if scenario == "conflicts" {
                            tx.update("core.Person", person, vec!["changed".into()])
                                .unwrap();
                        } else {
                            tx.insert("core.Person", vec!["independent".into()])
                                .unwrap();
                        }
                        let start = Instant::now();
                        match tx.commit() {
                            Ok(_) => samples.push(start.elapsed().as_secs_f64() * 1e6),
                            Err(Error::Conflict) => conflicts += 1,
                            Err(error) => panic!("commit: {error}"),
                        }
                    }
                    (samples, conflicts)
                })
            })
            .collect();
        barrier.wait();
        for worker in workers {
            let (mut latencies, rejected) = worker.join().unwrap();
            samples.append(&mut latencies);
            conflicts += rejected;
        }
        let elapsed = started.elapsed();
        done.store(true, Ordering::Release);
        (elapsed, reader.join().unwrap())
    });
    let after = db.group_commit_stats();
    let accepted = samples.len();
    let groups = after.durable_groups - before.durable_groups;
    let reader_max = reader_samples.iter().copied().fold(0.0, f64::max);
    let output = serde_json::json!({
        "scenario": scenario, "writers": writers, "transactions_per_writer": per_writer,
        "max_group_transactions": group, "collection_window_us": window_us,
        "accepted": accepted, "conflicts": conflicts, "durable_groups": groups,
        "transactions_per_durable_group": accepted as f64 / groups as f64,
        "transactions_per_second": accepted as f64 / elapsed.as_secs_f64(),
        "commit_p50_us": percentile(&mut samples, 0.5), "commit_p99_us": percentile(&mut samples, 0.99),
        "reader_p99_us": percentile(&mut reader_samples, 0.99), "reader_max_us": reader_max,
        "reader_samples": reader_samples.len(), "elapsed_seconds": elapsed.as_secs_f64(),
    });
    db.admin().snapshot().verify_indexes()?;
    let expected = if scenario == "conflicts" {
        1
    } else {
        accepted + 1
    };
    assert_eq!(db.admin().snapshot().scan("core.Person")?.len(), expected);
    drop(db);
    let recovered = Database::open_with_options(
        directory.path().join("db"),
        DatabaseOptions {
            checkpoint_policy: None,
        },
    )?;
    assert_eq!(
        recovered.admin().snapshot().scan("core.Person")?.len(),
        expected
    );
    assert_eq!(recovered.admin().snapshot().revision(), accepted as u64 + 1);
    println!("{output}");
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let per_writer = args
        .get(1)
        .map(|v| v.parse::<usize>().expect("positive transaction count"))
        .unwrap_or(64);
    assert!(per_writer > 0);
    let scenario = args.get(2).map(String::as_str).unwrap_or("independent");
    assert!(["independent", "conflicts", "checkpoint"].contains(&scenario));
    for writers in [1, 8, 32] {
        for (group, window) in [(1, 0), (64, 0), (64, 100), (64, 500)] {
            run(writers, per_writer, group, window, scenario)?;
        }
    }
    Ok(())
}
