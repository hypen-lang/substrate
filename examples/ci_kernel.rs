//! Portable, fixed-workload PR benchmark. Prints exactly one protocol-1 JSON object.
//! CPU/RSS belong to the invoking process monitor, not platform-specific code here.
use serde_json::{Value as Json, json};
use std::{
    collections::BTreeMap,
    hint::black_box,
    path::Path,
    sync::{Arc, Barrier, OnceLock},
    time::{Duration, Instant},
};
use substrate_liquid::*;

type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const FAMILY: &str = "bench.Item";
const PAYLOAD_BYTES: usize = 64;

#[derive(Clone, Copy)]
struct Options {
    rows: usize,
    duration: Duration,
}
impl Options {
    fn parse(args: &[String]) -> AnyResult<Self> {
        if args.len() > 2 {
            return Err("usage: ci_kernel [ROWS [SECONDS]]".into());
        }
        let rows = args
            .first()
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or(10_000);
        let seconds: f64 = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(0.25);
        if !(8..=1_000_000).contains(&rows) {
            return Err("ROWS must be between 8 and 1000000".into());
        }
        if !seconds.is_finite() || !(0.01..=60.0).contains(&seconds) {
            return Err("SECONDS must be finite and between 0.01 and 60".into());
        }
        Ok(Self {
            rows,
            duration: Duration::from_secs_f64(seconds),
        })
    }
}

enum Command {
    Benchmark(Options),
    Memory { rows: usize },
}
impl Command {
    fn parse(args: &[String]) -> AnyResult<Self> {
        if args.first().is_some_and(|arg| arg == "--memory") {
            if args.len() != 2 {
                return Err("usage: ci_kernel --memory ROWS".into());
            }
            Ok(Self::Memory {
                rows: Options::parse(&args[1..])?.rows,
            })
        } else {
            Ok(Self::Benchmark(Options::parse(args)?))
        }
    }
}

fn id(index: usize) -> Id {
    Id::from_u128(index as u128 + 1)
}
fn name(index: usize) -> String {
    format!("item-{index:08}")
}
fn values(index: usize, counter: i64) -> Vec<Value> {
    vec![
        Value::Text(name(index)),
        Value::Int(counter),
        Value::Bytes(vec![(index % 251) as u8; PAYLOAD_BYTES]),
    ]
}
fn module() -> Module {
    Module::new(
        "bench",
        1,
        vec![],
        vec![Relation {
            id: Id::from_u128(0x63695f6b_65726e65_6c000000_00000000),
            name: FAMILY.into(),
            roles: vec![
                Role::new("name", DataType::Text),
                Role::new("counter", DataType::Int),
                Role::new("payload", DataType::Bytes),
            ],
            unique: vec![],
            checks: vec![],
        }],
    )
}
fn install_and_load(db: &Database, rows: usize) -> AnyResult<()> {
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(module(), Visibility::Private)?;
    tx.grant("reader", FAMILY, Grant::read_only())?;
    tx.grant(
        "writer",
        FAMILY,
        Grant {
            write: true,
            ..Grant::read_only()
        },
    )?;
    tx.commit()?;
    for first in (0..rows).step_by(256) {
        let mut tx = db.admin().transaction();
        for index in first..(first + 256).min(rows) {
            tx.insert_with_id(FAMILY, id(index), values(index, 0))?;
        }
        tx.commit()?;
    }
    Ok(())
}
fn verify_row(row: &Row, index: usize, counter: i64) {
    assert_eq!(row.id, id(index));
    assert_eq!(row.values.as_ref(), values(index, counter).as_slice());
}
fn verify_fixture(db: &Database, rows: usize) -> AnyResult<()> {
    let snapshot = db.session("reader").snapshot();
    let found = snapshot.scan(FAMILY)?;
    assert_eq!(found.len(), rows);
    for (index, row) in found.iter().enumerate() {
        verify_row(row, index, 0);
    }
    assert!(matches!(
        db.anonymous().snapshot().get(FAMILY, id(0)),
        Err(Error::Denied)
    ));
    assert!(matches!(
        db.session("unrelated").snapshot().scan(FAMILY),
        Err(Error::Denied)
    ));
    assert!(matches!(
        db.session("unrelated")
            .snapshot()
            .lookup(FAMILY, "name", &Value::Text(name(0))),
        Err(Error::Denied)
    ));
    assert!(snapshot.get(FAMILY, id(rows))?.is_none());
    for index in [0, rows / 2, rows - 1] {
        let found = snapshot.lookup(FAMILY, "name", &Value::Text(name(index)))?;
        assert_eq!(found.len(), 1);
        verify_row(&found[0], index, 0);
    }
    db.admin().snapshot().verify_indexes()?;
    Ok(())
}
fn metric(
    metrics: &mut BTreeMap<String, Json>,
    name: &str,
    value: f64,
    unit: &str,
    direction: &str,
) {
    assert!(
        value.is_finite() && value > 0.0,
        "invalid metric {name}: {value}"
    );
    assert!(matches!(direction, "lower" | "higher"));
    assert!(
        metrics
            .insert(
                name.into(),
                json!({"value":value,"unit":unit,"direction":direction})
            )
            .is_none()
    );
}
fn seconds(elapsed: Duration) -> f64 {
    // Some clocks coarsen very short intervals; never emit zero/NaN metrics.
    elapsed.as_secs_f64().max(1e-9)
}
fn repeat(duration: Duration, mut operation: impl FnMut(usize) -> AnyResult<()>) -> AnyResult<f64> {
    let start = Instant::now();
    let mut count = 0usize;
    loop {
        operation(count)?;
        count += 1;
        if start.elapsed() >= duration {
            break;
        }
    }
    Ok(count as f64 / seconds(start.elapsed()))
}
fn memory_reads(
    db: &Database,
    options: Options,
    metrics: &mut BTreeMap<String, Json>,
) -> AnyResult<()> {
    let session = db.session("reader");
    let retained = session.snapshot();
    let names: Vec<_> = (0..options.rows).map(|i| Value::Text(name(i))).collect();
    for fresh in [true, false] {
        let rate = repeat(options.duration, |iteration| {
            let index = iteration.wrapping_mul(104_729) % options.rows;
            let row = if fresh {
                session.snapshot().get(FAMILY, id(index))?
            } else {
                retained.get(FAMILY, id(index))?
            }
            .ok_or("missing point row")?;
            assert_eq!(row.id, id(index));
            assert_eq!(row.values[0], names[index]);
            assert_eq!(row.values[1], Value::Int(0));
            black_box(row);
            Ok(())
        })?;
        metric(
            metrics,
            if fresh {
                "fresh_read_ops_per_second"
            } else {
                "retained_read_ops_per_second"
            },
            rate,
            "ops_per_second",
            "higher",
        );
    }
    let rate = repeat(options.duration, |iteration| {
        let index = iteration.wrapping_mul(104_729) % options.rows;
        let found = retained.lookup(FAMILY, "name", &names[index])?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, id(index));
        black_box(found);
        Ok(())
    })?;
    metric(
        metrics,
        "indexed_lookup_ops_per_second",
        rate,
        "ops_per_second",
        "higher",
    );
    let rate = repeat(options.duration, |_| {
        let found = retained.scan(FAMILY)?;
        assert_eq!(found.len(), options.rows);
        black_box(found);
        Ok(())
    })?;
    metric(
        metrics,
        "owned_scan_rows_per_second",
        rate * options.rows as f64,
        "rows_per_second",
        "higher",
    );
    Ok(())
}
fn percentile(sorted: &[Duration], percent: usize) -> f64 {
    let index = (sorted.len() * percent).div_ceil(100).saturating_sub(1);
    seconds(sorted[index.min(sorted.len() - 1)]) * 1e6
}
fn writers(
    db: &Database,
    count: usize,
    duration: Duration,
    expected: &mut [i64],
    metrics: &mut BTreeMap<String, Json>,
) -> AnyResult<()> {
    let barrier = Arc::new(Barrier::new(count + 1));
    let start = Arc::new(OnceLock::new());
    let (completed, elapsed) = std::thread::scope(|scope| -> AnyResult<_> {
        let mut handles = Vec::new();
        for worker in 0..count {
            let session = db.session("writer");
            let barrier = barrier.clone();
            let start = start.clone();
            handles.push(scope.spawn(move || -> AnyResult<Vec<Duration>> {
                barrier.wait();
                let deadline = *start.get().ok_or("missing start time")? + duration;
                let mut timings = Vec::new();
                loop {
                    let began = Instant::now();
                    let mut tx = session.transaction();
                    let row = tx.get(FAMILY, id(worker))?.ok_or("missing writer row")?;
                    let Value::Int(counter) = row.values[1] else {
                        return Err("invalid counter".into());
                    };
                    tx.update(FAMILY, id(worker), values(worker, counter + 1))?;
                    tx.commit()?;
                    timings.push(began.elapsed());
                    if Instant::now() >= deadline {
                        break;
                    }
                }
                Ok(timings)
            }));
        }
        let began = Instant::now();
        start.set(began).map_err(|_| "start time already set")?;
        barrier.wait();
        let mut completed = Vec::new();
        for handle in handles {
            completed.push(handle.join().map_err(|_| "writer panicked")??);
        }
        Ok((completed, began.elapsed()))
    })?;
    let mut timings = Vec::new();
    for (worker, samples) in completed.into_iter().enumerate() {
        expected[worker] += i64::try_from(samples.len())?;
        timings.extend(samples);
    }
    assert!(!timings.is_empty());
    metric(
        metrics,
        &format!("durable_{count}_writer_tps"),
        timings.len() as f64 / seconds(elapsed),
        "transactions_per_second",
        "higher",
    );
    timings.sort_unstable();
    metric(
        metrics,
        &format!("durable_{count}_writer_p50_us"),
        percentile(&timings, 50),
        "microseconds",
        "lower",
    );
    metric(
        metrics,
        &format!("durable_{count}_writer_p99_us"),
        percentile(&timings, 99),
        "microseconds",
        "lower",
    );
    verify_counters(db, expected)
}
fn verify_counters(db: &Database, expected: &[i64]) -> AnyResult<()> {
    let rows = db.session("reader").snapshot().scan(FAMILY)?;
    assert_eq!(rows.len(), expected.len());
    let mut sum = 0i64;
    for (index, row) in rows.iter().enumerate() {
        verify_row(row, index, expected[index]);
        let Value::Int(counter) = row.values[1] else {
            return Err("invalid counter".into());
        };
        sum += counter;
    }
    assert_eq!(sum, expected.iter().sum::<i64>());
    db.admin().snapshot().verify_indexes()?;
    Ok(())
}
fn durable(options: Options, metrics: &mut BTreeMap<String, Json>) -> AnyResult<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("database.redb");
    let began = Instant::now();
    let db = Database::open(&path)?;
    install_and_load(&db, options.rows)?;
    metric(
        metrics,
        "durable_load_seconds",
        seconds(began.elapsed()),
        "seconds",
        "lower",
    );
    verify_fixture(&db, options.rows)?;
    let began = Instant::now();
    db.checkpoint()?;
    metric(
        metrics,
        "checkpoint_seconds",
        seconds(began.elapsed()),
        "seconds",
        "lower",
    );
    metric(
        metrics,
        "checkpoint_file_bytes",
        std::fs::metadata(&path)?.len() as f64,
        "bytes",
        "lower",
    );
    drop(db);
    let began = Instant::now();
    let db = Database::open(&path)?;
    verify_row(
        &db.session("reader")
            .snapshot()
            .get(FAMILY, id(0))?
            .ok_or("missing reopened row")?,
        0,
        0,
    );
    metric(
        metrics,
        "reopen_seconds",
        seconds(began.elapsed()),
        "seconds",
        "lower",
    );
    let retained = db.session("reader").snapshot();
    let mut expected = vec![0; options.rows];
    for count in [1, 8] {
        writers(
            &db,
            count,
            options.duration.max(Duration::from_secs(1)),
            &mut expected,
            metrics,
        )?;
        verify_row(
            &retained.get(FAMILY, id(0))?.ok_or("missing retained row")?,
            0,
            0,
        );
    }
    drop(retained);
    drop(db);
    // Reopen after the last acknowledged update, without a manual flush. This
    // checks normal close/reopen persistence; separate crash tests cover crashes.
    verify_recovered(&path, &expected)?;
    Ok(())
}
fn verify_recovered(path: &Path, expected: &[i64]) -> AnyResult<()> {
    let db = Database::open(path)?;
    verify_counters(&db, expected)?;
    assert!(matches!(
        db.anonymous().snapshot().scan(FAMILY),
        Err(Error::Denied)
    ));
    Ok(())
}
fn run(options: Options) -> AnyResult<Json> {
    let mut metrics = BTreeMap::new();
    let began = Instant::now();
    let db = Database::memory();
    install_and_load(&db, options.rows)?;
    metric(
        &mut metrics,
        "memory_load_seconds",
        seconds(began.elapsed()),
        "seconds",
        "lower",
    );
    verify_fixture(&db, options.rows)?;
    memory_reads(&db, options, &mut metrics)?;
    drop(db);
    durable(options, &mut metrics)?;
    Ok(json!({
        "protocol": 1, "suite": "kernel", "rows": options.rows, "metrics": metrics,
        "correctness": {"schema":"bench.Item","rows":options.rows,"payload_bytes":PAYLOAD_BYTES,
            "stable_ids":true,"typed_values":true,"indexes":true,"private_denial":true,
            "retained_immutable":true,"exact_durable_counters":true,"reopen":true}
    }))
}

/// A separate process with identical work for both revisions. Timed writer runs
/// retain a machine-speed-dependent number of versions and latency samples, so
/// their process peak RSS is unsuitable as a fixed-work memory regression gate.
fn run_memory(rows: usize) -> AnyResult<Json> {
    const UPDATES: usize = 128;
    let db = Database::memory();
    install_and_load(&db, rows)?;
    verify_fixture(&db, rows)?;
    let retained = db.session("reader").snapshot();
    let writer = db.session("writer");
    let mut expected = vec![0; rows];
    for update in 0..UPDATES {
        let index = update % rows;
        let mut tx = writer.transaction();
        let row = tx.get(FAMILY, id(index))?.ok_or("missing fixed-work row")?;
        assert_eq!(row.values[1], Value::Int(expected[index]));
        tx.update(FAMILY, id(index), values(index, expected[index] + 1))?;
        tx.commit()?;
        expected[index] += 1;
    }
    assert_eq!(expected.iter().sum::<i64>(), UPDATES as i64);
    verify_counters(&db, &expected)?;
    let original = retained.scan(FAMILY)?;
    assert_eq!(original.len(), rows);
    for (index, row) in original.iter().enumerate() {
        verify_row(row, index, 0);
    }
    assert!(matches!(
        db.anonymous().snapshot().get(FAMILY, id(0)),
        Err(Error::Denied)
    ));
    assert!(matches!(
        db.session("unrelated").snapshot().scan(FAMILY),
        Err(Error::Denied)
    ));
    Ok(json!({"protocol":1,"suite":"kernel-memory","rows":rows,"verified":true}))
}

fn main() -> AnyResult<()> {
    let command = Command::parse(&std::env::args().skip(1).collect::<Vec<_>>())?;
    let output = match command {
        Command::Benchmark(options) => run(options)?,
        Command::Memory { rows } => run_memory(rows)?,
    };
    println!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cli_rejects_invalid_or_extra_arguments() {
        for args in [
            vec!["7"],
            vec!["8", "NaN"],
            vec!["8", "inf"],
            vec!["8", "0"],
            vec!["8", "0.1", "extra"],
            vec!["--unknown"],
        ] {
            assert!(
                Options::parse(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err()
            );
        }
        assert_eq!(Options::parse(&[]).unwrap().rows, 10_000);
    }
    #[test]
    fn deterministic_fixture_enforces_values_indexes_and_private_access() {
        let db = Database::memory();
        install_and_load(&db, 8).unwrap();
        verify_fixture(&db, 8).unwrap();
        assert_eq!(
            db.admin().snapshot().schema(FAMILY).unwrap(),
            module().relations[0]
        );
    }

    #[test]
    fn memory_mode_requires_exactly_one_valid_row_count() {
        for args in [
            vec!["--memory"],
            vec!["--memory", "7"],
            vec!["--memory", "8", "0.1"],
            vec!["--memory", "unknown"],
        ] {
            assert!(
                Command::parse(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err()
            );
        }
        assert!(matches!(
            Command::parse(&["--memory".into(), "8".into()]).unwrap(),
            Command::Memory { rows: 8 }
        ));
        assert!(matches!(
            Command::parse(&[]).unwrap(),
            Command::Benchmark(_)
        ));
    }

    #[test]
    fn fixed_memory_workload_verifies_all_updates_and_retained_state() {
        assert_eq!(
            run_memory(8).unwrap(),
            json!({"protocol":1,"suite":"kernel-memory","rows":8,"verified":true})
        );
    }
}
