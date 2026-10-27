//! Explicitly invoked storage-only measurement. Counters observe redb's backend
//! calls, not OS attribution or SSD-controller traffic. Never run in normal tests.
use super::*;
use redb::StorageBackend;
use std::{
    fs::OpenOptions,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug, Default)]
struct Counters {
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    write_calls: AtomicU64,
    sync_calls: AtomicU64,
}
impl Counters {
    fn snapshot(&self) -> [u64; 4] {
        [
            &self.read_bytes,
            &self.write_bytes,
            &self.write_calls,
            &self.sync_calls,
        ]
        .map(|counter| counter.load(Ordering::Relaxed))
    }
}
#[derive(Debug)]
struct CountedBackend {
    inner: redb::backends::FileBackend,
    counts: Arc<Counters>,
}
impl StorageBackend for CountedBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)?;
        self.counts
            .read_bytes
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()?;
        self.counts.sync_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)?;
        self.counts
            .write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.counts.write_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}
fn diff(before: [u64; 4], after: [u64; 4]) -> [u64; 4] {
    std::array::from_fn(|i| after[i] - before[i])
}
#[derive(Default)]
struct Phase {
    groups: u64,
    transactions: u64,
    counts: [u64; 4],
    seconds: f64,
    encoded_bytes: u64,
    max_seconds: f64,
    compactions: u64,
}
impl Phase {
    fn add(&mut self, transactions: usize, counts: [u64; 4], seconds: f64, bytes: u64) {
        self.groups += 1;
        self.transactions += transactions as u64;
        for (total, next) in self.counts.iter_mut().zip(counts) {
            *total += next;
        }
        self.seconds += seconds;
        self.max_seconds = self.max_seconds.max(seconds);
        self.encoded_bytes += bytes;
    }
    fn emit(&self, rows: usize, group_size: usize, name: &str, format: u8) {
        println!(
            "STORAGE_IO {}",
            serde_json::json!({
                "rows":rows, "transactions_per_group":group_size,"phase":name,
                "groups":self.groups,"transactions":self.transactions,"seconds":self.seconds,
                "max_seconds":self.max_seconds,"compactions":self.compactions,"checkpoint_format":format,
                "backend_read_bytes":self.counts[0],"backend_write_bytes":self.counts[1],
                "backend_write_calls":self.counts[2],"backend_sync_calls":self.counts[3],
                "encoded_written_bytes":self.encoded_bytes,
                "bytes_per_transaction":(self.transactions != 0).then(||self.counts[1] as f64 / self.transactions as f64),
                "durability":"Immediate; FileBackend::sync_data delegated unchanged",
            })
        );
    }
}
fn identity(n: usize) -> Id {
    Id(uuid::Uuid::from_u128(n as u128 + 1))
}

#[test]
#[ignore = "explicit storage I/O experiment; run alone with --ignored --nocapture"]
fn checkpoint_backend_io_probe() {
    let rows: usize =
        std::env::var("SUBSTRATE_IO_PROBE_ROWS").map_or(100_000, |v| v.parse().unwrap());
    assert!((128..=1_000_000).contains(&rows));
    let updates: usize =
        std::env::var("SUBSTRATE_IO_PROBE_UPDATES").map_or(32768, |v| v.parse().unwrap());
    let format: u8 = std::env::var("SUBSTRATE_IO_PROBE_FORMAT").map_or(3, |v| v.parse().unwrap());
    assert!(matches!(format, 2 | 3));
    let groups = std::env::var("SUBSTRATE_IO_PROBE_GROUPS").unwrap_or_else(|_| "32,128".into());
    for group_size in groups.split(',').map(|v| v.parse::<usize>().unwrap()) {
        assert!(group_size > 0 && group_size <= 128 && updates.is_multiple_of(group_size));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let counts = Arc::new(Counters::default());
        let db = redb::Database::builder()
            .set_cache_size(64 * 1024 * 1024)
            .create_with_backend(CountedBackend {
                inner: redb::backends::FileBackend::new(file).unwrap(),
                counts: counts.clone(),
            })
            .unwrap();
        let (mut durable, mut state) = Durable::recover(db, DatabaseOptions::default()).unwrap();
        durable.force_v2_checkpoint = format == 2;
        durable.force_full_compaction =
            std::env::var("SUBSTRATE_IO_PROBE_COMPACTION").is_ok_and(|value| value == "full");
        let relation = Relation::new("probe.Counter", vec![Role::new("n", DataType::Int)]);
        let tid = relation.id;
        let install = Op::Install(Module::new("probe", 1, vec![], vec![relation]));
        state.apply(&install, 1);
        state.revision = 1;
        durable.append(1, &[install], &state).unwrap();
        for start in (0..rows).step_by(1000) {
            let mut operations = Vec::new();
            state.revision += 1;
            for n in start..rows.min(start + 1000) {
                operations.push(Op::Identity(identity(n)));
                operations.push(Op::Put(
                    tid,
                    Row {
                        id: identity(n),
                        values: vec![Value::Int(0)].into_boxed_slice(),
                    },
                ));
            }
            for op in &operations {
                state.apply(op, state.revision);
            }
            durable.append(state.revision, &operations, &state).unwrap();
        }
        durable.checkpoint(&state).unwrap();
        let mut phases = [Phase::default(), Phase::default(), Phase::default()];
        let mut random = 1729u64;
        for _ in 0..updates / group_size {
            let mut records = Vec::with_capacity(group_size);
            let mut revision = state.revision;
            for _ in 0..group_size {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let id = identity((random % rows as u64) as usize);
                let Value::Int(old) = state.row(tid, id).unwrap().values[0] else {
                    unreachable!()
                };
                let op = Op::Put(
                    tid,
                    Row {
                        id,
                        values: vec![Value::Int(old + 1)].into_boxed_slice(),
                    },
                );
                revision += 1;
                state.apply(&op, revision);
                records.push((revision, vec![op]));
            }
            state.revision = revision;
            let records: Vec<_> = records
                .iter()
                .map(|(r, ops)| (*r, ops.as_slice()))
                .collect();
            let before_io = counts.snapshot();
            let before = durable.info.persistence.clone();
            let start = Instant::now();
            durable.append_group(&records, &state).unwrap();
            let seconds = start.elapsed().as_secs_f64();
            let after = &durable.info.persistence;
            let checkpoint = if after.checkpoint_compactions != before.checkpoint_compactions {
                2
            } else {
                usize::from(after.checkpoints != before.checkpoints)
            };
            phases[checkpoint].add(
                group_size,
                diff(before_io, counts.snapshot()),
                seconds,
                after.checkpoint_written_bytes - before.checkpoint_written_bytes
                    + after.redo_written_bytes
                    - before.redo_written_bytes,
            );
            phases[checkpoint].compactions +=
                after.checkpoint_compactions - before.checkpoint_compactions;
            assert!(durable.info.log_commits < 128 && durable.info.log_bytes < 8 * 1024 * 1024);
            assert!(
                durable.info.overlay_keys <= MAX_OVERLAY_KEYS
                    && durable.info.overlay_bytes <= MAX_OVERLAY_BYTES
            );
        }
        for (phase, name) in phases.iter().zip(["redo", "checkpoint", "compaction"]) {
            phase.emit(rows, group_size, name, format);
        }
        let before = counts.snapshot();
        let start = Instant::now();
        let checkpoint = durable.checkpoint(&state).unwrap();
        let mut tail = Phase::default();
        tail.add(
            0,
            diff(before, counts.snapshot()),
            start.elapsed().as_secs_f64(),
            checkpoint.written_bytes,
        );
        tail.emit(rows, group_size, "final_checkpoint", format);
        drop(durable);
        drop(state);
        let start = Instant::now();
        let (_durable, recovered) = Durable::open(&path, DatabaseOptions::default()).unwrap();
        let recovery_seconds = start.elapsed().as_secs_f64();
        let sum: i64 = recovered.families[&tid]
            .values()
            .map(|row| {
                let Value::Int(n) = row.values[0] else {
                    unreachable!()
                };
                n
            })
            .sum();
        assert_eq!(sum, updates as i64);
        assert_eq!(recovered.families[&tid].len(), rows);
        println!(
            "STORAGE_IO {}",
            serde_json::json!({"rows":rows,"transactions_per_group":group_size,"recovered_updates":sum,"recovery_verified":true,"checkpoint_format":format,"recovery_seconds":recovery_seconds,"overlay_keys":_durable.info.overlay_keys,"overlay_bytes":_durable.info.overlay_bytes})
        );
    }
}
