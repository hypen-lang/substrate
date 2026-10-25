//! Controlled durable append+head storage experiment, not a new Substrate backend.
use redb::{ReadableDatabase, TableDefinition};
use rusqlite::{Connection, params};
use std::{path::Path, time::Instant};

const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("records");
const HEAD: &[u8] = b"head";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

enum Store {
    Redb(redb::Database),
    Sqlite(Connection),
    #[cfg(feature = "rocks")]
    Rocks(rocksdb::DB, std::path::PathBuf),
}
impl Store {
    fn open(kind: &str, path: &Path) -> Result<Self> {
        Ok(match kind {
            "redb" => {
                let db = redb::Database::create(path)?;
                let tx = db.begin_write()?;
                tx.open_table(KV)?;
                tx.commit()?;
                Self::Redb(db)
            }
            "sqlite_wal_full" => {
                let db = Connection::open(path)?;
                db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA wal_autocheckpoint=1000; CREATE TABLE IF NOT EXISTS records(k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;")?;
                let mode: String = db.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
                let sync: u64 = db.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
                let full: u64 = db.query_row("PRAGMA fullfsync", [], |r| r.get(0))?;
                let checkpoint_full: u64 =
                    db.query_row("PRAGMA checkpoint_fullfsync", [], |r| r.get(0))?;
                let checkpoint_pages: u64 =
                    db.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))?;
                assert_eq!(mode, "wal");
                assert_eq!(sync, 2);
                assert_eq!(full, 1);
                assert_eq!(checkpoint_full, 1);
                assert_eq!(checkpoint_pages, 1000);
                Self::Sqlite(db)
            }
            #[cfg(feature = "rocks")]
            "rocks_sync_full" => {
                let mut options = rocksdb::Options::default();
                options.create_if_missing(true);
                options.set_compression_type(rocksdb::DBCompressionType::None);
                options.set_use_fsync(true);
                options.set_write_buffer_size(128 * 1024 * 1024);
                options.set_disable_auto_compactions(true);
                Self::Rocks(rocksdb::DB::open(&options, path)?, path.to_owned())
            }
            _ => return Err(format!("unsupported backend {kind}").into()),
        })
    }
    fn append(&mut self, records: &[(Vec<u8>, Vec<u8>)], head: u64) -> Result<()> {
        let head_bytes = head.to_le_bytes();
        match self {
            Self::Redb(db) => {
                let mut tx = db.begin_write()?;
                tx.set_durability(redb::Durability::Immediate)?;
                {
                    let mut table = tx.open_table(KV)?;
                    for (k, v) in records {
                        table.insert(k.as_slice(), v.as_slice())?;
                    }
                    table.insert(HEAD, head_bytes.as_slice())?;
                }
                tx.commit()?;
            }
            Self::Sqlite(db) => {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                {
                    let mut statement =
                        tx.prepare_cached("INSERT OR REPLACE INTO records(k,v) VALUES(?1,?2)")?;
                    for (k, v) in records {
                        statement.execute(params![k, v])?;
                    }
                    statement.execute(params![HEAD, head_bytes.as_slice()])?;
                }
                tx.commit()?;
            }
            #[cfg(feature = "rocks")]
            Self::Rocks(db, _path) => {
                let mut batch = rocksdb::WriteBatch::default();
                for (k, v) in records {
                    batch.put(k, v);
                }
                batch.put(HEAD, head_bytes);
                let mut options = rocksdb::WriteOptions::default();
                options.set_sync(true);
                options.disable_wal(false);
                db.write_opt(batch, &options)?;
                // librocksdb-sys's build does not define HAVE_FULLFSYNC. Add
                // Rust's drive-cache flush before acknowledgement on macOS.
                // This bounded experiment stays below the write-buffer limit.
                #[cfg(target_os = "macos")]
                {
                    let mut flushed = 0;
                    for entry in std::fs::read_dir(_path)? {
                        let file = entry?.path();
                        if file.extension().is_some_and(|s| s == "log") {
                            std::fs::OpenOptions::new()
                                .read(true)
                                .write(true)
                                .open(file)?
                                .sync_data()?;
                            flushed += 1;
                        }
                    }
                    assert!(flushed > 0, "expected active WAL for full-flush experiment");
                }
            }
        }
        Ok(())
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(match self {
            Self::Redb(db) => db
                .begin_read()?
                .open_table(KV)?
                .get(key)?
                .map(|v| v.value().to_vec()),
            Self::Sqlite(db) => {
                use rusqlite::OptionalExtension;
                db.query_row("SELECT v FROM records WHERE k=?1", [key], |r| r.get(0))
                    .optional()?
            }
            #[cfg(feature = "rocks")]
            Self::Rocks(db, _) => db.get(key)?,
        })
    }
}
fn record(revision: u64, size: usize) -> (Vec<u8>, Vec<u8>) {
    let mut key = vec![b'r'];
    key.extend(revision.to_be_bytes());
    let bytes = (0..size)
        .map(|i| ((revision.wrapping_mul(17) + i as u64 * 31) % 251) as u8)
        .collect();
    (key, bytes)
}
fn verify(store: &Store, count: usize, size: usize) -> Result<()> {
    assert_eq!(
        store.get(HEAD)?,
        Some((count as u64).to_le_bytes().to_vec())
    );
    for revision in 1..=count as u64 {
        let (k, v) = record(revision, size);
        assert_eq!(store.get(&k)?, Some(v));
    }
    assert_eq!(store.get(&record(count as u64 + 1, size).0)?, None);
    Ok(())
}
fn percentile(values: &mut [f64], q: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * q) as usize]
}
fn run(kind: &str, group: usize, count: usize, size: usize, repeat: usize) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("database");
    let mut store = Store::open(kind, &path)?;
    let data: Vec<_> = (1..=count as u64).map(|n| record(n, size)).collect();
    let mut times = Vec::new();
    let start = Instant::now();
    for (i, records) in data.chunks(group).enumerate() {
        let before = Instant::now();
        store.append(records, ((i * group) + records.len()) as u64)?;
        times.push(before.elapsed().as_secs_f64() * 1e6);
    }
    let elapsed = start.elapsed().as_secs_f64();
    verify(&store, count, size)?;
    drop(store);
    let reopened = Store::open(kind, &path)?;
    verify(&reopened, count, size)?;
    println!(
        "{}",
        serde_json::json!({"backend":kind,"repeat":repeat,"records":count,"payload_bytes":size,"group_size":group,"durable_groups":times.len(),"records_per_second":count as f64/elapsed,"group_p50_us":percentile(&mut times,0.5),"group_p99_us":percentile(&mut times,0.99),"elapsed_seconds":elapsed,"reopen_verified":true,"macos_full_flush":cfg!(target_os="macos")})
    );
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|s| s == "verify") {
        let store = Store::open(&args[2], Path::new(&args[3]))?;
        verify(&store, args[4].parse()?, 256)?;
        return Ok(());
    }
    if args.get(1).is_some_and(|s| s == "crash-write") {
        let kind = &args[2];
        let path = Path::new(&args[3]);
        let count = args[4].parse::<usize>()?;
        let mut store = Store::open(kind, path)?;
        for start in (1..=count as u64).step_by(8) {
            let rows: Vec<_> = (start..start + 8).map(|n| record(n, 256)).collect();
            store.append(&rows, start + 7)?;
        }
        // Avoid all destructors: acknowledged data must survive process loss.
        std::process::exit(73);
    }
    let kind = args.get(1).map(String::as_str).unwrap_or("redb");
    let count = args
        .get(2)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(1024usize);
    assert!(count > 0 && count <= 16384 && count.is_multiple_of(32));
    for repeat in 0..3 {
        for group in [1, 8, 32] {
            run(kind, group, count, 256, repeat)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_batches_preserve_records_and_head_after_reopen() -> Result<()> {
        for kind in backends() {
            run(kind, 8, 32, 256, 0)?;
        }
        Ok(())
    }
    fn backends() -> Vec<&'static str> {
        #[allow(unused_mut)]
        let mut values = vec!["redb", "sqlite_wal_full"];
        #[cfg(feature = "rocks")]
        values.push("rocks_sync_full");
        values
    }
}
