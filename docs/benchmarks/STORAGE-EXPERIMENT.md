# Durable storage candidates

This isolated experiment compares redb 3.1.3, SQLite through rusqlite 0.32.1,
and RocksDB through rocksdb 0.25.0 (librocksdb-sys 0.19.0 / RocksDB 11.8.1).
It does not replace Substrate's backend or measure the entire engine.

## Method

Each fresh database receives 1,024 deterministic 256-byte records. Every group
atomically writes its records and a final revision head. Groups contain 1, 8, or
32 records; each combination runs three times in release mode. Allocation of
input payloads and database creation are outside timing. Timings include batch
construction, commit and required synchronization. All records and the head are
verified before close and after reopen.

- redb: one table, `Durability::Immediate` per group.
- SQLite: one `WITHOUT ROWID` key/value table, WAL, `synchronous=FULL`,
  `fullfsync=ON`, `checkpoint_fullfsync=ON`, automatic checkpoint threshold 1,000.
  Every group uses one immediate transaction; the configured pragmas are checked.
- RocksDB: one atomic WriteBatch, WAL enabled, `sync=true`, `use_fsync=true`,
  compression disabled. A 128 MiB write buffer and disabled automatic compaction
  keep this small append experiment below a memtable flush. This deliberately
  does **not** characterize sustained compaction, space amplification or deletes.

On this macOS machine, flush strength needs special care. Rust's `sync_data`
uses `F_FULLFSYNC`, and redb calls that API. SQLite explicitly enables its full
flush option. The pinned librocksdb-sys build does not define `HAVE_FULLFSYNC`,
which guards RocksDB's corresponding native call. Therefore the RocksDB harness
additionally opens each active WAL and calls Rust `sync_data` before acknowledging
the group. The reported RocksDB cost includes this wrapper, directory inspection,
and extra system calls. It measures this durability-matched configuration, not
an optimized native RocksDB backend. A future adapter should configure and verify
native full-flush support instead of copying this bounded-workload workaround.

Sources: [RocksDB synchronous writes](https://github.com/facebook/rocksdb/wiki/Basic-Operations#synchronous-writes),
[SQLite synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous),
[SQLite fullfsync](https://www.sqlite.org/pragma.html#pragma_fullfsync),
[Rust 1.94 Unix filesystem implementation](https://github.com/rust-lang/rust/blob/1.94.0/library/std/src/sys/fs/unix.rs).

## Verification and limits

The unit test verifies records and head across all three backends after reopen.
A subprocess test exits without running destructors immediately after acknowledged
groups, then a new process verifies every record and the head for each backend.
Both tests and strict all-target Clippy pass. These are process-loss checks, not
physical power-cut testing.
Substrate's separate group-commit tests cover publication, conflict, authorization,
checkpoint and injected failure behavior; those are not duplicated by this storage
microbenchmark.

This is one coordinator issuing already-grouped writes, not concurrent native
writer execution. Transaction count per durable group is controlled, and no OS
sync-call count is inferred. Database setup, final close/checkpoint, indexes,
recovery-time scaling, backups and long-running storage maintenance are excluded.

## Results

Measured on arm64 macOS 26.2, using release builds with rustc 1.94.0. Each cell
below is the median of three runs. Backend runs were serialized, with no other
repository benchmark or build running. Backend order was redb, SQLite, RocksDB;
this was not a randomized device-isolated trial.

| Backend | Records/group | Records/s | Group p50 (ms) | Group p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| redb Immediate | 1 | 247 | 3.999 | 6.026 |
| SQLite WAL FULL | 1 | 247 | 3.998 | 6.214 |
| RocksDB sync + full flush | 1 | 249 | 3.994 | 5.810 |
| redb Immediate | 8 | 1,971 | 3.997 | 5.118 |
| SQLite WAL FULL | 8 | 1,869 | 4.034 | 5.936 |
| RocksDB sync + full flush | 8 | 1,980 | 3.998 | 5.108 |
| redb Immediate | 32 | 7,835 | 3.997 | 4.718 |
| SQLite WAL FULL | 32 | 7,648 | 4.016 | 4.972 |
| RocksDB sync + full flush | 32 | 7,855 | 4.003 | 4.938 |

[Raw samples](storage-candidates.jsonl) include every run, group count and reopen
verification. With only 32 groups in a group-size-32 run, the p99 estimate is
coarse; it is not a production tail-latency characterization.

Keep redb for this milestone. All three spend about 4 ms per durable group on this
machine. At group size 32, the alternatives are within about 2.4% of redb's
throughput; this does not establish a useful backend advantage. Increasing the
redb group from 1 to 32 yields about 31.7× more records/s in this already-grouped
microbenchmark. That is distinct from the full engine's measured 15× improvement
at 32 concurrent writers, where queueing, validation and partially filled groups
are included.

This evidence supports amortizing synchronization before replacing storage.
It does not rank these backends for large indexed working sets, read amplification,
long-running compaction, larger records, sustained checkpoint traffic or multi-file
parallelism. Those need separate representative workloads before a backend change.

## Reproduction

```sh
cargo test --release --manifest-path experiments/storage/Cargo.toml --features rocks
cargo build --release --manifest-path experiments/storage/Cargo.toml --features rocks
experiments/storage/target/release/substrate-storage-experiment redb 1024
experiments/storage/target/release/substrate-storage-experiment sqlite_wal_full 1024
experiments/storage/target/release/substrate-storage-experiment rocks_sync_full 1024
```

RocksDB is optional because its native build requires a C++ toolchain and libclang.
The isolated Cargo.lock pins dependencies; the main crate's backend is unchanged.
