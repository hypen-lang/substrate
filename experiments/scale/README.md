# Durable scale and concurrency experiments

This harness measures the existing engine. It does not change storage, authorization,
or query execution. Run from the repository root on macOS with Rust and PostgreSQL
CLI tools installed. PostgreSQL runs in a fresh private Unix-socket cluster; no
existing database is used. All data files are disposable, while results are retained.

```sh
cargo test --offline --example scale_benchmark
cargo build --release --offline --example scale_benchmark
python3 experiments/scale/run.py --output /tmp/substrate-scale-results
python3 experiments/scale/analyze.py /tmp/substrate-scale-results
```

Default sizes are **100, 1,000, 10,000, 100,000, 1,000,000, and 10,000,000 total
application rows**. Default concurrency is 1, 2, 4, 8, 16, 32, 64, and 128 clients,
with separate read-only, write-only, and four-readers-plus-N-writers runs. Each
configuration gets two seconds, followed by completion of outstanding operations.
Use `--seconds`, `--clients`, `--rows`, `--modes`, and `--backends` to select follow-up runs. Substrate tuning overrides are `--checkpoint-commits`, `--checkpoint-bytes`, `--cache-bytes`, `--group-size`, and `--collection-us`; the example records their effective values. PostgreSQL defaults to read committed; `--postgres-isolation serializable` makes the transaction isolation comparison stricter. Each server records its actual settings.
Use `--point-api session` to select the fresh Rust `Session::get` point-read API; the default `snapshot` retains the earlier per-operation `snapshot().get()` workload. This affects native Substrate point reads only, not SQL, writes, or SQLite. Results record the selected API.
Use `--held-read` to retain SQLite/PostgreSQL read transactions throughout the SQL comparison, matching Substrate’s retained snapshot.
256 clients is the harness ceiling, not an engine limit. Repeat promising and
saturated configurations for longer before claiming sustained capacity.

The retained follow-ups can be reproduced into fresh output directories:

```sh
python3 experiments/scale/run.py --rows 1000000 --clients 32 --seconds 60 --modes write --held-read --output /tmp/scale-sustained32
python3 experiments/scale/run.py --rows 1000000 --clients 128 256 --seconds 3 --held-read --output /tmp/scale-saturation
python3 experiments/scale/run.py --rows 10000000 --backends postgres --clients 1 8 32 --seconds 3 --modes write mixed --held-read --output /tmp/scale-pg10m
```

The runner guards against more than 5 GiB summed process physical footprint,
less than 3 GiB free disk, or more than 900 seconds in one process phase. These
are adjustable safety limits. A guarded run preserves progress and marks the
stage incomplete; it is not a result at the requested target row count. Dataset
files are removed after each backend to limit peak disk use.

## Fixture and correctness

Five populated tables cross the `core`, `fitness`, and `crm` schemas. Each complete
person has two workouts, two workout links, two deals, and two deal links: nine
rows. A final person with no links makes every requested size exact. Substrate
also installs the demo's empty `TrainingGoal` definition and internal visibility
records; these are not counted as application data rows. All identifiers are 16-byte
UUIDs. Child IDs use a separate upper-64-bit family tag, avoiding collisions at
large sizes. SQLite uses BLOB UUIDs, unlike the older TEXT-UUID comparison harness.
PostgreSQL has native UUIDs. SQLite flattens the three namespaces into table names.

All backends keep primary keys, foreign keys, and indexes on the seven ordinary
fields/reference roles. Substrate also maintains identity allocation, incoming
references, conflict versions, and authorization metadata. Schemas are private;
Substrate's benchmark principal has explicit grants. SQL comparison uses the same
nine query shapes as `benches/comparison.rs`. Returned values are canonicalized and
hashed, and every measured query is checked against its first result. The runner
compares digests across all completed backends and records missing coverage.

Writers choose random workouts, increment one integer, and commit. Substrate uses
an optimistic read-modify-write transaction; conflicts are counted without retry.
SQLite uses `BEGIN IMMEDIATE` and a 250 ms busy timeout. PostgreSQL uses atomic
`UPDATE` at read committed isolation. Each successful update increments exactly
once; aggregate changes must equal successful commits. Readers fetch random
people and their names. Small datasets intentionally contend on fewer rows.

## What the numbers include

- **SQL query latency:** seven warm prepared executions plus a first execution.
  Rust materializes and canonicalizes rows; PostgreSQL uses libpq and Python string
  rows, including local IPC. Seven samples are screening medians, not reliable p99s.
- **Concurrent TPS:** Substrate native API, embedded SQLite SQL, PostgreSQL SQL over
  persistent local sessions. This is not a DataFusion TPS benchmark or a remote
  Substrate service benchmark. Report successful write TPS and read operations/sec
  separately. Failed attempts do not contribute to throughput.
- **Concurrency timing:** Rust excludes connection/thread setup, releases workers
  at a barrier, and includes final drain. PostgreSQL reports both pgbench pool TPS
  and common-wall TPS including connections. Mixed PostgreSQL pools start
  independently. These timing boundaries are not identical.
- **Latency:** per-operation successful and failed latencies for Rust, with
  logarithmic histograms whose percentile error is at most 2%. PostgreSQL records
  every completed transaction in pgbench latency logs. Reports include p50/p95/p99;
  short runs remain susceptible to noise and starvation.
- **Loading:** atomic batches of up to 1,000 people (9,000 rows), with indexes and
  references active. Rust constructs and inserts records on the client; PostgreSQL
  generates rows on the server. Loading times are not equivalent client-ingest
  throughput. Final Substrate checkpoint / SQLite checkpoint and ANALYZE /
  PostgreSQL ANALYZE are recorded separately.
- **CPU:** exact getrusage CPU for Rust; sampled process-tree CPU for PostgreSQL.
  One core-equivalent means one CPU core continuously busy. macOS absolute-time
  ticks are converted using `mach_timebase_info`, not assumed to be nanoseconds.
- **Memory:** Rust lifetime high-water RSS and sampled current RSS/physical footprint.
  PostgreSQL sums across server/client processes and can count shared pages more
  than once. Samples can miss short peaks and final work of short-lived processes.
  These are process measurements, not exclusive allocations per record.
- **Storage:** apparent and allocated Rust file bytes including WAL/SHM when present;
  PostgreSQL table-plus-index, database, and whole-cluster bytes are separate.
  Size per record includes fixed overhead, particularly visible on tiny fixtures.
- **Bandwidth:** logical application bytes (16-byte ID plus integer/name), PostgreSQL
  WAL bytes, and sampled OS-attributed disk bytes. Logical bytes exclude protocol,
  index, and log overhead. These are not network throughput or SSD-controller
  bandwidth measurements. PostgreSQL name-byte throughput is an explicitly labeled
  estimate from the dataset's average name length.
- **Recovery:** warm-cache reopen and usable point read; PostgreSQL also includes
  server launch/polling. Abrupt process termination preserves an acknowledged
  sentinel update. This is process-crash recovery, not simulated power loss.

## Durability matters on macOS

redb Immediate calls Rust `File::sync_data`, which uses `F_FULLFSYNC` on Apple.
SQLite uses WAL with `synchronous=FULL`, `fullfsync=ON`, and
`checkpoint_fullfsync=ON`. PostgreSQL uses `synchronous_commit=on`, `fsync=on`,
`full_page_writes=on`, and `wal_sync_method=fsync_writethrough`. This matches the
hardware-flush intent, although engines perform different work and numbers of
flushes. PostgreSQL's default `fsync` is substantially faster on this machine but
is not the same flush request. Do not mix those results.

Reference: [Rust's Apple synchronization implementation](https://doc.rust-lang.org/src/std/sys/fs/unix.rs.html).

## Files

`metadata.json` records the executable hash, platform, configuration, and source
base commit. Per-phase JSONL retains measurements and failures. Resource JSONL
retains half-second samples. `summary.json` records completed/guarded stages.
`query-parity.json` includes per-query backend coverage. `analyze.py` adds normalized
JSON plus CSVs for loading, SQL, and concurrency. Results come from an interactive
machine, not a dedicated performance lab. Existing background work and cache state
can affect timing; no OS cache flush or clock/frequency control is attempted.

## Native read sampling

`profile_reads.py --binary PATH --output NEW_DIRECTORY` builds a disposable million-record fixture and samples only its child read process with macOS `sample`. It has a 5 GiB footprint/120-second workload guard and removes its database afterward. Use `--point-api session` for the borrowed session path. The sampled workload incurs profiler overhead; use separate unsampled `run.py` runs for throughput/latency comparisons. The ownership report is generated from retained outputs by `ownership_report.py`.
