# Same-runner SQL and native benchmark

Build once, then run every engine serially on the **same machine**:

```sh
cargo build --release --offline --example paged_sql_bench
python3 experiments/paged_sql/run.py --rows 10000 100000 --rounds 3 --output /tmp/paged-sql-results
```

Default modes: resident Substrate, paged Substrate, SQLite, PostgreSQL 18. The
runner records the same executable hash for all four and rotates execution order.
Every size/mode/round gets a fresh client process and database. PostgreSQL requires
`pg_config` and the matching installed server/libpq; use `--pg-config PATH` when
needed. The runner creates and removes a disposable cluster for every PostgreSQL
case, listens only on a private Unix socket, and never uses an existing database.
All setup/server logs and raw results remain in the output directory.

`--rows` counts **people**. An equally large Score family makes total application
rows `2 × people`, across two relation families and two namespaces. Each person has
one score with a typed foreign key. The same Rust helpers generate parameters and
expected results for every backend; every timed result is checked. All engines use
256-row atomic loading batches and indexes on the same logical fields. PostgreSQL
uses UUID identities; SQLite uses text identities.

Reads use prepared statements and one retained snapshot per case. PostgreSQL uses
a read-only REPEATABLE READ transaction over a persistent libpq connection, SQLite
an explicit read transaction, and Substrate a retained snapshot. The warm parameter
case repeats the exact 2,000-key sequence used by the preceding first-pass case.
The OS cache is not flushed: "first pass" is not a claim of cold physical disks.

Use `--reference-order reversed` to exercise descending reference IDs while score
rows remain ordered by identity. The default is `ordered`. The same mapping and
inverse join expectation apply to all four engines, and every result records its
fixture order. Reversed loading also inserts people in descending identity order
so each referenced person exists before its score, retaining the same atomic
batches. Consequently, its loading numbers include changed insertion order.
An older benchmark binary without this option is accepted only for ordered runs;
the runner rejects a missing or incorrect fixture marker for reversed runs.

Physical cache targets are 8 MiB; paged Substrate adds a 1 MiB decoded-row cache.
PostgreSQL has 4 MiB `work_mem`, parallel queries disabled, and verified recorded
settings. Durability is redb Immediate, SQLite WAL/FULL with macOS fullfsync enabled,
and PostgreSQL fsync/synchronous_commit/full_page_writes on with macOS
fsync_writethrough (fdatasync on Linux). PostgreSQL ANALYZE and server startup are
separate reported costs, outside query and row-loading timings.

## Interpret the numbers carefully

- PostgreSQL query latency includes **local IPC and text protocol conversion**;
  the other modes call embedded engines directly. Same-runner end-to-end access
  costs are useful, but these are not isolated query-engine execution rankings.
- PostgreSQL loading uses individual prepared row inserts over that connection,
  in the same atomic batches as the others. It is **not a COPY/bulk-loader maximum**.
- Substrate reads enforce grants and reference visibility. SQLite and PostgreSQL
  have equivalent logical rows/indexes/foreign keys, but no matching authorization
  policy layer in this control.
- `resources`/`client_resources` mean the benchmark **client** process. Do not call
  PostgreSQL client RSS its database memory. `server_resources` separately records
  the postmaster's OS accounting and sampled server-process CPU/RSS. Summed process
  RSS double-counts shared pages; sampled CPU is a lower bound. The 250 ms server
  monitor adds overhead only to PostgreSQL cases and records its sampling time.
- Cache targets are not RSS limits. Three repeated runs are diagnostic comparisons,
  not a statistical regression gate or a concurrency/TPS saturation test.

Use `--rows 1000000 --rounds 1` for the two-million-row SQL fixture, or `--modes`
to select backends. Do not overlap timed runs with builds, tests or other benchmarks.
The runner requires 5 GiB free before each case (`--min-free-gib` changes this
margin); this is a preflight check, not a storage quota or a capacity prediction.
Allow additional headroom for large fixtures. Client temporary databases are owned
by the runner and removed even when a timed child is interrupted or killed.

See the measured four-engine comparison (historical results removed).

Validate the runner without a database using:

```sh
python3 -m unittest discover -s experiments/paged_sql -p 'test*.py' -v
```

### Filtered and grouped aggregate suite

`--suite reads` remains the default: the original six cases and original two-role
Score schema. `--suite aggregates` adds a `bucket = score_ordinal % 32` integer
role and its index to Score in every engine. It runs the same six cases plus:

- Equality-filtered count/sum (one row).
- Range-filtered count/sum (1% of scores).
- Broad-filtered count/sum (90% of scores).
- Count/sum grouped into 32 buckets.
- Count/sum grouped into 32 buckets after selecting the upper half of scores.

All group queries order by bucket, and every result is checked. New aggregate
expectations are precomputed before timing with a separate arithmetic reference.
Before/after comparisons must use the **same suite**; the additional role/index
changes storage and loading work. Older binaries without a suite marker are only
accepted for the default reads fixture. The environment variable
`SUBSTRATE_PAGED_SQL_SUITE` controls direct executable invocation; the runner
removes any ambient value and sets it explicitly.

```sh
python3 experiments/paged_sql/run.py --suite aggregates --rows 10000 100000 --rounds 3 --output /tmp/paged-filtered-grouped
```

### Reproducible family identities

Fixture version `fixed-schema-ids-v1` uses explicit relation identities:
`core.Person` is `01000000-0000-0000-0000-000000000001`, and `app.Score` is
`02000000-0000-0000-0000-000000000002`. Role identities derive from the relation
identity and stable role slot. These are disjoint from occurrence IDs and internal
control-plane relation identities. Every engine emits the same complete logical
`schema_manifest` and fixture version; for SQLite/PostgreSQL these describe the
logical comparison fixture, not their internal table object IDs. The runner rejects
mismatched manifests across engines or rounds.

Earlier binaries generated fresh relation UUIDs per process, which changed
physical family ordering and could influence cache behavior. Those earlier runs
are diagnostic evidence, not controlled before/after comparisons against this
fixed fixture. Legacy binaries remain executable for investigation and are labeled
`legacy-random-schema-ids` in summaries. Rebuild both the saved baseline source and
the candidate with the same current harness for a controlled comparison.

`--suite aggregates-wide` keeps the same schema, rows, indexes and queries but
uses **1,024 buckets**. This exercises grouping beyond the executor's small
numeric-group cache. It has its own suite marker and cannot be combined with
32-bucket results. The expected results are independently checked for both suites.

Query records also include `client_cpu_us` when the platform provides process CPU
accounting. This includes every client thread and excludes the PostgreSQL server;
server CPU remains separately sampled. CPU and elapsed time are both means per
execution in the timed loop, including the same result validation. Older saved
binaries may omit CPU; their unavailable CPU is not treated as zero.

### Overall million-record comparison and diagnostics

`compare_paged.py --before BASELINE --after CANDIDATE --output DIRECTORY`
runs the saved paged binaries, SQLite and PostgreSQL sequentially, rotating the
order across rounds. Add `--control CONTROL` to isolate an intermediate change
with its own binary hash and results. All three paged binaries must implement the
same fixture; the control uses the candidate's declared storage format. Use
`--rows 500000 --rounds 3` for one million total records. Run `--io-diagnostics`
separately: its instrumented timings are not headline measurements.

Use `--suite aggregates --rows 500000 --rounds 3` for **one million total
application records** and all eleven query cases. Default modes include both
resident and paged Substrate alongside SQLite and PostgreSQL. Resident Substrate
keeps current rows/indexes in memory; it is a distinct memory/performance tradeoff,
not the bounded-memory storage mode. This suite measures single-client prepared
reads and loading in 256-record transactions; it does not measure concurrent TPS
ceilings, per-request p99, network bandwidth, or remote Substrate service latency.

New binaries collect `file_footprint` after all timed queries while embedded
databases and their snapshots remain open. It includes the main database and
explicitly known SQLite WAL/shared-memory/journal sidecars, logical file lengths,
and filesystem-reported allocated bytes on Unix. It does not force a checkpoint,
compact files, or measure peak temporary disk usage. PostgreSQL `relation_bytes`
includes the two relations and indexes; `database_bytes` includes its database
catalogs but excludes cluster-wide WAL and other cluster files. These scopes are
reported separately, not presented as equivalent total storage measurements.

SQLite `sqlite_query_plans` and PostgreSQL `postgres_query_plans` capture untimed
plans for the range, broad, grouped, and filtered grouped cases after all timed
queries. PostgreSQL explains the existing named prepared statement with the same
fixture parameters and session, without `ANALYZE`. These are planner output, not
execution profiles. Older binaries lack footprint/plan telemetry; missing metrics
must be shown as unavailable, not zero.
The inspected plan is selected after the timed loop; it is not a historical trace
of each execution. PostgreSQL may reconsider custom/generic plans during EXPLAIN,
and SQLite recompiles the diagnostic statement. Whole-process resources include
these diagnostics, while per-query latency and CPU exclude them.

### Phase memory and allocator diagnostics

New embedded benchmark binaries record `peak_rss_phases` after loading, read
setup, timed queries and final diagnostics. Each query also records
`peak_rss_bytes_after_case`. These are process-lifetime high-water RSS snapshots
at untimed boundaries: subtracting them does **not** give current memory,
allocations made by a query, or bytes freed afterward. Whole-process client RSS
still includes loading and diagnostic work. Missing fields in older binaries or
other modes are unavailable measurements, never zero.

Only explicitly instrumented I/O runs on macOS collect `allocator_phases` and
`allocator_after_case`. These sum allocator zones after loading/setup/cases/final
diagnostics and distinguish live heap (`size_in_use`) from allocator-reserved
bytes (`size_allocated`); neither is process RSS. Zone traversal occurs outside
timed loops and is disabled for ordinary headline runs. Diagnostic heap traversal
can itself affect later process state, so its observations must stay separate
from the headline comparison. Other platforms return unavailable allocator data.

### Before/after comparison and retained results

`compare_paged.py` runs the aggregate suite using two saved executables, SQLite
and PostgreSQL in rotating order. Its default storage expectations are LPG3 for
both binaries; `--before-format`/`--after-format` explicitly select older layouts.
It verifies all eleven cases, schema fixtures and result parity, and records
binary/runner hashes, resources and run order. For one million total records:

```sh
python3 experiments/paged_sql/compare_paged.py \
  --before /tmp/substrate-before --after /tmp/substrate-after \
  --rows 500000 --rounds 3 --output /tmp/substrate-comparison
python3 experiments/paged_sql/compare_paged.py \
  --before /tmp/substrate-before --after /tmp/substrate-after \
  --rows 500000 --io-diagnostics --output /tmp/substrate-comparison-io
```

Add `--pg-config PATH` if required. Output destinations must be fresh. Ordinary
runs include four engines; the diagnostic invocation contains only the two paged
binaries. Each output contains combined `results.json`, `summary.json`,
`plan.json`, and per-run environments and process artifacts. Summaries report
medians of round means, not per-request percentiles.

The repository retains only the latest completed comparison under
`docs/benchmarks/latest/`, with its report, comparison/I/O evidence, environments
and validation/review records. Run candidates in fresh working directories;
replace the retained evidence only after completing and validating the new set.
Do not combine old and new rounds. Runnable experiments remain in the repository
when historical result artifacts are removed. Saved metadata and logs redact
personal home/workspace paths without modifying subprocess execution arguments.

### Public native API matrix

Existing mode names keep their SQL meaning and remain the default. Opt in to all
four Substrate combinations alongside both SQL controls:

```sh
python3 experiments/paged_sql/run.py --suite aggregates --rows 10000 100000 --rounds 3 \
  --modes resident paged native-resident native-paged sqlite postgres \
  --output /tmp/substrate-api-matrix
```

`native-resident` and `native-paged` load the same durable fixture and retain the
same authenticated reader snapshot. They never construct SQL. Points use `get`,
equalities use public `lookup`/`lookup_page`, and the selective join uses two
public equality lookups with a small client join. Public native calls return full
owned rows, then the benchmark projects the requested fields; SQL may push down
its projection. All eleven operations are checked against the same reference results, including reversed references.

Native aggregate cases measure **client-side count/sum/group folds over today's
public native APIs**, not a native aggregate planner. Equality uses the public
index lookup. Range and broad filters scan then filter on the client. Resident
`scan`/`lookup` eagerly produce `Vec<Row>`; paged scans retain at most a 256-row
page. Fold state holds at most the fixture's 32 or 1,024 groups and uses checked
integer arithmetic. This difference is intentional and appears in each result's
`execution_path` and `native_source_materialization`. It means this matrix cannot
be interpreted as SQL parsing overhead alone: SQL aggregate execution can stream
inside the engine and use range indexes that the current public native interface
does not expose. Native `prepare_us` is zero and marked not applicable.

Results explicitly label `interface` and `storage`. The runner rejects native
results missing these labels or any case. SQL results from older saved binaries
remain readable. File footprints and query-plan inspection run after timing.
Paged modes also rerun each case once using opt-in logical read counters on an
authorized reader snapshot. Those counts are **not physical disk reads or redb
page-cache misses**. Host-only table storage accounting is separate and runs last;
it excludes file headers, allocators, free pages, and historical retained pages.
Diagnostics affect whole-process resource totals, never per-query timing or CPU.

### Separate backend I/O diagnostics

Backend instrumentation is off in ordinary runs. Use a separate output directory
and explicitly select paged modes for diagnostic-only processes:

```sh
python3 experiments/paged_sql/run.py --suite aggregates --rows 100000 --rounds 1 \
  --modes paged native-paged --io-diagnostics --output /tmp/substrate-io-diagnostics
```

This opens the database with the opt-in `FileBackend` counter wrapper. Results
are labeled `timing_class: instrumented-diagnostic`, use `-io-diagnostic` artifact
names, and cannot be mixed with headline results by the runner. Their latency
includes counter-update overhead. Ambient diagnostic environment flags are
removed; only the explicit runner option enables them.

Each query reports total backend request counts and requested byte counts for
its measured iterations, excluding warmup and preparation. The subsequent single
logical diagnostic execution also reports its backend-counter delta. These are
requests reaching the stock file backend, which can still hit the OS cache;
they are **not physical disk I/O, syscall counts, or confirmed transferred bytes**.
Failed requests contribute requested bytes and increment errors. Opening/loading
work is excluded from the per-query deltas. Ordinary runs retain the original
uninstrumented storage backend.

New binaries also record `load_backend_io_counts` immediately after database
creation and loading in instrumented runs. This is cumulative opening/loading
work, not a per-query delta. `paged_diagnostics.commits` records completed durable
groups and accepted write transactions since open, including schema setup. These
engine counters are not filesystem synchronization counts or a concurrency/TPS
measurement. Serial loading normally has one transaction per group; group commit
can combine only concurrently outstanding transactions.

The completed million-record six-mode matrix (historical results removed) includes all eleven cases, CPU/memory, matched old-adapter controls, and separate I/O diagnostics.
