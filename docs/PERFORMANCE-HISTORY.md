# Performance history

Narrative benchmark notes moved out of the top-level README. Many linked reports were removed; the figures below are kept as historical context. The current, reproducible comparison is in [benchmarks/latest](benchmarks/latest/README.md).

## Disk-backed current data

Use `paged::PagedDatabase::open(path, options)` when current data should live on
disk with bounded caches. It now supports DataFusion SQL, the existing local/TLS
servers, generated Rust records and TypeScript SDKs, named writes, nested reads,
and online schema proposal/install. Credentials retain scoped permissions, expiry,
rotation, delegation and live revocation across these operations.

Current rows, indexes, identity history and conflict records stay on disk. Startup
compiles bounded schema/policy metadata instead of rebuilding every row. Existing
resident `Database` remains available; storage selection is explicit and existing
files are never converted automatically. LPG1/LPG2/LPG3 files can be converted
explicitly with `PagedDatabase::import_paged_file`.
`PagedDatabase::import_resident_file` streams LDB1/LDB2/LDB3 checkpoints and redo,
preserving typed rows and supported policies. Unsupported control metadata or
ownership mappings that would broaden access are rejected; see
[the import guide](PAGED-REPLICATION.md).

Run `npm run demo:paged` for a complete TypeScript/TLS example, or
`cargo run --example paged_sql` for SQL joins and aggregates. Existing CLI commands
accept `--paged`, including `serve`, `serve-tls`, `query` and manifest installation.
See [paged storage](PAGED-STORAGE.md) and the
integration benchmarks (historical results removed) for cache bounds,
authorization semantics, remaining limitations and measured performance.
Scalar aggregates, typed filters and direct-field grouping use bounded execution
with live authorization and reference checks. Prepared singleton reads and joins
reuse shared rows without copying unselected fields. See the
small-read and aggregate benchmarks (historical results removed)
and broad-scan follow-up (historical results removed) for the same-runner
SQLite/PostgreSQL results and remaining gaps.
The one-million-record overall comparison (historical results removed)
covers all eleven SQL cases, loading, CPU, memory, storage footprints and query plans
for both Substrate storage modes, SQLite and PostgreSQL on the same runner.
The newer native/SQL × resident/paged matrix (historical results removed)
adds all four Substrate paths alongside SQLite and PostgreSQL at one million
records, with cache/I/O diagnostics and a table-by-table storage breakdown (historical results removed).
Native aggregate figures include client folds over public APIs; they are not
isolated SQL parsing-overhead measurements.
The packed-scan follow-up (historical results removed) tests direct typed
field access and singleton setup, including unsuccessful batching variants and
another complete million-record SQL comparison. A separate
dense-layout experiment (historical results removed) measures
scan speed, space and update write amplification. New paged databases now use
[production dense family blocks](PAGED-STORAGE.md#dense-family-blocks-lpg3),
shared by native reads, SQL and SDK services. Existing files retain their format;
authorization and logical transaction conflicts remain independent of block layout.
The [production dense comparison](benchmarks/latest/README.md) covers
the same million-record workload against the previous adapter, SQLite and PostgreSQL.

## Performance and next priorities

The scale and concurrency report (historical results removed) measures 100 through
10 million total rows, up to 256 clients, CPU, memory, disk size, logical/disk
throughput, latency, and recovery against SQLite and PostgreSQL. That report predates
paged storage: the **resident** engine completed one million rows, and its
ten-million-row load exceeded the experiment’s 5 GiB process footprint budget after
2.619 million confirmed rows. Paged mode removes that structural limit.

The latest [one-million-record paged comparison](benchmarks/latest/README.md)
contains all eleven SQL cases, CPU, memory, loading, storage and separate I/O
measurements against SQLite and PostgreSQL on the same runner. Broad and grouped
scans take 33.19/33.31 ms; filtered grouping takes 28.03 ms, 16.9% below an
intermediate baseline and 9.6% below the isolated borrowed-iterator control.
The 1% range aggregate falls 29.6% to 278.93 µs. Substrate remains 21.5% slower
than PostgreSQL on filtered grouping; the range gap is 9.5%. Loading is 29.78 s
with overlapping before/after ranges. Small reads are
mixed, with point/equality regressions against the control and a selective-join
regression against the intermediate baseline. Peak client RSS is 44.66 MiB versus
44.59 MiB control and 37.42 MiB baseline; identical sampled live heap but larger
allocator reservation does not establish the cause. The baseline already includes
earlier fixes, so these are not total start-of-task improvements.

A separate three-round matrix includes resident native/SQL and paged native/SQL.
Resident storage is durable; peak client RSS is 2,210/1,672 MiB for native/SQL,
versus 33.1/44.7 MiB for paged native/SQL. Native client folds and pushed-down SQL
aggregates do different work. A single-observation 1,024-group diagnostic leaves
filtered grouping 52.7% slower than PostgreSQL. This workload does not measure SQL
writes, imports, concurrent group commit or consensus. See all results, ranges,
provenance and scope limits before treating these figures as a general ranking.

The subsequent capacity tuning report (historical results removed) records
atomic snapshot publication, compact index postings, a configurable 64 MiB backend
cache, cheaper checkpoint replacements, adaptive commit grouping, and indexed SQL
EXISTS execution. At one million records and 32 concurrent writers, a 60-second
run improved durable throughput from **3,240 to 4,778 TPS**, with median latency
falling from **8.21 to 5.11 ms** and p99 from **17.09 to 14.30 ms**. At 128 writers,
throughput rose from 8,389 to 10,859 TPS, with p99 slightly higher at 22.11 versus
21.25 ms. The PostgreSQL SERIALIZABLE control at 32 writers reached 3,850 TPS with a
13.89 ms p99. These are workload-specific local API/IPC comparisons of the resident
engine, whose mutable data and indexes reside in RAM; disk amplification remains a priority.

The ownership and query follow-up (historical results removed)
profiles shared reference-count contention and adds `Session::get(relation, id)`
for fresh point reads. Warm scalar reads borrow session-owned handles while retaining
live authorization checks. The subsequent follow-up also caches compiled capability
scope for scalar reads; reference-bearing reads keep the snapshot path. Use
`Snapshot` when several operations must share a revision.
The same milestone adds bounded indexed top-one-per-partition SQL and removes
temporary checkpoint row/operation copies. The report includes before/after
million-record runs, rejected experiments, SQLite results, and exact query parity.
The storage I/O probe (historical results removed) separately measures backend
write amplification; it remains a storage-layout problem, not an ownership fix.

The storage, memory and scoped-read follow-up (historical results removed)
adds bounded checkpoint overrides, compact conflict-version maps, allocation-free
compiled grant lookups, and
`Session::get_shared` for authorized immutable rows without payload copies.
The [working example](../examples/shared_reads.rs) demonstrates filtered capability
reads, stable old results, and live credential revocation. Shared handles retain
their payload until released; they do not turn the engine into disk-backed mutable
storage. The report separates isolated allocation/read results from complete
engine measurements and SQLite/PostgreSQL controls.

Performance depends on query shape, authorization, result size and durability.
Open PRs have paired SQL and kernel performance checks using the
same runner for the base and proposed merge revisions. See the
[performance CI guide](../tools/perf/README.md) for coverage, regression budgets,
raw artifacts, fairness limits, and the required **Performance gate** branch rule.
The tiny-join milestone comparison (historical results removed) measures the
preceding DataFusion adapter, SQLite compatibility and direct indexed SQLite at
2,000 and 20,000 people. These are local embedded reads, excluding transport and
writes, with three alternating process pairs and exact result checks.

At 20,000 people / 180,000 occurrences, warm prepared admin reads measured these
medians in microseconds:

| Query | Current | Previous adapter | Direct SQLite |
| --- | ---: | ---: | ---: |
| Identity lookup | 0.450 | 0.438 | 0.429 |
| Relationship lookup | 1.475 | 54.221 | 1.317 |
| Five-family selective join | 4.221 | 151.421 | 3.188 |
| Grouped join / top 25 | 14,155.658 | 15,095.567 | 42,021.725 |
| Nested window / top 1 | 9,079.604 | 9,702.987 | 26,475.746 |

The tiny joins improved roughly 36×. That milestone also introduced restricted
non-join read overhead, addressed in the read and contention follow-up (historical results removed).
Against `2dac848`, restricted point reads improved from 0.783 to 0.404 µs,
indexed equality from 91.112 to 57.521 µs, and range/top-100 from 25.646 to
18.054 µs at 20,000 people. Direct SQLite measured 0.362, 81.217 and 59.579 µs
respectively. These are warm embedded executions; a fresh restricted point-query
handle still costs about 130 µs including catalog construction and preparation.
SQLite does not perform Substrate's authorization or reference-closure checks.
Fresh handles, unseen bindings, all query shapes and regressions are included in
the report; the preceding execution report (historical results removed)
retains the earlier adapters' evidence.

Native measurements (historical results removed) show a 32-field partial
update improving from 34.167 to 9.000 µs and projection of 256 payload-heavy rows
from 570.917 to 16.875 µs. Projection allocation traffic fell from 16.87 MB to
29 KB. The follow-up removes repeated scan lookups and policy work; projection
improves further from 18.916 to 6.334 µs in its matched runs. Full owned scans
remain copy-bound and effectively flat (649 → 642 µs with substantial run
variation), while durable sequential updates remain near 4 ms. The original
four-reader workload's writer median improves from 23.250 to 4.375 µs. A new
sustained-reader control shows roughly unchanged writer latency and improved
reader p99 (58.667 → 33.208 µs). These are workload results, not universal speedups.

Separate native and durability measurements remain useful context:

- Shared-reference visibility at 1,000 roots: **248.497 ms → 1.128 ms**, using snapshot-local completed-result caching.
- Default durable group commit at 32 concurrent writers: **274 → 4,103 tx/s** (three-run local medians); sequential writers were essentially unchanged.
- Incremental checkpoints: **17,408 → 2,048 row writes** over 2,048 inserts. Total insertion time was essentially unchanged; individual durable commits still dominated.

These historical focused measurements have different boundaries from the SQL
comparison and are not universal speedups. See [visibility](VISIBILITY-PERFORMANCE.md),
[group commit](group-commit.md), checkpoint measurements (historical results removed),
and [operational hardening](OPERATIONAL-HARDENING.md).

The candidate experiments (historical results removed) informed adoption of
`imbl` for persistent collections and DataFusion for SQL reads. `imbl` improved
the tested memory-only inserts and updates by roughly twofold, with mixed native
read results. DataFusion versus DuckDB (historical results removed)
favored DataFusion for grouped joins, windows and correlated EXISTS; point queries
were slower. These are workload-specific results, with preparation costs reported
separately. JSON parsing and durability-matched storage tests did not justify
replacing serde_json or redb. Reports include raw results and reproduction commands.

SQL now uses join-driven native index probes for eligible selective inner joins,
ordered numeric range/top-k pushdown, and compiled identity/equality/range reads that
reuse typed bindings without per-value planning or Arrow conversion. Each path
retains native visibility checks, live revocation and per-query budgets. Both
`EXPLAIN` and `EXPLAIN ANALYZE` show the physical join plan.

General analytical queries still use DataFusion and its bounded source-page cache.
Complex queries retain a one-binding plan cache; audited joins, aggregates, sorts
and `ROW_NUMBER` windows reuse templates with fresh execution state. Larger joins
use ordinary DataFusion execution. Fresh full scans still pay conversion costs.
Native writes now avoid duplicate packed encoding and unchanged index mutations;
scoped snapshot construction holds the publication lock briefly, and
`projected_table` copies only selected result fields. Transaction and authorization
semantics are preserved; see [native overheads](NATIVE-OVERHEADS.md). See the
[SQL guide](DATAFUSION-SQL.md) for precise eligibility and cancellation limits,
and the [beyond-SQL investigation](PERFORMANCE-BEYOND-SQL.md) for the next
write, native-read and transport opportunities.

