# Native DataFusion SQL with imbl

**Analytical queries improved, but SQL point and selective joins regressed
substantially.** At 180,000 occurrences, the native admin SQL path takes 12.31 ms
for the grouped join versus direct SQLite's 37.49 ms, and 7.61 ms for the window
query versus 24.36 ms. Its primary-key query takes 122 µs versus SQLite's 0.36 µs;
its selective five-way join takes 1.73 ms versus SQLite's 3.18 µs.

At the smaller size, the historical SQLite-backed Substrate point query took
0.77 µs versus the new 104.67 µs, roughly a 136× regression. The selective
five-way join rose from 6.62 to 1,239.22 µs, roughly 187×. These are SQL API
measurements; native indexed reads were not changed. Snapshot construction adds
another material cost: at the larger size, catalog materialization takes about
35 ms for admin and 111 ms for the authorized reader, before query preparation
and execution. The new default is not a universal query-speed improvement.

This report measures the production Rust `Snapshot::sql()` / prepared-query API,
after adoption of imbl and DataFusion. It is separate from the Python engine
comparison: it includes Substrate's actual authorization checks, SQL validation,
runtime dispatch, parameter handling, fresh physical-plan execution and typed
result conversion. The production adapter also uses checked arithmetic and SUM
handling to return errors for overflow instead of silently wrapping; that work
was not present in the raw Python candidate experiment. Native relation/index
APIs are unchanged.

## Method

`benches/comparison.rs` builds a deterministic five-family fixture with nine
occurrences per person and a read-only principal authorized for all five families.
It compares the same ordered query results with ordinary indexed in-memory SQLite.
Nine workloads cover point/equality/range reads, selective relationship joins,
grouped joins, correlated existence, nested windows and a full-scan aggregate.
The fixture contains exact UUID strings and signed integer/decimal minor units.

The benchmark measures fixture creation, admin SQL catalog materialization,
restricted-reader catalog materialization, and each query's preparation separately.
Warm execution reuses each prepared logical query; the adapter creates fresh
physical execution state on every call. Each process records one warm-up followed
by seven samples of ten executions. Reported timings are batch means rather than
per-operation latency percentiles. Three process repetitions run at 2,000 and
20,000 people, with no other task compilation or benchmark running concurrently.
This remains a shared development machine, not an isolated performance lab.

Every query verifies exact ordered rows and a SHA-256 digest against SQLite before
timing, then verifies results again after repeated execution. The latter protects
against stale dynamic-filter/LIMIT state. SQLite uses `EXPLAIN QUERY PLAN`; the
native DataFusion adapter uses `EXPLAIN`. Physical plans and all seven sample means
are included in raw output. PostgreSQL is not run in this measurement.

SQL catalog materialization is a real upfront cost: this adapter copies authorized
rows into typed Arrow batches. Warm timings must not be presented as cold request
latencies or as a speedup for single-use SQL snapshots. Data remains pinned to its
source snapshot; live authorization is rechecked by the production API.

The report also compares overlapping 2,000-person queries with the
[prior SQLite-backed Substrate run](2026-10-25-comparison-after.json). That is a
historical, nonpaired comparison on the same development machine, not a controlled
before/after trial. The engine, collection dependency, timing occasion and SQL
snapshot construction differ. It is useful for exposing workload regressions and
gains, but its ratios must not be attributed to DataFusion alone. Its eight-query
fixture lacks the newly added full-scan aggregate.

## Reproduction

```sh
cargo build --offline --release --bench comparison -j 3
```

Run the resulting `target/release/deps/comparison-*` executable with
`SUBSTRATE_BENCH_ROWS=2000` or `20000` and without `SUBSTRATE_BENCH_PG_DSN`, three
times for each size. Do not select the `.d` dependency file. The report includes
the DataFusion facade version, resolved execution-crate version, imbl version,
SQLite version, platform, and all timing boundaries.

Alternatively, pass the exact executable to
`python3 experiments/datafusion/run_native.py --binary <executable>`.
The helper removes the PostgreSQL environment setting, records the executable's
SHA-256, and verifies result digests agree across all process repetitions.

## Measured build and verification

The optimized benchmark uses DataFusion facade 55.0.0 with execution crates 55.2.0,
imbl 7.0.2 and SQLite 3.46.0, on arm64 macOS 26.2 / Rust 1.94.0. It uses the
production adapter's one-partition configuration and private two-worker runtime.
The native adapter's memory budgets and checked arithmetic differ from the Python
experiment. The linked, unstripped release benchmark is 121,132,464 bytes; this
is not a minimal application or a measured resident-memory footprint.

All six runs passed all nine result comparisons before and after repeated query
execution. Digests also agree between process repetitions. All nine match the
independent Python DataFusion/DuckDB fixture digests. No PostgreSQL instance was
used. No default was changed by the benchmark itself.

## 2,000 people / 18,000 occurrences

Times below are microseconds per warm prepared execution, median of three process
medians. The historical column is the earlier single-run SQLite-backed Substrate
admin result and is explicitly **nonpaired**.

| Workload | Historical Substrate admin | New DataFusion admin | New DataFusion reader | Same-run direct SQLite |
| --- | ---: | ---: | ---: | ---: |
| Primary key | 0.77 | 104.67 | 135.42 | 0.38 |
| Indexed equality | 10.11 | 125.72 | 146.90 | 8.47 |
| Range/top 100 | 133.40 | 225.73 | 224.03 | 28.70 |
| Relationship lookup | 2.29 | 356.16 | 319.85 | 1.43 |
| Selective five-way join | 6.62 | 1,239.22 | 1,174.51 | 3.11 |
| Grouped five-way join/top 25 | 10,252.88 | 2,904.07 | 2,909.43 | 4,182.90 |
| Correlated EXISTS | 2,893.68 | 583.43 | 514.12 | 490.13 |
| Nested window/top 1 | 5,758.52 | 3,512.25 | 3,514.28 | 2,964.12 |
| Full-scan aggregate | — | 237.99 | 208.78 | 113.59 |

Relative to the historical Substrate admin run, grouped joins improved about
3.53×, correlated existence 4.96× and the nested window 1.64×. Five of the eight
overlapping workloads became slower. The severe point/selective regressions are
consistent with replacing indexed virtual-table access with materialized Arrow
scans and adding DataFusion planning/runtime dispatch. A component profile would
be needed to assign exact shares of that overhead; these measurements do not do so.

## 20,000 people / 180,000 occurrences

| Workload | DataFusion admin | DataFusion reader | Direct SQLite |
| --- | ---: | ---: | ---: |
| Primary key | 122.21 | 117.59 | 0.36 |
| Indexed equality | 176.22 | 175.23 | 79.61 |
| Range/top 100 | 330.87 | 331.24 | 56.45 |
| Relationship lookup | 508.00 | 490.66 | 1.11 |
| Selective five-way join | 1,727.26 | 1,757.63 | 3.18 |
| Grouped five-way join/top 25 | 12,306.00 | 12,268.77 | 37,485.09 |
| Correlated EXISTS | 1,051.93 | 991.00 | 446.52 |
| Nested window/top 1 | 7,606.85 | 7,506.05 | 24,355.81 |
| Full-scan aggregate | 260.15 | 251.59 | 999.19 |

The admin grouped join is about 3.05× faster than same-run direct SQLite; the
window query is 3.20× faster and the full-scan aggregate 3.84× faster. Direct
SQLite remains faster for the other six workloads. The engine replacement trades
selective indexed efficiency for larger analytical execution; reproducing the
Python candidate's timings is not automatic once production checks and dispatch
are included.

## Upfront costs and first-use interpretation

All values are milliseconds, medians across three process runs.

| People | Admin catalog | Reader catalog | Admin primary-key prepare | Reader primary-key prepare | Admin grouped-query prepare | Reader grouped-query prepare |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2,000 | 3.955 | 12.110 | 0.884 | 0.179 | 0.513 | 0.259 |
| 20,000 | 35.277 | 111.385 | 0.573 | 0.094 | 0.412 | 0.206 |

The first admin preparation also pays initialization of the shared SQL runtime;
later preparations run against that already-created runtime. A single-use SQL
snapshot must pay catalog creation and preparation in addition to the warm query
time. These component measurements are not a separate cold-request latency
distribution, and they exclude process startup and durable database recovery.
Reusing a pinned SQL snapshot and prepared queries amortizes setup, subject to
the live authorization checks.

Source fixture construction measured 64.95 ms / 640.88 ms in Substrate and
363.65 ms / 31,824.96 ms in the direct SQLite fixture at the two sizes. **Do not
use those figures as an insertion-throughput comparison.** The ordinary SQLite
fixture is loaded from a large generated SQL script rather than a tuned reusable
prepared-insert pipeline. It exists to establish identical indexed data for the
read comparison; setup is outside all query timings.

The memory-only database also excludes durable commit, transport, TLS, request
JSON parsing and client decoding. Scoped row-filter policy complexity, concurrent
queries and retained Arrow snapshot memory are not measured by this fixture.

Raw reports: [2k run 1](datafusion-native-2000-1.json),
[2k run 2](datafusion-native-2000-2.json), [2k run 3](datafusion-native-2000-3.json),
[20k run 1](datafusion-native-20000-1.json),
[20k run 2](datafusion-native-20000-2.json), [20k run 3](datafusion-native-20000-3.json).
