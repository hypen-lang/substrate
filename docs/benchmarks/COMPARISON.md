# SQLite, PostgreSQL, and Substrate read workloads

The `comparison` benchmark runs eight increasingly complex workloads against one
deterministic logical dataset. It verifies **every returned cell, row order, and
occurrence identity** before timing. Every workload records a SHA-256 digest of
canonical JSON rows plus the SQL and query plans. PostgreSQL is implemented and
was actually run locally; it is not an estimated result.

## Reproduce

```sh
# SQLite + Substrate; no server needed.
cargo bench --offline --bench comparison > comparison.json

# All three: requires installed initdb, pg_ctl and psql binaries.
# Creates and removes its own temporary cluster, uses a private Unix socket,
# disables TCP, and never connects to or changes an existing database.
tools/bench-comparison-postgres.sh > comparison-postgres.json
SUBSTRATE_BENCH_ROWS=200 tools/bench-comparison-postgres.sh > comparison-small.json
```

`SUBSTRATE_BENCH_ROWS` accepts 10–100,000 people (default 2,000). The upper bound is
an input safety limit, not a claim that the full range was measured. Only 200 and
2,000 people were measured here. `SUBSTRATE_BENCH_PG_DSN` can connect the harness
to an explicitly provisioned **empty benchmark database**; it creates `core`,
`fitness`, and `crm` schemas and does not drop them. Prefer the disposable runner.
The runner requires permission to launch a local server in sandboxed environments.
It installs no packages and does not download dependencies.

## Fixture and workload ladder

Each person has two workout occurrences and two CRM deal occurrences. Independent
`Performed` and `CustomerDeal` relationship occurrences reference the same stable
person identity; there are nine total rows per person. IDs, names, numeric values,
and relationships are deterministic. Workouts use integer minutes; deal amounts
are exact signed minor units, matching the Substrate scale-2 decimal model. No
floating-point arithmetic is used. Explicit relationship IDs survive all joins.

1. Primary-key lookup: one person by UUID.
2. Indexed scalar equality: workouts of exactly 45 minutes.
3. Range filter, deterministic ordering, and first 100 rows.
4. Relationship lookup: a person's links joined to their workouts.
5. Five-way cross-application point join: shared person, workouts, and CRM deals.
6. Five-way grouped aggregate: count and sums for each person, ordered top 25.
7. Correlated existence filter over relationships, ordered first 100 people.
8. Filtered per-person top-one workout using a window function, first 100 parents.

The last workload returns a flat relational representation of nested results. It
measures the filtering/ranking/join execution shared by nested loaders, not SDK
object-tree construction. The separate [nested-read benchmark](NESTED.md) measures
the native and socket APIs and compares batched loading with per-parent requests.
These eight workloads are all reads: concurrent writes, durability, installation,
recovery, and larger-than-memory behavior are outside this comparison.

## Comparable semantics, explicit timing boundaries

All backends have primary keys, foreign keys, and a single-column index on each
scalar/reference role. Ordinary SQLite uses flat tables with text UUID keys and
foreign keys enabled. PostgreSQL uses schemas, native UUID keys, BIGINT values,
and foreign keys. Substrate stores native UUIDs in packed relation families and
exposes their text representation through its SQLite virtual-table adapter. Both
ordinary databases run `ANALYZE`; Substrate's virtual tables supply their current
planner estimates. There are no extra compound indexes or denormalized tables.
Query plans are retained so different optimizer decisions can be inspected.

SQLite and Substrate use in-memory fixtures in one process. Their timings cover
prepared execution, complete result materialization, and the same canonical scalar
conversion. Preparation and fixture installation are outside timed samples. The
Substrate SQL connection and snapshot are reused. Setup milliseconds are recorded
separately, but are **not comparable ingestion throughput**: SQLite/PostgreSQL
load SQL text while Substrate loads through typed Rust operations.

PostgreSQL uses a regular durable temporary cluster with default settings. After
setup, all measured operations are warm reads over the same quiescent dataset.
One `psql` session per workload prepares the query once, warms it once, and runs
70 executions. Its `\timing` values include local protocol/IPC and text result
handling; query output goes to `/dev/null`. Process/connection startup and PREPARE
are excluded. This is a useful local client measurement, **not the same timing
boundary as the embedded engines**. Do not rank engine-only point-query latency
using the PostgreSQL column. No network delay is injected.

Each backend uses seven batches of ten executions. Results are the median batch
mean, with min/max batch means retained in raw JSON. These are warm-cache
microbenchmarks on one development machine, not p95/p99 latency or concurrency
throughput. Fixed values are deliberately reused; this favors cached query/data
paths and does not represent a production workload distribution.

Substrate has two measured modes. `admin` gives the closest baseline to the
ordinary database connections. `reader` uses explicit read-only grants on all five
families and still performs Substrate's reference-closure authorization checks.
All fixture rows are visible. SQLite and PostgreSQL do **not** implement equivalent
row policies in this suite, so reader-minus-admin is a useful local policy-cost
observation, not a claim that the ordinary databases provide the same security.
No arbitrary ReBAC, delegation, strict live revocation, or hostile-agent isolation
is implied. A private socket does not isolate processes with the same OS user.

## Recorded runs — 2026-10-24

Raw reports include SQL, exact result counts/digests, timings, query plans, versions,
and setup costs:

- [200 people / 1,800 rows](2026-10-24-comparison-200.json)
- [2,000 people / 18,000 rows](2026-10-24-comparison-2000.json)

The principal finding of this baseline was that Substrate execution was correct across all
these query shapes, but range ordering and broader join/filter workloads are
substantially slower than ordinary SQLite. At that baseline, the virtual-table adapter
advertised equality constraints and did not push ordered range scans into the
storage indexes. Authorization/reference traversal adds further cost for broad
reads. These measurements identify concrete optimization work; they do not support
an overall performance claim against PostgreSQL or SQLite.

Median batch mean in **microseconds**. PostgreSQL includes local client/protocol
cost; see the boundary notes above. Platform: arm64 macOS, SQLite 3.46.0,
PostgreSQL 18.3, Substrate 0.1.0, release build.

### 200 people / 1,800 occurrences

| Workload | SQLite embedded | Substrate admin | Substrate reader | PostgreSQL local psql |
|---|---:|---:|---:|---:|
| Primary key | 0.333 | 0.762 | 0.796 | 19.800 |
| Indexed equality | 1.075 | 2.171 | 2.158 | 17.700 |
| Range + top 100 | 16.413 | 81.987 | 98.187 | 31.700 |
| Relationship lookup | 0.975 | 2.546 | 2.987 | 33.300 |
| Five-way point join | 2.592 | 6.771 | 9.408 | 56.700 |
| Grouped cross-app aggregate | 297.212 | 1,198.979 | 1,673.529 | 244.600 |
| Correlated EXISTS | 107.379 | 340.267 | 491.071 | 51.700 |
| Filtered per-person top one | 264.404 | 597.913 | 777.183 | 128.800 |


### 2,000 people / 18,000 occurrences

| Workload | SQLite embedded | Substrate admin | Substrate reader | PostgreSQL local psql |
|---|---:|---:|---:|---:|
| Primary key | 0.412 | 0.921 | 0.946 | 12.400 |
| Indexed equality | 7.642 | 13.967 | 14.633 | 27.300 |
| Range + top 100 | 24.554 | 865.904 | 993.071 | 37.500 |
| Relationship lookup | 1.058 | 2.896 | 3.383 | 18.100 |
| Five-way point join | 2.862 | 7.429 | 9.596 | 25.200 |
| Grouped cross-app aggregate | 3,196.804 | 12,514.217 | 18,023.342 | 2,212.800 |
| Correlated EXISTS | 415.938 | 3,736.938 | 5,439.896 | 208.400 |
| Filtered per-person top one | 2,273.650 | 6,050.712 | 7,904.425 | 884.100 |

All 16 workload/scale cases matched before measurement. The range plan at
2,000 people confirms SQLite uses `workout_duration` while Substrate reports
`family_scan` and a temporary sort. Aggregate/window results show additional
work where better planner statistics, range pushdown, and authorization-path
measurement should precede optimization. The suite makes those regressions
repeatable without baking noisy timing thresholds into correctness tests.

These recorded timings precede the scoped delegation/live-gate milestone. They
do not measure its authorization overhead; see [security semantics](../SECURITY.md).

## After optimization — 2026-10-25

The [new embedded-only run](2026-10-25-comparison-after.json) uses the same
2,000-person / 18,000-occurrence fixture and verifies all eight result digests and
orders against SQLite. PostgreSQL was not rerun. Timing boundaries are unchanged:
prepared warm execution, including result materialization; setup, transport and
writes are excluded. This is one local run during development, not a latency SLO.

| Workload | SQLite µs | Substrate admin µs | Substrate reader µs | Admin / SQLite |
| --- | ---: | ---: | ---: | ---: |
| 01_primary_key | 0.304 | 0.771 | 0.983 | 2.53× |
| 02_indexed_equality | 7.217 | 10.108 | 9.750 | 1.40× |
| 03_range_top_100 | 25.158 | 133.400 | 133.225 | 5.30× |
| 04_relationship_lookup | 1.000 | 2.288 | 2.783 | 2.29× |
| 05_cross_app_five_way | 2.842 | 6.621 | 8.188 | 2.33× |
| 06_cross_app_group_top_25 | 3120.304 | 10252.883 | 12922.337 | 3.29× |
| 07_correlated_exists | 399.067 | 2893.675 | 3944.796 | 7.25× |
| 08_filtered_nested_top_1 | 2303.696 | 5758.521 | 6371.942 | 2.50× |

Range/top-100 now uses a bounded role-index range, but its two-column ordering still
requires SQLite sorting; the remaining 5.30× ratio is not the single-column ordered
probe in QUERY-PLANNING.md. The historical repeated baseline was about 33.7×.
Point queries, joins and aggregates remain slower than direct embedded SQLite.
The correlated EXISTS shape in this ladder differs from the deliberately broad-
equality-first regression probe; its remaining 7.25× ratio identifies further
planning work. No overall database performance advantage is claimed.
