# Index-backed DataFusion SQL and planning reuse

This report compares the production index-backed DataFusion adapter against the
explicit previous SQLite-backed Substrate adapter and ordinary indexed SQLite
in the same process. It also reruns the exact eager DataFusion release binary
from commit `772968f`, whose SHA-256 matches the six previous raw reports.
Old/new process order alternates by repetition. This fresh before/after comparison
is primary; [the previous report](DATAFUSION-NATIVE.md) and its raw files remain
historical evidence. Native relation/index APIs remain unchanged.

## Method and timing boundaries

`benches/comparison.rs` builds the same deterministic five-family fixture as the
previous measurement: 2,000 or 20,000 people, with nine occurrences per person.
The ordinary SQLite tables have equivalent explicit indexes. Substrate's reader
principal can read all five families; it is separate from admin to include the
actual live authorization checks. The explicit `sqlite_compat()` API exercises
the old adapter against the same pinned Substrate database and imbl state.

Nine workloads cover primary-key and typed equality, numeric range/top-k,
relationships, selective and grouped five-family joins, correlated existence,
nested windows, and a full-scan aggregate. Every ordered result is checked against
ordinary SQLite before and after repeated execution, and cross-process SHA-256
checks ensure outputs remain stable. EXPLAIN plans are included in raw reports.

Catalog construction, query preparation, first execution of each prepared query,
and warm execution are measured separately. First-execution timings follow the
listed workload order using the same SQL handles, so earlier workloads may have
populated source caches; these are not fresh-process/cold-snapshot latencies.
Each workload additionally creates fresh admin/reader SQL handles, measuring
catalog, preparation and first execution separately and verifying results again.
Their authorized Arrow-page caches are unprimed, while the native database and
CPU/OS caches remain warm. This exposes conversion cost without claiming cold IO.
Each process uses one warm-up, then seven samples of ten executions. Times are
sample means, summarized by the median of three independent process medians; they
are not single-request latency percentiles. Three process runs at each size are
serialized with the paired eager baseline, without other task builds or benchmarks. This remains a shared
macOS development machine rather than an isolated performance lab.

Additional prepared-query cases cover one repeated ID parameter and cycles of
16 ID, numeric equality and numeric range parameter sets. Each parameter set has
exact ordered pre/post checks. The implementation's one-entry bound-plan cache
makes a repeated parameter a hit and a changed parameter a miss; the alternating
cases intentionally measure misses rather than hiding planning cost behind a
large cache. The timed calls include parameter binding, live authorization and
result conversion. Output cardinality varies for numeric equality/range queries.

A separate projection fixture contains 256 rows with 16 KiB text payloads. SQL
projects only an integer or COUNT, so copying the payload is unnecessary. A
restricted reader sees exactly the rows with `visible=true`; its direct SQLite
comparison includes the equivalent WHERE clause. The admin and restricted result
sets are verified independently. This fixture checks projection and row-policy
behavior but is not a process-RSS measurement.

Fixture construction is outside every read timing. SQLite now uses prepared
inserts rather than the old giant SQL script; the resulting rows/indexes are the
same. Setup timings are **not** insertion-throughput comparisons. No PostgreSQL,
network transport, TLS, durable writes or request JSON encoding are measured.

## Reproduction

```sh
cargo build --offline --release --bench comparison -j 3
python3 experiments/datafusion/run_indexed.py --binary target/release/deps/comparison-<hash> \
  --baseline-binary /tmp/substrate-eager-datafusion-772968f
```

Use the executable, not its `.d` dependency file. The helper removes the optional
PostgreSQL environment variable, requires at least three repetitions, records the
binary's SHA-256 and size, and verifies result digests across runs. New output
files are named `datafusion-indexed-{people}-{repetition}.json` and
`datafusion-eager-baseline-{people}-{repetition}.json`; existing eager DataFusion
raw reports are not overwritten. The baseline executable was preserved before
rebuilding; reproduce it by building `comparison` from commit `772968f` in a
separate checkout. Its old script-based fixture loading takes longer but remains
outside all read timings. The helper compares result digests across both builds.

## Results

All twelve processes completed successfully. All nine ordered outputs matched
SQLite and the eager baseline at both sizes, before and after repetitions.
Parameter rebinding, projection, and restricted-row outputs also passed exact
checks. The machine was macOS/aarch64; the harness reports DataFusion facade
55.0.0, execution crates 55.2.0, imbl 7.0.2 and SQLite 3.46.0.

The integration improves warm point and selective-join reads and removes eager
row materialization from catalog creation. It is **not a universal SQL speedup**:
20k range, EXISTS and window workloads regress against eager DataFusion, and
SQLite remains substantially faster for small selective reads. Warm results
also hide first-use Arrow conversion; the fresh-handle measurements below are
necessary when assessing short-lived SQL sessions.

### Warm execution

All numbers are **microseconds per query**, the median of three process medians.
The compatibility column uses Substrate's explicit SQLite-backed adapter, not
ordinary SQLite. These are prepared, repeatedly executed queries, including
live authorization and typed result conversion in the Substrate paths.

#### 2,000 people / 18,000 occurrences

| Workload | Eager DF admin | Indexed DF admin | Indexed DF reader | SQLite compatibility admin | Direct SQLite |
|---|---:|---:|---:|---:|---:|
| Primary key | 75.80 | 20.26 | 21.34 | 0.59 | 0.40 |
| Typed numeric equality | 108.24 | 71.34 | 72.55 | 9.50 | 7.25 |
| Range / top 100 | 187.16 | 172.50 | 181.13 | 118.42 | 24.52 |
| Relationship lookup | 256.49 | 168.11 | 163.68 | 2.08 | 0.99 |
| Selective five-family join | 857.63 | 484.99 | 485.19 | 5.95 | 2.77 |
| Grouped five-family join / top 25 | 1,739.98 | 1,638.92 | 1,652.51 | 9,542.99 | 3,149.10 |
| Correlated EXISTS | 397.33 | 431.67 | 440.40 | 2,504.89 | 395.33 |
| Nested window / top 1 | 2,189.65 | 2,380.08 | 2,499.31 | 4,800.54 | 2,258.16 |
| Full-scan aggregate | 163.94 | 121.31 | 120.78 | 399.90 | 102.34 |

#### 20,000 people / 180,000 occurrences

| Workload | Eager DF admin | Indexed DF admin | Indexed DF reader | SQLite compatibility admin | Direct SQLite |
|---|---:|---:|---:|---:|---:|
| Primary key | 117.05 | 15.56 | 17.10 | 0.65 | 0.34 |
| Typed numeric equality | 180.41 | 149.20 | 152.82 | 103.19 | 78.41 |
| Range / top 100 | 367.62 | 1,236.42 | 1,501.72 | 1,432.30 | 54.40 |
| Relationship lookup | 525.17 | 329.98 | 323.81 | 2.29 | 1.12 |
| Selective five-family join | 1,963.26 | 843.80 | 845.85 | 6.40 | 3.11 |
| Grouped five-family join / top 25 | 13,014.00 | 12,466.79 | 12,627.64 | 118,774.69 | 36,905.98 |
| Correlated EXISTS | 1,142.66 | 1,587.17 | 1,747.92 | 30,186.52 | 437.43 |
| Nested window / top 1 | 8,092.85 | 15,096.28 | 17,918.68 | 88,253.45 | 24,365.04 |
| Full-scan aggregate | 277.38 | 247.09 | 245.12 | 5,379.43 | 983.08 |

At 20k, the indexed adapter is **7.52× faster for primary-key reads** and
**2.33× faster for the selective five-family join** than the paired eager
DataFusion baseline. Grouped joins are only 1.04× faster; the full-scan aggregate
is 1.12× faster. Against direct SQLite, the indexed grouped join is 2.96× faster
and the aggregate is 3.98× faster, while primary-key SQL remains about 46× slower.

The range workload is **3.36× slower**, EXISTS **1.39× slower**, and the nested
window **1.87× slower** than eager DataFusion at 20k. The native range path still
converts selected occurrences on every execution; it does not reuse the cached
full-family Arrow pages. This is a plausible contributor, not a component-level
profile proving the entire regression. Order/limit-aware index execution and
join-time index probes remain follow-up work. DataFusion currently does the
remaining joins, sorting, aggregation and window execution.

### Catalog and first-use preparation

Catalog creation is measured before any SQL query in that process, with admin
first and reader second. Times below are **milliseconds**. The eager catalog
converts all readable rows; the indexed catalog registers lazy providers. The
first admin call also includes process/runtime startup effects. The substantial
variation in those first calls prevents attributing every difference solely to
the adapter.

| People | Eager admin catalog | Indexed admin catalog | Eager reader catalog | Indexed reader catalog | Compatibility admin catalog |
|---|---:|---:|---:|---:|---:|
| 2,000 | 3.1570 | 0.4005 | 9.0358 | 0.0444 | 0.2402 |
| 20,000 | 37.2835 | 16.8004 | 114.7685 | 0.0698 | 3.1504 |

The 20k indexed first admin catalog runs were 20.130, 0.462 and 16.800 ms;
first point preparation was 25.199, 0.525 and 22.995 ms; first point execution
was 17.621, 0.433 and 22.234 ms. These measured initial outliers are retained in
the raw files. They are not representative steady-state preparation costs.
At 20k, initial point preparation medians were 1.867 ms eager versus 22.995 ms
indexed; there is **no demonstrated first-process preparation improvement**.
Later reader point preparation was 0.115 ms, and fresh-handle admin point
preparation was 0.059 ms. The reduction in repeated planning comes from reusing
prepared bound plans, not from claiming that all first-time SQL preparation
became faster.

### Fresh handles and unprimed authorized page caches

Each row constructs a new SQL handle after the corresponding shared-handle
workload and checks the result. All numbers below are **milliseconds at 20k**.
These are unprimed SQL page caches over an already loaded native database, not
cold disk measurements. Admin/reader authorization paths differ; the restricted
reader pays per-row policy checks when materializing uncached pages.

| Workload | DF admin catalog | DF admin prepare | DF admin first execution | DF reader first execution | Compatibility admin first execution |
|---|---:|---:|---:|---:|---:|
| Primary key | 0.0895 | 0.0591 | 0.0946 | 0.0820 | 0.0035 |
| Typed numeric equality | 0.0804 | 0.0796 | 0.2271 | 0.3076 | 0.1109 |
| Range / top 100 | 0.0847 | 0.1375 | 2.3135 | 4.1644 | 1.8301 |
| Relationship lookup | 0.0811 | 0.1178 | 6.6788 | 21.4055 | 0.0138 |
| Selective five-family join | 0.0805 | 0.1864 | 13.3742 | 41.5483 | 0.0166 |
| Grouped five-family join / top 25 | 0.0826 | 0.2347 | 43.4596 | 114.7889 | 116.5900 |
| Correlated EXISTS | 0.0753 | 0.1361 | 12.7239 | 50.2415 | 30.3196 |
| Nested window / top 1 | 0.0753 | 0.2513 | 28.5430 | 67.4373 | 85.4847 |
| Full-scan aggregate | 0.0747 | 0.1269 | 5.1390 | 18.8987 | 5.4491 |

A warm 0.247 ms aggregate becomes 5.139 ms on a fresh admin handle; a warm
12.467 ms grouped join becomes 43.460 ms. Eager DataFusion paid source conversion
in catalog setup, whereas the lazy adapter pays it when a query first needs
those rows. Comparing only warm execution or only catalog construction would
hide that cost transfer.

The full-family source-page cache is bounded to 64 MiB and 4,096 FIFO entries
per SQL snapshot. It caches authorized Arrow pages by family, projection and
cursor, and still checks live authorization and charges source budgets on reuse.
Indexed equality/range pages are not cached. Workloads larger than the cache,
changing projections, or short-lived handles can therefore perform differently.
Shared-handle `first_execution_ms` values in the raw files follow the listed
workload order and can inherit compatible cached pages from earlier workloads.

### Parameters and projection

Prepared parameter executions at 20k, **microseconds per query**:

| Parameters | Indexed DF admin | Indexed DF reader | Compatibility admin | Direct SQLite |
|---|---:|---:|---:|---:|
| Same ID repeatedly | 19.43 | 20.91 | 0.63 | 0.37 |
| 16 different IDs | 62.29 | 63.89 | 0.70 | 0.41 |
| 16 numeric equality values | 186.09 | 205.67 | 104.62 | 88.48 |
| 16 numeric range values | 440.79 | 580.44 | 417.38 | 56.18 |

The same-ID case hits the bounded one-entry cache; cycling 16 IDs misses it.
The 19.43 versus 62.29 µs result demonstrates the practical repeated-plan benefit,
while retaining the cost of changed parameters. A larger reusable parameterized
physical-plan strategy has not been implemented or benchmarked here.

The separate 256-row projection fixture has 16 KiB unselected text per row.
In the 20k-run reports, selecting only the integer costs 75.16 µs for DF admin
and 65.84 µs for the restricted reader (128 visible rows), versus 17.59/9.67 µs
for equivalent direct SQLite. COUNT costs 69.08/67.87 µs versus 0.18/1.20 µs.
All restricted result checks pass. This verifies functional projection and
row-policy preservation; it does not claim that DataFusion beats SQLite on this
small fixture or quantify peak memory savings.

## Raw results

Each report contains all seven timing samples, result digests, repeated-result
checks, preparation/setup costs and EXPLAIN plans. The indexed reports additionally
contain fresh-handle, explicit compatibility, parameter and projection timings.

| People | Indexed reports | Paired eager baseline reports |
|---|---|---|
| 2,000 | [1](datafusion-indexed-2000-1.json), [2](datafusion-indexed-2000-2.json), [3](datafusion-indexed-2000-3.json) | [1](datafusion-eager-baseline-2000-1.json), [2](datafusion-eager-baseline-2000-2.json), [3](datafusion-eager-baseline-2000-3.json) |
| 20,000 | [1](datafusion-indexed-20000-1.json), [2](datafusion-indexed-20000-2.json), [3](datafusion-indexed-20000-3.json) | [1](datafusion-eager-baseline-20000-1.json), [2](datafusion-eager-baseline-20000-2.json), [3](datafusion-eager-baseline-20000-3.json) |

Measured indexed binary SHA-256:
`461529237dbbad67054c46eed91a269acae3d22e223de8dca2e793aa8be1dd5f`.
Preserved eager binary SHA-256:
`ea991198ad0172a22c1a52ca7134c4de8349f6439c2768d9632b5d1f8f7c34d3`.
Historical `datafusion-native-*` files remain unchanged; the paired eager runs
above, rather than their historical timings, supply this report's baseline.
