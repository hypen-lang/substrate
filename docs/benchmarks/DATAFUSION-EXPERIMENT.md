# DataFusion and DuckDB on identical authorized Arrow data

DataFusion is faster for the large grouped join, correlated existence and nested
window workloads in this comparison. It is not the fastest engine for every SQL
query: indexed DuckDB wins the point/selective queries and the simple full-scan
aggregate. The existing SQLite adapter was already faster for small indexed
queries in the earlier comparison. Replacing an SQL engine therefore changes
workload tradeoffs; the results do not establish a universal winner.

## Versions and method

The executable harness is [experiments/datafusion](../../experiments/datafusion/README.md).
It uses Python package `datafusion==55.0.0`, whose runtime `SELECT version()` reports
**Apache DataFusion 55.2.0**, `duckdb==1.4.1`, and `pyarrow==21.0.0`, on arm64
macOS 26.2 / Python 3.13.12. The official Python release's
[Cargo.lock](https://github.com/apache/datafusion-python/blob/55.0.0/Cargo.lock)
also resolves the DataFusion Rust crates to 55.2.0. Python package and native
engine versions must not be conflated. The native DataFusion 55 line requires
Rust 1.94, beyond the prototype's former 1.89 floor.

The fixture is the same five-family authorized export and nine query shapes as
the [DuckDB experiment](ANALYTICS-EXPERIMENT.md). Both engines consume exactly the
same Arrow columns. DataFusion registers in-memory record batches with one
partition, disables repartitioning, uses a 512 MiB tracked memory pool, and disables
disk spilling. `TOKIO_WORKER_THREADS=1` is set before loading its extension.
DuckDB uses one thread and a 512 MB configured memory limit. These settings do
not constrain total process RSS or memory already held by Arrow inputs.

DuckDB has both direct-Arrow and copied/indexed modes. The indexed mode has an
occurrence-ID index and an index on each role, as in the ordinary SQLite fixture.
DataFusion's in-memory provider has no equivalent persistent per-role indexes.
That is a real architectural difference, not an omitted timing cost.

Each size runs in three separate processes, alternating engine order. Each query
has a warm-up, then five samples of three executions. The tables below are medians
of the three process medians, in **microseconds per execution**; individual samples
are retained in the raw reports. The other task's builds and benchmarks were
paused during measurement. Background operating-system activity was not isolated.
All returned rows are collected and canonicalized inside timing. Native/Python FFI
and result-conversion costs differ, so this compares working query paths rather
than isolated execution kernels.

## A correctness issue found during the benchmark

The first attempted harness reused a DataFusion physical execution plan directly.
It returned correct first results but incorrect later results for the range/top-100
query. Its mutable dynamic filter survived execution. Those timings were discarded.
The corrected harness caches the optimized **logical** plan and creates a fresh
physical plan for every execution. That per-call work is included in the reported
time. DuckDB uses `PREPARE`/`EXECUTE` and handles its own execution-state lifecycle.

DataFusion's documented
[`reset_plan_states`](https://docs.rs/datafusion-physical-plan/55.2.0/datafusion_physical_plan/execution_plan/fn.reset_plan_states.html)
also explicitly excludes reuse of dynamic-filter and recursive plans. Blindly
caching an `ExecutionPlan` or using that reset helper for every possible query
would be incorrect. The native adapter must preserve fresh execution state, not
copy the rejected fast path.

Every measured query checks exact ordered rows and SHA-256 against the authorized
Substrate/SQLite reference before timing, and checks rows again after repeated
execution. Four additional tests cover null versus text, signed int64 extremes,
UUID strings, duplicates, empty results, invalid SQL, unavailable relations and
20 repeated top-k executions. All pass. The fixture does not establish complete
SQL dialect, collation, decimal, floating-point or null-ordering compatibility.

## 2,000 people / 18,000 occurrences

| Workload | DataFusion | DuckDB Arrow | DuckDB indexed |
| --- | ---: | ---: | ---: |
| Primary key | 68.89 | 158.75 | 34.61 |
| Indexed equality | 102.53 | 166.50 | 77.89 |
| Range/top 100 | 168.18 | 219.54 | 94.72 |
| Relationship lookup | 190.12 | 478.22 | 122.69 |
| Selective five-way join | 568.49 | 1,126.26 | 292.00 |
| Grouped five-way join/top 25 | 962.65 | 1,721.56 | 1,143.82 |
| Correlated EXISTS | 271.39 | 821.32 | 483.29 |
| Nested window/top 1 | 1,797.60 | 1,911.24 | 1,447.69 |
| Full-scan aggregate | 134.47 | 101.38 | 42.03 |

## 20,000 people / 180,000 occurrences

| Workload | DataFusion | DuckDB Arrow | DuckDB indexed |
| --- | ---: | ---: | ---: |
| Primary key | 104.14 | 164.00 | 35.29 |
| Indexed equality | 250.36 | 242.40 | 230.06 |
| Range/top 100 | 283.85 | 349.25 | 168.49 |
| Relationship lookup | 351.50 | 1,448.08 | 213.72 |
| Selective five-way join | 1,287.24 | 3,150.50 | 391.19 |
| Grouped five-way join/top 25 | 6,582.49 | 9,725.67 | 7,951.57 |
| Correlated EXISTS | 702.18 | 2,590.61 | 2,021.85 |
| Nested window/top 1 | 6,765.78 | 8,576.54 | 7,635.18 |
| Full-scan aggregate | 133.88 | 141.67 | 69.83 |

Compared with indexed DuckDB at the larger size, DataFusion reduced grouped-join
time by about 17%, window-query time by 11%, and correlated-existence time by 65%.
DuckDB's point query took about one third of DataFusion's time, its selective
five-way join about 30%, and its simple aggregate about 52%. DataFusion generally
beats direct Arrow DuckDB for joins, but the cost of building DuckDB's indexed
copy can be amortized when that copy is reused.

## Preparation and transfer cost

| People | DataFusion setup | DuckDB Arrow setup | DuckDB indexed setup | DataFusion including bridge | DuckDB indexed including bridge |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2,000 | 1.14 ms | 4.31 ms | 12.67 ms | 15.55 ms | 27.29 ms |
| 20,000 | 1.23 ms | 4.32 ms | 79.10 ms | 163.08 ms | 240.57 ms |

Setup starts with existing Arrow data; DataFusion registers its batches without
copying into an indexed row store. The bridge columns include authorized source
export, JSON serialization/read/decode and Arrow construction. They exclude source
fixture construction, process startup/imports, report assembly/file output, and
per-query preparation. Each query's preparation cost is separately recorded.
Thus these bridge totals are lower bounds, not complete cold-request latencies.

Source exports were generated by the earlier compiled Substrate fixture, before
the production imbl adoption; its recorded Substrate/SQLite reference timings are
context and correctness checks, not a freshly rebuilt native-adapter comparison.
The DataFusion-versus-DuckDB timings themselves are paired over identical data.
The production Rust integration needs its own measurements, including authorization,
conversion, limits and dispatch. Avoid treating these Python timings as its latency.

## Integration implications

The results support DataFusion for the requested Rust SQL read engine, especially
the heavier joins, but do not justify describing it as faster for all reads.
Native indexed APIs need not change. An engine replacement must preserve the
existing authorized relation boundary, use exact type conversions, reject side
effects/external table access, and check live authority before returning results.
Detached Arrow data in this trusted-host experiment does not itself enforce live
revocation. The separate native implementation must provide that enforcement.

Raw reports: [2k run 1](datafusion-2000-1.json), [2k run 2](datafusion-2000-2.json),
[2k run 3](datafusion-2000-3.json), [20k run 1](datafusion-20000-1.json),
[20k run 2](datafusion-20000-2.json), [20k run 3](datafusion-20000-3.json).
Official API references: [configuration](https://datafusion.apache.org/python/user-guide/configuration.html)
and [SessionContext](https://datafusion.apache.org/python/autoapi/datafusion/context/index.html).
