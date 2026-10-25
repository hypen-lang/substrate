# Authorized columnar export and DuckDB experiment

This isolated experiment evaluates whether columnar execution helps the existing SQL query ladder. It does not replace Substrate's SQL layer or add a supported application API. The default remains rusqlite over authorized virtual tables.

## Measured result and recommendation

On macOS 26.2 / arm64, with SQLite 3.46.0, Python 3.13.12, DuckDB 1.4.1 and PyArrow 21.0.0, all nine workloads matched exactly in all six runs. The tables below show medians of three independently measured process medians, rather than the fastest run. Raw samples, versions, digests, and costs are in [`analytics-2000-1.json`](analytics-2000-1.json), [`analytics-2000-2.json`](analytics-2000-2.json), [`analytics-2000-3.json`](analytics-2000-3.json), [`analytics-20000-1.json`](analytics-20000-1.json), [`analytics-20000-2.json`](analytics-20000-2.json), and [`analytics-20000-3.json`](analytics-20000-3.json).

**Keep rusqlite as the default; a separate reusable analytical adapter is justified for further work.** At 20,000 people, warm indexed DuckDB executes the grouped cross-application aggregate about **20× faster than Substrate**, and about **4.8× faster than ordinary SQLite**. The window query is about **13.7× faster than Substrate**. However, the materialized point read takes **35.4 µs versus Substrate's 1.14 µs**, and the selective five-way join takes **394 µs versus 9.10 µs**. This is a workload-specific architectural option, not a universal speedup.

Warm execution at **20,000 people / 180,000 occurrences**, in microseconds:

| Workload | SQLite | Substrate reader | DuckDB Arrow direct | DuckDB materialized + indexes |
| --- | ---: | ---: | ---: | ---: |
| Primary key | 0.40 | 1.14 | 175 | 35.4 |
| Indexed equality | 79.2 | 113 | 265 | 227 |
| Range top 100 | 55.1 | 1,780 | 347 | 176 |
| Relationship lookup | 1.17 | 3.17 | 1,679 | 218 |
| Selective five-way join | 3.19 | 9.10 | 3,612 | 394 |
| Grouped five-way join, top 25 | 37,832 | 158,298 | 9,964 | 7,932 |
| Correlated existence | 442 | 45,065 | 2,622 | 2,031 |
| Filtered window / nested top 1 | 24,689 | 108,897 | 9,037 | 7,934 |
| Full single-family aggregate | 1,002 | 6,238 | 148 | 70.2 |

The ordinary SQLite correlated-existence query remains faster than either DuckDB mode. That result supports improving Substrate's current SQL planning/pushdown paths independently of any analytical adapter.

At **2,000 people**, the grouped aggregate is 12.97 ms in Substrate, 3.14 ms in SQLite, 1.78 ms via Arrow-direct DuckDB, and 1.19 ms after DuckDB materialization. The full scan aggregate is 521 / 103 / 110 / 41.8 µs respectively. The materialized point read remains about 34 µs versus Substrate's 1.01 µs. Indexed DuckDB group timings ranged from 1.17–1.40 ms across the three small runs and 7.89–8.54 ms across the larger runs.

Batch acquisition is material:

| Cost, median milliseconds | 2,000 people | 20,000 people |
| --- | ---: | ---: |
| Authorized source export | 11.6 | 132.8 |
| Serialize typed columns as JSON | 0.65 | 8.04 |
| Bridge read + JSON decode | 2.22 | 18.87 |
| Convert decoded columns to Arrow | 0.75 | 8.47 |
| DuckDB Arrow registration/setup | 4.64 | 7.25 |
| DuckDB table/index materialization/setup | 13.6 | 81.0 |
| Complete measured bridge + Arrow setup | 19.7 | 174.9 |
| Complete measured bridge + indexed materialization | 28.5 | 248.5 |

Component medians do not necessarily sum to the median total. The 20k fixture contains 14.56 MB of Arrow logical buffers and a roughly 14.03 MB JSON bridge file; this excludes allocator, engine, index, and process memory. Imports add a separately measured median 48.5 ms for the small runs and 126.5 ms for the large runs, illustrating why a fresh Python process is not the intended reuse model.

For the 20k grouped query, the materialized path pays roughly 249 ms before querying, versus a roughly 150 ms warm saving per query over Substrate. The median per-run break-even estimate is **1.70 repetitions**, so the second identical query amortizes the measured lower-bound preparation cost in a long-lived process. The first materialized query is roughly 256 ms, slower than the approximately 158 ms existing Substrate query. Compared with ordinary SQLite, preparation takes roughly **8.47 repetitions** to amortize. The window query needs about **2.46 repetitions versus Substrate** or **14.95 versus SQLite**. The full aggregate, despite its large warm speedup, needs about **40 repetitions versus Substrate** because its original absolute execution time is already small. These estimates exclude the cold costs described below and only apply when the same authorized snapshot can be reused.

A native Rust Arrow provider could avoid this experiment's JSON/Python handoff, but that improvement is unmeasured. The next meaningful implementation experiment is an optional authorized batch provider with reuse/invalidation and a query routing policy, followed by end-to-end application benchmarks. Do not replace the current point-lookup and selective-join paths on this evidence.

## Experiment boundary

The Rust exporter builds the same deterministic five-family fixture as `benches/comparison.rs`: shared people, workouts, relationship occurrences, deals, and customer relationships, nine occurrences per person. A read-only `reader` principal obtains each schema and pages each family through the normal authorized snapshot API. UUIDs remain their exact canonical strings; integers and decimal minor units become Arrow `int64`, never floating point. The fixture has no nulls. This is deliberately a limited encoding, not a complete Liquid-to-Arrow type mapping.

Python constructs typed PyArrow tables and runs DuckDB 1.4.1 with one execution thread, a 512 MB DuckDB memory limit, and external access disabled. The limit does not bound the separate Rust export, JSON objects, Python process, or Arrow buffers. Two modes are measured:

- **Arrow direct:** DuckDB views over registered Arrow tables, without relational indexes.
- **Materialized and indexed:** copies those Arrow tables into DuckDB and adds a unique occurrence-ID index plus an index on every role, matching the ordinary SQLite fixture's indexing. Table copying, indexes, and statistics are included in setup time.

The original eight workloads cover point reads, indexed equality, range ordering, relationship joins, five-way cross-application joins, grouped aggregation, correlated existence, and a nested window query. A ninth workload scans and aggregates one complete relation. Every execution mode must return the same ordered rows, duplicate multiplicities, UUIDs, and integer values as Substrate and ordinary SQLite. SHA-256 digests record this agreement. Timing starts after statement preparation, includes retrieving all results and converting scalars to canonical strings, and uses one warm-up followed by five samples of three executions. The report preserves every sample. Each dataset runs in three separate processes, at 2,000 and 20,000 people (18,000 and 180,000 occurrences).

Rust and Python FFI/result-conversion costs differ. These are usable query-path measurements, not an isolated comparison of database kernels. The ordinary SQLite fixture has equivalent data and indexes but no runtime authorization. These are read-only in-memory query comparisons, not durability or write benchmarks.

## Reproduction

From the repository root, with Python 3.13 and `uv` installed:

```sh
UV_CACHE_DIR=/tmp/substrate-analytics-uv-cache uv venv --python python3.13 experiments/analytics/.venv
UV_CACHE_DIR=/tmp/substrate-analytics-uv-cache uv pip install --python experiments/analytics/.venv/bin/python -r experiments/analytics/requirements.txt
cargo test --offline --manifest-path experiments/analytics/Cargo.toml --target-dir target
experiments/analytics/.venv/bin/python -m unittest discover -s experiments/analytics -p 'test_*.py'
experiments/analytics/benchmark.sh
```

Run without other benchmarks or heavy builds on the same machine. `Cargo.lock` and exact Python dependency pins are local to the experiment. The script keeps temporary source exports outside the repository and saves compact result files under `docs/benchmarks/analytics-*.json`. The local virtual environment occupies about 145 MiB; this is installed disk footprint, not peak resident memory.

## Timing interpretation

The report separates source fixture creation, authorized row export, JSON serialization, bridge reading and decoding, Arrow conversion, DuckDB registration/materialization, Python import, and warm query execution. Source fixture creation is excluded because the source database already exists in a real adapter. Python import is recorded separately, since a long-lived process pays it once.

`bridge_plus_setup_ms` includes authorized export, JSON column serialization, bridge file reading/decoding, Arrow conversion, and DuckDB setup. It excludes source report assembly/file writing, process launch, imports, and SQL preparation; therefore it is a lower-bound onboarding cost for this executable bridge. The report's first-query time adds one warm query to this cost and is not a complete cold process latency. Break-even repetitions divide that cost by the per-query saving relative to Substrate (and, separately, ordinary SQLite); no break-even is reported when DuckDB is slower. Repeated queries must reuse the same materialization and data snapshot for that estimate to apply. These costs would change with a native Arrow adapter and should be measured again, not assumed to disappear.

## Authorization and production requirements

The exporter uses authorized schema lookup and paged reads, then revokes `core.Person` and checks that the retained source snapshot refuses further reads. Unit tests also check a principal without grants and verify that a source row filter excludes unauthorized rows from exported columns. This verifies the existing source boundary, not live revocation of detached Arrow copies. Exported data stays in a trusted host process; it is not a durable capability or a live application session. Once bytes are copied, revocation cannot retract them.

A production analytical adapter would need to apply row policies before batch creation, bind any reusable batch/cache to its principal and authorization dependencies, validate live revocation/expiry again before releasing results, prevent protected schema/statistics leakage, bound memory and cancellation, and invalidate/rebuild batches after source changes. External access is disabled in the experiment, but that flag alone is not an application security model. No server path or untrusted SQL endpoint is introduced here.

## Why DuckDB first; DataFusion remains unmeasured

DuckDB provides vectorized execution and direct queries over Arrow tables, making a pinned prebuilt Python wheel a small executable prototype. See the version-matched [vector execution documentation](https://duckdb.org/docs/1.4/internals/vector) and [Arrow integration documentation](https://duckdb.org/docs/1.4/guides/python/sql_on_arrow).

DataFusion is a plausible native Rust implementation: its execution model uses Arrow batches, and custom table providers could expose authorized Liquid relations directly. Its [official introduction](https://datafusion.apache.org/user-guide/introduction.html) describes the available SQL/DataFrame planning and execution components. It is not benchmarked here. Neither design makes a crate-name substitution sufficient: both require a typed batch representation, an authorization-preserving provider, and decisions about scan/index pushdown, snapshot lifetime, and materialization reuse.
