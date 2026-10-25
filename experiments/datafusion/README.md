# DataFusion versus DuckDB SQL experiment

This isolated Python harness executes native DataFusion and DuckDB query engines
over the same typed Arrow batches exported through Substrate's authorized
snapshot API. It does not add Python to the production engine.

```sh
UV_CACHE_DIR=/tmp/substrate-analytics-uv-cache uv pip install --python experiments/analytics/.venv/bin/python -r experiments/datafusion/requirements.txt
experiments/analytics/.venv/bin/python -m unittest discover -s experiments/datafusion -p 'test_*.py'
cargo build --offline --release --manifest-path experiments/analytics/Cargo.toml --target-dir target
target/release/substrate-analytics-experiment 20000 /tmp/datafusion-source.json
experiments/analytics/.venv/bin/python experiments/datafusion/run.py /tmp/datafusion-source.json /tmp/datafusion-result.json
```

Run at 2,000 and 20,000 people with three independent process repetitions,
alternating `--reverse` to rotate engine order. Do not run compilers or other
benchmarks concurrently. DataFusion uses one target partition, disabled
repartitioning, a 512 MiB tracked memory pool and disabled disk spilling;
`TOKIO_WORKER_THREADS=1` is set before importing the native extension. DuckDB uses
one thread and a 512 MB memory limit. These settings do not bound total process
RSS or memory held by already-created Arrow inputs.

Both engines parse and prepare outside the timed loop. DataFusion retains an
optimized logical plan and rebuilds its physical execution plan on every call;
that rebuild is included in timing. Directly reusing a physical plan produced
incorrect repeated top-k results because dynamic filters retain mutable state.
DuckDB uses `PREPARE`/`EXECUTE`. Timings include collecting
and canonicalizing all result rows. Every query verifies exact ordered rows and
SHA-256 before timing, then verifies results again after repeated execution.
Setup, query preparation and input export/conversion are reported separately.

The correctness tests preserve nulls separately from text, signed int64 extremes,
UUID strings, duplicate multiplicity, order, empty results and repeated-plan
execution. The representative fixture uses integer/string values; this is not a
complete cross-engine SQL dialect or scalar-type compatibility test.
