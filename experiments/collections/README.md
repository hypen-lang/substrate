# Persistent collection experiment

This experiment compares the real engine with `im = 15.1.0` against the same
frozen source with `im = { package = "imbl", version = "=7.0.2" }`. It does not
change the production manifest. The small `fetch` package prepares the candidate
dependency without modifying the engine lockfile.

Prepare two copies in a **new** temporary directory:

```sh
python3 experiments/collections/prepare.py /tmp/substrate-collections-run
cp experiments/collections/imbl.Cargo.lock /tmp/substrate-collections-run/imbl/Cargo.lock
cargo build --offline --release --manifest-path /tmp/substrate-collections-run/im/Cargo.toml --example collections_probe
cargo build --offline --release --manifest-path /tmp/substrate-collections-run/imbl/Cargo.toml --example collections_probe
```

The candidate first needs its dependencies fetched with `cargo fetch --manifest-path
experiments/collections/fetch/Cargo.toml`. Both copies initially retain the original
lockfile; the recorded candidate lock above pins the measured transitive versions.
For common dependencies, an explicit shared `--target-dir` can reuse build outputs.
Copy the resulting example binaries to separate paths before building the next
variant if sharing the target directory: Cargo's example executable path is shared.

Run each binary with its backend label (`im` or `imbl`). Each emits JSONL results
for 1,000 and 10,000 records. Alternate execution order across at least three runs;
do not build or run other benchmarks concurrently. Both are optimized builds with
the same compiler and flags. Run the copied candidate's full Rust test suite too,
with local socket permission for its integration tests.

`source-sha256.json` in the prepared directory identifies the exact source fixture.
The probe checks retained snapshots, optimistic conflicts, index consistency,
final row counts and deterministic row digests. Setup and integrity checks are
outside timed query/write loops. Snapshot construction/cloning, row materialization,
engine validation, encoded queue admission and commit publication are included
where the normal API performs them. Storage is in memory; this does not predict
durable throughput dominated by synchronization.

For the existing eight-workload prepared SQL ladder, build `--bench comparison`
in each frozen copy with `cargo build --offline --release`. Copy each executable
reported by Cargo to `im-sql-probe` or `imbl-sql-probe` inside the prepared directory.
Cargo's `--message-format=json` output supplies the executable path in a
`compiler-artifact` record. Then run:

```sh
python3 experiments/collections/run_sql.py /tmp/substrate-collections-run docs/benchmarks/collections-sql.json > docs/benchmarks/collections-sql-summary.json
```

The default fixture has 2,000 people and 18,000 total occurrences. The runner
executes three alternating pairs, retains each benchmark's JSON (including
SQLite results and query plans), and checks result hashes and row counts across
variants. It explicitly removes `SUBSTRATE_BENCH_PG_DSN`, so this experiment never
uses an externally configured PostgreSQL database. Each workload uses seven
samples of ten warm prepared executions. Preparation and fixture loading are
outside query timing; result materialization and normalization are included.
