# Query benchmarks and flamegraphs

Use this tool to investigate a specific read-only SQL query against a local,
offline database or consistent backup. It records an unsampled benchmark first,
then runs the same query repeatedly under a native sampling profiler. No profiler
dependency or instrumentation is added to production query execution.

## Quick start

Create `query.sql`, for example:

```sql
SELECT p.name, w.durationMinutes
FROM core.Person p
JOIN fitness.Performed f ON f.athlete = p.id
JOIN fitness.Workout w ON w.id = f.workout
WHERE p.name = ?1;
```

Create `params.json` using the same tagged SQL values as the client protocol:

```json
[{"Text":"Alice"}]
```

Install a profiler once, then run from the repository root:

```sh
cargo install --locked samply
node tools/profile-query.mjs /path/to/database.redb fitness query.sql \
  --params params.json --out target/query-profile --seconds 10
samply load target/query-profile/profile.json
```

Samply provides an interactive flamegraph, call tree, timeline and source view.
The wrapper only saves the profile; `samply load` opens the viewer when requested.
Profiles remain local unless explicitly uploaded through the viewer.

For a standalone SVG instead:

```sh
cargo install --locked flamegraph
node tools/profile-query.mjs /path/to/database.redb fitness query.sql \
  --params params.json --profiler flamegraph --out target/query-svg
```

For benchmarking without installing either profiler:

```sh
node tools/profile-query.mjs /path/to/database.redb fitness query.sql \
  --params params.json --profiler none --iterations 1000
```

The wrapper builds offline using the optimized `profiling` Cargo profile, with
debug symbols and Rust frame pointers. Fetch dependencies with `cargo fetch`
first if they are not cached. `--binary PATH` skips building; the caller is
responsible for that binary's optimization and symbols. Cargo's reported artifact
path is used, including when `CARGO_TARGET_DIR` is customized.

The default output is a new timestamped directory under `target/query-profiles/`.
An explicit output directory must not exist. Paths with spaces are supported;
subprocesses are launched without shell interpolation.

## Choose the timing boundary

| Mode | Work measured in each repeated execution |
|---|---|
| `--mode prepared` (default) | Execute and materialize/drop results using one pinned snapshot and prepared statement. Visibility caches stay warm. |
| `--mode request` | Create an authorized snapshot, register the SQL context, prepare, execute, materialize/drop results, and dispose the context. |

Both modes report initial database open/recovery, initial snapshot acquisition,
SQL context registration, preparation, and the first execution separately.
The first execution is **not a cold-disk measurement**: database open already loads
the working state. Request mode recreates query state but does not reopen the
database. Neither mode includes socket transport, client validation, wire JSON,
result printing, writes, commit synchronization or client concurrency.

`benchmark.json` contains:

- Per-execution wall-clock samples, mean, min/max and nearest-rank p50/p95/p99.
- Initial setup timings and database open time, separate from repeated timings.
- Actual measured row-count bounds, so empty or unexpectedly broad results stand out.
- `EXPLAIN QUERY PLAN` output, collected after timing with the same parameters.
- The initial revision, execution mode, warmup count, iterations, OS, architecture,
  SQLite/package versions and whether debug assertions are enabled.

The first query is executed once, then `--warmup` additional times (default 10),
then `--iterations` measured times (default 200). Percentiles are individual local
execution samples, not server request percentiles. Very short queries are sensitive
to timer overhead and scheduling noise; use more iterations and repeat runs on an
otherwise idle machine. SQL results are fully materialized, as in the native SQL
API; use `LIMIT` where appropriate. Volatile SQL can return different results on
each execution, and the report intentionally does not claim result equality.

Parameters support `"Null"`, `{"Integer":"9223372036854775807"}`,
`{"Real":1.5}`, `{"Text":"Alice"}`, and `{"Blob":[0,255]}`. Integers use strings
to preserve exact 64-bit values. Omit `--params` for queries without bindings.

## Read the flamegraph

Focus the call tree on `substrate_liquid::profiling::execute_prepared` for execution
or `execute_request` for request setup plus execution. These roots are deliberately
not inlined. A recording also includes startup/recovery, warmup and shutdown;
filter those out when investigating steady-state execution. `--seconds` controls
the repeated workload, not an individual query timeout. A long query can overrun it.

Wide frames indicate sampled inclusive cost; widths are not exact stopwatch
durations or a chronological sequence. Look underneath the execution roots for
SQLite planning/execution, Liquid cursors, policy checks, index traversal,
allocation and value conversion. Use the unsampled benchmark to quantify a change.

Samply's sampling differs by platform: on Linux it currently samples on-CPU work;
on macOS it also records off-CPU stacks. A CPU flamegraph alone cannot tell you
how much wall time was spent waiting on disk, locks or network. This query tool
does not measure durable writes; profile a write workload separately for fsync.

Linux sampling may require permission to access perf events. SVG generation uses
the platform tools required by Flamegraph (perf on Linux, xctrace on current
macOS releases). macOS may request developer/profiling permission. The wrapper
does not use sudo or change system settings. See the upstream platform guidance:
[Samply](https://github.com/mstange/samply) and
[Flamegraph](https://github.com/flamegraph-rs/flamegraph), including its linker
guidance for Linux lld/mold stack unwinding. If sampling fails, the completed
benchmark report is preserved and the command returns a failure.

## Direct CLI and embedded use

```sh
cargo run --offline --profile profiling -- bench-query database.redb fitness query.sql \
  --params params.json --mode request --iterations 1000 --output benchmark.json
```

Without `--output`, the CLI prints JSON. Output files must not already exist.
The local CLI is a trusted administrator tool: selecting a principal is not
authentication. `admin` selects unrestricted access; any other value exercises
that principal's grants. The database must exist and must not be open by another
host. Automatic checkpoints are disabled for this tool, but opening a legacy
database can still perform format maintenance; use a backup when investigating it.

For an in-memory fixture or an already authenticated/capability-scoped session,
call `substrate_liquid::profiling::benchmark_query(&session, sql, params, options)`.
Call `sample_query` from an executable under a native profiler for stack samples.
Prepared mode pins its snapshot. Embedded request mode can observe newer revisions
if other threads write concurrently, which should be accounted for in comparisons.

Reports omit SQL text, parameter values and returned cells, but query plans may
include schema/expression information. Native profiles can include local paths
and symbols. Inspect artifacts before sharing them.
