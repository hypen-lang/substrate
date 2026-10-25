# First SQLite baseline — 2026-10-24

Historical first-slice measurements. The subsequent storage milestone is documented
in [Checkpoint recovery](CHECKPOINTS.md); it now bounds redo and supports atomic
checkpoints.

Run `cargo bench --bench sqlite`; set `SUBSTRATE_BENCH_ROWS` to change the default
2,000 people. Raw output is in [2026-10-24-run.txt](2026-10-24-run.txt).

Environment: arm64 macOS 26.2 (25C56), Rust 1.94.0, Cargo release/bench profile,
locked dependencies. Exact CPU model was unavailable from the sandbox. Dataset:
2,000 people, workouts, performed links, deals, and customer/deal links: 10,000
occurrences. Durations cycle through 0..119; amounts are deterministic minor units.
UUIDs are random v4 and reused between compared engines in the query benchmarks.

The ordinary SQLite representation has typed tables, BLOB UUID primary keys,
foreign-key validation enabled, and indexes corresponding to every populated Liquid
role. Native Liquid tests use its administrative snapshot, so policy traversal cost
is not represented. Query correctness is checked before timing.

Each workload has a warmup and seven timed batches. Reported numbers are the median
**batch mean** microseconds per operation, not per-request p50 or tail latency.
Lookups use 1,000 operations per batch; point joins use 200. All query data is in
memory; these are warm-cache tests. The runs are not isolated hardware benchmarks.

| Workload | Liquid | SQLite | Interpretation |
| --- | ---: | ---: | --- |
| Indexed duration lookup, returning matching IDs and values | 3.397 µs | 2.391 µs | Native typed result API vs prepared SQLite query |
| Prepared five-family cross-app point join | 2.933 µs | 1.211 µs | Liquid virtual tables vs ordinary indexed SQLite tables |
| Cross-app point join, prepare on each call | 7.780 µs | — | Shows Liquid adapter parsing/planning cost separately |
| Additive install, initially empty database | 15.925 µs | — | Memory backend, one new family per install |
| Additive install, 10,000 existing occurrences | 19.877 µs | — | Same operation, existing data is not scanned |
| Durable reopen and full redo replay | 45.373 ms | — | 10,000 occurrences, indexes reconstructed |

Native index lookup was about 1.42× SQLite; prepared SQL point joins about 2.42×.
These measurements do not establish mature performance targets, scaling laws,
concurrent write throughput, authorization overhead, or analytical performance.
The install test adds more schema definitions during measurement, so metadata cost
grows; it is not evidence that every schema operation is constant-time.

Durable file sizes at this scale: Liquid 3,153,920 bytes, SQLite 1,048,576 bytes.
The former stores packed redo plus backend allocation overhead and rebuilds indexes
on open; the latter stores current tables and indexes. These are **different durable
representations**, not a normalized bytes-per-current-row comparison. Liquid's working
indexes and retained snapshots consume additional RAM, which this test does not measure.
The durable fixture performs one catalog transaction and one data transaction; SQLite
uses FULL synchronous mode and one data transaction after schema creation. Write timing
is not compared.

Next measurements should separate policy overhead, current-state versus history size,
cold reads, memory usage, larger datasets, skew, range/multiway scans, and commit latency.
The next optimization should target observed costs: candidate-row cloning, UUID text
conversion across the SQL boundary, streaming, and foreground checkpoint cost.

## Post-checkpoint implementation rerun

The same SQLite harness was rerun after compiling schema/policy metadata and adding
checkpoint support. [Raw output](2026-10-24-post-checkpoint-sqlite-run.txt) records
native lookup at 3.127 µs versus SQLite 2.217 µs, and prepared cross-app SQL joins at
2.725 µs versus 1.129 µs. Correctness comparisons passed. These modest differences
from the historical run are not isolated evidence of an optimization speedup.
The two-commit durable fixture stays below automatic checkpoint thresholds, so its
45.664 ms reopen still measures redo recovery. Use the dedicated recovery benchmark
for the checkpoint comparison.

See also [online schema-control measurements](SCHEMA-CONTROL.md) for durable proposal, approval, installation and first runtime operations with concurrent pinned readers.
