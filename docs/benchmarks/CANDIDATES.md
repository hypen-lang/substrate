# Candidate engines and libraries after optimization

These experiments follow the implementation of group commit, incremental
checkpoints, shared query rows, index/planner improvements and visibility caching.
They evaluate the current implementation rather than extrapolating from upstream
benchmark claims. This report records the pre-adoption experiments at `abb0a72`.

**Follow-up:** production now uses imbl 7.0.2 and DataFusion for SQL reads, as
requested after these comparisons. Native queries and redb remain unchanged.
See [DataFusion behavior and compatibility](../DATAFUSION-SQL.md), the
[candidate comparison](DATAFUSION-EXPERIMENT.md), and the
[final native benchmark](DATAFUSION-NATIVE.md). The decisions below are the
historical experiment results, not the current dependency inventory.

The [updated full read comparison](COMPARISON.md) verifies eight identical results
at 2,000 people. Substrate still takes 2.53× direct SQLite time for point reads,
2.33× for a five-family point join, 3.29× for the grouped aggregate, and 5.30× for
range/top-100. These are prepared, warm embedded reads; neither transport nor
database construction is included. PostgreSQL was not rerun for this comparison.

For durable writes, [group commit](../group-commit.md) improved the three-run median
at 32 concurrent writers from 274 to 4,103 transactions/s, with commit p99 falling
from 138.6 to 9.86 ms. Sequential writers remained essentially unchanged. It
amortizes durable synchronization across independent transactions; redb still has
one active physical writer per file.

## Experiments

- [Persistent collections](COLLECTIONS-EXPERIMENT.md): actual frozen engine,
  `im` versus `imbl`, full candidate correctness suite and memory-only read/write
  workloads with retained snapshots.
- [JSON parsing](JSON-EXPERIMENT.md): actual typed requests and schemas,
  `serde_json` versus `simd-json`, input-copy and dispatch costs, malformed input
  and recursion-limit behavior.
- [Analytical queries](ANALYTICS-EXPERIMENT.md): authorized Substrate export into
  Arrow and DuckDB, ordinary SQLite and Substrate references, nine query shapes,
  conversion/materialization costs and reuse break-even estimates.
- [Durable storage](STORAGE-EXPERIMENT.md): redb, SQLite WAL FULL and synchronous
  RocksDB with platform-matched full flushing, identical append/head groups,
  reopen verification and acknowledged-write process-loss tests.

All timing runs are serialized against the other experiments and repository
builds. This is still a shared development machine, not an isolated performance
lab. Each report records pins, workload boundaries, raw results and reproduction
commands. Different reports measure different layers; their timings cannot be
combined into an end-to-end speedup prediction.

## Verification baseline

The final shared checkout passed 320 Rust tests including the doctest and strict
all-target Rust Clippy. The earlier integrated TypeScript run passed all 50 tests;
remote TLS and Effect examples also ran successfully. The frozen collection
candidate separately passed its 316-test suite. Candidate-specific tests are
recorded in each report: nine JSON tests, two storage tests across all backends,
and two Rust plus three Python analytical tests. All three isolated Rust experiment
crates also passed Clippy with warnings denied.
Passing tests are evidence for those cases; they do not establish production
readiness, physical power-loss safety on every storage device, or a complete
authorization-preserving analytical adapter.

## JSON decision

Keep `serde_json`. In three-run medians, simd-json's copy-inclusive path reduced
parsing time for a 100-write batch from 58.83 to 50.01 µs and a schema proposal
from 80.18 to 68.72 µs. A small read became slower (0.257 to 0.415 µs), and a
900 KiB text payload became slower (79.17 to 142.89 µs). Reusing parser scratch
buffers helped small inputs, but did not address correctness differences.

Actual authenticated read dispatch measured 24.254 µs with serde_json versus
24.614 µs with the copied simd-json input: no demonstrated end-to-end improvement.
The tested simd-json version accepts lone high-surrogate escapes as NUL where
serde_json rejects them, and its default recursion limit also differs. Explicit
depth configuration fixes the latter alone. This is not a compatible one-line
replacement, and parsing is not the observed dominant cost of that dispatch path.

## Persistent collections

`imbl` is the strongest direct replacement candidate for memory-only writes.
At 10,000 records, single inserts improved from 41.79 to 18.23 µs per row,
updates from 39.22 to 18.05 µs, and updates with retained snapshots from 38.08
to 18.41 µs. Batches of 32 improved from 8.33 to 4.76 µs per row; eight
independent writers improved 1.30×. The candidate required only a dependency alias
change and passed the frozen engine's full 316-test suite.

Native reads were mixed: point reads went from 0.125 to 0.154 µs, full scans
from 1,172 to 1,244 µs, and indexed equality from 14.98 to 14.39 µs. These are
owned-row API measurements, and the tiny snapshot capture/clone differences do
not establish a meaningful benefit. Durable synchronization and resident memory
are outside this experiment; a twofold memory-write speedup cannot be applied to
the durable throughput numbers directly.

The additional paired SQL comparison at 18,000 occurrences is more encouraging:
several admin queries improved about 3–8%, and several restricted-reader queries
about 10–22%. Admin point reads were effectively tied; the nested admin query's median
was about 5% slower, with overlapping run ranges. Every result matched SQLite and
the other variant. Keep the current default for this evaluation, but prioritize
`imbl` for a controlled write-heavy adoption trial with retained-snapshot memory
and mixed durable workloads measured before making it the default.

## Durable storage decision

Keep redb. With matched durable acknowledgements, all three backends measured
about 4 ms per group. At 32 records/group, throughput was 7,835 records/s for redb,
7,648 for SQLite WAL FULL and 7,855 for RocksDB with the macOS full-flush wrapper.
The alternatives were within about 2.4% of redb. These small, bounded append/head
workloads provide no evidence that replacing redb solves the current synchronization
cost. They do show why grouping is effective. Sustained compaction, indexed
large-state reads, checkpoint workloads and the full engine were not measured by
this storage-only harness.

## Analytical adapter decision

Pursue a separate columnar analytical adapter, keeping the indexed transactional
query path. At 20,000 people / 180,000 occurrences, materialized DuckDB measured
7.93 ms for the grouped cross-application aggregate versus Substrate's 158.30 ms
and direct SQLite's 37.83 ms. The nested window query measured 7.93 ms versus
108.90 ms and 24.69 ms respectively. A full-relation aggregate measured 70 µs
versus 6,238 µs and 1,002 µs. All nine ordered result comparisons passed across
three runs at both dataset sizes.

These warm-query gains require amortization. Authorized export, JSON bridge,
Arrow conversion and materialization/indexing cost about 248.5 ms at the larger
size; direct Arrow registration cost about 174.9 ms. Those are lower bounds that
exclude process launch, imports and some report/file overhead. For the grouped
aggregate, about two repeated queries against the same materialization recover
the measured onboarding cost relative to Substrate. A point read is much worse:
35.38 µs in materialized DuckDB versus 1.14 µs in Substrate.

DuckDB was benchmarked; DataFusion was assessed as a possible native Rust/Arrow
adapter, not timed or ranked. The experiment exports through existing authorized
APIs and tests row filtering and source revocation. Detached Arrow data is a
trusted-host copy: production reuse still needs principal-bound caches, source
revision handling and live authorization checks before returning results. This
is a working experiment, not a production remote query endpoint.
