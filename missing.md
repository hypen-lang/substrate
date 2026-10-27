# Substrate: remaining work and implementation queue

Updated: 2026-10-27. Package publication and Bend verification remain excluded.

Current integration adds typed NULL/finite floats, authorized cascade deletion,
explicitly deferred foreign keys, bounded remote SQL writes, paged group commit,
paged/resident-file conversion and replication, and optional OpenRaft consensus. See the
[README implementation matrix](README.md#implemented-and-planned) for the current
contract and [the latest benchmark](docs/benchmarks/latest/README.md) for the exact
measured binary boundary. Final validation passed 794 engine tests (11 ignored),
41 vendored-backend tests, the 10-test iterator check, one borrowed-lifetime
compile-fail check and strict Clippy. These suites overlap; unchanged SDK/Python
suites were not rerun in this iteration. The completed one-million-record SQL
comparison, native/API matrix, separate I/O and 1,024-group diagnostics are
published in the linked report. Earlier counts and timings below describe earlier revisions.

Loading and small-read fixes are implemented and tested, but the surviving
comparison baseline is an intermediate build that already contains them; total
start-of-task improvements cannot be established. Final filtered grouping is
28.03 ms, 16.9% below that baseline, still 21.5% slower than PostgreSQL. The 1%
range aggregate is 278.93 µs, still 9.5% slower. Small-read regressions and cold/load
variance remain; peak RSS is 44.66 MiB versus 37.42 MiB baseline despite identical
sampled live heap. Allocator reservation is a lead, not a proven cause. Native
public folds still pay decoding/copying costs, and the 1,024-group diagnostic
leaves a 52.7% filtered-grouping gap. These remain open performance work.

Remaining adoption work includes production authenticated Raft peer serving,
snapshot transfer/log compaction and membership changes; broader SQL mutation
syntax; deferred uniqueness/CHECK constraints; import mappings for unsupported
resident control metadata;
operator authentication/enrollment and durable audit; general distributed
transactions and sharding; and analytical/cache improvements backed by repeatable
measurements. Paged current data and indexes are already disk-backed. Bounded
working structures do not make the configured cache size a process RSS limit.

Earlier sections retain chronological implementation history and are superseded
by this current status and the linked implementation guides.

Three subagents can run concurrently with the coordinating agent. Remaining agents
start as slots become available. Each workstream must report implemented behavior,
verification, measured results where appropriate, and the parts still missing.
Broad milestones are split into reviewable vertical slices; production distributed
operation is not implied by a small replication/sharding implementation.

## Active and queued work

| Workstream | Agent/status | First acceptance criteria | Further work within the milestone |
| --- | --- | --- | --- |
| SQL execution and planning | Native slice and indexed DataFusion implemented/tested | Authorized indexes, ordered top-k, bounded join probes, compiled identity/equality/bounded-range reads, fresh plan reuse, bounded caches and paired measurements | Better selectivity statistics, wider range domains, broader join eligibility, parameterized complex plans and batch-native analytical sources |
| Operational hardening and transport | `operational_hardening` — initial slice implemented/tested | Bounded per-peer admission/failure limits, safe observability, remove accept polling/duplicate serialization where feasible; preserve per-operation auth and test real sockets | Enrollment/OIDC or mTLS mapping, certificate reload/rotation, durable audit history, stronger CPU/memory isolation, cancellation and request CPU isolation |
| Incremental checkpoints | `checkpoint_scaling` — incremental v2 implemented/tested | Avoid rewriting unchanged checkpoint rows; retain atomic head/checkpoint/prune, integrity and old-file compatibility; crash tests and write-amplification measurements | Background work with bounded publication stalls, recovery-cost bounds under maintenance lag |
| Durable group commit and separate publication lock | `durable_group_commit` — implemented/tested | Bounded queue; ordered OCC/auth validation against evolving tentative state; one revision per accepted transaction; one Immediate durable redb transaction per group; publish/ack only after persistence; individual conflicts and fail-stop errors | Benchmark optional collection windows and caps,1/8/32 writers,p50/p99,reader stalls,checkpoint/crash boundaries |
| Background checkpoint scheduling | `background_checkpoints` — implemented/tested | Move checkpoint preparation/maintenance off the publication path while preserving ordered durable commits and bounded recovery; prove reader/commit progress and crash safety | Resource budgets, maintenance lag and backpressure |
| Persistent transport sessions | `persistent_transport` — implemented/tested | Reuse bounded TLS/Unix sessions with framing, deadlines, shutdown and authentication on every operation; test rotation/expiry between requests and uncertain writes | Pooling, cancellation and backpressure |
| Authorization traversal | `visibility_scaling` — snapshot slice implemented/tested | Memoize completed visibility safely in an immutable authorization context; correct cycles and negative results; retain every transaction dependency and final revocation gate; shared-chain scaling test | Cache bounds, invalidation cost and policy/reference-graph stress testing |
| Schema/compiler scaling | `schema_compiler_scaling` — implemented/tested | Deterministic graph-based module ordering; precomputed name lookup; reference/cycle/duplicate tests and long-chain scaling | Large-catalog tooling and declaration/contract compilation costs |
| Richer constraints and schema evolution | Typed constraints implemented/tested | Stable identities, conservative planning, explicit NULL/finite floats, CHECKs, authorized cascades and explicitly deferred foreign keys | Cardinality bounds, deferred UNIQUE/CHECK, explicit conversion/backfill plans, safe destructive lifecycle and ownership transfer |
| Durable views | `durable_views` — native slice implemented/tested | Durable typed view definition and authorized execution across restart; live revocation, dependency validation and read-only behavior | Materialized maintenance, published disclosures, richer composition and schema evolution |
| Storage beyond RAM | Mutable paged engine implemented/tested | Disk-backed current rows/indexes with bounded caches, transactions, SQL, SDKs and remote access; explicit offline resident/paged import | Cache efficiency, allocation/footprint measurements, safe tombstone reclamation and broader workloads beyond cache capacity |
| Replication and consensus | Trusted-host followers and optional static OpenRaft implemented/tested | Atomic ordered apply, durable cursors, quorum acknowledgements, fenced failover, bounded recovery and explicit topology/trust boundaries | Production authenticated peer serving, snapshot transfer/log compaction, membership changes and stronger operational tooling |
| Sharding | `sharding` — fixed local routing implemented/tested | Explicit stable partition/routing contract and bounded executable behavior, with cross-shard invariants stated and tested | Routing changes, rebalancing, global constraints, cross-shard transactions and distributed authorization |

## Performance review checklist

The supplied review contains local synthetic probes, not production latency claims.
Source: user attachment `41592749-ff9d-4cc1-bb85-989857d2db5d/Pasted text.txt`.

| Finding | Owner | Verification |
| --- | --- | --- |
| Predicate-order-dependent equality choice | SQL execution | Both predicate orders select identity; differential scaling tests |
| Repeated reference-graph visibility traversal | Authorization traversal | Cycles, negative results, revocation gates and shared-chain scaling |
| Deep cloning unselected payloads | SQL execution | Shared cursor rows and large-payload COUNT benchmark |
| Full readable SQL catalog per request | SQL execution | Explicit selected-family API; implicit discovery still missing |
| Fixed-interval full checkpoints | Checkpoint scaling | Incremental row/metadata counters, crash atomicity, v1 compatibility |
| Repeated remaining-module scans | Schema/compiler scaling | Deterministic graph ordering and reverse-chain scaling |
| Missing range/order pushdown | SQL execution | Numeric range/ordering differential tests; broader domains remain |
| Polling accept/new connection per request | Operational hardening + transport | Readiness accept and persistent sessions with per-request authorization |
| Duplicate response serialization | Operational hardening | One serialization preserves frame limits and errors |
| Quota scans clone ledger rows | Authorization traversal | Family counts retain OCC dependencies without row clones |
| Discarded snapshot dependency sets | Authorization traversal | Snapshot reads omit unused sets; transaction dependencies retained |
| One durable commit per transaction | Group commit | Concurrent writers, individual conflicts, latency, reader stalls and crash recovery |

## Existing guarantees that all work must preserve

- Stable entity, relationship and schema identities; additive modules do not rewrite old application rows.
- Exact typed constraints, referential integrity and atomic transactions.
- Indexed execution agrees with reference/native/SQLite comparisons where semantics overlap.
- Shared identities do not confer access to unrelated application data.
- Capability attenuation, credential rotation, expiry and live revocation cover retained reads and commits, including schema operations.
- Checkpoints/logs validate explicit versions and integrity, recover atomic states, and retain bounded redo behavior.
- SQL/client optimizations must not silently change coercion, bag semantics, ordering guarantees or authorization.
- No automatic retry of an uncertain remote mutation.

## Excluded from this run

- Publishing npm/crates.io packages. Repository SDKs and examples remain supported.
- Bend 2 executable reference verification. Rust property/differential tests remain the current oracle.

## Baseline evidence

The preceding milestone passed 204 Rust tests plus one doctest, 41 TypeScript tests,
Clippy and the remote/Effect examples. See `docs/IMPLEMENTATION.md`.
`docs/PERFORMANCE-ANALYSIS.md` records two embedded SQLite/Substrate runs: all eight
result comparisons passed; the repeat range/top-100 case was approximately 33.7×
SQLite time. Those measurements exclude transport and credential-bound sessions.
New work must add its own evidence instead of reusing baseline timings as proof of
an improvement.

## First-wave progress (verification in progress)

- SQL agent reproduced predicate-order scaling before editing and has passing
  SQL/security/nested regressions for identity selection, shared cursor rows and
  numeric range ordering. Explicit selected-family APIs are added; implicit raw-SQL
  catalog discovery remains outstanding.
- Operational agent has passing admission, TLS and Unix tests. Configurable limits,
  a bounded nonblocking host audit stream, readiness-based accept and single response
  serialization are implemented. Audit persistence/enrollment/certificate automation
  remain outstanding. Persistent sessions have their own queued agent.
- Checkpoint agent has passing existing and new durable crash/corruption tests for
  incremental v2 row and metadata storage and v1 upgrade. At 2,048 inserted rows,
  the focused workload wrote 2,048 row entries versus 17,408 previously. Background
  scheduling and disk-backed serving remain separate workstreams.

These are focused subagent results, not a substitute for the coordinated full-suite
verification after integration. Final measurements live in the agents' reports.

## Initial implementation reports

- SQL: `docs/benchmarks/QUERY-PLANNING.md`; local focused correlated EXISTS at
  2,000 rows: 552.86 ms → 1.10 ms; numeric range/top 100: 440 µs → 50 µs;
  unused 64 KiB payload COUNT: 2,613 µs → 64 µs. Selected 1,000-family catalog
  setup: 20.16 ms → 46 µs. Automatic raw-SQL selection is not implemented.
- Transport/hardening: `docs/OPERATIONAL-HARDENING.md`; loopback TLS median
  6,279.542 µs → 297.959 µs, Unix 6,289.584 µs → 93.917 µs. Connections are still
  per-request in this slice; TLS session resumption may occur. These are local
  measurements with concurrent development activity, not production estimates.
- Checkpoints: `docs/INCREMENTAL-CHECKPOINTS.md` and its benchmark report;
  17,408 → 2,048 row writes at 2,048 inserted rows, median checkpoint time
  200.062 → 105.685 ms. Total insert workload time was essentially unchanged.
  Old checkpoint v1 upgrades once; newly written v2 checkpoints require a new binary.

First-wave integration: the complete Rust suite (including socket/TLS and subprocess crash tests) passed after combining SQL, checkpoint and operational changes. Later-wave changes still need their own integration run.

## Added priority: durable group commit

User-directed addition: independent outstanding transactions may share a physical
synchronization while retaining separate logical revisions and outcomes. Default
coordination drains work already queued, without a deliberate collection delay.
A failed/conflicting transaction must not poison otherwise valid group members.
Authorization and OCC checks apply in order to each evolving tentative revision.

The published-state lock must not be held through disk I/O. Readers use the latest
published durable snapshot while persistence is running. Successful commit still
means durable; uncertain storage outcomes stop further writes until reopen. Tests
must cover revocation/grant changes within a group, expiry while queued, checkpoint
boundaries, and crashes around persistence/publication. No relaxed-durability mode
is included. Sequential write/await clients still need atomic batches to amortize
synchronization; group commit targets concurrent outstanding transactions.

Background scheduling, disk-backed serving and replication build on this commit
boundary; their dedicated agents have now started.

## Later-wave progress

- Durable invoker views: 12 integration tests, one deterministic expiry unit test,
  and checkpoint/restart example passed. Definitions use a private compiled ledger;
  view permission never grants source access. See `docs/DURABLE-VIEWS.md`.
- Schema compiler: deterministic graph ordering tested against the previous ordering
  across randomized graphs; 8,000-module ordering measured 334.927 → 2.037 ms.
  See `docs/benchmarks/MANIFEST-ORDERING.md`.
- Visibility: snapshot-only completed-result memoization passed graph properties and
  authorization regressions. A 1,000-root shared-chain scan measured 248.497 → 1.128 ms.
  Transaction dependency tracking remains intact. See `docs/VISIBILITY-PERFORMANCE.md`.
- TypeScript integration: 46 tests passed after enabling the local socket access
  required by persistent-connection tests. The initial sandbox run blocked four
  socket tests; it did not expose a client assertion failure.

## Durable group commit result

`docs/group-commit.md` documents the implementation, bounds, durability and expiry
semantics. Thirteen focused coordinator tests pass, including property comparisons,
blocked synchronization/reader progress, failure injection and eight subprocess
crash scenarios. Default grouping uses no deliberate delay.

Three-run local medians comparing group size 1 against default grouping:

| Writers | Single-transaction groups | Default grouping | Transactions per default group |
| --- | ---: | ---: | ---: |
| 1 | 255 tx/s | 257 tx/s | 1 |
| 8 | 266 tx/s | 1,131 tx/s | 4 |
| 32 | 274 tx/s | 4,103 tx/s | 16 |

At 32 writers commit p99 measured 138.6 → 9.86 ms. These count Immediate redb
transactions, not filesystem sync calls. The baseline already uses the separated
publication lock, so the benchmark isolates grouping. Final integrated validation
is recorded below.

## Integrated checkpoint

The intermediate combined Rust suite passed 297 tests including one doctest after
integration of grouping, persistent sessions, views, routing, constraints and initial
replication. Final verification, including maintenance and replication refinements,
passed 316 tests as recorded below.
All requested workstreams have now had dedicated agents; publication and Bend remain
excluded. The disk-backed read slice is deliberately separate from converting the
entire mutable kernel to an out-of-core engine.

## Maintenance and replication

- Background scheduling: nine tests, including periodic triggers, coalescing, clean
  lifetime, injected failure and four subprocess crash points, pass. The existing
  synchronous hard redo bounds remain. Enqueue latency improved, but the paired
  workload showed essentially unchanged throughput and worse commit p99; see
  `docs/BACKGROUND-CHECKPOINTS.md`. Background scheduling is not parallel redb IO.
- Replication: twelve tests and `examples/replicated.rs` pass. Versioned seeds and
  ordered packets persist with their cursor before publication; exact cursor reads
  require a trusted matching barrier. Follower authorization remains eventual until
  updates arrive. See `docs/REPLICATION.md` for bounds and source-restart/reseed limits.
- Integrated TypeScript suite now passes all 50 tests, including generated checks
  and persistent sessions. Publication and Bend are still excluded.

## Updated embedded read comparison

`docs/benchmarks/2026-10-25-comparison-after.json` records a new 2,000-person run;
all eight SQLite/Substrate result comparisons passed. Range/top-100 measured 5.30×
SQLite time (historical repeated baseline ~33.7×); point lookup 2.53×, five-family
point join 2.33×, grouped aggregate 3.29×. Correlated EXISTS remains 7.25× in this
ladder, distinct from the fixed predicate-order regression probe. PostgreSQL was
not rerun. See `docs/benchmarks/COMPARISON.md` for timing boundaries and all results.

## Completed integration and new candidate experiments

All original workstreams have implemented, documented slices. Final combined core
verification: 316 Rust tests including the doctest and 50 TypeScript tests passed;
remote TLS and Effect examples also passed. The independent integration review
reported no actionable defect in group commit, maintenance or replication.

Disk-backed historical read leases preserve live source authority and retain no
source row/state Arcs. Eight tests passed. Lease-exclusive Rust allocations measured
50.9 MB → 1.18 MB, with slower point reads (1.67 → 125.75 µs p50). The mutable engine
still requires RAM. See `docs/DISK-SNAPSHOTS.md`.

The initial user-requested experiments were isolated from production dependencies.
The subsequent adoption milestone is recorded below:

| Candidate | Status | Findings / remaining work |
| --- | --- | --- |
| im → imbl | Adopted imbl 7.0.2 after isolated engine experiment | Candidate passed 316 frozen-engine tests; 10k-row single inserts/updates about 2.2× faster, native reads mixed; paired SQL comparison and raw results in the collections report |
| serde_json → simd-json | Completed; retain serde_json | Nine tests expose malformed-surrogate and default depth differences; structured parsing improves ~14–15%, authenticated read dispatch does not; large text regresses |
| Analytical DataFusion / DuckDB | Both measured; DataFusion adopted for SQL reads | Initial candidate favored DataFusion for analytics; production now uses lazy authorized index-backed providers and bounded planning/page caches. See the paired adapter report below for current costs and remaining gaps |
| redb → SQLite WAL / RocksDB | Completed; retain redb | All ~4 ms/durable group with matched platform flush; 32-record throughput within ~2.4% of redb; reopen/process-loss tests pass; sustained compaction and full-engine adapters remain unmeasured |

No candidate is adopted just because an upstream microbenchmark claims it is faster.
See candidate decisions and reproduction links (historical results removed).

Final post-experiment shared-checkout verification passed all **320 Rust tests**,
including the doctest, and strict all-target Clippy. The frozen imbl comparison
retains its separately verified 316-test count. Isolated parser tests (9), storage
tests (2, covering all three backends), and analytical tests (2 Rust + 3 Python)
passed; all isolated Rust experiment crates passed strict Clippy. No production
dependency was switched in that experiment checkpoint. The earlier 50-test TypeScript result is unchanged.


## DataFusion SQL and imbl adoption — 2026-10-25

The user selected DataFusion as the SQL read engine. `Snapshot::sql()` and
transactional SELECTs now use isolated authorized Arrow catalogs, retained live
revocation/expiry/credential guards, typed parameters and fresh physical execution
plans per call. Native queries, typed writes and redb storage retain their APIs.
SQLite remains an explicit dialect compatibility API and the SQL mutation adapter;
there is no silent read fallback. Production persistent collections use imbl 7.0.2.

Independent review found wrapping integer arithmetic and an OVERLAY allocation
amplification path. Checked integer operators/SUM and both OVERLAY syntax guards
address these findings, with regression tests. Signed SUM DISTINCT and unsigned
or decimal arithmetic without an explicit cast remain unsupported. Detailed SQL
semantics and resource bounds are in [the adapter guide](docs/DATAFUSION-SQL.md).

Remaining SQL work: lazy/index-aware authorized providers, cheaper selected catalog
construction, precise transaction read dependencies, larger-than-catalog-budget
queries, hard CPU/RSS isolation, wider checked arithmetic/aggregate coverage, and
broader dialect compatibility. The current pool and deadlines are accounting and
cooperative limits, not a sandbox against arbitrary untrusted SQL. Point queries
can be slower than SQLite; the native benchmark (historical results removed)
separates catalog setup, planning and warm execution from raw candidate results.


Integrated verification: **341 Rust tests including the doctest** and **50
TypeScript tests** passed. Strict all-target Clippy and formatting checks passed.
The DataFusion example returned the expected joined aggregate, preserved native
lookup behavior, and denied its retained SQL handle immediately after revocation.
The review report records both findings and their fixes. Native benchmark results
and timing boundaries are recorded separately in the linked report.


The final native SQL benchmark completed all six runs (2k/20k people, three repeats)
with nine exact ordered result comparisons before and after repeated execution.
At 180k occurrences, grouped joins, nested windows and the full-scan aggregate
beat direct SQLite by about 3.0×, 3.2× and 3.8× respectively. SQL point and selective
join overhead remains substantial: 122 µs and 1.73 ms versus direct SQLite's
0.36 µs and 3.18 µs. Authorized catalog setup takes 35 ms admin / 111 ms reader.
Optimizing prepared planning and lazy/index-aware SQL providers is a priority;
physical execution state must remain fresh to preserve repeated-query correctness.
These figures exclude transport and are not native indexed-query measurements.


## Index-backed DataFusion and planning reuse — 2026-10-25

Implemented lazy native identity/equality/numeric-range providers with projection
and safe LIMIT pushdown. SQL catalog construction registers schemas without copying
rows. Only exact typed comparisons are pushed; residual conditions remain in
DataFusion. Native APIs and storage are unchanged. Optimized planning is cached
for one exact parameter binding per prepared query; simple physical templates are
rebuilt into fresh operators, while mutable joins/sorts get newly planned trees.
Stable/volatile expressions and subqueries bypass planning reuse.

A 64 MiB / 4,096-entry FIFO cache stores projected authorized pages for unlimited,
predicate-free family scans, scoped to one pinned SQL snapshot and principal.
Cached reads still enforce live authority and fresh per-execution source budgets.
This preserves analytical reuse without eager catalog-wide materialization.

Remaining work: indexed join probes driven by join results, native ordered-range
and top-k pushdown, cheaper changed-parameter planning, precise transactional read
dependencies, larger-than-RAM serving and hard CPU/RSS isolation. Numeric range
queries still materialize their candidates before DataFusion sorts them. The
paired benchmark (historical results removed) separates unprimed source
conversion from warm repeats and compares both previous adapters with SQLite.

Integrated verification: **363 Rust tests including the doctest** and **50
TypeScript tests** passed, as did strict all-target Clippy and the SQL example.
Independent review found an unnecessarily one-sided range scan; intersecting
both bounds fixed it. No actionable review finding remains in this milestone.

The final twelve-process paired benchmark verified all query outputs. At 180k
occurrences, warm SQL point reads improved 117 → 15.6 µs and the selective join
1.96 → 0.84 ms. Range/top-k regressed 0.37 → 1.24 ms, EXISTS 1.14 → 1.59 ms,
and windows 8.09 → 15.10 ms. Unprimed source conversion remains expensive; native
ordered-range/top-k execution and join-driven index probes remain priorities.
Direct SQLite's 0.34 µs point reads also show substantial SQL dispatch/planning
overhead remains. These are memory-resident synthetic reads, not native API,
transport or durable-write measurements.

## Range, EXISTS and window follow-up — 2026-10-25

Implemented bounded projected-page reuse for unlimited numeric ranges, including
exact predicate keys, candidate-budget charging and live authorization on hits.
Deterministic subqueries can reuse optimized logical plans after recursively
checking raw expressions; execution-time functions remain uncached and physical
join/sort state is fresh. All **370 Rust tests** and strict all-target Clippy pass.
Independent review's cache-key accounting concern was fixed; no actionable
finding remains. Native query and storage code is unchanged.

Next SQL pain points, in priority order:

1. Join-driven identity/equality probes: selective SQL still scans whole joined
   families when their IDs become known only during execution.
2. Ordered-range/top-k pushdown: the provider still retrieves all range candidates
   before DataFusion sorts and limits them. Warm caching does not improve the
   first traversal or eliminate sorting.
3. Small-query execution overhead: executor dispatch, task setup, authorization,
   Arrow conversion and result allocation surround an already indexed lookup.
4. Changed-parameter planning: the one-entry bound-plan cache misses on new
   values; a safely reusable parameterized plan remains future work.
5. Cold conversions and cache pressure: short-lived SQL handles and working sets
   beyond the bounded page cache still pay conversion costs. Native engine state
   still lives in RAM, and SQL transaction read dependencies remain conservative.

The follow-up benchmark (historical results removed) records
measurement boundaries and both previous adapters alongside SQLite.

All eighteen benchmark processes passed result checks. At 180k occurrences,
warm range/EXISTS/window queries took 0.195/1.384/10.892 ms versus the previous
indexed adapter's 2.569/3.423/23.780 ms and eager DataFusion's 0.462/1.506/11.550 ms.
Point/selective paths did not improve. In the current same-fixture run, point
reads cost 32.1 µs versus direct SQLite's 0.425 µs; the selective join cost
1.22 ms versus 0.0075 ms. A separate one-row release probe measured a 10.4 µs
executor round trip and 19.8 µs prepared constant query, showing substantial
fixed overhead without assigning additive phase costs. New handles still pay
source conversion; larger caches alone will not solve the remaining pain points.

## Indexed SQL execution follow-up — 2026-10-25

The previous section records the earlier milestone's measurements. Join-driven
probes, ordered range/top-k pushdown and compiled simple reads are now implemented
in the SQL adapter. Native APIs and storage are unchanged. Simple identity,
role-equality and numeric-range descriptors accept unseen exact typed parameter values without
replanning or Arrow conversion. Selective inner joins can avoid full-family
conversion, and compatible numeric top-k reads stop at the required visible rows.
Audited hash joins, sorts, built-in aggregates and `ROW_NUMBER` windows can also
reuse physical templates for the same binding, with fresh mutable execution state.

Remaining boundaries:

- Join probes currently require a single-key inner join, compatible key types,
  one partition and a provably bounded input of at most 128 rows. Wider joins,
  large/unknown inputs and ordered joins with pushed fetch retain ordinary execution.
- General DataFusion queries still cache one exact binding. Parameterized logical
  templates for complex expressions, multi-role ranges and joins remain further work.
- Full analytical scans still convert rows into Arrow; the bounded page cache
  cannot remove first-use cost or hold arbitrarily large working sets. Larger
  caches are not a substitute for selective execution or a batch-oriented source.
- Inline simple reads check deadlines cooperatively between native operations;
  they cannot interrupt a blocked native call.
- SQL transaction read dependencies remain conservative; native state and indexes
  remain memory-resident.

See the execution benchmark (historical results removed) and
[independent review](docs/reviews/DATAFUSION-EXECUTION.md). The
[beyond-SQL investigation](docs/PERFORMANCE-BEYOND-SQL.md) prioritizes duplicated
redo encoding, unnecessary index updates, authorization work under the publication
lock, native projection/copy costs and transport queueing. Those are investigated
opportunities, not changes to native semantics in this SQL milestone.


## Tiny joins, native overheads and schema visibility — 2026-10-25

Compiled bounded inner joins now avoid Arrow and executor setup for supported
exact equality bindings, including changing parameters. Unsupported expressions,
coercions and large fanouts retain DataFusion execution. The fallback retains
spent source budgets and remaining time; materialized tuples are bounded, while
native authorization traversal remains cooperatively interruptible only between
calls.

Native changes remove disposable packed redo encoding during admission, skip
unchanged index mutations while preserving OCC version invalidations, construct
scoped snapshot permissions outside the publication lock, and project native
results before copying unselected values. Owned native APIs retain their result
semantics. See [native details](docs/NATIVE-OVERHEADS.md).

New relation families support public, authenticated (default), and private
visibility. Typed policies are separate from immutable manifests and old packed
rows. Namespace owners can propose initial policy and change it live; credential
scopes still cap permission. Explicit anonymous clients can read public data but
cannot write/reference. Old databases remain grant-only. See
[visibility semantics and examples](docs/SCHEMA-VISIBILITY.md).

Remaining work includes broad analytical parameter templates and batch sources,
strict CPU cancellation inside denied-row/reference traversal, shared payloads in
index dependency keys, bounded native result streaming, transport queueing and
backpressure, and narrower visibility revocation generations. Current ambient
visibility changes conservatively invalidate all unscoped retained readers.


Final verification: **430 Rust tests including the doctest**, **53 TypeScript
tests**, strict all-target Clippy, formatting, and working local/TLS examples.
Two independent reviews completed and all concrete findings were fixed. Final
paired reports: tiny joins (historical results removed) and
native overheads (historical results removed), with raw data from both
iterations. At 180,000 occurrences the two tiny admin joins improved about 36×.
32-field partial updates improved about 3.8× and selected native projection
about 34×. Restricted non-join reads still show new visibility overhead; full
owned scans and contention medians did not improve. Durable sequential writes
remain synchronization-bound. These remain explicit performance work, alongside
broader query planning and memory/transport limits above.

## Restricted reads and contention follow-up — 2026-10-25

Implemented and independently reviewed:

- Compiled reference slots and snapshot-local family read policies preserve ambient visibility, filtered grants, capability scope, and reference closure.
- Native scans borrow family rows directly; indexed reads look rows up once. Uniform scalar reads can avoid per-row policy work. Restricted output capacity never reveals hidden candidate counts.
- Capability grants are compiled once; a nonblocking, single-entry session cache reuses only the identical published state, generation, capability and credential. Cache contention falls back to ordinary authorization.
- Publication generations avoid repeated head-mutex acquisition for unchanged-state read fences. Commits, replication and every failure path invalidate the generation; expiry, attachment, poison and historical transaction checks remain enforced. Saturation disables the shortcut permanently.
- SQL keeps entry/page/final authorization fences and per-row deadlines without taking the head mutex for every buffered output row.

Against `2dac848`, three alternating SQL process pairs per size show 20k-person
restricted point/equality/range reads improving 0.783 → 0.404, 91.112 → 57.521,
and 25.646 → 18.054 µs. Nine native process pairs show finite-reader writer p50
23.250 → 4.375 µs. Keeping all readers active shows roughly unchanged writer
latency, reader p99 58.667 → 33.208 µs, and more completed reads during the same
256 commits. Full owned scans remain effectively flat (649 → 642 µs); payload
copying and run variation prevent a strong large-scan speedup claim. Projection
improves 18.916 → 6.334 µs. Sequential durable updates remain near 4 ms.

Verification: 446 Rust tests and 53 SDK tests passed, strict all-target Clippy,
formatting, independent review, and exact benchmark result checks. One manual
performance probe remains ignored. The review-found capacity disclosure and a
second-boundary race in the TLS expiry test are fixed. Full results, SQLite
controls, cold-handle costs, limitations, raw data and fingerprints are in
READ-REGRESSIONS (historical results removed).

Remaining performance work includes owned-result payload allocation/copying,
sustained writer pressure under unlimited readers, cold SQL setup, broader SQL
plan eligibility, and the previously documented durable synchronization costs.


### Scale measurements (2026-10-25)

The scale/concurrency report (historical results removed) and raw measurements now
cover 100–10,000,000 total application rows, 1–256 clients, resource usage, durable
TPS, query output parity, and process-crash recovery. Completed and resource-limited
runs are distinguished. Substrate completed one million rows but its ten-million
load hit a 5 GiB process-footprint guard after 2.619 million confirmed rows.
Resident state/index overhead, recovery/checkpoint allocations, durable write
amplification, and large query paths remain priorities. This
benchmark work does not implement a larger-than-RAM mutable working set or establish
a production concurrency maximum.

### Capacity tuning after the scale measurements (2026-10-25)

Implemented and tested: atomic publication of data/authorization/failure state;
fresh-snapshot authority payload reuse without retaining old row state; adaptive
singleton/small-array index postings; configurable backend cache (64 MiB default);
direct checkpoint replacements and skipping discarded redo encoding; persistence
stage counters; 128-transaction groups with a 500 µs collection window only under
current/recent concurrent demand; authorized indexed execution for a bounded subset
of correlated EXISTS queries. The default checkpoint bound remains 128 commits or
8 MiB: experiments with larger bounds increased checkpoint tail latency.

The capacity report (historical results removed) contains sustained before/
after comparisons, SQLite/PostgreSQL controls, raw counters, query parity, and
resource-limited larger-load results. The full Rust library/integration suite passed
470 tests; all 16 commit tests and five benchmark-example tests passed, along with
strict Clippy. Enqueue notifications now fire only when a batch fill threshold is
crossed; completion still wakes all callers. The final sustained measurements
include this fix for unnecessary thread wakeups.
Three subagents implemented/reviewed the snapshot, posting, and EXISTS changes.

At this earlier resident-only milestone, mutable disk-backed serving was missing.
The [paged engine](docs/PAGED-STORAGE.md) now supplies it. Remaining resident work
includes packed/chunked rows, compact internal handles, safe reclamation of
historical conflict keys and larger-scale recovery without rebuilding resident indexes. Fresh reads still do not
scale linearly with cores; analytical joins/window execution and storage write
amplification need further work. Immutable disk read leases alone do not fill that
gap; mutable paged storage is a separate backend. No PostgreSQL parity or production writer maximum is claimed.

### Shared ownership and indexed partition reads (2026-10-25)

Implemented: `Session::get` for fresh owned point reads, borrowing publication and
authority handles on warm unscoped scalar reads; transaction construction moves
temporary handles; bounded top-one-per-identity SQL windows use authorized index
probes; checkpoint encoding borrows rows and accepted operations. Existing snapshot
semantics, capability scopes, reference visibility, durable acknowledgements and
entry/release revocation checks remain. Shared `Arc` ownership is retained.

The ownership/query report (historical results removed) includes
million-record before/after runs, SQLite controls, failed optimization experiments,
native stack samples and reproduction. The new API's gain does not apply to
unchanged snapshot callers, capability-scoped credentials or relationship reads.
The SQL optimization applies to a proven deterministic, bounded ROW_NUMBER shape;
general analytical queries retain DataFusion. Three subagents implemented and
reviewed the read, SQL and checkpoint changes; the integrated Rust suite passed
483 tests, with two manual performance probes ignored (storage probe run separately).

Next bottlenecks, in evidence order:

1. **Checkpoint write amplification:** roughly 880 KB of backend writes per 7.3 KB
   encoded row payload in the storage probe. Atomic log-table recreation wrote
   exactly the same bytes and was rejected. A compact versioned segment/delta
   checkpoint with crash-safe compaction needs a separate format milestone.
2. **Mutable data beyond RAM:** rows and indexes still materialize in memory;
   bounded cache serving and recovery without rebuilding all resident indexes remain.
3. **Retained/scoped read contention:** global weak-handle upgrades remain in the
   retained snapshot path; the new fresh-session shortcut deliberately excludes
   capability-scoped and reference-bearing reads.
4. **Result allocation and wider analytical plans:** owned rows still copy selected
   payloads, and unbounded joins/grouping/windows retain their general execution cost.

See backend I/O evidence (historical results removed) for actual backend-call
bytes rather than estimates of device traffic. No new write-throughput improvement
or PostgreSQL parity is claimed by this read-focused milestone.

### Checkpoint amplification, conflict memory and scoped reads (2026-10-25)

The next follow-up addresses three items above:

- V3 checkpoints retain a bounded map of latest row overrides/tombstones, partially
  flushing sorted non-pending entries into the base. Both 4,096-key and 8 MiB bounds
  are enforced at every durable checkpoint; rows, metadata, manifest, head and redo
  pruning remain one Immediate transaction. V1/V2 recovery is supported, with an
  atomic upgrade on the next checkpoint. Older binaries cannot open V3 files.
- Conflict timestamps use typed maps grouped by relation/role instead of repeating
  large enum keys. All historical keys remain for correct optimistic conflicts;
  this is a layout change, not tombstone reclamation.
- Warm scoped scalar reads reuse compiled capabilities while retaining entry and
  release authorization checks. `Session::get_shared` avoids owned payload copies;
  expiry clock reads avoid unnecessary shared atomic writes within the same second.
  Relationship reads keep the complete reference-visibility path.
- Compiled grants are grouped by principal and relation. Runtime permission checks
  borrow principal names without allocating lookup keys; ordered durable grant
  declarations and capability isolation remain unchanged.

The follow-up report (historical results removed) separates whole-engine
before/after measurements from isolated allocation and read probes. It includes
SQLite/PostgreSQL controls, query parity, crash recovery, and rejected checkpoint
experiments. [The shared-read example](examples/shared_reads.rs) demonstrates
filtered capability access, immutable returned rows and live revocation.

Still outstanding: mutable serving beyond RAM; compact resident row handles and
payloads; safe conflict-key reclamation; startup without rebuilding all indexes;
lower checkpoint page amplification; broader analytical execution; and scoped
reference traversal. Partial checkpoint merging reduces measured I/O but retains
synchronous merge costs and needs tail-latency monitoring. Shared rows retain their
payload until released. No production concurrency maximum or PostgreSQL parity is
claimed.


### Paged native current storage (2026-10-25)

The opt-in `paged::PagedDatabase` stores current rows, ordered role/unique/incoming
indexes, allocated identities and conflict timestamps in redb with bounded caches.
Opening compiles the bounded catalog without rebuilding rows/indexes. Typed native
writes, independent point transactions, additive schema installation, retained
snapshots, visibility/grants, bounded pages and crash recovery are implemented.
See [paged storage](docs/PAGED-STORAGE.md) for the exact boundary.

Follow-up integration now implements paged DataFusion SQL, existing SDK/local/TLS
transport, scoped credentials, rotation/expiry/delegation/live revocation, runtime
schema proposal/install, named/typed operations and nested reads. Repeated table
opens, quadratic batch uniqueness, redundant scan seeks and row sizing allocations
were removed. Bound inner equijoins now use reusable index probes. New LPG2 files
omit redundant singleton-unique role postings; legacy LPG1 layouts are preserved.
See integration benchmarks (historical results removed), including the
remaining cold-read, aggregate and large-load costs.

Whole-family scalar COUNT/checked SUM/numeric MIN/MAX now have a bounded streaming
path with compiled row policies, full reference validation, and a bidirectional
target cursor. It removes cache churn, page copies and Arrow conversion for this
shape. Typed WHERE/FILTER comparisons and direct-field GROUP BY now compile too:
selective ranges use validated role/unique indexes, broad scans stream, and group
state has an explicit 32 MiB/query-limit ceiling. Compiled singleton read/join
probes avoid whole-row copies and generic pages. DISTINCT, HAVING, expression
keys, windows and broader analytics retain DataFusion. Paging and reference
validation still have measurable costs. See the
small-read and filtered/grouped follow-up (historical results removed).
See the aggregate follow-up (historical results removed) for the repeated
SQLite/PostgreSQL comparison and reverse-reference control.

[Paged group commit, replication and bounded resident conversion](docs/PAGED-REPLICATION.md)
now work. Remaining paged work includes persistent named-view management, broader
range/order/analytic SQL parity and disk-version/tombstone reclamation. Resident `Database` still needs its current
working set in RAM. Cache budgets are not hard process RSS limits; larger-than-cache
capacity results must not be advertised as zero-cost paging or production readiness.


Broad scans now reuse value-slot arrays, and single-field groups use borrowed hash
lookups plus a bounded numeric cache. Authorization/reference validation and the
32 MiB group-state limit remain intact. Physical cache misses, per-reference row
validation, variable payload allocation and multi-field group keys remain costs.
Read-ahead, threaded prefetch and a redb cursor fork were measured and rejected;
none ships. See the same-runner follow-up (historical results removed).

Fixed-width SQL aggregate sources now use validated compiled offsets and direct
numeric/reference accessors, with the general authorized path retained for
unsupported schemas and row filters. Singleton streaming setup is lazy. Retaining
batches of row guards was tested and rejected after million-record regressions.
The packed-scan follow-up (historical results removed) records modest
broad/grouped median gains, other regressions and an 18% wider-record scanner
improvement; it does not establish universal non-regression or PostgreSQL parity.

Before dense storage, the million-record broad query requested about 168 MiB
through 42,973 backend reads. A dense family-block experiment (historical results removed)
provided evidence for the new production LPG3 family-block layout. The physical
row store now sits below native, SQL, reference and SDK operations; existing
LPG1/LPG2 files remain supported. Automatic legacy conversion and background
sparse-block compaction remain unimplemented. Separate index/identity/version
B-trees and reference visibility work still contribute to scan and storage cost.

The [latest dense comparison](docs/benchmarks/latest/README.md) measures
80.59 MiB through 20,631 backend requests per broad query. Borrowed index
visitation and compiled fixed-key checks reduce indexed aggregate CPU without
changing backend request counts. Broad/full scans still trail PostgreSQL by
9.0%/15.0%; small-group grouping is faster, but filtered and wider grouping remain
slower. Loading issues unchanged backend writes and sync requests, and its
serial transactions cannot demonstrate group-commit amortization. Next work
includes native aggregate pushdown, small-read variance, allocator reservation,
index/tombstone space, reference-validation work and serial write amplification.
Production durable-update amplification and concurrent TPS for LPG3 still need
separate measurements.
