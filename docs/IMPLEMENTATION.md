# Implementation report — 2026-10-25

The initial engine, checkpointed storage, SQL + DX vertical slice, typed queries,
batched nested reads, authorized discovery and scoped online schema control are
implemented and verified. TLS remote access, atomic durable credential rotation,
capability expiry, cleaner generated names and an Effect 3 integration are now included;
see [remote security](REMOTE-SECURITY.md), [Effect](EFFECT.md) and the
[current performance analysis](PERFORMANCE-ANALYSIS.md). Scoped delegation and live revocation are included;
see [security semantics](SECURITY.md). See [DX guide](DX.md) and [feature matrix](DX-MATRIX.md) for the new API. The starting
repository contained only an empty README. The supplied master architecture and
security documents informed the implementation; their long-term feature list is
not presented as completed work.

## What changed

| Module | Responsibility |
| --- | --- |
| `src/model.rs` | UUIDs, stable relation IDs, typed values/roles, schema modules, grants |
| `src/storage.rs` | Persistent working trees, role/unique/reverse indexes, recovery validation |
| `src/compiled.rs`, `src/wire.rs` | Compiled role/constraint/policy metadata, frozen v1 metadata envelope |
| `src/durable.rs` | Atomic checkpoints, bounded redo, durable head and recovery counters |
| `src/codec.rs` | Versioned, schema-directed packed durable row encoding |
| `src/engine.rs` | Sessions, snapshots, atomic schema/data/policy changes, dependency validation |
| `src/query.rs` | Native bag selection, projection, equijoin and distinct |
| `src/sql.rs` | Snapshot virtual tables, native transactional SQL DML, paging and query budgets |
| `src/manifest.rs`, `src/codegen.rs`, `src/dx.rs` | Stable contracts, code generation, typed/named writes, conflict retries |
| `src/nested.rs`, `src/catalog.rs` | Bounded nested execution, authorized paged schema discovery |
| `src/schema_control.rs`, `src/runtime.rs` | Scoped plans/approval/online install, quotas/lifecycle, runtime descriptors |
| `src/capability.rs` | Principal-bound attenuation, compiled scopes, root invalidation and cascading revocation |
| `src/auth.rs`, `src/remote.rs` | Compiled durable credentials, atomic rotation, expiry gates, verified TLS host/client |
| `src/service.rs`, `src/local.rs` | Principal-bound protocol, atomic batches, private Unix host/client |
| `sdk/typescript/` | Schema authoring, strict validators, Standard Schema V1, Node client |
| `src/demo.rs`, `src/main.rs` | Cross-app demonstration and local development CLI |
| `examples/schemas/` | JSON core, Fitness and CRM declarations |
| `tests/` | Kernel, CLI, concurrency, property/reference, recovery and SQL/security checks |
| `benches/sqlite.rs` | Equivalent indexed SQLite queries and reproducible warm-cache measurements |
| `benches/recovery.rs` | Same-state full replay versus checkpoint and checkpoint-plus-tail |

The crate compiles with the checked-in lockfile. Build and verification used Rust
1.94.0 on arm64 macOS 26.2, with dependencies available offline.

## Demonstrated behavior

The demo creates Alice as `core.Person`, retains a baseline snapshot, and installs
Fitness and CRM through separate application sessions with namespace grants. Both
apps reference Alice's existing UUID. Installation preserves old query results,
schema definitions and row allocations; a unit test additionally checks that old
physical row and definition Arcs are shared, not reconstructed.

The first cross-app SQL result is:

```text
name  | durationMinutes | amount (minor units, scale 2)
Alice | 45              | 125050
```

It comes from Person → Performed → Workout joined to Person → CustomerDeal → Deal.
Native relational operators produce the same logical result. The SQL adapter serves
logical tables from Liquid's immutable snapshot, and EXPLAIN confirms equality
lookup uses the role index. SQLite does not own canonical Liquid data.

The demo also demonstrates an indexed person-to-workout lookup, denial of Fitness
access to CRM data, rollback of a whole transaction after an invalid reference,
independent app writes from one starting snapshot, retained snapshot consistency,
index verification, statistics, and an equivalent ordinary SQLite query. Later demo
writes intentionally add a second occurrence of the same customer/deal tuple; this
preserves distinct occurrence IDs and demonstrates bag semantics. The original
retained snapshot still returns the earlier single occurrence.

Run `cargo run -- demo`, or use a fresh persistent path. The CLI integration tests
run the durable demo, reopen it, inspect it, query as Fitness, and confirm CRM denial.
They also verify that check/diff do not install while install does.

## Verification executed

- `cargo test --offline` and final targeted reruns: **132 Rust test functions plus 1 README
  doctest passed** (includes the subprocess test entry point).
- Three original reference/property suites each run 64 cases: operation histories against an
  independent map/enumeration oracle, authorized-input aggregate comparisons, and
  durable histories mixing checkpoints/restarts, mutations, schema additions and
  grants/revocations. Another 512 cases exercise arbitrary and mutated log bytes.
  Original subtotal: 704 generated cases; the two DX suites below add 192, for 896 before this milestone; nested/catalog suites add 192, totaling 1,088.
- Concurrency cases cover same-person independent threads, distinct occurrences in
  one family, lost-update rejection, unique-key races, empty-predicate phantoms,
  write skew, delete/reference races in both commit orders, catalog conflicts,
  schema/data ID collisions, and missing-schema predicate reads.
- Recovery cases reopen the durable catalog/data/grants, rebuild and check indexes,
  reject another writer on the same file, and recover after a child exits without
  running destructors. Uncommitted child writes remain absent.
- 35 additional subprocess fault schedules exercise checkpoint boundaries (first,
  replacement, automatic), append publication, interrupted writes/syncs, torn writes,
  and injected I/O errors. A successful commit acknowledgement requires the new
  revision to survive. Otherwise recovery may choose either complete state, never
  mixed schema/data/policy state. Three in-process error scenarios verify fail-stop
  handles. This is not exhaustive hardware power-loss testing; redb supplies the
  underlying durability protocol.
- Checkpoint tests cover count/byte budgets, oversized commits, tighter budgets on
  reopen, empty/repeated checkpoints, overlapping writes, retained snapshots/OCC,
  retired identities, memberships, revocations, rebuilt indexes, and key order that
  precedes import/reference targets. Corruption tests check missing sections/rows,
  wrong keys/counts, unsupported formats, truncation, final-tail loss and tail gaps.
- Schema compatibility tests load a frozen LQ01 metadata fixture, reject omitted
  durable IDs/invalid constraints/policies, and assert old packed bytes/role IDs
  remain identical after independent extensions.
- SQL/security cases exercise joins, aggregation, row filtering before aggregation,
  reference closure, old/new update checks, reference-only capabilities, revocation
  conflicts, pinned policy snapshots, read-only views, native NULL absence versus SQL
  outer-join NULL, multiplicity, BINARY/NOCASE behavior, parameter coercion fallbacks,
  SQL mutation rejection, and index-plan selection.
- Two additional DX property suites run 96 cases each: SQL mutation histories match
  a separate map oracle after each operation/index rebuild, and paged scans/index
  predicates match full reference enumeration. Subtotal before nested/catalog additions: **896 generated cases**.
- Twelve DX tests cover stable locks, immutable history, typed writes, retries,
  selected-module contracts, same-ID layout mismatch, and Rust generation round trips.
  Eight gateway, three socket, and seven SQL-write tests exercise auth isolation,
  malformed inputs, contract mismatches, value/work/output limits, transactional
  rollback, reference integrity, row policy and write-only grants.
- `npm ci --offline --ignore-scripts` and `npm run test:ts`: **18 tests passed**, plus
  strict TypeScript compilation and negative `@ts-expect-error` assertions. Tests
  cover every scalar codec, strict patches/reads, Standard Schema, SQL result
  validation and the TS-to-install-to-catalog-to-codegen round trip.
- `npm run demo:processes`: independent Fitness/CRM Node processes reference one
  Person; cross-app reads fail; authorized SQL join matches; SIGKILL/reopen preserves
  acknowledged commits and exact 64-bit values. This found a macOS accepted-socket
  nonblocking inheritance issue, fixed and covered by a delayed-frame socket test.
- Packed encoding round trip and every truncation of a mixed-scalar fixture pass.
- `cargo clippy --offline --all-targets -- -D warnings`: clean.
- `cargo fmt --all -- --check`: clean.
- `git diff --check`: clean.
- `cargo run --offline -- demo` (initial milestone) and `cargo run --offline --example dx`: completed with expected SQL/native results.
- `cargo bench --offline --bench sqlite`: completed; [results](benchmarks/README.md).
- `cargo bench --offline --bench recovery`: completed; [results](benchmarks/CHECKPOINTS.md).

Tests establish evidence for this supported subset, not a formal proof or a mature
production reliability claim. The Rust oracle does not share production index or
join algorithms. Bend 2 remains a subsequent independent oracle milestone.

## Decisions and limitations

See [architecture](ARCHITECTURE.md) for exact state, encoding and transaction semantics,
and [ADRs](adr/README.md) for alternatives and reasoning.

The main practical limits are:

1. Current data/indexes still fit in RAM. Startup loads a checkpoint and a bounded
   redo tail. Checkpoints are synchronous full-state copies, not incremental or
   background maintenance. Retired IDs remain allocated. There is no physical
   erasure or historical revision API; file sizes are not directly comparable to
   SQLite's current-state indexed representation.
2. Commit publication and durable appends serialize. Independent writes avoid false
   logical conflicts, but no multi-writer throughput or distributed scaling claim
   follows from that behavior.
3. The local host maps private tokens to principals; Database/admin/raw storage remain
   trusted. Retained restricted snapshots now have live authorization gates.
   TLS and durable scoped credentials now work; general ReBAC, field policies and public-service hardening remain absent.
   Frame/output/VM limits exist; comprehensive CPU/memory policy budgets do not.
4. SQL supports read-only snapshots and explicit embedded transactional DML, with
   one equality index pushed per scan. Snapshot cursors page 64 rows; mutable
   statement candidates are buffered. Projection/limit/range pushdown and columnar
   execution are absent. SQLite's attached-schema limit bounds selected SQL
   namespaces (all readable namespaces for raw SQL). Decimals expose exact minor units, not a native SQL decimal type.
5. Constraints are immediate and restricted to arity, types, references, uniqueness
   and functional properties. Deferred cyclic inserts, arbitrary cardinalities,
   general constraints and automatic cascades are absent.
6. Named SQL views are local to a snapshot, not durable catalog resources. Generated
   SDKs and a local server protocol now work. Semantic version ranges, derive macros,
   rich ORM/traversals, module/principal UUID registries, Bend, replication and sharding
   remain planned. The Node transport is Unix-only; it is not a published SDK package.

## Measurements and remaining work

At 2,000 people / 10,000 occurrences, native indexed lookup measured 3.397 µs versus
SQLite's 2.391 µs; a prepared cross-app point join measured 2.933 µs versus 1.211 µs.
An unprepared Liquid SQL join measured 7.780 µs. These are warm-cache median batch
means with query equality checked, not production latency distributions. Durable
reopen/replay measured about 45 ms for this fixture. Full settings and interpretation
are in the benchmark report; no aspirational performance target is claimed achieved.

Checkpointed persistence is now implemented, with automatic tail limits, explicit
maintenance, strict recovery checks, and crash/I/O fault coverage. For a 1,000-row
current state after 500 update commits, measured median warm open fell from
174.107 ms to 11.135 ms (15.64×); a checkpoint plus ten commits took 16.803 ms.
[The report](benchmarks/CHECKPOINTS.md) includes ranges, maintenance cost, and caveats.

Remaining storage work includes incremental/background maintenance, larger-than-memory
scans, deliberate audit/history retention and analytical providers. Bend 2, broader
capability/security semantics, replication and sharding are subsequent milestones. JSON and Rust builders already enter the same validated semantic model;
new syntax need not become a new storage or authorization engine.


DX performance: a 501-relation/11-module generated TypeScript fixture checked in
528 ms cold and 200 ms incrementally, with 127,519 KiB compiler memory and 384,311
bytes of generated TS. Editor latency and bundle size were not measured. The updated
SQLite comparison measured native lookup at 3.053 µs vs 2.275 µs and prepared point
join at 2.746 µs vs 1.114 µs. These results are not a throughput/scalability claim.
See [DX measurements](benchmarks/DX.md) for methodology and regression ceilings.


## Typed query, nested-read and authorized-discovery milestone

Added typed TypeScript projections/inner joins, parameterized predicates, sorting,
limits, EXPLAIN, and exact scalar result decoding. Nested reads use a Rust ReadPlan,
one data/policy snapshot and one statement per nonempty selected relationship node.
Reference visibility and row policy precede per-parent ranking. Duplicate link
occurrences retain IDs; shared targets are fetched once per branch. Selected SQL
catalogs exclude unrelated readable modules. Packed storage and redo formats are
unchanged.

Authorized catalog discovery returns stable IDs, scalar domains, constraints and
operation flags for granted families, redacts inaccessible reference targets, and
omits imports, hidden siblings and policy values. Legacy capability output now omits
families whose full declaration would reveal a hidden target. At that milestone the new local command
was read-only. Scoped proposal/install and delegation are added in later sections.

Verification at the nested/discovery milestone:

- **132 Rust tests + 1 doctest**, including all previous recovery/socket suites.
- **18 Node tests**, strict TypeScript compilation and negative type assertions.
- **1,088 property cases** per standard run: the previous 896 plus 96 independent
  nested-read enumeration cases and 96 catalog grant/pagination combinations.
- Twelve nested tests cover per-parent limits after policy, reference closure,
  shared targets/multiplicity, empty/denied branches, parameter injection, contracts,
  snapshot consistency, unrelated namespaces, alias collisions and output budgets.
- Nine catalog tests cover hidden metadata, missing/denied equivalence, read/write/
  reference-only grants, all-false grants, revision guards, online additions,
  retained snapshots, namespace permissions, scalar fidelity and random grants.
- Seven query SDK tests and three catalog SDK tests validate compilation/decoding,
  selected field domains, malformed output, request shape and invalid options.
- Updated separate-process example checks introspection, typed five-family joins,
  one-request nested reads, denied cross-app branches, exact i64 values and
  acknowledged results after host crash/reopen.
- Formatting, all-target Clippy and whitespace checks pass.
- Native nested, local socket, large-schema TypeScript and cross-engine benchmarks
  executed. Eight workloads at two scales matched SQLite, PostgreSQL 18.3 and
  Substrate (admin and authorized reader), with raw plans/digests preserved.

For 100 parents, nested loading used three statements and one socket request;
the per-parent baseline used 301. The measured socket medians were 10.485 ms and
1,856.786 ms, strongly influenced by the host's 5 ms idle connection polling. Embedded
medians were 0.965 ms and 0.918 ms: batching is not automatically faster without
transport. At 500 parents the embedded medians were 3.597 ms and 4.689 ms.
See [nested measurements](benchmarks/NESTED.md), [cross-engine results](benchmarks/COMPARISON.md),
[query API](QUERY-DX.md) and [discovery/security contract](CATALOG.md).

The next section completes the following requested work: scoped schema proposals
and plans, online apply protocol, dynamic runtime descriptors and lifecycle/quotas.
The kernel's existing additive transaction semantics do not by themselves make
untrusted self-service safe. Fixed local credentials, no general ReBAC and
same-OS-user filesystem access remain limitations. Live gates and bounded delegation
are implemented in the final section below.


## Scoped schema control and dynamic operations — final verification

Host-provisioned namespace policies authorize bounded additive proposals. Plans
compile through the existing schema validator, check reference permissions and
minimal imports, report quotas/index/packed-event costs, and bind exact definitions
to a policy generation and digest. Trusted host approval or explicit automatic
authorization permits online apply. Schema definitions, grants for only new families,
and the durable Installed receipt commit together. Managed namespaces reject legacy
restricted installs so broad namespace grants cannot bypass the workflow.

Four private typed relation families persist policies, receipts, aliases and lifecycle
through existing packed logging/checkpoints. No log ABI change or application row
backfill is introduced. Lifecycle is monotonic metadata; retirement prevents new
managed references/aliases while preserving existing reads, writes and data. Runtime
Rust and TypeScript descriptors compile authorized discovery into validated named
operations. Dynamic schemas can participate in existing batched nested reads.

Final verification on 2026-10-24:

- **154 Rust tests + 1 doctest passed** (`cargo test --offline`).
- **23 Node tests passed**, including strict TypeScript compilation/type assertions.
- **1,152 property cases** per standard run: prior 1,088 plus 64 generated schema
  install/cancellation histories checked against independent quota/count state.
- Nineteen new integration tests cover approval/receipt semantics, cross-principal
  denial, missing/hidden metadata equivalence, each quota dimension, immutable
  ownership, policy-generation changes, stale plans, concurrent installers/readers,
  dynamic CRUD, aliases/retirement, legacy bypass prevention and enrollment races.
- Three abrupt-process phases verify that uncommitted installation is absent,
  approval survives, and committed schema/grants/receipt recover together. Separate
  checkpoint/reopen coverage includes aliases/lifecycle and runtime rows.
- Three new unit tests verify unchanged old packed bytes/shared row/schema allocations,
  reference-policy revocation conflicts and lifecycle-change conflicts after validation.
- `npm run demo:schema` passed: automatic and manual host approval, runtime writes,
  exact bigint values, batched nested reads, isolation, lifecycle, concurrent current
  readers and checkpoint/reopen. The final run observed 127 successful concurrent reads.
- `npm run demo:processes` passed again, retaining the generated two-app workflow,
  SQL/native query agreement, exact integers and acknowledged writes after crash/reopen.
- All-target Clippy with warnings denied, formatting and whitespace checks pass.

The benchmark subagent implemented and ran fresh durable proposal/approval/apply
at 10 and 2,000 existing people, with identical original records/query digests and
concurrent pinned readers. At 2,000 people the median complete pipeline was
**20.156 ms**; runtime describe was **6.583 µs**. SQLite's additive table/index DDL
was **0.283 ms**, but does not include approval, quotas, grants or receipts. This
is not an equivalent security-workflow comparison. PostgreSQL installation was not
measured; the previous eight-workload three-engine query suite remains available.
See [raw methodology and results](benchmarks/SCHEMA-CONTROL.md).

[SCHEMA-CONTROL.md](SCHEMA-CONTROL.md) documents the supported API and trust boundary.
An operator approval UI, arbitrary ReBAC, broader resource budgets,
append-only approval history, ownership transfer and destructive retirement are
still absent. The implementation enables explicitly provisioned local and remote agents; it
does not claim public hostile-agent self-service is production-ready.


## Scoped capability delegation and live revocation — final state

Host-issued roots are bounded by existing owner grants and namespace policy.
Authenticated principals can delegate strict subsets with bounded depth; explicit
selection replaces ambient authority. Equality filters compile to existing typed
role predicates and apply to old/new writes and reference-closed reads. One optional
namespace permits proposal/receipt/cancel/approved apply, with reference scopes and
host approval still enforced. Newly installed families grant only the namespace
owner; existing capabilities do not gain new relation IDs.

A reserved typed capability family preserves scopes/revocation through the existing
packed log/checkpoints. The engine maintains compiled ID/ancestor/root-by-grant
state, checks live authority before read-result release and under write publication,
and cooperatively interrupts SQL. Grant replacement/removal atomically invalidates
affected roots; regrant does not resurrect descendants. Direct grant additions
preserve existing readers. Old data remains snapshot-consistent; invalidated handles
fail instead of mixing historical data with new row policies. Weak host references
also fail closed once the original host closes.

Native full-schema APIs now deny/omit declarations with hidden reference targets,
matching the complete-descriptor boundary. Delegated receipt reads deny out-of-scope
references and redact fingerprints with hidden target domains. Ordinary catalog
introspection still supports its safe partial representation.

Final verification:

- **178 Rust tests + 1 doctest**, including all recovery/socket regressions.
- **27 TypeScript tests**, strict compilation and existing negative type assertions.
- **1,344 property cases** per standard run: previous 1,152 plus 96 delegated-row
  filter comparisons and 96 randomized revocation trees against an independent
  ancestor model.
- Twenty-one delegation integration tests cover subject binding, ambient-scope
  isolation, non-amplification, old/new row filters, reference closure, compiled
  runtime operations, retained SQL/native/catalog/page handles, scoped revocation,
  no regrant resurrection, namespace policy generations, host-approved installation,
  receipt metadata redaction, exact i64 policy wire values, reserved metadata,
  ledger quotas, checkpoint/tail replay and abrupt-process issuance/revocation.
- Three new unit tests force revocation during SQL execution, revocation between
  schema validation and commit, and root issuance racing direct grant revocation.
- Previous tests that explicitly required retained **policy** access after revoke
  now require Denied; their snapshot **data** consistency assertions remain.
- `npm run demo:security`, `npm run demo:schema` and `npm run demo:processes` pass.
  The security example checks narrowed runtime writes/nested reads, manual approval,
  cascading revocation, staged-write denial and checkpoint/reopen. The earlier
  examples retain additive-schema and cross-app behavior.
- All-target Clippy with warnings denied, formatting and whitespace checks pass.

No new security performance claim is made. Previously recorded benchmark timings
precede these live gates and do not measure their overhead. [SECURITY.md](SECURITY.md)
and [ADR 009](adr/009-delegation-revocation.md) specify linearization precisely:
revocation suppresses results whose final gate has not passed, but cannot recall
returned values or buffers already authorized before the revocation commit.
Remote authentication and credential rotation/expiry are delivered in the next section.
Distributed revocation, general ReBAC, field policies and stronger resource isolation remain future work.

## Authenticated remote access and SDK additions — 2026-10-25

Implemented TLS 1.3 Rust/Node transport with CA and hostname verification, durable
scope-bound credentials, atomic immediate rotation, independent credential
revocation and inherited/shortened capability expiry. Existing native, SQL,
runtime and schema-control gates now include credential row versions and effective
deadlines. Capability format 2 is validated and format 1 remains readable. The
credential registry compiles typed records into ID lookups; secrets are not persisted.

The generated TypeScript API now offers unambiguous short names such as `Person`
while retaining `core_Person` compatibility exports. Effect 3 integration provides
scoped layers, typed failures, typed/nested queries and atomic batches without
implicit retries. Its interruption behavior explicitly does not promise rollback.
README now describes the database, useful application categories and current limits.

Verification: 204 Rust tests and one doctest pass across the full suite and the
additional focused concurrency/schema/deadline tests. The TypeScript suite has 41
passing tests, including ten Effect adapter tests, naming/compiler checks and
expiry-wire checks. All-target Clippy passes with warnings denied. Both
`demo:remote` and `demo:effect` ran successfully against real separate-process
clients/hosts. Rotation tests crash subprocesses immediately before/after durable
publication and checkpoint commit, verifying one active credential and an atomic
old/new state. TLS tests reject wrong CA/name/plaintext and preserve scope after
rotation. Independent review caught and fixed a redundant outer response gate
that could misreport a successfully committed self-revocation as denied.

Two fresh eight-workload SQLite/Substrate comparison runs verified identical
results. The largest measured gap remains range + ordering without index pushdown;
see [performance analysis](PERFORMANCE-ANALYSIS.md) for raw reports, plans and
shared-machine caveats. These runs do not measure credential/TLS/Effect overhead.
Operational security, trustworthy time across restart, larger-than-RAM serving,
range planning, durable views, distributed work and Bend verification remain
outstanding. This milestone does not claim public-service production readiness.


## DataFusion SQL and imbl — 2026-10-25

The pre-change implementation was committed as `abb0a72`. Production persistent
collections now use imbl 7.0.2; the existing native read/write APIs and physical
redb storage remain intact. SQL reads now execute through DataFusion, including
transactional reads, durable invoker views, nested SQL loading and query profiling.
SQLite is retained explicitly for dialect compatibility and SQL mutation translation
into Liquid transactions; queries never silently fall back after a DataFusion error.

The adapter creates an isolated authorized Arrow catalog, retains live authority,
binds typed parameters, and creates fresh physical execution plans for each call.
It checks signed arithmetic and SUM overflow before optimizer folding and after
parameter binding. Resource accounting, output bounds and cooperative deadlines
limit queries; they are not hard process isolation. Transactional reads currently
track all visible families conservatively. See [the SQL guide](DATAFUSION-SQL.md)
for supported types, syntax differences and current limits.

The executable [example](../examples/datafusion_sql.rs) combines a parameterized
relationship join/aggregate, native indexed lookup and live revocation. The
[independent review](reviews/DATAFUSION.md) found integer wrapping and OVERLAY
allocation amplification; both were fixed and re-reviewed. Candidate and final
production measurements are separate: [raw engine comparison](benchmarks/DATAFUSION-EXPERIMENT.md)
and [native adapter benchmark](benchmarks/DATAFUSION-NATIVE.md).


Integrated verification: **341 Rust tests including the doctest** and **50
TypeScript tests** passed. Strict all-target Clippy and formatting checks passed.
The DataFusion example returned the expected joined aggregate, preserved native
lookup behavior, and denied its retained SQL handle immediately after revocation.
The review report records both findings and their fixes. Native benchmark results
and timing boundaries are recorded separately in the linked report.


## Index-backed DataFusion — 2026-10-25

The eager authorized Arrow catalog was replaced with lazy native-index providers.
Identity/equality/range predicates, projection and safe LIMIT pushdown preserve
row policies, transitive reference visibility and pinned data with live authority.
Prepared execution reuses safe planning work for one exact typed binding and
creates fresh execution state. Stable/volatile functions and subqueries bypass
reuse. Unlimited family scans can reuse bounded projected source pages within a
single SQL snapshot; cache hits recharge execution budgets and recheck authority.

The [independent review](reviews/DATAFUSION-INDEXED.md) covered exact predicate
semantics, authority, parameter/cache isolation and error paths. It identified a
one-sided range scan that was fixed by intersecting both bounds. The
[paired benchmark](benchmarks/DATAFUSION-INDEXED.md) compares the eager DataFusion
baseline, SQLite compatibility adapter and direct SQLite, and reports first-use
costs separately. Native query and storage source code is unchanged.

Integrated verification passed **363 Rust tests including the doctest**, **50
TypeScript tests**, and strict all-target Clippy. The executable example verifies
an identity-index plan, repeated prepared reads, the joined aggregate, native
lookup and immediate denial after revocation. Regression coverage includes
typed/coercion differential queries, exact index candidate counts, bounded cache
eviction, concurrent cache fills, and revocation/expiry between cached pages.

All twelve paired benchmark processes passed exact result and repeated-query
checks. At 180k occurrences, warm point reads improved 117 → 15.6 µs and the
selective five-family join 1.96 → 0.84 ms. Range/top-k, correlated EXISTS and
windows regressed; first-use conversion and changed-binding planning remain
material costs. The report includes these regressions, both SQLite comparisons,
and separate catalog/preparation/fresh-handle timings.

## Range, EXISTS and window follow-up — 2026-10-25

Unlimited numeric-range scans now reuse bounded authorized projected pages.
Keys include typed predicates, bounds, projection and continuation; hits charge
the original candidate-row count even when other predicates discarded rows.
Reads with a pushed LIMIT remain uncached. Deterministic EXISTS, IN and scalar
subqueries can reuse optimized logical plans after recursive inspection of raw
subquery/view expressions. Stable/volatile functions still prevent reuse, and
joins/sorts receive fresh physical plans.

All **370 Rust tests including the doctest** passed, together with strict
all-target Clippy. One manual optimized performance probe is intentionally
ignored by the regular suite. The [independent review](reviews/DATAFUSION-RANGE-EXISTS.md)
identified an increased cache-key bookkeeping cost; explicit inline sizes and
dynamic allocations now contribute to the estimate. No actionable finding
remains. Native query and storage files are unchanged.

The [paired comparison](benchmarks/DATAFUSION-RANGE-EXISTS.md) measures both earlier
DataFusion adapters, explicit SQLite compatibility and direct indexed SQLite,
including changed parameters and first-use costs.

All eighteen measured processes passed ordered result and repeated-query checks.
At 180k occurrences, warm range/EXISTS/window medians improved from the first
indexed adapter's 2.569/3.423/23.780 ms to 0.195/1.384/10.892 ms. Fresh eager-adapter
medians were 0.462/1.506/11.550 ms. Smaller margins against eager DataFusion vary
between runs; this is a shared-machine benchmark. Point/selective paths did not
improve, and cold conversion remains significant. The separately executed manual
release probe also passed and records the executor round-trip cost without
claiming additive component profiling.
