# First vertical slice — completed

- [x] Inspect repository and supplied specifications (repository initially empty).
- [x] Record storage, identity/schema, SQL and authorization decisions.
- [x] Implement typed families, explicit memberships and additive schema imports.
- [x] Implement durable atomic commits, snapshots and fine-grained dependencies.
- [x] Add native algebra, read-only SQL virtual tables and equality pushdown.
- [x] Demonstrate Person → Fitness + CRM with isolated application permissions.
- [x] Run reference/property, concurrency, recovery, SQL/security and CLI tests.
- [x] Benchmark equivalent SQLite workloads and publish factual results/limitations.

# Checkpoint and compiled-schema milestone — completed

- [x] Persist atomic current-state checkpoints and prune covered redo.
- [x] Bound automatic replay by commit count and encoded bytes.
- [x] Keep live snapshots and conflict dependencies intact across maintenance.
- [x] Validate legacy events, checkpoint manifests, rows and redo sequence/head.
- [x] Exercise process crashes, torn writes, I/O failures and logical corruption.
- [x] Compare randomized restart/checkpoint histories with independent reference state.
- [x] Compile role/unique-key and policy metadata; expose scoped stable role IDs.
- [x] Benchmark full replay versus same-state checkpoints and short redo tails.

Checkpoint milestone verification: 79 test functions plus 1 README doctest; 704 generated property cases.
See docs/IMPLEMENTATION.md, docs/adr/005-checkpoints.md and docs/benchmarks/CHECKPOINTS.md.

Remaining: incremental/background checkpoints, larger-than-memory serving, explicit
history retention, Bend 2, advanced capabilities, analytical
execution and distributed infrastructure.


## SQL + DX milestone

- [x] Audit planner requirements in docs/DX-MATRIX.md before implementation.
- [x] Stable manifests, conservative lock history, exact imports and contract hashes.
- [x] TypeScript schema authoring and Rust builder/catalog generation round trips.
- [x] Generated Rust/TS records, typed identities and strict runtime validators.
- [x] Standard Schema V1, exact scalar codecs and explicit SQL result contracts.
- [x] Named native writes, partial TS patches and conflict-only transaction retries.
- [x] Embedded SQL DML via native transactions; read-only remote SQL.
- [x] Unix host, principal credentials, atomic batches, bounded indexed pages.
- [x] Separate-process two-app demonstration, isolation and crash/reopen verification.
- [x] 501-relation TypeScript performance fixture with regression ceilings.
- [x] SQL mutation/reference and pagination property tests; security/transport tests.

Remaining scope at that milestone was recorded in docs/DX-MATRIX.md: rich query builder/nested reads,
Rust derives, richer constraints/evolution, capability delegation, remote service
hardening, large working sets, persistent views, distributed storage and Bend.
redb still serializes durable writes; this milestone makes no parallel-writer claim.

Final SQL + DX verification: 111 Rust tests, 1 doctest, 8 TypeScript tests, 896 generated property cases; formatting and Clippy clean.


## Typed queries, nested loading and authorized discovery — completed

- [x] Typed TS projections, inner joins, predicates, order/limit and EXPLAIN.
- [x] One-request/snapshot nested reads, per-parent limits and batched target IDs.
- [x] Authorization before filtering/ranking, exact reference domains and contracts.
- [x] Output/work budgets and explicit tree/occurrence semantics.
- [x] Process example, compile-time negative tests and reference property tests.
- [x] Native, socket and large-schema compiler benchmarks.
- [x] Authorized runtime schema catalog with pagination and hidden-target redaction.
- [x] Cross-engine benchmark subagent: eight workloads, SQLite/PostgreSQL/Substrate,
      two scales, exact output verification, plans/digests and timing caveats.

Priorities recorded at that milestone: restricted schema proposal/planning; approved online install and
conflict protocol; runtime descriptor operations; lifecycle/ownership/quotas; further
query conveniences. Security prerequisites remain explicit in docs/CATALOG.md.

Final combined verification: 132 Rust tests + 1 doctest, 18 TypeScript tests,
1,088 generated property cases. See docs/IMPLEMENTATION.md for evidence and limits.


## Scoped online schema control and runtime operations — completed

- [x] Host-provisioned namespace ownership, policy generations and bounded quotas.
- [x] Exact additive plans with reference authorization, costs and zero backfills.
- [x] Manual host approval or explicit bounded automatic approval.
- [x] Atomic online schema/grants/receipt install and stable retry reconciliation.
- [x] Concurrent installer conflicts, policy revocation/enrollment races and readers.
- [x] Rust/TypeScript runtime descriptors, named CRUD and batched nested reads.
- [x] Immutable aliases, advisory deprecation and non-destructive retirement metadata.
- [x] Private typed control records, checkpoint/replay and abrupt-process recovery.
- [x] Working separate-process example with both approval paths and restart.
- [x] Benchmark subagent measured fresh durable installs against SQLite additive DDL.

Final verification: 154 Rust tests + 1 doctest, 23 TypeScript tests, 1,152 generated
property cases; both process examples, formatting and Clippy pass. See
[workflow](docs/SCHEMA-CONTROL.md) and [measurements](docs/benchmarks/SCHEMA-CONTROL.md).

Remaining at the schema-control milestone: delegation, remote authenticated transport,
strict live revocation,
CPU/memory/data-volume quotas, operator approval UI, append-only approval audit,
ownership transfer, destructive retirement/erasure, nonadditive evolution and the
previously recorded larger-than-memory/distributed/Bend work. Local scoped schema
control does not claim to provide hostile-agent isolation.


## Scoped capability delegation and live revocation — completed

- [x] Host-issued roots limited to existing owner relation and namespace authority.
- [x] Principal-bound child chains with explicit selection and strict attenuation.
- [x] Typed equality policies, exact scalar wire values and compiled/indexed scopes.
- [x] Durable cascading revocation; grant replacement/regrant cannot revive roots.
- [x] Live entry/result/commit gates and cooperative SQL interruption.
- [x] Runtime, SQL, catalog, nested and schema-control authorization preserved.
- [x] Scoped proposal receipt disclosure, namespace generation checks and owner-only new grants.
- [x] Bounded depth and lifetime ledger, reserved metadata and trusted host-only root issuance.
- [x] Deterministic races, independent property models, crash/checkpoint recovery.
- [x] Separate-process Rust/TypeScript example and both earlier process examples.

Verification: 178 Rust tests + 1 doctest, 27 TypeScript tests, 1,344 property cases;
formatting and Clippy clean. See [security API/semantics](docs/SECURITY.md).

Next security work: remote authentication and credential lifecycle, expiry,
stronger OS/resource isolation, generalized ReBAC/field policies, administrative
approval/audit tooling and distributed revocation semantics. Existing data/storage,
nonadditive evolution and Bend milestones remain as previously recorded. Returned
copies and buffers authorized before a revocation commit cannot be recalled.
