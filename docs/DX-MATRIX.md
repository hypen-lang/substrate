# DX implementation matrix

Planner review, updated through 2026-10-26. The baseline was recorded before implementation; the table below is updated to the verified final state. “Implemented and tested” describes the named subset, not every aspirational API in the planner. See DX.md for the final supported workflow and limitations.

| Feature | Status after this milestone | Scope / next step |
|---|---|---|
| Typed, versioned canonical Rust IR | Implemented and tested | Module, Relation, Role, DataType; JSON is a transport |
| Stable manifest IDs and immutable role slots | Implemented and tested | Lock preserves IDs; incompatible edits rejected |
| Canonical contract fingerprint | Implemented and tested | Versioned SHA-256 manifest/relation hashes |
| Namespaces, versions, imports | Implemented and tested | Exact integer versions; semver ranges deferred |
| Entities, memberships, n-ary relations | Implemented and tested | Unified relation occurrences with stable IDs |
| Cardinality, unique keys, references | Partially implemented | NULL-distinct unique keys, references, authorized cascades and explicitly deferred foreign keys; richer cardinality deferred |
| Independent optional extension properties | Implemented and tested | optionalProperty emits separate unique-owner family; absence means optional |
| Index declarations | Partially implemented | Every role indexed; custom/composite index selection deferred |
| Portable validation constraints | Partially implemented | Scalar domains, unique, references, integer-range and byte-length CHECKs; arbitrary refinements/regex deferred |
| Security declarations | Partially implemented | Schema visibility plus separately persisted scoped grants and ownership; no serialized executable policies |
| Named views and documentation metadata in IR | Partially implemented | Resident durable read-only view definitions; paged/remote view management and declaration documentation metadata remain deferred |
| TypeScript schema compiler and inference | Implemented and tested | Builder emits IR; Rust remains semantic authority |
| Rust explicit builders | Implemented and tested | Module / Relation / Role constructors |
| Rust derive macros | Intentionally deferred | Builders and generated Record suffice for P0/P1 |
| Generated Rust/TS records and typed references | Implemented and tested | Stable manifest-driven types; no global Person union |
| Distinct insert/update/read models | Partially implemented | TS insert/partial update/read models; Rust full records and named patches |
| Generated Serde/wire Rust models | Partially implemented | Typed native Record, shared transport DTOs; no per-record Serde derives |
| Native runtime validators and structured paths | Implemented and tested | Scalar/strict object/patch/read/wire validation; constraints limited to supported IR |
| Standard Schema V1 | Implemented and tested | Structural validator interface; no Effect/Zod dependency |
| Effect/Zod adapters, application refinements | Intentionally deferred | Standard Schema first |
| Exact scalar codecs | Implemented and tested | i64 bigint/string, fixed-scale decimal, UTC microseconds, byte arrays, explicit NULL and finite Float |
| Custom codecs / server-generated fields | Intentionally deferred | IDs generated; no defaults/computed properties |
| Typed insert/read/update/delete and relationships | Implemented and tested | Rust records, Node descriptors, partial TS patches, batch reference aliases |
| Nested relationship loading | Implemented and tested | One snapshot/request; batched branches, per-parent limits, reference closure |
| findUnique/upsert/count helpers | Intentionally deferred | Bounded pages and raw SQL available |
| Typed relational query builder | Partially implemented | TS projection/inner joins, predicates/order/limit/explain; Rust ReadPlan, no fluent Rust builder |
| Parameterized SQL, joins, aggregates, explain | Implemented and tested | Authorized DataFusion reads; embedded DML and bounded remote INSERT/UPDATE/DELETE |
| Runtime SQL result contracts | Implemented and tested | queryChecked accepts Standard Schema validator; duplicate aliases rejected |
| Query tracing, N+1 warnings | Partially implemented | Nested statement/fetch/output counters and typed EXPLAIN; no automatic warnings |
| Static selected-module contracts | Implemented and tested | Exact roots plus transitive imports/prior revisions; unrelated modules excluded |
| Authorized runtime introspection | Implemented and tested | Paged catalog, stable IDs/domains/constraints/flags, opaque hidden targets; no imports/policy values |
| Runtime descriptor operations | Implemented and tested | Rust and TS descriptors, runtime field/scalar validation, named CRUD and nested reads; dynamic names lack static inference |
| Agent schema proposals/install/lifecycle | Implemented bounded subset | Host-provisioned ownership/quotas, concrete plans, host or automatic approval, atomic online apply, aliases and monotonic lifecycle metadata; no hostile public service |
| Generation from installed catalog | Implemented and tested | TS and Rust round trips, actual process example uses regenerated client |
| Deterministic/offline code generation | Implemented and tested | Stable output; module artifacts/watch mode deferred |
| Incremental artifact writes | Implemented and tested | Unchanged outputs retain mtime; replacements written atomically |
| Compatibility/diff and transactional install | Implemented additive subset | Exact approved plans, current-policy revalidation, atomic schema/grants/receipt, concurrent install conflicts; nonadditive evolution deferred |
| Schema mismatch detection | Implemented and tested | Generated IDs and relation contract hashes checked against installed definitions |
| Error codes, phases, retryability | Partially implemented | Server categories; field paths and richer metadata pending |
| Rust transaction retries | Implemented and tested | Conflict-only, fresh snapshots, explicit budget |
| Remote transactions | Partially implemented | Atomic mutation batches with server conflict retries |
| Multi-process access | Implemented and tested | Unix host, independent Node apps, concurrent Rust clients, crash/reopen |
| Authorization and capability isolation | Implemented and tested | Default deny, row policies, reference closure, principal-bound tokens |
| Capability attenuation / delegation | Implemented bounded subset | Principal-bound chains, relation/row/namespace scopes, depth/ledger bounds, durable cascading revocation |
| Live revocation | Implemented and tested | Entry/result/commit gates, retained-handle invalidation, cooperative SQL cancellation; no recall of returned copies |
| Remote TLS / credential rotation / expiry | Implemented and tested | TLS 1.3, verified CA/name, durable scope-bound credentials, atomic rotation, entry/result/commit deadlines |
| Effect integration | Implemented and tested | Effect 3 service/layers, typed failures, scoped lifetime, real host example; no implicit mutation retries |
| Clean generated names | Implemented and tested | Unambiguous short exports plus qualified compatibility aliases |
| Bounded query output and pagination | Implemented and tested | Native indexed pages and SQL budgets; no persistent remote snapshot handles |
| Large-schema TS performance | Implemented and tested | 501 relations / 11 modules, cold/incremental compiler, memory/source size and regression ceilings |
| Editor responsiveness / IDE integration | Intentionally deferred | Compiler timings are not an editor measurement |
| Onboarding init / sample project generator | Intentionally deferred | Runnable checked-in example first |
| Subscriptions, typed materialized views, packages, visualizer | Intentionally deferred | P3 |
| Replication, consensus and sharding | Implemented bounded subsets | Paged/resident trusted-host followers, optional static OpenRaft cluster, fixed local routing; production authenticated peer serving, snapshot transfer/log compaction, membership changes and distributed transactions remain |
| Parallel durable writers | Not implemented | redb keeps one active write transaction; independent transactions can share a durable group |
| Bend executable reference model | Intentionally deferred | Rust property tests remain current oracle |

Delivered: TS declaration → Rust compiler → transactional install → catalog export → generated validators/client → typed reads/writes/SQL; imported application schema and Rust authoring pass the same round trip. Preserve the existing storage and compiler.

The [scoped schema workflow](SCHEMA-CONTROL.md) now connects discovery, proposals,
approval, online apply, runtime operations and bounded lifecycle/quotas. Host
administration remains trusted. [Scoped delegation and live revocation](SECURITY.md)
now work, together with [TLS authentication, rotation and expiry](REMOTE-SECURITY.md).
Next security work includes enrollment, auditability, admission limits and stronger
resource isolation. Metadata
discovery itself does not grant installation or data access.
