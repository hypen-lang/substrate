# ADR 005 — Atomic checkpoints, bounded redo, and compiled metadata

Status: implemented. Extends ADR 001's initial log-only storage.

## Recovery contract

`Database::open` uses a default redo budget of 128 commits or 8 MiB of encoded
records. The commit that would reach either budget goes directly into a new
checkpoint, atomically with prior state. Thus the committed tail contains fewer
than 128 records and fewer than 8 MiB; a single oversized transaction does not
create an oversized committed tail. `open_with_options` accepts different positive
budgets; `None` deliberately disables automatic checkpointing. `checkpoint()`
explicitly compacts the current state without changing its logical revision.

An old unbounded file or a tighter budget requires reading the existing history
once before checkpointing it. `RecoveryInfo` reports that actual work, while
`StorageInfo` describes the resulting persisted state. Do not claim a hard startup
latency bound: loading the checkpoint, allocated identities, schemas, policies, and
rebuilding all indexes still costs work proportional to those structures. Allocated
identities include retired IDs and are monotone even when live row count shrinks.

## Atomic publication

Three redb tables are used:

- `liquid_log_v1`: revision -> versioned `LQ01` transaction record.
- `liquid_control_v1`: format `LDB1`, durable head, checkpoint manifest, catalog.
- `liquid_checkpoint_rows_v1`: 32-byte (type UUID, occurrence UUID) -> one packed
  row record at the checkpoint revision.

A checkpoint's catalog records all installed module versions, allocated identities,
current grants, and schema capabilities. All definitions load before rows so that
sorted checkpoint order need not match referential insertion order. Compiled schema
and policy metadata, indexes, and conflict epochs are derived, never serialized
caches. Final validation requires all references/imports to resolve. Redo operations
preserve immediate referential ordering as well as type and uniqueness constraints.

The `LCP1` manifest contains seven little-endian u64 fields: revision, row count,
identity count, module count, grant count, schema-grant count, and payload bytes.
Payload bytes count the catalog and row values, excluding keys/manifest/redb overhead.
The durable head is an eight-byte little-endian revision. The checkpoint is replaced,
its covered log deleted, and the durable head published in **one** redb transaction
with Immediate durability. No independently renamed files or two-phase prune window.

A normal append stores its record and head in one equivalent transaction. On open,
all tail revisions must be consecutive and end exactly at the durable head. This
also detects a missing *last* record. Missing sections, wrong counts/lengths, row-key
mismatches, malformed typed events, and unsupported formats fail closed. Page integrity
and crash atomicity rely on redb's checksums and commit protocol; Liquid's envelope
is not an authentication mechanism. Raw database files remain privileged storage.

An I/O error can leave an uncertain commit outcome. The engine keeps its previously
published in-memory snapshot and refuses further commits/checkpoints until reopening.
Callers must reconcile outcome before retrying non-idempotent operations. Old snapshots
remain usable as old snapshots, not evidence of the latest disk outcome.

## Compatibility and declarations

Original log-only files are accepted, validated, and upgraded with format/head
metadata. Existing `LQ01` bytes remain readable. Older binaries do not understand the
checkpoint tables and must not be used on upgraded files; downgrade is unsupported.

The frozen v1 metadata envelope in `wire.rs` converts to typed semantic operations.
Stored relation IDs are required even though authoring JSON can allocate an omitted
ID. A golden legacy schema fixture asserts identical decoding/encoding, and invalid
schema/policy events are rejected. Future incompatible changes require a new decoder
version and explicit conversion; no future format is implied supported today.

`Module`, `Relation`, `Role`, `DataType`, and `Grant` are Rust declarations independent
of JSON. Install validates them and compiles role-name maps, unique-key slots, and
row-policy slots. Runtime policies are indexed by (principal, relation UUID). The
policy language consists only of typed All/Equals predicates; no JSON expression is
executed. Rust builders already enter the same path. A DSL, TypeScript definitions,
and generated clients will target this declaration model, not the durable wire DTO.

Type and relation identities are UUIDs. Role/packed-attribute identity is explicitly
`RoleId { relation: UUID, slot: u32 }`. Slots and existing layouts are immutable;
names do not form those IDs. Omitted declaration UUIDs are provisional until persisted;
a reproducible schema package should carry explicit UUIDs. Independently renamed or
reordered roles, alias changes, and replacement layouts are not supported. A new
optional attribute or relationship uses its own independent relation family. Tests
assert old packed row bytes and role IDs remain identical after such extensions.

## Costs and retained history

Checkpoints serialize under the publication mutex and copy the entire current state.
Retained snapshots and active transaction dependencies remain intact; concurrent
transactions may prepare while publication waits. Checkpoint latency is a foreground
commit cost when automatically triggered. This is a deliberate first implementation,
not incremental/background checkpointing or larger-than-memory serving.

Schema installation requires no change to old row layouts, data values, or indexes.
It can coincide with periodic checkpoint maintenance that copies unchanged packed
rows; that maintenance is not a schema backfill. A layout change under an existing
UUID is prohibited, so packed decoding never needs to guess a schema revision.

Covered redo is discarded; there is no historical time-travel/audit-log retention
API. Catalog versions and allocated identities remain. Pruning permits page reuse,
but neither guarantees smaller filesystem files nor securely erases old bytes.
Live state/indexes still fit in RAM. Future work includes incremental checkpoints,
on-disk scans, explicit retained-history policies, and background scheduling.

## Verification

Subprocess tests exit without destructors at eight checkpoint boundaries, each for
first/replacement/automatic checkpoints, plus append publication boundaries. Custom
redb backends interrupt writes/syncs, write partial buffers, and inject errors.
Every outcome must recover one complete state, including schemas/grants/data/indexes.
These are controlled process/I/O faults, not exhaustive hardware power-loss simulation.

Generated histories mix inserts, updates, deletes, schema additions, grants/revocations,
manual/automatic checkpoints and restarts against a plain map oracle. Corruption tests
remove or mutate checkpoint sections and tail records. See the checkpoint benchmark
report for reproducible before/after opening times on the same logical state.
