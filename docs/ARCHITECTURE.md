# Liquid SQL: implemented semantics

## Logical state

At revision t, L_t = (U, S_t, X_t, V_t, A_t). U is the open identity namespace;
the stored allocation set is finite and monotonically grows. S is an immutable,
versioned catalog. X maps type IDs to identity-bearing tuples. A contains protected
capabilities. The current V implementation is snapshot-local SQL views; persistent
view definitions remain future work.

Schema objects and typed role maps follow the category/instance direction of
[Spivak](https://arxiv.org/abs/1009.1166). Scalar roles and graph-like structures
also have precedent in [attributed C-sets](https://arxiv.org/abs/2106.04703).
This implementation supports a restricted typed relational signature, not a
category-theory proof assistant or general pushout algorithm.

A family R has a stable UUID and an ordered list of required roles. Its instance
X(R) is a set of occurrence identities, with each role a total typed function on
that set. An occurrence can itself be a reference target. Rows with distinct IDs
may have identical role values. Native projection and SQL retain multiplicity;
`distinct` explicitly removes duplicate projected tuples.

Identity and membership are separate: the same UUID can be a key in several
explicitly declared families through `add_membership`. This does not equate the
meanings of their types. References validate a specific (type ID, identity) pair.
Deleting a membership leaves its identity allocated and other memberships intact.
No name-based or value-based entity merging occurs.

A zero-role family is a pure membership relation. Dense entity scalar properties
are packed into that family's row. Optional and multivalued extensions use new
families, such as `TrainingGoal(person, value)` with `unique(person)`. Absence is
no occurrence, never an implicit NULL or a security redaction.

## Conservative schema extensions

For a schema-only installation of A into D, restriction to the old signature
satisfies restrict_S(extend(D,A)) = D. Schema installation only adds catalog entries and
empty families; its logical operation does not iterate over old rows. A commit can
trigger checkpoint maintenance of earlier changes or backend page rewrites. This
requires no schema backfill or layout alteration. A later data transaction may
intentionally change old query results.

The installer checks namespace ownership, contiguous positive module versions,
exact installed imports, target type visibility within those imports, reference
capabilities, unique definition IDs, case-insensitive name collisions, role types,
and uniqueness declarations. Existing definitions and role positions cannot be
changed. Imports of version n expose earlier definitions from the same namespace,
not definitions first introduced at n+1. New constraints belong only to new families.
Required fields on old rows and existing-family index backfills are not supported.
All defined roles have an immediately ready equality index.

An app needs a privileged `SCHEMA_EXTEND`-equivalent namespace grant to install.
It automatically receives read/write/reference grants only for its newly introduced
families. Imports do not grant access to data. Metadata and physical statistics are
restricted: app schema enumeration only lists relations it can read; global stats
and integrity verification require the administrative session.

Types/relations carry UUIDs; role and packed-attribute IDs are `(relation UUID, slot)`,
exposed by `Relation::role_id`. Role-name maps and unique-key slot vectors are compiled
once per definition. The immutable relation UUID identifies its single physical layout.
JSON and Rust builders enter the same typed validation path; future DSL/TypeScript
frontends will target that model.

Names are ASCII identifier components, qualified by module, and immutable aliases
for schema UUIDs. SQL resolves case-insensitively; the native API resolves exact
names. Case-only collisions are rejected to prevent divergent SQL interpretations.
`id` is reserved for occurrence identity. `SELECT *` over an existing family keeps
its shape across additive installations because old role lists cannot change.

## Physical state and indexes

The kernel is separated into model, compiled metadata, working storage, durable
checkpoints, transaction, native query, SQL, wire-envelope and codec modules within one small crate. No speculative distributed-backend trait is
required to use it. Private storage operations accept only validated logical deltas.

The working state uses persistent ordered B-trees (`im::OrdMap`):

- type ID → family → identity → shared schema-ordered row;
- (type ID, role position, typed value) → ordered occurrence IDs;
- (type ID, unique-key ordinal, typed tuple) → occurrence ID;
- (target type ID, target identity) → incoming (source type, identity, role) entries;
- typed conflict domains → last mutation revision, including deleted/empty key
  tombstones. Row/incoming timestamps are grouped by relation; bucket/unique
  timestamps by relation and role. Scalar catalog/visibility timestamps remain
  separate. This avoids repeated IDs and large enum-key padding without dropping
  historical conflicts or changing transaction semantics.

Rows are one ordered tuple, not one EAV allocation per scalar. The durable codec
packs role values without repeated attribute names or per-value type tags. Sparse
properties use their own optional relation occurrences. Specializing in-memory
Value enums into columnar or unboxed buffers is still an optimization opportunity.

Reference indexes change when edges change, but attaching an edge does not update
the referenced person's row version. Incoming-index reads are needed for deletion,
not ordinary inserts. Shared target reads therefore do not conflict with each other.

The durable backend stores revision → redo record in a redb table named
`liquid_log_v1`. redb performs the atomic durable commit. The engine publishes its
new in-memory state only after that commit succeeds. Recovery loads the latest
checkpoint, replays its consecutive bounded tail, and rebuilds correctness-critical
indexes and compiled metadata.
A storage commit failure can be ambiguous; the handle refuses later commits and
must be reopened before retrying. Do not blindly retry non-idempotent operations.

Atomic checkpoints persist the current catalog, allocated identities, policies, and
packed rows, and discard the covered redo. The default policy keeps the tail below
128 commits and 8 MiB, including when a single transaction exceeds that size. The
first upgrade of an unbounded file replays once before compaction. Startup work still
includes current state/index reconstruction, and retired allocated identities remain.
Incremental checkpoints process coalesced changed rows and metadata. V3 retains
up to 4,096 materialized row overrides or 8 MiB, flushing sorted subsets into the
base before exceeding either limit; initial and larger batches write directly to
the base. The overlay contains latest
row images/tombstones, not transaction history. Compaction and redo pruning share
the atomic durable commit. The published snapshot remains readable during disk I/O.
An optional background worker requests early checkpoints; hard recovery bounds
still apply on the commit path. File shrinkage and secure erasure are not promised.

The full current state and indexes must fit in memory. Old snapshots retain shared
trees independently of checkpointing. Historical revision lookup is not implemented.
Durable files are privileged engine storage; redb protects their page integrity.
See [ADR 005](adr/005-checkpoints.md) and [ADR 011](adr/011-bounded-checkpoint-overlay.md)
for tables, manifests, atomic publication, recovery validation, fail-stop errors,
compatibility, and retention semantics. V1/V2 files remain readable; the next
checkpoint upgrades atomically to V3, which older binaries reject.

The physical backend cache is explicit: `StorageOptions` defaults to a 64 MiB
redb cache target, configured through `Database::open_with_storage_options` when
needed. This does not bound the resident logical state or process RSS. Checkpoint
upserts account for the prior logical row, whether stored in the base or overlay.
Rows, catalog, manifest, head and pruning still
commit atomically. Process-local persistence counters distinguish redo, checkpoint,
compaction and Immediate-commit work; actual backend-write probes measure page
amplification separately from encoded record sizes.

### Version-1 redo format

All integers in the redo encoding are little endian. A commit has magic `LQ01`, a
u64 revision, a u32 operation count, and ordered operations:

| Tag | Payload |
| --- | --- |
| 0 | u32 byte length + UTF-8 JSON for Install/Grant/SchemaGrant metadata |
| 1 | 16-byte type UUID + 16-byte occurrence UUID + schema-directed values |
| 2 | 16-byte newly allocated identity |
| 3 | 16-byte type UUID + 16-byte retracted occurrence UUID |

Bool is one byte (0/1). Integers, decimal minor units and timestamps are signed
64-bit values. References use 16 UUID bytes. Text and bytes are u32 length-prefixed.
Text must decode as UTF-8. Unknown magic, revision mismatch, truncation, invalid bool,
invalid UTF-8, missing row schemas, and trailing bytes are rejected. The backend's
checksums cover page corruption; the Liquid header is not an authentication scheme.
Metadata uses a frozen v1 envelope, converted to validated semantic operations.
Unlike authoring JSON, stored schema IDs are mandatory. A golden old-format fixture
tests compatibility. New incompatible formats require an explicit decoder/version migration, not silently
reinterpreting old bytes. Tests cover a round trip and every truncation of a fixture.

### Scalar semantics

Native equality is typed: `Int(1)` is different from `Decimal(1)`. UTF-8 text compares
lexicographically with no locale collation. Byte strings compare lexicographically.
Decimals are i64 minor units with fixed role scale 0..18; differing scales are
separate types, and no arbitrary precision arithmetic is provided. Timestamps are
UTC microseconds since Unix epoch, excluding leap seconds. UUIDs compare by bytes.

SQL IDs are canonical lowercase UUID strings; bool/int/timestamp/decimal are INTEGER,
text is TEXT, and bytes are BLOB. Decimal arithmetic in SQL therefore operates in
minor units; division by 100.0 can introduce floating point and is not an exact
money-arithmetic API. Outer joins and SQL expressions use SQLite NULL/three-valued
logic. No stored native role can be NULL. SQL coercion is deliberately distinct from
native typed equality.

## Transactions and isolation

A transaction captures a consistent state root (data, schemas, grants, indexes),
accumulates private edits, and records dependencies against the original snapshot.
It reads its own edits. Any failed mutation poisons the whole transaction; commit
returns Aborted and publishes nothing. Read errors do not poison the transaction.
Dropping a transaction discards all staged operations.

Commit takes a publication mutex, verifies dependency revisions against the current
head, and applies the validated operation list to that head, preserving unrelated
commits. Every changed occurrence has a write dependency. Reads track:

- occurrence keys, including nonexistent identities;
- exact equality buckets, including empty predicates;
- entire family versions for scans;
- composite uniqueness keys;
- incoming-edge predicates for RESTRICT deletion;
- referenced rows for type validation and visibility;
- catalog versions for installation and missing-name reads;
- the principal's policy version for every restricted transaction.

This gives serializable optimistic transactions for the exposed native transactional
operations, including prevention of write skew and phantoms. Successful read-only
transactions also validate at commit. A standalone Snapshot is a read of an already
committed revision, not a transaction that may subsequently write. Snapshot SQL remains read-only. The explicit `SqlTransaction` adapter routes DML
and transactional reads through the same native transaction dependencies, supports
read-your-writes and poisons the whole transaction on SQL failure. Remote SQL is
read-only; remote atomic batches invoke native writes.

Unrelated transactions can prepare concurrently and commit without logical conflicts.
The commit coordinator validates independent transactions in order and groups
accepted records into one durable redb write. Persistence remains serialized;
grouping does not provide parallel write execution or distributed transactions.
Snapshot acquisition captures an atomically published data/authorization/failure
bundle without the writer mutex. Catalog installs conservatively conflict with
other installs; grant changes conservatively invalidate all in-flight writes for the
principal. Range queries currently scan and thus use whole-family conflict tracking.

References are checked immediately against the transaction's current state. Insert
targets before referring rows. Cyclic creation requiring deferred foreign keys is
unsupported. Uniqueness can be composite; a unique owner role gives maximum cardinality
one. Deletion is RESTRICT, including references from other apps. No implicit cascades.

## Authorization and views

The host authenticates callers and supplies a Session; Database and its admin session
are privileged capabilities. Principals currently use host-assigned string labels,
not a full principal-identity registry. Applications cannot modify grants through a
restricted Session. The CLI explicitly assumes trusted local administrative access.

Grants are indexed by (principal, relation UUID). On installation/recovery, equality
predicates compile to validated role slots and typed values. Permission evaluation
never parses JSON or scans logs.

Each grant independently enables read/write/reference and carries either `All` or a
single typed equality predicate. Updates must pass the predicate on both the old and
new row. Reference capability permits attaching data, but does not imply reading a
target's attributes. Even with a read grant on a relationship, that row is suppressed
unless all its participants and their reachable references are readable. Visibility
uses an iterative traversal with a visited set and records dependency reads.

Thus queries evaluate Q(Visible_p(X)); they do not filter aggregates after execution.
Snapshot SQL only registers readable relation schemas and obtains rows through the same
visibility checks. A published/redacted view that discloses otherwise hidden data is
not implemented. Snapshot-local invoker views are read-only and share the source
snapshot and principal. Prepared queries are also bound to that snapshot.

Restricted snapshots pin data and effective grants but now carry a live authority
gate. Grant replacement/revocation, ancestor revocation or a bound namespace policy
change invalidates affected handles. Reads check before releasing results; writes
check under the publication lock. Adding independent grants preserves existing
readers. Scoped delegation replaces ambient permissions and never expands existing
capabilities when installing a new family. See [SECURITY.md](SECURITY.md) and
[ADR 009](adr/009-delegation-revocation.md) for the exact read/revocation linearization
points, persistence and remaining trust limits. Already returned values cannot be
recalled. Database/admin handles and raw storage remain privileged.

## Distribution and verification roadmap

Placement and replication are outside logical semantics. Future placement can use
tenant and anchor affinity with routing indirection, without changing identity.
Incoming edges and unique keys are transaction dependencies that future backends must
preserve across partitions. Sharding must not weaken typed referential validation.
Evaluate FoundationDB before introducing custom replication/consensus. A custom
backend would need committed logs, safe follower reads, atomic cross-shard commit,
and serializable dependencies, not just two-phase commit.

Policies must travel with data revisions; a replica unable to meet an authorization
freshness requirement must refuse the read. Published results, raw physical replicas,
and revocable query access have different trust boundaries. Materialized views need
an explicit freshness frontier.

The independent test oracle uses simple maps and enumeration; production uses
persistent trees and indexes. Generated operation sequences compare normalized states,
index results, native joins, SQL joins, and authorization-filtered aggregates. Bend 2
is the next independent oracle milestone: exchange explicit operation traces and
canonical identity symbols, compare outputs, and test the adapter separately. No
Bend executable, formal proof, generic category solver, or distributed implementation
is included in this slice.


## Schema frontends and application boundary

The DX milestone adds locked versioned manifests, canonical SHA-256 contract hashes,
TypeScript authoring, generated Rust/TS records, native Standard Schema validators,
named writes and conflict-only retries. These reuse the existing compiler, immutable
roles and packed layouts. See [DX.md](DX.md) for canonicalization, codecs, static versus
dynamic contracts, optional properties and exact compatibility semantics.

A bounded Unix-socket host authenticates tokens to fixed restricted principals.
It exposes native batches, capability discovery, keyset pages and read-only SQL.
Multiple application processes share one database owner; redb/publication remain
serialized. Snapshot SQL cursors materialize 64-row pages, while mutable SQL cursors
retain statement candidates. Neither output limits nor VM budgets provide complete
resource isolation. Working state and indexes are still resident in memory.
