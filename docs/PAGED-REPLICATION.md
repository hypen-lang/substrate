# Paged import, group commit and replication

Paged storage now has an explicit offline format converter, bounded group commit,
and a trusted-host replication stream. Application rows and secondary indexes stay
on disk. These APIs do not implement an authenticated replication service or turn
manual promotion into consensus.

## Explicit file conversion

`PagedDatabase::import_paged_file(source, destination, options)` converts LPG1,
LPG2 and LPG3 paged files into dense LPG3. Close the source in other processes
first. The destination must not exist. The converter audits the source, streams
rows and index entries through transaction-sized batches, preserves stable IDs,
deleted-ID history, revisions, credentials, grants, capabilities and namespace
ownership, then audits the candidate. LPG1 redundant unique-role indexes are
removed according to the destination's physical layout.

Only the verified file is published. A same-directory hard link creates the final
name without overwriting a competing destination, and Unix directory syncs make
publication durable. A crash may leave an unpublished `.substrate-import-*`
staging file. Remove such files only after confirming no importer is running.
If a filesystem error occurs after final-name publication, inspect that complete
file before retrying: an error does not imply the destination is absent.

The input remains logically unchanged. Import does not invent transaction
revisions or infer a different owner/visibility policy. The configured row,
transaction and catalog bounds still apply. Cache accounting does not equal total
process memory.

`PagedDatabase::import_resident_file(source, destination, options)` separately
imports resident LDB1/LDB2/LDB3 checkpoint, overlay and redo files. It replays
application rows and the allocation ledger into disk scratch tables, retaining
only bounded schema/control metadata in memory. It validates checkpoint counts,
byte accounting, redo order, typed values, uniqueness and references, then builds
dense LPG3 and audits the final file before publication. Stable IDs, deletion
history, logical revision, grants and visibility survive conversion. Imported
visibility policy occurrence IDs are retained in a validated optional catalog map
and remain stable across later visibility changes and schema extensions. Canonical
namespace, capability and credential records reuse the existing policy compiler;
credentials keep their scope, deadlines and revocation state. Historical OCC
stamps become the imported baseline; no transaction from the old process can span
this offline conversion.

Unsupported named-view/control modules, explicit control-family grants and
ownership mappings that would broaden write/reference authority fail closed.
Large legacy monolithic catalogs or individual redo records above the configured
bounds require explicitly larger configured limits or checkpointing/rebatching
through the resident engine before import. The importer does not silently raise
its memory limits. A data
record or catalog remains subject to its own size limit; bounds are allocation
accounting rather than an exact RSS ceiling. The converter does not load the
resident database through `Database::open`.

Intermediate scratch writes use relaxed durability only while the file is
unpublished. A final explicit Immediate commit persists all preceding scratch
work before verification and atomic publication. Ordinary transaction durability
is unchanged. Both converters create staging files with mode0600 on Unix and
preserve that mode at publication. `cargo run --example paged_import` demonstrates
checkpoint-plus-redo conversion and permission preservation.

## Durable groups

Normal `PagedTransaction::commit` submissions use a bounded caller-elected queue.
The default drains already queued requests with no deliberate wait.
`PagedDatabase::open_with_group_commit` accepts `GroupCommitOptions` for explicit
queue/group transaction and byte bounds and an optional collection window.
`group_commit_stats()` reports accepted writes and durable groups; a durable group
is one successful Immediate redb commit, not a count of physical sync syscalls.

The coordinator validates requests in queue order against the published snapshot
and a bounded overlay of earlier accepted changes. It precomputes row encoding and
version-key effects before changing storage. Preparation charges old decoded
payloads, newly encoded buffers, version/overlay copies, catalog serialization
and optional journal buffers before retaining them; both transaction and group
budgets apply. Shrinking large old rows can exceed this bound even when the staged
new values are small, so callers must reduce transaction size. OCC conflicts and validation failures
are rejected individually. Every accepted write receives its own revision. Rows,
indexes, catalog, authorization epoch and optional replication events are then
written in one Immediate transaction, followed by one published snapshot and
acknowledgements. Readers can use the preceding durable snapshot during disk I/O.

Live fences run during admission and before persistence. A deadline expiring while
a group is staged can abort that still-uncommitted group; no member receives a
false durable acknowledgement. An uncertain commit/publication error stops further
writes and reads until reopening. A serial client awaiting each commit has little
to combine; concurrent submissions or deliberate application batches are needed
for throughput gains.

## Paged followers

Enable future event retention with `db.enable_replication()`. The returned cursor
contains a durable stream UUID, logical revision and SHA-256 lineage digest.
`replication_next(cursor)` returns one retained transaction without advancing a
hidden client cursor. Keep it until the follower acknowledges; retrying the latest
already applied event is idempotent, including after reopen. Earlier duplicates,
gaps, wrong streams and divergent digests fail explicitly.

The stream retains at most 128 events and 64 MiB, with a 16 MiB per-event cap. Slow
followers can lose required history and must bootstrap again. Journal startup
checks are bounded by these caps; the current database is not replayed from its
first transaction. Checksums detect corruption and divergent history, not malicious
authorship. Exported catalog and control records are sensitive privileged data.
Only a trusted host should obtain or apply them.

Bootstrap currently uses `PagedReplica::create_from_file`, which imports a closed,
journal-enabled source into a new follower file. The read-only marker is durable
before the destination is published. Reopening that file through ordinary
`PagedDatabase` still rejects transactions. `PagedReplica::apply` validates typed
rows, references, uniqueness, conservative catalog evolution, stable allocations
and authorization epoch changes before atomically persisting the transaction,
indexes, catalog and cursor. Schema/grant metadata is serialized JSON and compiled
into validated Rust structures; ordinary row events contain versioned packed bytes.

`snapshot_eventual`, `anonymous_eventual`, `authenticate_eventual(token)` and
`snapshot_at_cursor` return ordinary
paged snapshots, including their native and SQL read interfaces. Applied revocations
invalidate retained restricted snapshots. An eventual follower can lag a leader's
revocation; a caller-supplied cursor barrier proves only that exact trusted state,
not that no later revocation exists. Principal selection is trusted embedding;
`authenticate_eventual` additionally applies locally replicated credential scope
and expiry checks. Neither replaces an authenticated network transport.

Internal consensus hooks prepare proposals without committing locally and persist
an opaque bounded applied-log marker in the same transaction as the application
change. Blank/membership entries can persist only that marker. These hooks are a
storage boundary, not a claim of elections, durable quorum or safe failover by
themselves. The optional [OpenRaft integration](CONSENSUS.md) uses them with
validated marker ordering, stale-leader fencing and quorum decisions. The
standalone follower API does not elect or automatically promote a leader.

Run `cargo run --offline --example paged_replication` for the local stream example.
Focused tests cover format conversion, independent/conflicting groups, corruption,
retention, authorization and crashes around atomic publication and replica apply.
