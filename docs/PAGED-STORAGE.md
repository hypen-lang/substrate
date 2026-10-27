# Paged current storage

`paged::PagedDatabase` is an explicit storage mode whose current rows and
indexes live in redb B-trees. It can serve a database larger than its configured
caches without reconstructing every row or index at startup. Paged snapshots now
support DataFusion SQL, the existing local/TLS application protocol, generated
Rust records and TypeScript clients. `Database` remains the resident option.
Choosing the storage backend is explicit; opening a file with the wrong engine
fails before initializing another format in it.

The earlier RAM limit was structural: resident recovery rebuilt `State.families`,
role postings, unique/incoming indexes, allocated identities and conflict timestamps
as persistent Rust collections. Historical snapshot files only released old row
allocations. Lowering redb's cache did not bound those current-state structures.
The paged engine stores **all six data-sized structures on disk**, not just payloads.
Only a bounded catalog, bounded decoded-row cache and active transaction work stay
in application memory.

## Working native API

```rust,no_run
use substrate_liquid::{paged::{PagedDatabase, PagedOptions}, *};
# fn main() -> Result<()> {
let db = PagedDatabase::open("people.paged.redb", PagedOptions::default())?;
let mut tx = db.admin().transaction()?;
tx.install(Module::new("people", 1, vec![], vec![Relation::new(
    "people.Person", vec![Role::new("name", DataType::Text)],
)]))?;
let person = tx.insert("people.Person", vec!["Ada".into()])?;
tx.commit()?;
let snapshot = db.session("calendar")?.snapshot()?;
let row = snapshot.get("people.Person", person)?;
let page = snapshot.scan_page("people.Person", None, 100)?;
# let _ = (row, page);
# Ok(())
# }
```

`install_owned(module, owner, visibility)` registers additive schemas and initial
visibility atomically. The default is authenticated read visibility, with a
reserved host owner that cannot be impersonated by an application session.
Namespace owners can add versions, change visibility and manage grants. Point
reads, equality-index pages, ID-order pages, typed insertion/update/delete,
constraints, uniqueness, reference integrity and shared-identity extension are
implemented. `extend(source, target, id, values)` requires reference permission on
the existing source membership and write permission on the new target membership.
A new app adds a separate family; no old packed row is rewritten.

`session(principal)` is a **trusted local embedding API**, not a credential verifier.
Only a trusted host should choose principals. `authenticate(token)` verifies
host-issued, scoped credentials. The existing TLS server authenticates those
credentials on every request; expiry, rotation, delegation and live revocation
use the same policy implementation as resident storage. Public/authenticated/private visibility
and explicit row-filtered grants apply to discovery and reads; write and reference
permissions remain separate. Reference-bearing reads check the complete referenced
visibility closure, with bounded traversal. A public link cannot disclose a private
participant. The default host owner and internal namespaces are reserved. Namespace policy,
proposal/approval, lifecycle and aliases use the existing bounded control plane.
Direct installation cannot bypass configured namespace approval or quotas.

Data snapshots pin one redb read transaction and its matching compiled catalog.
Row writes and additive schema installation leave older snapshots readable. Grant
or visibility changes conservatively invalidate **all** retained non-admin paged
snapshots; callers refresh them. Reads check live authority before work and before
release. Cache entries contain raw rows keyed by relation, occurrence and immutable
snapshot revision, never cached permission decisions. A hot hit avoids both row
decoding and a B-tree lookup. A new revision deliberately misses even for unchanged
rows; old snapshots retain their own correct cache keys. Credential/capability
deadlines are checked at entry and result release, including retained SQL plans.
Revoking a grant permanently invalidates dependent capability roots; restoring
the grant does not resurrect old credentials.

Live fences read an atomic authorization-generation mirror rather than taking the
published-head read lock. After persistence, the serialized writer acquires the
head write lock, stores the new generation with Release ordering, and swaps the
head without intervening fallible work. New snapshots acquire that same head lock;
existing snapshots compare the generation with Acquire ordering. This can reject
an old reader slightly early, but cannot expose a new view with an old generation.
Normal commits, replication apply and consensus marker publication use this one
publication path. Failure flags, poisoned writer/head locks and credential deadlines
still fail closed. Direct SQL reads and indexed joins also run the final authority
fence before releasing data-dependent errors, not only successful results.

Decoded rows use an ordered map and a separate FIFO eviction queue. Keys include
snapshot revision, and the cache never substitutes for authorization checks. A
hash-map experiment did not reproduce its diagnostic memory improvement in the
ordinary repeated benchmark and regressed changing-parameter warm reads; it was
reverted. Cache accounting and process RSS remain different measures.

Each decoded cache entry also retains its validated packed byte length, charged to
the cache budget. Singleton unique reads reuse that length for their page-size
limit instead of reinterpreting every typed field. The length and row share the
same snapshot-revision key; a later shorter or larger row cannot replace the old
snapshot's length. Prepared probes retain the immutable compiled relation and use
stack buffers for fixed scalar unique keys and row keys. Variable-width keys keep
the canonical encoder; its initial capacity also accommodates common integer and
reference keys without immediately reallocating. Index-to-row equality, reference
visibility and live checks still run; this is not a cached permission or SQL result.

## Transactions and persistence

Transactions retain bounded row changes and read-version dependencies. Allocated
identities and absent-row/unique/incoming conflict timestamps persist on disk, so
delete/reinsert histories do not escape conflict detection. Distinct row writes
can commit from the same initial revision. A bounded paged coordinator validates
queued transactions in order, assigns separate revisions, persists accepted writes
in one Immediate redb transaction and publishes once. See [group commit and
replication](PAGED-REPLICATION.md) for queue bounds and uncertain outcomes.

Under the serialized writer, commit validation compares the transaction's pinned
view with the current view by exact shared-object identity. If they are the same
immutable view, a dependency already captured from it needs no second disk lookup.
Changes from earlier accepted members of the same group are checked first through
the tentative version map, including authorization and catalog dependencies. A
different view still requires the normal durable dependency checks, even if its
logical revision happens to match. The final request does not build a tentative
map for nonexistent later validators; it still writes every required durable
version. This removes redundant serial-commit work without accepting stale or
conflicting transactions, changing durability, or adding parallel physical writers.

Live memberships reuse the revision already stored in each packed row for conflict
checks; deleted memberships retain separate version tombstones. Allocation, unique
key and incoming-reference history remain durable. Legacy redundant row versions
remain readable and are removed when those rows are updated.

Commit preparation collects both old and replacement unique/incoming dependency
keys into its admitted version set. Physical index updates then avoid re-encoding
and writing the same dependency versions again; each distinct key receives the
accepted transaction's revision once. Deleted and replaced keys still advance,
including keys needed to reject stale uniqueness and incoming-reference checks.
NULL-containing unique tuples remain excluded from uniqueness; their ordinary
role-index entries are preserved. Transaction/group memory admission and ordered
conflict validation still cover the complete version set.

References to rows already staged in the same transaction are checked through a
borrow of that staged row, avoiding a payload clone and temporary shared handle.
The row dependency is still recorded first, and Reference permission is checked
independently of Read permission. A staged deletion still makes the target absent;
failed checks poison the transaction. Ordinary index and dependency-key buffers
reserve enough space for common fixed-width keys, without changing their encoded
bytes or adding a persistent cache.

One redb transaction updates rows, all indexes, allocation/version records, catalog,
authorization generation and durable revision, using Immediate durability. Only
then is the new snapshot published and success returned. A persistence or publication
failure makes the handle unusable until reopen. Failed transaction operations poison
the transaction; an earlier successful staged operation is not partially committed.
Crash tests cover the boundaries before commit and after persistence/before publication.

The current tables are authoritative. Reopening reads the format/head and compiles
the bounded catalog; it does not scan all rows or reconstruct indexes from redo.
An enabled replication journal has a bounded integrity audit; optional consensus
can replay committed entries from its separate durable Raft log. Schema/grant
metadata uses validated typed JSON; runtime uses compiled role offsets, constraints
and policies. New files use `LPG3`, with dense family blocks containing versioned
`PG01` rows, or `PG02` for nullable/float families. Like `LPG2`, a single-role unique
constraint supplies its role lookup index when the role is nonnullable, avoiding
a second copy of the same posting. Nullable unique roles retain the ordinary
role index because NULL-containing tuples are excluded from uniqueness. Existing `LPG1` and `LPG2` files keep
their original physical layouts on reads and writes; opening does not relabel or
rewrite them. Older binaries reject `LPG3`. An existing file does not gain dense
storage merely by being reopened.
Ordered index keys preserve `Value::Ord`, including signed integers, embedded NULs,
UTF-8 prefixes and typed composite uniqueness. redb supplies page integrity; row
decode additionally checks framing, types, arity, constraints and trailing bytes.

Fast open validates catalog and table structure, not full relational integrity.
Malformed accessed records fail explicitly. The separate administrator integrity
audit streams rows and indexes to check forward and reverse consistency without
materializing the database. It is an explicit maintenance operation, not startup work.

## Memory and performance boundaries

Defaults are a 64 MiB redb cache target and an 8 MiB decoded-row cache budget.
The decoded budget accounts for rows and estimated entry overhead; it is not an
exact allocator-byte ceiling. Hash buckets and the eviction queue can retain
capacity up to the bounded historical maximum entry count.
Options also bound catalog bytes, row bytes, transaction bytes/rows/dependencies,
page bytes/rows and reference traversal. These bounds prevent the current data and
index population from becoming a mandatory Rust heap working set.

They are **not an exact process RSS limit**. redb metadata and dirty pages, temporary
encoding, allocator overhead and OS file caching use additional memory. A caller can
retain returned rows or many snapshots; those allocations and historical disk pages
remain caller-owned retention. Long snapshots can increase disk usage. A full result
must be consumed page by page to keep client memory bounded.

The resident engine's hot query path is unchanged. Paged cache hits still require authorization; cache misses access B-trees and
may require I/O. Zero latency cost cannot be promised. Measure throughput, latency and peak memory for the actual
working set. The [capacity probe](../examples/paged_capacity.rs) compares both
engines with identical typed records, Immediate durability, batch sizes and physical
cache targets, verifies complete paged reads, and reports reopen/read/load costs.

```sh
cargo build --release --example paged_capacity
python3 experiments/paged_capacity/run.py --output /tmp/paged-capacity-results
```

Each size/mode gets a fresh process and file; raw CPU/RSS and timing output are
retained. This is a capacity experiment, not the statistical PR regression gate. See the
measured capacity and resident control report (historical results removed).

## SQL and existing clients

`PagedSnapshot::sql()` returns the same result and parameter types used by resident
SQL. A custom DataFusion table provider streams bounded Arrow pages from the pinned
redb snapshot. Point and equality filters use native indexes; simple prepared reads
reuse compiled projections and probes across parameter values. Inner equijoins of
up to four relations can start at a bound equality predicate and probe connected
relations through indexes. Both sides retain full authorization and reference
checks; duplicates are preserved. Compiled identity and singleton-unique probes
project shared rows without cloning unselected fields or building generic pages.
Other joins and unsupported analytic shapes execute in DataFusion. There is no whole-database conversion to an
in-memory table. Query memory, result sizes, source work and deadlines are bounded;
large queries can return a resource-limit error rather than consuming unbounded RAM.

```rust,no_run
use substrate_liquid::{paged::{PagedDatabase, PagedOptions}, *};
# fn main() -> Result<()> {
let db = PagedDatabase::open("people.paged.redb", PagedOptions::default())?;
let snapshot = db.session("calendar")?.snapshot()?;
let query = snapshot.sql()?.prepare("SELECT name FROM people.Person WHERE id = ?1")?;
let result = query.query(&[rusqlite::types::Value::Text(Id::new().to_string())])?;
# let _ = result; Ok(())
# }
```

`LocalServer::bind`, `RemoteServer::bind` and `Gateway` accept either backend.
The wire protocol and generated TypeScript clients are unchanged: named writes,
atomic batches, contract checks, selected-schema SQL, typed/nested reads, runtime
schema discovery and proposal/install work through the same interfaces. Native Rust
clients also have `Record`, named-field and runtime-descriptor operations and retry
helpers. The local fixed-credential server remains a trusted-host option; use TLS
with issued credentials for remote access.

```sh
cargo run --example paged_sql
npm run demo:paged
# Existing CLI commands accept explicit backend selection:
cargo run -- manifest-install --paged people.redb examples/dx/manifest.json
cargo run -- serve-tls --paged people.redb 127.0.0.1:8443 certificate.pem private-key.pem
```

The [TLS/TypeScript example](../examples/paged_host.rs) creates its own fixture and
credentials and runs [the existing runtime SDK](../examples/dx/paged-app.ts), then
checks rotation, revocation, expiry and durable reopen. Use that example for a
complete setup; the CLI server expects host-provisioned credentials and certificates.

## Bounded control-plane state

Schemas, grants, namespace policies, proposals, capabilities and credential hashes
are bounded by the catalog byte budget. Their typed records compile using the same
policy compiler and control operations as the resident engine. Only this metadata
is adapted to a transient resident control database for administrative operations;
application rows, identity history and indexes stay on disk. Ordinary data reads
use compiled policies, not a transient control database or parsed JSON.

A control mutation is checked against the initiating credential and exact catalog
snapshot, then committed with its schema/grant changes in one redb transaction.
Concurrent changes conflict and retry. Read-only control operations do not write a
new revision. Internal metadata identities and relation IDs are reserved with the
same conflict checks as application identities. Older paged files without control
metadata remain readable; newer metadata fields are not understood by old binaries.

## Bounded aggregate execution

Whole-family scalar `COUNT`, checked integer `SUM`, and numeric `MIN`/`MAX`
compile into a bounded streaming aggregate path. It keeps one accumulator per
aggregate, validates each row and referenced record, and avoids converting the
input to Arrow. Scalar families and single references into scalar families also
bypass the decoded-row cache; more complex reference closures retain the existing
bounded traversal and cache. Grants and reference visibility
are checked using compiled policies; credential scopes override schema visibility.
Periodic and final authorization fences preserve revocation and expiry, including
when execution returns an error. Empty inputs, overflow and output/source budgets
retain their SQL semantics. There is no cached aggregate result to invalidate. A scalar query with `WHERE`
and no effective per-aggregate filters shares one accepted-row count and compact
SUM/MIN/MAX state. Aggregate filters, SQL UNKNOWN and unsupported coercions retain
their existing paths; the smaller state is covered by the previous memory reservation.

A single-reference scan retains one target cursor. In dense files it borrows one
target block, reuses its validated directory for nearby/repeated references, and
advances at most one block before seeking after a larger jump. Reverse and arbitrary
reference order are supported without building an identity map. Legacy files keep
the logical cursor that advances at most eight entries before seeking. Matching targets still undergo full packed validation. Targets
without a row predicate can be validated without allocating text/byte values,
using the same typed parser as normal decoding. These checks do not assume that
an unrestricted grant makes a dangling or corrupt reference acceptable.
Source work uses an execution-local counter; deadlines are checked on first work,
every 256 checked nodes, and before releasing a result. This is bounded polling,
not hard wall-time preemption. Credential expiry and authorization epochs remain
independently fenced.

Typed conjunctions of direct-field comparisons in SQL `WHERE` and per-aggregate
`FILTER` also compile. Bindings are validated on each execution, so prepared plans
remain reusable across parameters; unsupported coercions use DataFusion. Equality
and two-sided range predicates traverse ordered role/unique indexes and validate
each index entry against its row. Numeric one-sided predicates can use the index
when a bounded endpoint estimate predicts at most 50% of values. Packed range
execution makes the same cutoff available to eligible fixed-width sources;
other source shapes keep their existing general range path. Eligibility uses the
same compiled schema/policy checks as actual execution. This threshold is a
planner hint, not a guarantee that the selected plan is fastest. The estimate
assumes a uniform distribution, requires unrestricted visibility of both roots and
terminal references, and is never used as a count or permission decision. Unknown
or broad estimates retain the family scan; skew can still make this plan suboptimal.
Residual predicates and authorization both remain enforced.

Multirow ranges reuse row buffers and stream ordered occurrence runs without
filling the decoded-row cache. Random occurrence order starts with point lookups;
eight consecutive ordered steps promote to a bounded cursor, and direction changes
reset it. Singleton ranges retain the decoded cache. Index-to-row checks, packed
reference validation, source budgets and live authorization fences apply in both
paths. No row format or cache-budget change is required.

Eligible fixed-width ranges read selected records directly from packed storage,
rather than decoding whole owned rows. Every candidate's unique or role-index
key must exactly match the canonical encoding of its row before visibility is
considered. A query-local validator compiles the family/index prefix, type tag
and fixed payload width once. For supported boolean, integer, decimal, timestamp
and reference fields, it compares the stored payload directly with the validated
packed value, checks exact key length, and verifies the unique tuple terminator
or occurrence-ID suffix. It avoids constructing a temporary typed value and
rebuilding a complete key per candidate. Singleton reads retain their existing
canonical encoder, and unsupported kinds retain the general comparison. The
validator holds constant schema metadata, not cached rows or permissions.
Root framing, every boolean, field count, row length and snapshot
revision are checked; terminal referenced records receive complete typed validation,
including unprojected text and denied targets. The indexed predicate narrows the
records visited, so this is not a full-family corruption audit. Residual SQL
predicates and aggregate filters run only after those checks.

For multi-candidate packed ranges, random IDs use one direct row guard. Eight
ordered steps promote to a cursor retaining one root block; direction changes
reset it. A separate one-block cursor serves terminal references. The common
same-family current/adjacent lookup stays separate from seeking and directory
search, preserving exact family and identity checks. Singleton ranges keep the
existing cached-row/graph path. Source/page/reference budgets and callback/error
ordering remain enforced in both forms. Unsupported schemas or scoped row filters
decline this path before consuming a source or invoking a visitor; execution never
switches to a fallback after delivering partial rows. Native query APIs are unchanged.

After the first two index entries establish whether a range is a singleton,
multi-candidate packed ranges use the local redb `Range::visit_borrowed` API.
Its callback receives key/value slices borrowed from the current immutable leaf,
avoiding owning access guards and their page reference-count updates for each
remaining index entry. Higher-ranked callback lifetimes prevent those slices from
escaping. The range retains its transaction guard and bounded traversal state;
this changes neither row-cache limits nor disk layout.

Borrowed traversal and ordinary forward/reverse iterators share the same bound,
crossing and storage-error transitions. A callback break consumes its entry and
preserves its application error; it cannot be replaced by a later storage error.
The iterator can resume after a break, including from the opposite end. Existing
owning guards remain pinned across commits. Packed range callbacks still perform
the complete index-to-row comparison, row/reference validation, work charging and
authority checks described above. This API does not authorize raw page access for
application callers through the database service.

Broad scans reuse the root and filtered-reference value-slot arrays. Every packed
row is still validated before use; variable text/blob payloads replace their old
allocations rather than retaining historical per-column capacities. The backend
uses redb 3.1.3 with the documented local read-path fixes in
[`vendor/redb/SUBSTRATE.md`](../vendor/redb/SUBSTRATE.md). Cached reads fill their
final immutable buffer directly; dense point reads use a one-sided lower-bound
lookup. The configured cache budget, file format, write durability and
single-threaded scan execution are unchanged.

Backend page payloads use `Arc<Box<[u8]>>`: the payload has a page-sized allocation
separate from its reference count. This reduces allocator size-class overhead on
the measured macOS runner, at the cost of another allocation and pointer load.
An earlier same-feature inline-page control informed this representation choice;
the current load/read follow-up leaves the backend representation unchanged.
The [latest comparison](benchmarks/latest/README.md) states its own exact binary
boundary and measured tradeoffs. It must not be treated as an allocator comparison
unless an inline-page control is explicitly included. This is not a portable RSS
guarantee; cache limits still account for payload bytes.

Eligible SQL aggregates can read fixed-width packed fields directly. A compiled
layout supports up to 64 boolean, integer, decimal, timestamp or reference fields;
it validates the complete row framing, every boolean and the snapshot revision
before exposing any value. Numeric and reference accessors check the field kind
and use validated offsets. Same-kind numeric predicates compare values directly;
other comparisons retain the canonical typed ordering. This reads the existing PG01 encoding without changing
the logical row encoding or rewriting records.

The packed visitor requires an unrestricted root read policy and either no
references or one reference to a scalar family with an unconditional policy. It
still reads and fully validates referenced rows, including hidden targets.
For terminal reference families with at most 64 supported fields, a separate
compiled validation plan coalesces consecutive integer/reference fields into
bounded skips while validating every boolean, UTF-8 field, variable-length prefix,
header, field count, final byte length and snapshot revision. Unprojected target
fields are still checked, even when the SQL predicate later excludes the root.
The plan retains schema metadata only, with no row/page payloads. Nullable, float
or wider target families use the complete generic validator. Visitor callbacks and
corruption errors retain their source order; no partially delivered scan changes
to a fallback executor.
Variable-width or wider schemas, row-filtered credentials and more complex
reference graphs use the general authorized visitor. Discovery, credential
scope, source/page/visibility bounds and final live-authority checks remain in
force. The source retains constant cursor state, not a batch of row guards.
Native query APIs retain their existing execution paths.

The packed-scan measurements (historical results removed) include rejected
batching experiments and the complete SQL comparison. The earlier
dense-layout experiment (historical results removed) informed
the production layout described below.

Single-field grouping uses borrowed hash lookups, copying a key only when first
admitted. A fixed 64-slot numeric cache handles common small groups; collisions
compare exact keys and fall back to the hash map. Multi-field grouping retains an
ordered map. These structures, resize capacity and sorted output keys are included
in the conservative group-state reservations.

Repeated exact numeric cache hits update an already admitted accumulator through a
small hot path after evaluating the row predicate. They do not repeat map lookup,
initial allocation checks or an identical cache-entry write. Colliding, new and
non-numeric keys take the separate path that performs all key/group memory
reservations before allocation. Cache initialization contains no valid entries;
therefore a cache hit cannot bypass first-group admission. COUNT and widened SUM
remain checked, and aggregate FILTER semantics do not change.

Small packed validation, reference access and source-budget helpers are inlined
into scan/fold loops; malformed-row and overflow error construction stays out of
the hot path. Every existing bounds/type/revision check remains. The local redb
range iterator computes entry ranges once; ordinary iteration constructs one
owning entry guard, while borrowed visitation constructs none. Both iteration
directions retain their boundary/error logic and immutable page ownership. These are CPU/code-layout
changes, not larger caches, weaker validation or parallel scanning; their benefit
must be established by the reported end-to-end measurements.

Direct-field `GROUP BY` retains bounded accumulators per group. Group keys,
accumulators, conservative map overhead and projected output share a ceiling of
32 MiB (or the lower query byte limit), after reserving predicate-binding memory.
Row and output-cell limits also cap group count. Reservations precede copying large
keys or adding groups; exceeding a limit returns a resource error without retrying
an unbounded algorithm. Simple output-column ordering sorts those bounded results
in place, with a final deadline/authority check. Sorting is not preempted between
comparisons. Scalar aggregation remains constant in source cardinality; grouped
aggregation consumes memory proportional to admitted groups, up to its ceiling.

DISTINCT, HAVING, expression group keys, windowed aggregates and unsupported
expressions remain on DataFusion with its existing memory pool. Authorization row
filters remain enforced in both paths. Neither path materializes the database in
memory. See the broad-scan follow-up (historical results removed) and
the filtered/grouped and small-read comparison (historical results removed).
The one-million-record overall report (historical results removed) includes
all eleven SQL cases and resources. The subsequent native/SQL matrix (historical results removed)
measures optimized ranges and current public native paths at the same size.

## Host diagnostics

`PagedDatabase::with_read_metrics(snapshot)` attaches logical cache/decode/index
counters while preserving the supplied snapshot's authority. This requires the
host database handle; it is not an operation exposed to restricted snapshot holders.
`storage_breakdown()` additionally requires an admin snapshot and reports the seven
live tables' payload, tree metadata and fragmentation. These figures exclude file
headers, allocators, free pages and retained historical pages.

`PagedDatabase::open_with_io_metrics` is an opt-in wrapper around the stock redb
file backend. It counts requested bytes, calls and errors without changing cache or
durability behavior. Backend requests can hit the OS cache and must not be labeled
physical disk reads. Ordinary opens have no backend counter updates. Benchmark
instrumented runs separately from headline timings; storage accounting runs last
because it can warm pages. The [benchmark runner](../experiments/paged_sql/README.md)
documents the native/SQL matrix and diagnostic controls.

## Remaining boundaries

Storage selection is explicit; existing resident files are not silently converted.
Paged mode supports explicit LPG-file and bounded LDB resident-file import, group
commit and trusted-host replication. Unsupported resident control metadata fails
closed during conversion; the [import guide](PAGED-REPLICATION.md) defines the
supported metadata subset. Optional [static-peer consensus](CONSENSUS.md) uses a
host-provided authenticated peer transport and has no snapshot transfer or log
compaction. Persistent named-view catalog management and partition
routing remain separate boundaries. SQL
views within a query session are supported. Anchored inner equijoins have an indexed
probe path; other shapes use DataFusion. The resident engine's broader specialized
range, ordering and analytic optimizations are not all shared. These are performance/integration boundaries, not
fallbacks that load the database into RAM. See the
integration benchmark report (historical results removed) for measured costs.
The four-engine SQL comparison (historical results removed) also includes
PostgreSQL on the same runner, with repeated measurements and explicit IPC and
server-memory accounting limits.

## Dense family blocks (`LPG3`)

The production paged row table stores `LDB1` blocks keyed by a relation ID and the
largest occurrence ID in the block. Each block contains one relation family,
a sorted directory of stable occurrence IDs and offsets, and contiguous versioned
row payloads. IDs remain arbitrary UUIDs, including shared IDs in independent
application families. A point read seeks the first block whose upper key covers
the requested ID and searches its directory. Native pages, SQL sources, reference
validation, integrity audits, SDKs and local/TLS services share this row store.

Normal blocks target at most 3,500 encoded bytes and 256 rows, leaving space for
redb leaf framing. A larger variable-width row occupies a singleton block, subject
to the configured row limit. The block decoder checks the version, family, count,
strictly ordered IDs, offsets and upper key before exposing rows. Point reads
validate their selected block; cross-block overlap checks belong to scans, writes
and the explicit integrity audit. A point read is not a global corruption audit. Existing typed
row, revision and authorization validation still applies. redb supplies page
checksums; blocks do not add a second per-scan cryptographic checksum. Directory
validation uses fixed-width ID comparisons and checks every descriptor. Strictly
increasing offsets and the final payload bound validate all row boundaries before
any offset is dereferenced. Mutation/truncation tests compare this validator with
a scalar reference. A validation-only microbenchmark isolates this CPU work; it
does not measure database I/O, authorization, result copying or SQL end-to-end cost.

Native page/range cursors retain at most one block from each end, with returned
row guards sharing payloads. Internal SQL aggregate visitors instead borrow one
root block through a callback, avoiding an `Arc` clone/drop for every row. Values
cannot outlive the physical guard. Reference traversal has its own one-block
cursor; it never builds a relation-sized identity map. Both general typed and
fixed-width visitors retain row, revision, authorization and source-budget checks.
The decoded-row cache and SQL group-state budgets retain their existing limits.

Writes transfer ownership of each encoded row into their bounded change set.
Repacking merges staged changes and existing sorted rows into one reusable block
encoder, copying at most one existing block instead of materializing a second
map of the whole changed family. The existing block is fully validated before
copying; its validated count is carried with the exact owned bytes, avoiding a
second directory walk. Per-row size and predecessor-overlap checks remain intact. The output payload and encoded buffer can retain
the largest permitted row until commit finishes. This scratch space remains
proportional to configured transaction and row bounds, not total database size. A byte budget is not a bound on OS filesystem cache
or on the combined memory of arbitrarily many simultaneous client transactions.

Block rewrites, role/unique/incoming indexes, stable-ID allocation, logical row
versions, schema/policy metadata and the durable head commit in one redb write
transaction. OCC dependencies remain logical rows and index keys. Disjoint rows
in the same block can commit from the same initial snapshot; the later commit
merges with the latest durable block. Existing snapshots retain their previous
blocks. A new schema family does not rewrite another family's packed records.

Dense storage changes physical traversal, not access policy. Credential scope,
public/authenticated/private visibility, filtered grants, referenced-row visibility
and live revocation checks remain above the physical interface. Physical blocks
must never be exposed as an application read API. Administrator storage breakdowns
report **physical block counts** in the row table; `verify().rows` reports logical
row counts. Startup still reads bounded metadata rather than scanning blocks.

There is no automatic LPG1/LPG2 conversion or background compaction in this
milestone. Updates split oversized blocks; deletion may leave sparse blocks.
Index tables, identity allocation and version tombstones remain separate B-trees,
so dense payloads do not eliminate all scan or storage overhead.

The backend reuses an evicted page allocation only when its length matches the
new read and `Arc::get_mut` proves that no retained guard or weak owner can
observe mutation. It removes and debits the cache entry under the stripe lock,
releases that lock before I/O, zeroes the buffer, and publishes it only after a
successful read. Pinned or differently sized pages use the normal allocation
path. There is no spare-buffer pool. Cache budgets bound retained payloads, not
allocator reservation or whole-process RSS.

Numeric SQL grouping begins with 64 accelerator slots. Repeated misses can grow
that cache to at most 2,048 slots, charging the simultaneous old and new
allocations to the query state budget. Full keys distinguish collisions; the
canonical group map remains authoritative. Allocation or budget pressure keeps
the smaller cache. Discovering a new group releases optional larger capacity
before group admission, so the accelerator cannot consume memory required by
new groups. Group cardinality and results remain subject to existing query limits.

Compiled point reads precompute output-column byte counts and allocate space for
only one result row. Compiled small joins retain their at-most-four source rows
on the stack. Final SQL authorization fences gate successful, empty, and
data-dependent error results; point-probe callback fences and each join source's
authorization checks remain in place.
