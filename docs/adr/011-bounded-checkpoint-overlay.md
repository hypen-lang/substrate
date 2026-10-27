# ADR 011 — Bounded materialized checkpoint overrides

Status: implemented; extends the original checkpoint contract in ADR 005.

## Problem and decision

Changing a small row in a large redb B-tree can rewrite whole leaf and ancestor
pages. Measurements on 100,000 rows found roughly 880 KB of backend writes per
checkpoint for 7.3 KB of encoded row changes. Group commit reduces synchronizations
but does not eliminate this page amplification at a fixed redo budget.

V3 checkpoints retain the base row table and add a small materialized override
table. The override table stores **one newest row image or tombstone per row
identity**. It contains no revisions, transaction boundaries, or operation
history. Repeated updates replace the same key. Recovery reads the merged
checkpoint state, then replays the separately bounded redo tail.

The override table is capped at **4,096 distinct keys and 8 MiB of encoded key
plus value bytes**. A key is always 32 bytes. A tombstone has an empty value but
still consumes its 32-byte key. Exact preflight computes the coalesced post-write
key count and encoded byte size without encoding or copying row payloads. A write
that would cross either cap first merges a sorted slice of at least 128 eligible
existing overrides into the base, or enough entries to satisfy both caps. Rows
also changed by the pending checkpoint are retained so the free-space calculation
does not count keys that will immediately be reinserted. Initial checkpoints
and batches with more than 4,096 distinct changed rows or more than 8 MiB of new
images write directly into the base, fully merging any existing overrides first.
Those large batches already amortize base-page writes; repeated buffer fills
would add avoidable copy and compaction work.

These limits are fixed format constraints, validated on recovery. They do not
change the default redo limits: fewer than 128 commits and fewer than 8 MiB of
redo records after every acknowledged transaction. Explicit custom redo policies
retain their documented behavior. There is no hidden unbounded delta history.

## Format and atomicity

The control format is `LDB3`, the catalog marker is `LCC3`, and the manifest is
`LCP3`. Existing tables retain their names and key/value types:

- `liquid_checkpoint_rows_v1`: base rows, keyed by type UUID + occurrence UUID.
- `liquid_checkpoint_metadata_v2`: current typed schema/grant/identity metadata.
- `liquid_checkpoint_overlay_v3`: newest row images or empty tombstone values,
  keyed identically to base rows.
- `liquid_log_v1`: versioned redo transactions beyond the checkpoint revision.
- `liquid_control_v1`: manifest, catalog version, format, and durable head.

Nonempty row values retain the exact revision-independent single-Put LQ01
encoding. Packed layouts and stable identities do not change. Immutable schema
extensions do not require rewriting unrelated base or override rows.

The 84-byte V3 manifest contains its four-byte magic followed by ten little-endian
u64 fields: revision, logical row count, identity count, module count, grant count,
schema-grant count, logical checkpoint payload bytes, physical base row count,
override key count, and override key/value bytes. The first seven fields retain
their V2 meanings. Logical payload bytes count catalog, metadata, and the newest
visible row images, excluding hidden base images and tombstones. Physical override
bytes include keys and are accounted separately; neither counter describes redb
page allocation or SSD traffic.

Schema/grant metadata, row overrides, optional compaction, manifest, durable head,
and redo pruning commit in **one Immediate redb transaction**. There is no
intermediate durable state with new permissions and old data or missing redo.
Publication and acknowledgements follow successful persistence. Existing uncertain
I/O failure handling continues to stop writes until recovery.

Partial compaction streams selected sorted overrides into base rows and keeps
only their fixed-size keys in a bounded temporary list. After the iterator and
its page guards are dropped, it removes those keys individually. This preserves
untouched remainder pages without copying or rebuilding their row payloads; unlike
`retain(false)`, separate removals can reuse pages changed by this transaction.
Full/bulk compaction atomically deletes and recreates the untouched override table
under the same name/types. Neither path scans the entire base. Preflight is
necessary because redb 3.1.3 cannot safely delete a table after modifying its pages
in that same transaction. Metadata remains directly updated under the same
transaction. Deleting a row with a base image stores a tombstone; deleting an
override-only row removes its entry entirely. Reintroducing a row image replaces
its tombstone. Public identity-reuse restrictions are unchanged.

## Recovery, compatibility, and leases

V1 and V2 files remain readable. Their next checkpoint upgrades atomically to V3.
V1 catalog migration retains its existing full conversion; V2 base row bytes can
remain unchanged. Older binaries reject the unsupported `LDB3` marker. Production
offers no downgrade setting; the I/O test has an internal direct-V2 writer solely
for matched experiments.

Recovery validates manifest version, base/override counts, fixed bounds, exact
override bytes, row keys/types, canonical tombstones, logical counts, referential
integrity, and redo sequence/head. It merges overrides into base input before
materializing rows, so hidden old row images do not create transient duplicate
indexes or invalid reference states. Empty base values are invalid; tombstones
belong only in the override table. redb checksums protect physical pages, as with
earlier formats. Raw filesystem access remains privileged.

Pinned redb read transactions retain their original base and override pages
through compaction. The existing `DiskSnapshot` API materializes its own separate
redb file and retains its live authorization guard; this format change does not
change its read or revocation behavior. Tests cover both kinds of lease.

## Costs, counters, and limits

`StorageInfo.overlay_keys` and `overlay_bytes` expose the bounded current map.
`PersistenceStats.checkpoint_compactions`, `checkpoint_compacted_rows`, and
`checkpoint_compacted_bytes` report physical merge work separately from newly
encoded checkpoint changes. These process-local counters count encoded records,
not backend page writes. `checkpoint_retained_overlay_bytes` counts key/value
bytes copied into a replacement override table; it remains zero with selected-key
partial removal and full-table resets. The test-only storage probe measures actual backend
bytes, synchronization calls, maximum checkpoint duration, and reopen time.

Compaction is synchronous. Its bounded merge can produce a foreground latency
spike; throughput improvements must include repeated compactions rather than just
filling a fresh override table. Neither caps nor checkpoints imply bounded total
startup time: loading the base, allocated identities, metadata, and rebuilding
resident indexes still scale with database size. Mutable serving remains in RAM.
This is a small bounded write buffer, not a full LSM engine, background compactor,
or larger-than-memory mutable storage implementation.

The bound applies to committed overrides. redb can temporarily retain both old
and staged pages during a transaction, and an individually large row is written
directly into the base. This does not impose a new maximum row size or pretend
to bound a transaction's total memory, payload, or synchronization time.

## Verification

Tests cover exact key and byte limits, oversized values, repeated updates,
delete/reinsert and absent deletion, manifest accounting, legacy upgrades,
pinned base/override readers, malformed overlay data, and crash boundaries before,
during, and after compaction. Backend write, synchronization, and partial-write
faults verify that data/schema/grants recover together. Benchmarks must run at
least 32,768 random updates for a 100,000-row fixture to include multiple merges.
