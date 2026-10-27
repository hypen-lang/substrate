# Read-only lower-bound lookup

Substrate carries a narrow local patch to redb 3.1.3:
`ReadOnlyTable::first_at_or_after(key)` returns the first key/value pair whose
key compares greater than or equal to the supplied key, or `None` at the end of
the table. It has the same result as `table.range(key..)?.next().transpose()`.

The implementation descends only toward the lower bound, using redb's existing
typed `LeafAccessor::position` and `BranchAccessor::child_for_key` operations.
If the selected subtree has no candidate, it descends to the first entry of its
next sibling. It uses the cached root, ordinary page guards and the same page
read hints and error propagation as `get`/`first`. It has tree-height stack
space, no iterator parent-chain allocations, and no upper-endpoint search.

The public API is limited to read-only tables. Existing `get`, `range`, mutable
table APIs, file formats, checksums, commit durability and recovery are unchanged.
The method returns ordinary immutable page-backed `AccessGuard`s. Read-only
table handles keep their original transaction snapshot, and returned page guards
remain usable after the table handle is dropped. This is not a new integrity
audit or a replacement for redb's existing corrupted-file checks.

Substrate uses this method only to find the dense block containing a requested
logical identity. Complete dense-directory validation and typed row validation
still follow the physical lookup. No identity locator or second index is added.

`tests/redb_lower_bound.rs` compares against both `BTreeMap` and the unchanged
range API across exact keys, gaps, leaf/subtree boundaries, signed ordering,
variable-length keys, empty tables and end boundaries. It also exercises pinned
snapshots/guards during mutations and propagating backend read failures.

Remove this local API patch when an upstream redb release offers an equivalent
single-ended lower-bound lookup, after rerunning those tests and the cold-read
benchmark. This patch is separate from the local cache-buffer allocation change.

The local range iterator also avoids constructing and discarding an
`EntryGuard` merely to test whether a leaf has an entry. Forward and reverse
iteration return that first guard directly. This removes one redundant page
`Arc` clone/drop and one repeated entry-range calculation per result without
changing bounds, iterator state transitions, error propagation or guard
lifetimes. The mixed-direction range regression in `tests/redb_lower_bound.rs`
checks both ends, empty/singleton ranges, cross-page traversal and retained
guards across writes.
