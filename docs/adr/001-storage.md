# ADR 001 — Persistent working trees and durable redo

Status: accepted for the first slice; provisional for larger workloads.

The repository contained an empty README and no source, dependencies, test tooling,
or repository instructions. There was no backend to reuse.

Options evaluated were a memory-only kernel, SQLite canonical tables, redb, and a
future FoundationDB backend. Memory alone would not demonstrate recovery. SQLite
could supply a durable substrate, but using it for both storage and queries would
make it harder to validate an independent logical kernel and its conflict rules.
FoundationDB adds distributed operations before local semantics are established.

Use persistent in-memory ordered maps for snapshots and logical indexes, and redb
for atomic durable redo records. [redb's design](https://github.com/cberner/redb/blob/master/docs/design.md)
uses copy-on-write storage and a commit protocol; this prototype relies on its
Immediate durability mode instead of implementing a new filesystem commit protocol.
The kernel validates fine-grained optimistic dependencies and serializes publication.

The on-disk hot-row encoding is schema-directed and packed. Rare schema and grant
metadata use JSON inside a versioned binary envelope. This avoids an EAV row per
attribute while keeping metadata inspectable in tooling. Canonical records and all
required indexes become visible together; indexes are reconstructed on restart.

Consequences: snapshots share unchanged tree nodes and rows; schema installs do not
scan old data; independent writes merge. Commit publication remains one at a time,
and the entire working set stays in memory. The initial full-history replay has been
replaced by [atomic checkpoints and bounded redo](005-checkpoints.md), with crash/I/O
fault tests. Incremental persistence and serving datasets larger than RAM remain future work. Do not
add speculative distributed abstractions before this boundary is measured.
