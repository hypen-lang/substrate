# Disk-backed historical read leases

`Snapshot::spill(path, families, options)` materializes selected, authorized rows
into an immutable redb file. Its `DiskSnapshot` supports lazy point reads, ID-order
pages and indexed equality pages. It holds selected compiled schemas and a weak
live-authority guard, **not the original State or source Row Arcs**. This lets a
historical read lease release old Rust row/index allocations after the current
engine updates or removes those rows.

This is a bounded read-serving slice. The mutable engine still loads its current
state and indexes into RAM. Building a spill requires that existing snapshot in
memory. There is no disk-backed mutable working set, cache eviction for the main
engine, SQL adapter over disk leases, streaming remote endpoint, or concurrent
writer engine operating beyond RAM.

```rust,ignore
use substrate_liquid::disk_snapshot::SpillOptions;
let disk = session.snapshot().spill(
    "people.snapshot.redb",
    &["core.Person"],
    SpillOptions::default(),
)?;
let page = disk.page("core.Person", Some(("name", &"Ada".into())), None, 100)?;
let row = disk.get("core.Person", page.rows[0].id)?;
disk.verify()?;
```

Run `cargo run --release --offline --example disk_snapshot -- 256 65536` for a
working historical-read and memory example. It reads the old payloads after deleting
current rows, checks the complete disk index, and compares pinned-memory leases
against disk leases with the same data.

## Authority and lifecycle

Only explicitly selected families are materialized, and only rows visible through
the source snapshot's full reference-closure policy. Family selection requires
schema-discovery permission, including reference-domain metadata. Unselected
families return denial. A spilled link can refer to an identity in a source family
that was not selected; the file is a selected read result, not a self-contained
relational database or import seed. Duplicate tuples retain distinct occurrence IDs.

Every build stage and public read checks the source guard; data-returning reads also
check before release. Grant/capability revocation, credential rotation and expiry
invalidate leases with the same live dependencies as ordinary snapshots. Data itself
stays at the original revision. A data update alone does not rewrite historical
rows. Source host drop invalidates **all** leases, including administrator leases;
reopening the source creates a new authority and does not reactivate an old lease.

There is deliberately no public file reopen constructor. Rebuild from a fresh
session after restarting. The file is data, not a bearer permission or a portable
credential. Future reopen support needs a trusted binding to source identity,
authority and revision. Host code owns file cleanup. Files are created with
`create_new` and Unix mode 0600; existing paths are never overwritten. Raw filesystem
access by the same OS user remains trusted, as it is for the source database.
Revocation prevents API reads; it cannot erase already returned values or bytes
available to trusted filesystem readers.

## Format, bounds and indexing

The separate spill format is `LDS1`. Schema declarations are persisted as typed
JSON metadata, but active lookups use compiled Rust schemas and role offsets.
Rows encode the format marker, relation UUID, occurrence UUID and schema-directed
packed values, followed by SHA-256 of the record. Decode checks size, identity,
checksum, exact types, typed checks and trailing bytes. This does not change any
source log/checkpoint format or rewrite source records.

The primary B-tree key is `(relation UUID, occurrence UUID)`. Equality keys are
`(relation UUID, role slot, SHA-256(tagged value), occurrence UUID)`. Their fixed
68-byte size avoids copying large text/blob values into a second disk index.
Candidates are always compared with the exact decoded value, so even a digest
collision cannot return a false match. Ordering is by occurrence UUID, including
when an equality predicate is used. `after` and `next` have the same keyset meaning
as native pages; the returned revision remains fixed. This API does not offer value
range ordering.

Defaults: 8 MiB redb cache target, 1 MiB encoded row maximum, 8 MiB encoded page
maximum. A caller can choose a cache target from 1 MiB to 1 GiB. Row/page maximums
cannot exceed 64 MiB, and the row maximum cannot exceed the page maximum. At most
64 selected families with 256 roles each and 4 MiB total schema JSON are accepted.
Pages accept 1–1,000 rows and may stop earlier at the byte limit; follow `next`.
An oversized row fails the build rather than producing a partial usable lease.

The cache target is **not** a process-memory/RSS ceiling. redb metadata, dirty
pages, schemas, Rust containers and the OS file cache also consume memory. There is
no decoded-row cache. A read decodes at most a page plus one lookahead row; encoded
byte bounds exclude `Value`/container overhead, which is separately bounded by
role and row counts. Building uses existing shared source pages and commits bounded
batches at approximately 8 MiB of row/index bytes plus one row. Every batch uses
Immediate durability; metadata is completed last, and no lease is returned before
final persistence and the final authority check. A failed build removes its newly
created file when possible; a process crash can leave an incomplete orphan file.
There is no supported API for opening that file, and the host may remove it.

Point/page reads validate the records they touch. `verify()` performs a streaming
full metadata, count, checksum and equality-index audit, allocating one decoded
row at a time. It detects omitted indexes, unexpected entries and damaged rows.
A point read does not perform a whole-file audit on every access.

## Verification and memory measurement

Tests cover native point/page/equality parity across scalar domains, exact row
filters, reference closure, hidden metadata, live revocation, expiry, credential
rotation, source drop/reopen, historical reads, byte/count bounds, failed-build
cleanup, non-overwrite behavior, Unix permissions, record corruption and an injected
synthetic hash collision. A weak-Arc test proves original State and Row objects are
released while the disk lease remains usable.

The example installs a counting global allocator and reports **live Rust allocator
requested bytes**, not RSS, allocator arena retention, native-library allocations
or OS file cache. It builds 256 rows with 64 KiB blobs, creates a historical lease,
deletes current rows, measures retained allocations, reads the old rows, and measures
allocations freed by dropping only the lease. The latter isolates lease-exclusive
allocations from the current engine's schema, identity and index-version tombstones.
The redb cache and Rust-side redb allocations are counted. The current engine is
kept alive throughout so the weak authority guard remains valid.

Raw output: [disk-snapshot-memory.jsonl](benchmarks/disk-snapshot-memory.jsonl).
These local synthetic measurements illustrate the memory/latency tradeoff of a
historical read lease. They do not demonstrate a mutable database larger than RAM.

One local release run (256 × 64 KiB payloads, 1 MiB cache target) measured:

| Metric | Pinned memory | Disk lease |
| --- | ---: | ---: |
| Lease-exclusive live Rust allocations | 50,905,824 bytes | 1,176,519 bytes |
| Total live Rust allocations after deleting current rows | 67,852,923 bytes | 17,410,754 bytes |
| Historical point-read p50 | 1.667 µs | 125.750 µs |
| Historical point-read p99 | 2.417 µs | 176.209 µs |
| Lease construction | 0.000625 ms | 157.114 ms |

The disk artifact occupied 33,820,672 bytes. The live current-engine baseline still
contains identity/index-version tombstones, explaining why total retained allocations
exceed the lease-exclusive figures. Disk reads trade allocation retention for checksum,
decoding and storage work; they are considerably slower than in-memory point reads in
this probe. No production latency or RSS limit follows from this single local run.
