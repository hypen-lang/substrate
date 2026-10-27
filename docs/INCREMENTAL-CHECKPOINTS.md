# Incremental checkpoint storage

This documents the format-2 milestone. The current format-3 overlay, partial
compaction and upgrade rules are in [ADR 011](adr/011-bounded-checkpoint-overlay.md).
Checkpoint I/O now runs outside publication locking, and optional background
checkpoint scheduling is implemented; the mutable working set still resides in RAM.

Checkpoint format 2 updates only the row and catalog entries changed since the previous checkpoint. Repeated changes to one row coalesce to one final upsert or deletion. Additive schema modules, allocated identities, principal grants and schema grants are individually keyed metadata entries, so appending identities no longer rewrites the entire identity catalog every 128 commits.

This addresses the fixed-interval full-rewrite path. It does **not** move checkpoint IO outside the publication lock or make the working set disk-resident. Background checkpoints and larger-than-memory serving remain separate architectural work.

## Atomicity and recovery

The live `Durable` handle tracks a bounded-tail dirty set: row keys and the last metadata operation per logical key. After a normal redo append commits, its operations enter this set. Opening a database reconstructs the same set while validating/replaying its redo tail; there is no second independently persisted dirty-state ledger that could diverge from the log.

An automatic checkpoint includes the threshold-triggering transaction directly, preserving the existing strict redo bounds even for an oversized transaction. A manual checkpoint includes all acknowledged tail changes. One Immediate-durability redb transaction applies dirty row and metadata changes, replaces the manifest/head, changes the format marker when necessary and prunes the covered log. Publication remains serialized; failure continues to fail-stop the live engine until reopen. A crash sees the complete previous checkpoint/tail or the complete replacement, never a mixture.

The manifest still records revision, row/identity/module/grant/schema-grant counts and total encoded value bytes. The byte total is adjusted using replaced/deleted entry sizes; counting/encoding unchanged rows is unnecessary. Recovery checks that reconstructed counts and byte lengths match, validates every schema/record/reference, and verifies the head and contiguous redo sequence. Metadata keys are validated against their decoded operations. Duplicate/misplaced metadata, malformed values, missing sections and unsupported versions fail closed. Redb continues to provide its underlying page integrity and atomic commit protocol.

## Formats and compatibility

- Redo events remain `LQ01` and the frozen schema/grant event format remains unchanged.
- Existing log-only `LDB1` files and monolithic-catalog `LCP1` checkpoints still load.
- The first checkpoint of an `LCP1` database performs one complete conversion into format 2. Schema/entity IDs and logical rows remain unchanged.
- New checkpoints use the `LDB2` store marker, `LCP2` manifest, `LCC2` catalog marker and `liquid_checkpoint_metadata_v2` table. Older binaries reject this version rather than silently misreading it; downgrade is not supported.
- Format-2 checkpoint entries use revision-independent `LQ01` envelopes with revision zero. Their enclosing manifest determines the checkpoint revision. This keeps unchanged entry bytes stable across checkpoints. Checkpoint-loaded dependency versions are rebuilt on open, as before; the live engine's versions are never reset by checkpointing.
- Metadata keys have an operation-class prefix and length-framed components. Modules sort before allocated identities and policies; all metadata loads before rows. Import/reference closure is checked after reconstruction. Revoked grants are removed from checkpoint metadata, while allocated identity tombstones remain.

`CheckpointInfo` now reports `written_rows`, `deleted_rows`, `written_metadata`, `deleted_metadata` and `written_bytes`, alongside total live rows/bytes and pruned-tail counts. These are checkpoint table-entry work counters. They do not measure device writes, redb copy-on-write page amplification, fsync traffic or control-table overhead. Repeated no-op checkpoints change zero row/metadata entries, although a control transaction may still perform IO.

## Verification

The durability suite checks subprocess exits at all checkpoint publication boundaries, torn writes and injected IO/sync errors, plus fail-stop behavior. New tests cover:

- A checked-in frozen format-1 checkpoint, one-time conversion and stable identities.
- Recovery of the dirty set from a log tail, followed by exactly two changed-row writes and one deletion in a 100-row database.
- Byte-identical untouched rows across an incremental checkpoint.
- Coalesced repeated updates and identity retention for a row inserted then deleted before checkpoint.
- Deterministic no-op manifests and zero entry changes.
- Missing, truncated and incorrectly keyed metadata rejection.

The checkpoint integration suite and property-based histories also verify references, indexes, policies, threaded writes, strict automatic bounds and restarts. The format conversion preserves the existing capability/credential log and checkpoint recovery paths.

## Reproduce the scaling probe

Run `cargo run --offline --release --example checkpoint_scaling`. It performs 512, 1,024 and 2,048 single-row durable inserts, checkpoints every 128 inserts, runs three samples per size, then reopens each database and verifies the row count/indexes and an empty redo tail. Timings include immediate-durability commits; they are not memory-only throughput measurements.

Before/after raw results and the measurement discussion are in the benchmark report (historical results removed). The deterministic entry-write reduction is the primary complexity evidence; wall-clock results depend heavily on shared-machine disk contention.
