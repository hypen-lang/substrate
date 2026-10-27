# Durable group commit

Independent commits share one `redb::Durability::Immediate` transaction while retaining their own logical revisions and conflict outcomes. Successful durable commit acknowledgement follows persistence and publication. No relaxed durability mode or early acknowledgement is used.

`Database::open_with_options` uses group commit automatically. Configure grouping independently of checkpoint policy:

```rust
use substrate_liquid::{Database, DatabaseOptions, GroupCommitOptions};
use std::time::Duration;
let db = Database::open_with_commit_options(
    "liquid.redb",
    DatabaseOptions::default(),
    GroupCommitOptions {
        max_group_transactions: 64,
        collection_window: Duration::ZERO,
        ..Default::default()
    },
)?;
# Ok::<(), substrate_liquid::Error>(())
```

The default collects for up to 500 microseconds when the current queue or previous group shows concurrent demand, and caps a group at 128 transactions. Steady isolated traffic skips collection; the first request after a concurrent group can still wait once. Set `collection_window: Duration::ZERO` to drain only queued work. Calls arriving during persistence accumulate for the next group. Enqueue notifications fire only on a batch fill threshold crossing, avoiding repeated wakeups of waiting writers; completion still wakes every caller. A single client awaiting each commit cannot benefit from grouping; use larger application transactions when atomic batching is appropriate. `max_group_transactions: 1` disables grouping while preserving the same publication and durability machinery.

## Ordering and failure semantics

A submitting caller becomes coordinator for one group. It validates each request's original read dependencies and live authorization against the evolving tentative state, in queue order. Accepted writes apply to that state and each receive the next revision. A conflict or expired/revoked authority rejects only that request. The coordinator checks read-only transactions too; they retain the revision reached at their position and allocate neither a new revision nor their own disk commit. If they depend on earlier tentative changes, their acknowledgement waits for those changes to persist.

For a normal group, all accepted redo records and the final durable head are inserted in one Immediate redb transaction. Crossing a configured recovery bound instead folds the entire accepted group into one atomic incremental checkpoint, with the final revision and covered tail pruning. There is no intermediate on-disk or published snapshot between members of a group. Reopening recovers either the previous state or the complete group, while redo records retain individual revisions when not checkpointed.

The storage writer mutex serializes validation, persistence, and publication with explicit checkpoint maintenance. The writer uses a short publication mutex, while readers capture an immutable state/generation/failure object through `arc-swap`; reads and live authorization fences continue against the previous durable state during synchronization. Explicit checkpoints follow the same lock order. Storage/recovery information calls can wait for maintenance; normal snapshots do not acquire the storage writer lock.

After persistence, the coordinator publishes the final snapshot and releases acknowledgements. It hands coordination to another waiting submitter instead of draining indefinitely before returning its own result. There is no background worker retaining a database handle, no request-to-database ownership cycle, and retained snapshots remain weakly linked to live authority.

Any uncertain persistence failure marks the handle failed. Every otherwise accepted group member receives a storage error; later writes and maintenance require reopening. Rejected requests keep their conflict/authorization result. A storage error does not establish that the write is absent on disk; reopening determines the actual durable outcome. Successful acknowledgements are never returned before synchronization. As before, an unacknowledged commit can nevertheless be present after a crash.

Capability/credential expiry is checked at ordered acceptance. Expiry occurring during disk synchronization does not retract an already accepted durable write. Revocation becomes externally visible at group publication. A revocation earlier in a group prevents subsequent dependent requests from committing; requests ordered before it can commit.

## Bounds and metrics

Defaults are 1,024 queued requests / 256 MiB of queued encoded redo bytes, and 128 requests / 128 MiB per active group. Bounds apply separately to the pending queue and active group, so both may coexist. These are redo-size bounds, not total process-memory limits: snapshots, transaction construction, indexes, and request metadata consume additional memory. Admission validates and measures encoded size without producing redo bytes. Persistence encodes accepted redo once; count-bound checkpoint groups skip redo encoding entirely because those bytes would be discarded.

A full queue rejects admission with `Error::Constraint("commit queue is full; retry with backoff")`. A transaction larger than either byte limit is rejected, without consuming a revision; there is no oversized-transaction exception. Applications can increase both limits explicitly for large imports. Existing conflict-only retry helpers do not automatically retry queue-overload errors. Optional collection windows are bounded to one second; the benchmark evaluates 100 and 500 microseconds. The default is 500 microseconds under concurrent demand; collection is skipped for isolated writers.

`group_commit_stats()` reports successful write revisions, durable groups, largest successful write group, and current queue occupancy. A durable group counts one successful Immediate redb transaction, including an automatic checkpoint. It does **not** count underlying `sync_data`/filesystem calls: redb can synchronize multiple times internally. Explicit maintenance checkpoints and database creation are not counted as write groups. Stage counters cover writer-lock wait, validation/application, persistence, and publication. These are scoped cumulative wall times, not exhaustive CPU attribution. `StorageInfo::persistence` separately counts redo/checkpoint bytes, checkpoint rows, and nested Immediate-commit time; do not add overlapping timing categories together. Counters reset on open.

## Verification

`src/commit_tests.rs` exercises ordered conflicts and read fences; revocation and schema-install expiry; individual revisions; checkpoint boundaries; request/count/byte limits; concurrent submitters and explicit checkpoints; a property-based ordered reference model with index checks; retained-snapshot drop/reopen behavior; whole-group injected sync failure; and eight subprocess crash cases before/after persistence and before/after publication, with and without an automatic checkpoint.

A gated `StorageBackend::sync_data` proves restricted readers and their live authorization checks proceed on the previous state while disk synchronization is paused. It proves no acknowledgement escapes that gate, including checkpoint groups, and verifies that expiry during synchronization does not turn an accepted durable write into a false denial. Failure injection verifies every accepted member fails together and subsequent writes stop until recovery. These tests simulate process crashes and backend errors, not physical power loss on every filesystem.

## Reproducible benchmark

Run the working threaded example:

```sh
cargo run --release --example group_commit -- 64
cargo run --release --example group_commit -- 64 conflicts
cargo run --release --example group_commit -- 64 checkpoint
```

Each invocation measures 1, 8, and 32 concurrent writers with 64 attempts per writer, comparing single-transaction durable groups with default grouping and 100/500 microsecond collection windows. A concurrent point-reader samples latency. Commit latency measures the `commit()` call; throughput also includes transaction construction and thread startup. Every case checks indexes, row counts, revision counts, and recovery after reopening. The conflict scenario updates one shared row without retrying; the checkpoint scenario checkpoints every 32 accepted revisions. Reader timing excludes the 50 microsecond sampling sleep.

Historical measurements below used the earlier 64-transaction/zero-window defaults. Current defaults and comparisons are documented in capacity tuning (historical results removed). The historical measurements are medians of three runs on the same local macOS 26.2 filesystem with optimized Rust builds. This was a short benchmark on a shared development machine, not an isolated production load test. Raw results are in independent repeats (historical results removed), conflicts (historical results removed), and checkpoint boundaries (historical results removed).

| Writers | Group/window | Accepted tx/s | Tx/durable group | Commit p50 ms | Commit p99 ms | Reader p99 µs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Single / 0 | 255 | 1.00 | 3.983 | 4.679 | 0.75 |
| 1 | 64 / 0 | 257 | 1.00 | 3.979 | 4.163 | 1.04 |
| 8 | Single / 0 | 266 | 1.00 | 30.001 | 36.023 | 1.00 |
| 8 | 64 / 0 | 1,131 | 4.00 | 6.994 | 8.122 | 0.88 |
| 8 | 64 / 100 µs | 1,941 | 7.76 | 3.985 | 6.143 | 1.50 |
| 8 | 64 / 500 µs | 1,927 | 8.00 | 4.005 | 6.683 | 1.21 |
| 32 | Single / 0 | 274 | 1.00 | 116.980 | 138.601 | 2.00 |
| 32 | 64 / 0 | 4,103 | 16.00 | 7.903 | 9.862 | 1.92 |
| 32 | 64 / 100 µs | 5,131 | 20.69 | 4.178 | 9.792 | 1.92 |
| 32 | 64 / 500 µs | 7,340 | 31.03 | 3.988 | 7.824 | 2.71 |

Default grouping improved measured throughput about 4.3× with eight writers and 15.0× with 32 writers. The sequential case did not meaningfully improve. With automatic checkpoints, the single recorded 32-writer run improved from 275 to 4,129 accepted transactions/s with zero delay. For the shared-row conflict scenario, almost all concurrent contenders conflicted, accepted transactions per group remained 1, and grouping did not improve accepted throughput materially. Group commit amortizes persistence for independent accepted work; it does not make conflicting work independent.

This comparison isolates grouping against `max_group_transactions=1` in the same implementation. Both variants already have the separated publication lock, so these numbers do not quantify improvement over the former reader-blocking implementation. Reader maxima and sample counts are retained in the raw data; a low p99 does not establish absence of scheduling outliers. redb still executes one physical write transaction at a time.

The latest capacity tuning report (historical results removed) measures these defaults and the enqueue wakeup change at 32 and 128 writers, including the remaining tail-latency and storage-traffic tradeoffs.
