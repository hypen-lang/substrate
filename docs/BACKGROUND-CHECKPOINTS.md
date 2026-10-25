# Bounded background checkpoint scheduling

`Database::start_checkpointer` starts one optional host-owned maintenance worker.
It attempts early checkpoints at configurable redo thresholds and accepts explicit
coalesced requests. The existing synchronous `CheckpointPolicy` remains the hard
recovery bound. The worker does not change transaction acknowledgement semantics:
a successful application commit is already durable before maintenance runs.

```rust,ignore
let worker = db.start_checkpointer(CheckpointerOptions::default())?;
// The application continues writing and reading normally.
let ticket = worker.request()?;
let completion = worker.wait(ticket, std::time::Duration::from_secs(10))?;
println!("checkpoint covers revision {}", completion.revision);
worker.stop();
```

Run `cargo run --release --offline --example background_checkpoints` for a working
write, checkpoint and reopen example with paired foreground/background measurements.
`cargo test --offline engine::maintenance --lib` runs the focused tests.

## Scheduling and receipts

Defaults are a 250 ms polling/retry interval and early thresholds of 64 redo commits
or 4 MiB. Interval must be 1 ms through one hour and thresholds must be positive.
Explicit requests wake an idle worker immediately. There is exactly one worker per
database coordinator, even when the `Database` handle is cloned. The request queue
is one coalesced flag plus monotonic counters: there are no per-request channels,
callbacks, copied snapshots, or unbounded job list. Many tickets can be satisfied
by one checkpoint.

A ticket is a coverage request, not a promise to perform a distinct disk operation.
`wait` returns the completed ticket watermark and a covered logical revision.
An existing clean checkpoint can satisfy a request without another write. Requests
arriving during an active attempt are handled by a later attempt. Timeout does not
cancel work; calling `wait` again is safe. Unknown/zero tickets are rejected. A later
successful completion can satisfy an older ticket. Stats expose physical checkpoint
attempts that wrote, coalesced requests, busy retries, last covered revision, stop
state, and an error if the worker failed.

The worker uses a nonblocking attempt to acquire the existing persistence mutex.
When a writer owns it, maintenance retries later; it never queues a second physical
writer. After acquiring the mutex it obtains the current published state, releases
the publication mutex, and performs the ordinary atomic incremental checkpoint.
This ordering prevents a stale checkpoint from replacing a newer durable head or
pruning newer redo. Manual checkpoints and group commits share this same ordering.
Readers and live authorization gates continue using the published durable state
during disk IO. Commits still wait if maintenance already owns persistence.

A slow or starved worker cannot violate a configured hard tail bound: commits still
fold their group into a synchronous checkpoint at that bound. A host that explicitly
sets `checkpoint_policy: None` has opted out of that bound; periodic scheduling is
not a replacement guarantee. No hard limit on checkpoint duration or bytes of
maintenance work is added. Incremental dirty entries and the configured redo policy
limit usual work, but a legacy format upgrade may still require a full checkpoint.

## Lifetime and failure

The worker retains only weak database references while idle. Keeping a maintenance
handle alive after dropping all database/session/transaction handles does not keep
the file open. It notices closure on its next wake and stops. Dropping the maintenance
handle or calling `stop` wakes and joins the worker, cancelling unstarted requests.
An active disk operation completes before join returns; a stuck filesystem can
therefore delay shutdown. There is no unsafe interruption of redb persistence.
A new worker may be started after the previous one stops.

Checkpoint persistence failure stops the worker and marks the database failed,
matching manual checkpoint/commit uncertain-outcome handling. Further writes and
restricted live reads are denied until reopen and validated recovery. Successful
older receipts remain valid. No callbacks run under engine locks. This is not a
remote maintenance authorization API; only trusted embedding code holds `Database`.

## Verification

Nine focused tests cover 1,024 coalesced tickets, no-op receipts, invalid options,
periodic early thresholds, single-worker ownership, cancellation while the writer
is busy, file reopen with an idle handle still alive, concurrent independent writes
plus manual/background checkpoints, hard redo limits, retained snapshots and index
recovery. A gated storage backend proves restricted readers proceed while sync is
blocked and receipts cannot escape early; injected sync failure proves fail-stop.
Four subprocess crash points cover before writing, after pruning, before redb commit,
and after redb commit. Every crash recovers the same already-acknowledged logical
head, either from the previous checkpoint plus redo or the new checkpoint.

## Local measurement

[Raw paired runs](benchmarks/background-checkpoints.jsonl) use 256 sequential durable
writes of a 4 KiB payload, requesting maintenance every 16 writes. Foreground waits
for each explicit checkpoint; background enqueues requests and waits for final
coverage before measuring total completion. Both reopen with 256 rows and zero redo.
Three paired release runs on the shared arm64 macOS development machine measured:

| Median of run metrics | Foreground | Background |
| --- | ---: | ---: |
| Maintenance API call p50 | 5,029 µs | 3.083 µs |
| End-to-end writes/second | 221.3 | 224.0 |
| Commit p99 | 7.63 ms | 10.91 ms |

The small throughput difference is inconclusive; **this does not demonstrate a write
throughput improvement**. The enqueue call returns quickly because disk work happens
later. Background disk work still competes with commits and increased commit-tail
latency in this workload. Background runs performed 15–16 worker checkpoints versus
16 explicit foreground checkpoints; coalescing and any hard-bound maintenance are
not identical schedules. This slice provides bounded scheduling and host responsiveness,
not parallel disk writes or free checkpoint IO. Admission priorities, IO budgets,
maintenance lag observability and measured workload-aware scheduling remain future work.
