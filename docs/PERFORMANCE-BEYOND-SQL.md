# Performance opportunities beyond SQL

Investigated on 2026-10-25 against `ae58e56`. This is a source and existing-evidence
review, not a new benchmark run. No native query, storage, authorization or
transport implementation was changed. The SQL agents' current work is separate.
Historical timings below identify useful experiments; they are not measurements
of the current checkout under the current machine load.

## Recommended sequence

| Priority | Opportunity | Evidence | First safe slice |
| --- | --- | --- | --- |
| 1 | Remove duplicate write encoding and unnecessary index mutation | Directly visible in commit/staging/application paths | Exact encoded-size accounting or revision-independent encoding; skip physical mutations of unchanged index entries while preserving existing dependency versions |
| 2 | Shorten publication-lock work for capability-bound snapshots | Capability resolution and effective-grant construction run inside `Session::snapshot`'s head lock | Capture state/failure under the lock, resolve against that immutable state outside it, retain all live-authority fences |
| 3 | Avoid full payload copies in native result construction | Owned scan/lookup and native `Table` materialize before projection | Add bounded projected/cursor APIs over existing authorized shared-row primitives; keep owned APIs compatible |
| 4 | Make checkpoint scheduling respect foreground tail latency | Existing paired benchmark improved enqueue latency but worsened commit p99 | Measure writer wait, checkpoint duration/bytes and maintenance lag; defer optional work under pressure while retaining synchronous hard redo bounds |
| 5 | Bound and measure authorization/history memory | Snapshot memo grows with visited graph; mutable state and indexes remain in RAM | Memo-size/history-lifetime metrics, bounded memo experiments, dependency-aware transaction memoization |
| 6 | Remove transport head-of-line and connection overhead where needed | Persistent sessions help, but each session is sequential and each accepted connection owns a thread | Explicit bounded client pooling before multiplexing or an async-server rewrite |

Benefits are workload-dependent. No percentage speedup is promised for the
unimplemented slices. Native point lookup is already very cheap in small warm
fixtures; copying large results, write construction, lock hold time and resource
growth are more plausible first targets than replacing the collection crate again.

## Writes: duplicated construction and index work

`Transaction::stage` applies each operation to the transaction's persistent working
state; `Coordinator::process` applies accepted operations again to the evolving
committed state. That second application is necessary today because other commits
may have intervened. It must not simply publish the transaction's old working
state. See [engine staging](../src/engine.rs) and [ordered acceptance](../src/commit.rs).

`Transaction::commit` calls `codec::encode(0, ...)` solely to obtain its length.
`Durable::append_group` encodes the accepted records again with assigned revisions.
Even memory-only commits currently pay the admission encoding. The encoder walks
and copies packed values and serializes metadata events; it is not a cheap length
lookup. A checked size pass or reusable revision-independent payload could remove
allocation/copy work, with exact queue accounting and checksums recomputed as
required. Do not replace admission bounds with an unchecked estimate. Sources:
[commit admission](../src/engine.rs), [durable append](../src/durable.rs),
[versioned codec](../src/codec.rs).

`State::apply(Op::Put)` calls `remove_row`, then reinserts every role index,
reference edge and unique key. A one-field update therefore mutates indexes for
unchanged values too. Every role is indexed, including text/byte payloads, and
owned `Value` keys also occur in the version map. This makes wide rows and large
payloads important allocation/CPU fixtures. Start by avoiding physical index
mutations for unchanged values while retaining today's version/dependency
behavior. Relaxing unnecessary OCC invalidation can be a separate, explicitly
tested change. Opt-in index policies or shared immutable key payloads require a
larger schema/storage design and should not silently make existing lookups scan.
Source: [state layout and row application](../src/storage.rs).

Measure one-field versus full-row updates, 1/8/32 roles, 16 B–1 MiB payloads,
single versus 32-operation transactions, and retained versus unretained snapshots.
Report allocation bytes and CPU separately from durable latency. Existing imbl
experiments found roughly twofold faster isolated writes but mixed reads; they
did not remove this structural work. [Collection experiment](benchmarks/COLLECTIONS-EXPERIMENT.md).

## Concurrency: logical independence is not parallel commit execution

The coordinator's writer mutex covers ordered OCC validation, application,
persistence and publication, including memory-only databases. redb still has one
physical writer. Group commit correctly amortizes synchronization but does not
make this CPU work parallel. The prior benchmark measured about 257 sequential
durable commits/s, 4,103 tx/s with 32 writers and default grouping, and 7,340 tx/s
with a 500 µs collection window. Those are short historical shared-machine runs,
not a reason to add a global default delay. [Group commit evidence](group-commit.md).

First measure queue wait, validation/application time, encoding time and storage
time separately. Improve duplicate work before attempting parallel publication.
Parallel preparation could help large groups, but final dependency and authority
validation must remain ordered against the evolving accepted state. Preserve
durable acknowledgements, individual conflicts and uncertain-outcome fail-stop.

An independent smaller lock opportunity exists in `Session::snapshot`: it captures
the head, then resolves capability ancestry and builds effective grants while the
head mutex is still held. Capturing the immutable state and failure flag first
would let that work happen outside the publication lock. Every returned snapshot
must still carry the same revocation/credential/expiry dependencies, so a concurrent
revocation denies its next operation. Benchmark many readers and capability
scopes of different sizes, including revocation during construction. This is a
code-path opportunity, not a demonstrated contention bottleneck yet.
Source: [snapshot authorization and live guards](../src/engine.rs).

## Native reads: ownership and authorization costs

`Snapshot::read_rows` first collects all candidate identities, then clones each
visible `Row`. `Snapshot::table` calls the owned scan before native relational
projection, and `Table::project` clones selected values again. The shared-row
`get_shared`, `page_shared` and `range_page_shared` primitives already provide a
safer foundation for bounded projected access. Keep current public owned-row
semantics; add a new projection/streaming surface rather than unexpectedly changing
ownership, result order or lifetime. Never expose internal shared rows in a way
that can bypass their final live-authority check. Sources:
[owned reads](../src/engine.rs), [shared cursors](../src/dx.rs),
[native relational operations](../src/query.rs).

Snapshot visibility memoization already reduced a historical shared-chain scan
from 248.497 to 1.128 ms at 1,000 chain nodes/roots. Do not reimplement the old
per-row traversal. The remaining memo grows with all nodes visited in the snapshot
and uses one mutex shared by its clones. Independent snapshots have independent
memos. Transactional reads intentionally still traverse without this memo so they
retain every OCC dependency; copying the snapshot optimization into transactions
would be incorrect unless it also replays the complete dependency set.
[Visibility design/evidence](VISIBILITY-PERFORMANCE.md),
[memo and reference traversal](../src/storage.rs).

Useful next tests: concurrent clones of one snapshot versus separate snapshots;
large cycles with denied exits; repeated transactional references and complete
dependency parity; long-lived snapshots with changing current state. Capability
issuance also scans compiled capabilities for the per-issuer quota, despite its
8,192-entry global bound. An issuer count index is a possible small follow-up,
with quota phantom dependencies preserved. [Capability quota](../src/capability.rs).

## Checkpoints, recovery and memory

Background maintenance uses the same writer mutex as commits and performs the
checkpoint while holding it. Its enqueue API is fast, but the historical paired
workload showed essentially unchanged throughput and commit p99 worsening from
7.63 to 10.91 ms. This is already an observed non-SQL regression worth addressing.
Measure foreground wait, dirty bytes, flush duration and maintenance lag before
changing scheduling. Optional checkpoints can defer to pressure; configured hard
redo bounds must still trigger synchronous coverage. Never publish a stale
checkpoint or prune newer redo. [Evidence](BACKGROUND-CHECKPOINTS.md),
[maintenance locking](../src/maintenance.rs).

Incremental checkpoints reduce writes, not the size of the live mutable state.
Recovery still iterates all checkpoint rows and rebuilds in-memory state/indexes
before replaying the bounded tail. Thus bounded redo does **not** mean startup is
independent of total database size. Packed durable rows are decoded to owned
values and persistent indexes in RAM. Measure recovery time, RSS and allocations
at 100k/1m+ occurrences and wide payloads, separately for schema, row decoding,
index reconstruction and tail replay. [Recovery](../src/durable.rs),
[state representation](../src/storage.rs).

The disk-snapshot feature is an immutable historical lease, not an out-of-core
mutable engine. Its existing 256 × 64 KiB example reduced lease-exclusive Rust
allocations from 50.9 MB to 1.18 MB, but point-read p50 rose from 1.667 to
125.750 µs and spill construction took 157 ms. Extending this boundary with
selected lazy families/indexes is a reasonable measured prototype; replacing all
reads at once is not justified. Stable identity allocation and version/history
metadata also need explicit retention rules—blindly dropping old version keys
can break OCC for retained transactions. [Disk lease evidence](DISK-SNAPSHOTS.md).

## Transport and operational scale

Persistent TLS/Unix sessions already avoid reconnecting per operation, but normal
`request` methods still connect per call. Each persistent connection processes one
command at a time; Node callers queue and Rust uses mutable session access.
The server creates a bounded-admission worker thread per accepted connection.
Connection-per-request remains an easy accidental bottleneck in application code.
Historical loopback medians improved from 321 to 54 µs for persistent TLS and
86 to 40 µs for persistent Unix; these predate the latest SQL implementation and
must be rerun for current end-to-end claims. [Transport evidence](PERSISTENT-TRANSPORT.md),
[TLS server/client](../src/remote.rs), [Unix server/client](../src/local.rs).

Add an explicit small bounded connection pool for independent requests before
introducing protocol multiplexing. Preserve per-command authentication, sequential
transaction boundaries and the prohibition on automatic retry after uncertain
mutations. Measure queue wait and completion latency at 1/8/32/128 clients with
native reads, writes and slow commands—not just `SELECT 1`. Cancellation, CPU
admission and process memory limits remain separate work; socket deadlines do not
preempt a running engine computation.

Replication and sharding do not currently remove these local bottlenecks. Existing
replication is trusted-host ordered apply with eventual authorization propagation;
partitioning is fixed local routing. They are not consensus/failover or a proven
parallel distributed write backend. [Replication boundary](REPLICATION.md).

## Acceptance gates for follow-up implementations

Every optimization should retain a frozen baseline and run matched workloads with
the same durability, result ownership and authorization. Require exact result/index
parity, snapshot retention tests, revocation during operations, conflict/phantom
tests and crash recovery for persistence changes. Include p99, allocation/RSS and
queue/lock wait where relevant; do not judge work from throughput alone.

The SQL baseline from `ae58e56` was preserved before current builds as
`/tmp/substrate-range-ae58e56`, SHA-256
`dfdbb7733f28c08d67afa46f5c0a3d242122cdb5f5a592440837835b5e090584`, matching the
prior [range/EXISTS report](benchmarks/DATAFUSION-RANGE-EXISTS.md). This investigation
ran no builds, timings or external services.
