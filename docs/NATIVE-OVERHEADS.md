# Native write, snapshot and result overheads

This follow-up implements four opportunities from
[the investigation](PERFORMANCE-BEYOND-SQL.md). It does not change the durable
format, acknowledgement boundary, index definitions, or transaction dependencies.

## Admission without packed-row encoding

Commit admission now uses `codec::encoded_len`. The size pass validates the same
row arity/types and length limits as the encoder, counts fixed-width fields and
borrows variable-width payload lengths. Checked arithmetic retains exact queue
and group byte accounting. Durable commits still encode their redo once, after
revision assignment, and retain the existing checksum and Immediate redb commit.
Memory-only commits no longer construct a disposable redo payload.

Rare schema/grant events serialize into a counting sink for exact JSON escaping
and version behavior. They still perform metadata serialization again at durable
append; this optimization removes duplicate packed-row construction, not all
metadata traversal. No compatibility or crash-recovery format changes are needed.

## Differential physical index updates

A replacement row compares each role and unique-key tuple with its prior value.
Only changed role buckets, incoming reference edges and unique entries are
removed/reinserted. Unchanged persistent index nodes remain shared with old
snapshots. The family row itself is still replaced.

Every original version key is still updated: row, family, old/new buckets,
old/new incoming targets and unique keys. In particular, an unchanged bucket
continues to invalidate a transaction that read it. This is a physical-work
optimization, not weaker conflict detection. Compiled credential, capability,
view and visibility metadata still follows its normal remove/recompile path.
Large values can still be copied into dependency-version keys; this change does
not introduce shared-payload keys or optional indexing policies.

## Short snapshot publication lock

`Session::snapshot` captures the immutable state and failure flag under the
publication mutex, then resolves capabilities and constructs scoped grants after
releasing it. Live guards still carry the same credential, capability, expiry,
revocation and visibility dependencies. A concurrent revocation can invalidate
the newly returned snapshot before its next operation, exactly as with retained
snapshots. Commit serialization and the backend's single writer are unchanged.

## Projection before native payload copies

Native scan/lookup traversal borrows candidate IDs instead of first collecting
them all. Existing owned-row APIs retain their result types and ordering.
`Snapshot::table` also maps authorized rows directly into owned table values.
The new API projects before copying payloads:

```rust
let names = snapshot.projected_table("core.Person", "p", &["p.id", "p.name"])?;
```

Column names and duplicate/unknown-column errors follow `Table::project`.
An empty projection preserves row multiplicity. Authorization still checks the
complete source row and its reference closure, even when references are omitted
from output, and a final live check runs before returning owned values. This is
an eager result API, not a bounded-memory stream.

## Verification and measurement

Property tests compare exact sizing against both wire versions and every event
kind, including Unicode/escaped metadata, all packed scalar types and rejected
rows/length overflow. Differential index tests compare all indexes and version
maps against remove-and-reinsert histories and verify persistent sharing for
unchanged indexes. Native projection tests compare legacy projection results,
private reference filtering, empty/reordered projections and revocation; an OCC
regression proves unchanged bucket readers still conflict with payload updates.

The `native_overheads` example measures 2/8/32 scalar-role updates, unchanged
versus changed indexed fields, retained snapshots, 64 KiB payloads, durable
reopen/index verification, native projection and four-reader snapshot contention.
Each operation includes result disposal and excludes fixture setup, verification
and JSON reporting. Reported allocation bytes count Rust allocation requests
(including the requested size of reallocations), not live bytes or RSS. The
counter is enabled in both binaries and adds instrumentation overhead. Contention
latencies cover whole writer commits, not isolated mutex waiting.

The matched baseline is commit `41c7472`, archived in
`/tmp/substrate-native-baseline-41c7472`. The same example is copied there, with
the unavailable `projected_table` call replaced by its original
`table(...).project(...)` equivalent. The final current fixture explicitly installs
private families, matching the baseline grant-only access for benchmark readers;
the baseline uses its original `install` API. The current build also includes new
visibility enforcement, so whole-engine before/after timings cannot isolate each
individual patch. New schema visibility metadata is created during untimed setup.
Run release binaries serially, alternate their order, preserve three repetitions,
and do not build or run other benchmarks during timing.

Reproduce after building both frozen binaries:

```sh
python3 experiments/native/run_overheads.py \
  --baseline /tmp/substrate-native-overheads-before-41c7472 \
  --current /tmp/substrate-native-overheads-after \
  --output /tmp/substrate-native-overheads-repeat
```

The runner refuses existing result files, records executable fingerprints and
verifies identical workload descriptions. Binary assertions check retained
snapshots, current values, reopened durable values, indexes and projected output.

Final results and limitations: paired native benchmark (historical results removed).

## Restricted reads and contention follow-up

The follow-up to `2dac848` removes repeated family-tree lookups from full scans
and borrows indexed rows once. Snapshot scans acquire their visibility memo once
per operation; transactional scans still traverse every reference and record all
conflict dependencies, including denied targets. Compiled relations record their
reference slots. Snapshot-local family policies preserve the union of ambient
visibility and explicit grants; capability overlays still disable ambient access.
Only an unfiltered family without reference roles can skip per-row decisions.
Other families retain the complete reference-closure check.

Restricted output vectors grow only with authorized rows, unless the engine has
proved that every candidate is visible. Reserving space from a hidden physical
row count would expose that count through Rust's observable vector capacity.
Full owned reads still copy requested payloads; projection avoids those copies
only when the caller excludes those fields.

Capability installation now compiles the persistent scoped grant map once. A
capability session retains at most one effective snapshot, shared by its clones,
and reuses it only for the same published state, generation, capability, and
credential. Cache lookups and insertion never wait: contention falls back to
normal authorization outside the cache lock. Old cached snapshots are dropped
after unlocking. A new data revision always receives fresh data and authority
resolution; this is not an authorization cache across revisions.

Snapshot capture now loads one immutable atomic publication containing the state,
failure flag, and authority generation through `arc-swap`. Ordinary readers and
live fences do not acquire the writer/head mutex, including when the generation
has changed. Writers still serialize state publication; the atomic replacement is
the authority linearization point. State and generation are never captured from
separate publications.

An unchanged generation skips dependency traversal while retaining attachment,
poison, denial, and expiry checks. Changed generations validate dependencies
against the captured current publication. Failure is published atomically;
generation exhaustion permanently disables the shortcut. Historical transaction
bases explicitly disable it. Live guards hold weak references to the publication
slot and originating state, so they neither keep a detached database alive nor
retain later row snapshots. SQL retains entry, page, and final release fences.

Fresh snapshots share an immutable live-authority payload through a per-session
atomic cache keyed by weak state identity, generation, principal, and credential.
The cache retains no row state. Every snapshot still captures the latest published
data; a changed source resolves authority again. Transaction-specific expiry
changes use copy-on-write, so they cannot mutate another snapshot's authority.
Scalar point reads use the existing compiled row policy directly because they have
no reference closure to memoize. Reference-bearing rows keep the memoized closure.

`Session::get(relation, id)` returns an owned row from a fresh committed publication.
On a warmed scalar read it borrows both the publication and matching cached
authority. The session already owns the database handles, so its entry and release
fences can borrow those handles without incrementing global reference counts.
Live revocation, expiry, poisoning, and fail-stop checks remain in both fences.
Compiled grants use persistent nested maps keyed first by principal, then relation.
Permission checks borrow `&str` and allocate no temporary principal key. The same
representation serves capability overlays; canonical iteration remains ordered by
principal and relation, so log/checkpoint/replication declarations are unchanged.
The ordinary scalar owned path resolves its compiled grant once and reuses it for
relation and row permission checks. Filtered grants still combine with ambient
visibility; anonymous callers cannot acquire explicit-grant authority.
Capability-scoped scalar reads cache only the compiled capability metadata and
the same weak source key, including the capability ID in cache matching. They
apply the scope's compiled read grant and row filter without ambient visibility.
Reference-bearing reads use the existing snapshot path; a changed publication
first resolves authority through that path as well.
Use a retained `Snapshot` when several operations must observe the same revision.
Retained snapshot reads keep their original attachment fence and relation lookup.
Transaction construction moves the temporary snapshot's state and
authority handles instead of immediately cloning and dropping them.

`Session::get_shared` uses the same fresh-data and authorization path and returns
`Arc<Row>` without copying Text/Bytes payloads. The immutable row remains valid
across later writes; already released data cannot be recalled by revocation.
Owned result mapping is followed by a release fence, including on cache misses.
Credential expiry still reads current system time at both fences. The monotonic
clock now loads its last observed second first, performing an atomic maximum only
when time advances, avoiding a shared cache-line write for unchanged seconds.

The focused `read_contention` example compares fresh and retained snapshot reads,
fresh session reads, snapshot construction, and shared versus per-worker `Arc`
clone controls. These are distinct APIs/workloads: a faster `Session::get` result
does not imply the same gain for existing `snapshot().get()` callers. The probe
reports process CPU and sampled latency; run it alone to avoid CPU interference.

An October 25 code-generation check found that adding the scoped/shared mapping
helper caused LLVM to inline it into ordinary owned `Session::get`, interleaving
the implementations. On this arm64 release build, the function's stack allocation
grew from the preserved baseline's `sub sp, sp, #0x140` (320 bytes) to `#0x1d0`
(464 bytes). Keeping that helper out of line reduced the final allocation to
`#0x130` (304 bytes). This restored the separate ordinary path, but did **not**
eliminate the measured concurrent-read regression; stack size alone does not
establish its cause. The baseline binary remains unchanged at
`/tmp/substrate-scale-before-bottlenecks-20261025`. Reproduce the small assembly
excerpt with the following command, substituting that path for the current binary
to inspect the baseline:

```sh
otool -tvV -p __ZN16substrate_liquid6engine7Session3get17h7723acbcbfe56448E target/release/examples/scale_benchmark | head -25
```

The symbol hash is build-dependent; `nm BINARY | rg 'engine7Session3get'` locates
it. Matched profiles use `experiments/scale/profile_reads.py --binary BINARY
--output OUTPUT --point-api session --clients 32` with one million rows. Profiles
sample execution and add overhead, so their TPS is not an unsampled comparison.
The matched `bottlenecks-profile-before` and `bottlenecks-profile-after` artifacts
show allocator-library leaves at about 32% and 31% of collapsed samples,
respectively; ArcSwap and the borrowed authority guard each account for less than
1%. The largest deduplicated application symbol resolves via `nm` to two identical
`imbl::OrdMap::get` instantiations, accounting for approximately 25–26%. These
profiles include the harness's result validation, formatting, and latency
measurement. They do not establish that allocator layout caused the regression,
and do not support attributing it to newly contended authority reference counts.

This removes mutex acquisition and repeated authority construction, not all shared
reference-count traffic or per-snapshot allocation. Fresh-snapshot scaling must still be benchmarked independently of reads
within a retained snapshot. Tests cover capture during concurrent publication,
revocation, credential rotation, expiry, fail-stop, poisoned writers, generation
exhaustion, and continued read progress while the writer mutex is held.

The follow-up measurements (historical results removed) compare against
`2dac848` and SQLite. In addition to the original finite-reader workload, the
native harness keeps four readers running throughout all writes and records
their progress and latency. This distinguishes faster writer commits from
readers merely finishing sooner. The SQLite owned-scan control materializes the
same identities, integers, and 64 KiB BLOBs into the same Rust `Row` output type;
it does not implement Substrate authorization.
