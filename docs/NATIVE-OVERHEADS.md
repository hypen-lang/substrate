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

Final results and limitations: [paired native benchmark](benchmarks/NATIVE-OVERHEADS.md).

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

Live fences use a publication generation captured with the state under the head
mutex. An unchanged generation permits a read fence to avoid that mutex while
still checking database attachment, poison, denial, and expiry. Every commit and
replication publication advances the generation under the same mutex. Failure
and counter exhaustion permanently disable this shortcut. Changed generations
use the original locked dependency validation. Historical transaction bases
explicitly disable the generation shortcut. SQL retains entry, page, and final
release fences; constructing buffered output needs deadline checks per row but
does not need another head lock per output row.

The [follow-up measurements](benchmarks/READ-REGRESSIONS.md) compare against
`2dac848` and SQLite. In addition to the original finite-reader workload, the
native harness keeps four readers running throughout all writes and records
their progress and latency. This distinguishes faster writer commits from
readers merely finishing sooner. The SQLite owned-scan control materializes the
same identities, integers, and 64 KiB BLOBs into the same Rust `Row` output type;
it does not implement Substrate authorization.
