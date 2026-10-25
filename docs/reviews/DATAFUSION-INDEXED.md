# Indexed DataFusion and planning-cache review

Reviewed on 2026-10-25 against commit `772968f`. This was an independent review of
the implementation and upstream DataFusion 55.2 operator contracts. The reviewer
made no implementation edits and ran no competing builds or benchmark timings.

## Scope

- Lazy `LiquidTable` / `LiquidScanExec` reads through authorized pinned snapshots,
  identity and role-equality indexes, numeric ranges, exact predicate rechecks,
  canonical UUID strings, projection, residual filters and LIMIT handling.
- Live authorization and host-lifetime checks during planning, source reads and
  result release; row policies and relationship reference visibility; fresh
  per-execution source budgets and failure behavior.
- One-binding planning cache, typed parameter keys, optimized logical plans,
  physical-template cloning, volatility and view/subquery exclusion, repeated
  execution, failure eviction, and timeout/concurrency isolation.
- DataFusion's concrete filter, projection, limit, empty-row and cooperative
  wrapper implementations used by the physical-template whitelist. Unknown,
  stateful join/sort and dynamic-filter plans are not reused as physical templates.

No concrete authorization bypass, false-negative predicate pushdown, stale
parameter result, shared execution-state defect, or transaction-boundary change
was found in the reviewed code. Native query implementation files are unchanged.
This is a review, not a security proof or exhaustive validation of DataFusion.

## Finding and fix

**P2: two-sided numeric predicates initially scanned an unnecessarily large tail.**
The first provider implementation selected one range bound and rechecked the
opposite bound after reading candidates. A query such as `n >= 0 AND n < 5` could
scan an entire positive-valued family and exhaust the source-row budget despite
having only five results.

The provider now intersects the tightest lower and upper bounds for the selected
role, preserving inclusive/exclusive ties and empty contradictory ranges. The fix
was re-reviewed. Regression checks use EXPLAIN ANALYZE to require exactly five
source rows for a small interior range, including reversed conjunct order and
redundant bounds. Other predicates remain checked on candidates.

An additional performance observation was addressed during integration: parameter
ScalarValue conversion now happens only on a cache miss. The coordinating agent's
integration test also found that DataFusion wraps simple scans in CooperativeExec.
That exact wrapper was added to the whitelist after inspecting its replacement
and execution contracts: execution scheduling state belongs to each new stream,
not to the cached wrapper. Its children still require recursive validation.

## Verification evidence

The reviewer inspected completed logs showing **8 indexed integration tests** and
**8 focused DataFusion unit tests** passing. Coverage includes identity/equality/
range candidate metrics, differential range predicates and paging, row-policy and
reference-visibility checks, revocation, noncanonical UUIDs, unused large payloads,
parameter rebinding, bounded shared source accounting, fresh physical nodes,
failure eviction, and stable/volatile/view/subquery cache exclusion. At that stage, the broader integration suite and benchmarks were still being
coordinated separately. The final integrated verification was inspected later and
is recorded below.

No outstanding actionable finding remains in the reviewed scope.

## Remaining limits

The cache holds one binding per prepared query, so changing bindings requires
planning again. General joins and sorts still receive fresh physical plans;
index access does not imply an index nested-loop join implementation. Cold full scans
still convert authorized rows into Arrow during execution; eligible repeated
scans can reuse the bounded snapshot-local page cache reviewed below. Source-row/byte budgets
and cooperative cancellation are accounting controls, not hard CPU or process-RSS
isolation. Authorization checks are retained on cache hits and immediately before
results leave the adapter.


## Follow-up: bounded projected-page cache

The provider's later performance refinement was reviewed independently after the
initial index/planning review. One private BatchCache is created per SQL snapshot
and shared only by that snapshot's registered providers. Keys include the relation
identity, ordered projected-column list and page continuation. Only unbounded
family scans without pushed predicates use it; fixed 1,000-row source pages make
the key sufficient. Index candidates and partial LIMIT pages are not cached.

Cached pages hold immutable Arrow batches and continuation metadata, not execution
cursors or budgets. Every hit recharges the current execution's source-row and
payload-byte budgets and checks live authority before and after access. A cached
batch cannot escape through the public API. The cache is not shared across
principals or revisions, and retained pages remain pinned after ordinary data
updates. Revocation, expiry and host loss still deny further access.

The FIFO is bounded by 64 MiB of accounted buffers/bookkeeping and 4,096 entries.
Duplicate concurrent fills retain one entry and one charge; oversized pages are
not retained. Eviction releases the cache's ownership, while active executions
can temporarily keep their own immutable buffer references. Thus this cap bounds
retained cache entries, not all process memory or all in-flight buffers. Poisoned
locks return errors; conversion failures cannot insert invalid pages. No ownership
cycle between source snapshots and cached batches was found.

The reviewer inspected the completed follow-up logs: **11 indexed integration
tests** and **5 provider unit tests** passed. These cover warmed-page reuse,
projection separation, index-path exclusion, concurrent fills, pinned revisions,
admin/restricted isolation, FIFO/byte/entry limits, duplicate insertion,
oversized-page bypass, budget charging on hits, and deterministic revocation and
capability expiry between two already-cached pages. No additional actionable
finding remains from this follow-up review.


## Final integrated verification

After the focused reviews, the coordinating agent ran the complete integration
checks. The reviewer inspected their completed logs, rather than starting another
build during benchmark measurements:

- `/tmp/substrate-indexed-final.log`: **363 Rust tests including the doctest**
  passed. This includes the final typed-predicate parity test for booleans, blobs,
  text, NULL, floating comparisons and explicit-cast fallback against SQLite.
- `/tmp/substrate-indexed-ts.log`: **50 TypeScript tests** passed, zero failures.
- `/tmp/substrate-indexed-final-clippy.log`: the coordinator's strict all-target
  Clippy run completed successfully.

These are integrated results inspected from the coordinator's runs, distinct from
the focused provider/cache evidence described above. No functional implementation
changes were reported after the page-cache review; the final addition was test
coverage. Benchmark execution and performance conclusions remain in the separate
benchmark report. The reviewer did not run competing builds or measurements.
