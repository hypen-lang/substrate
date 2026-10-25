# DataFusion range, EXISTS and window follow-up review

Independent review against `94baeae`, conducted on 2026-10-25. The reviewer
inspected implementation changes and query plans without running competing builds
or benchmark timings. This report covers the follow-up to the indexed-adapter
review in `DATAFUSION-INDEXED.md`.

## Performance observations

The saved 20,000-person baseline plans identify concrete work that can be avoided:

- The ordered range query uses a TopK sort over a numeric index traversal that
  already emits rows in value/occurrence-ID order. Advertising only orderings
  actually preserved by that traversal could eliminate sorting and allow early
  termination. That optimization is deferred; this milestone changes neither
  ordering metadata nor LIMIT pushdown.
- Raw `EXISTS` expressions previously disabled planning reuse, including repeated
  decorrelation into semi-joins. Deterministic subqueries can reuse optimized
  logical plans while receiving fresh physical join and sort operators.
- The EXISTS and window workloads both read numeric ranges. Those projected
  pages were excluded from the snapshot-local Arrow page cache, repeating row
  conversion on every execution.

These are plan-based observations, not a quantitative attribution of total
latency. The benchmark report records the measured results separately.

## Deterministic subquery planning

Reviewed recursive inspection of raw EXISTS, IN and scalar-subquery plans before
optimization, including nested subqueries and view sources. Stable or volatile
functions still disable reuse before an optimizer can fold their results. The
recursion bound spans both views and subqueries. Correlated column references are
accepted only inside recursively inspected plans.

The cache still belongs to one prepared query over a pinned snapshot and retains
one exact typed parameter binding. Stateful joins and sorts remain outside the
physical-template whitelist and are freshly planned for each execution. Live
authority checks and error-driven cache eviction remain in place. No concrete
stale-binding, folded-time-value or shared-execution-state defect was found in
this change.

## Numeric range page caching

Reviewed extending the private snapshot-local cache to numeric ranges without a
pushed limit. The key includes relation identity, ordered projection, selected
role, inclusive/exclusive bounds, direction, all pushed predicates and numeric
continuation. Keeping all predicates in the key is necessary because predicates
on other roles can change output within the same candidate page.

Cached pages retain both emitted rows and the original authorized candidate-row
charge. Hits recharge that candidate count and projected payload bytes against
the current execution's fresh source budget. They retain the live checks before
and after access. Empty filtered pages still preserve continuation, and the
cache remains private to one principal and pinned revision. Identity/equality
lookups and partial pushed-LIMIT pages remain uncached.

No concrete cache-key collision, skipped-candidate, budget-bypass or live
authorization defect was found in the reviewed range-cache changes.

The reviewer flagged the old fixed 512-byte bookkeeping allowance after the
inline cache keys grew to include range state. The implementation now accounts
explicitly for both inline key copies and the cached-page structure, adds a
container/allocation allowance, and charges the dynamic projection/predicate/
bound heaps in both keys. The existing byte and entry caps remain. This is
retained-entry accounting, not a hard process-memory bound: container capacity
and buffers still held by active queries can outlive eviction.

## Verification status

Reviewed the new regression tests for tied-value pagination, changing bindings,
projection order, other-role predicate isolation, residual filtering, LIMIT,
window results, revision/principal separation and revocation. Private cursor
tests additionally exercise empty filtered pages, candidate-row and payload-byte
charges on warm hits, expiry between cached pages, and oversized predicate keys.
Subquery tests cover repeated and changed/NULL bindings, nested stable/volatile
functions, views, revocation and recovery after scalar-subquery cardinality
errors.

The reviewer inspected `/tmp/substrate-range-integration.log`: all 12 existing
indexed integration tests and all 3 new range-cache integration tests passed.
A preliminary unit-test failure was an overbroad assertion that all subqueries require fresh physical planning:
NULL bindings can legitimately optimize to immutable empty plans. The corrected
test restricts that assertion to matching non-NULL bindings and retains result
parity checks for NULL.

Final integrated evidence was then inspected from the coordinator's completed
logs:

- `/tmp/substrate-range-final.log`: **370 Rust tests including the doctest passed**,
  zero failures. The manual performance probe is intentionally ignored in this
  test run.
- `/tmp/substrate-range-clippy.log`: strict all-target Clippy completed
  successfully.

The final source diff remains limited to the DataFusion adapter, provider and
planning modules; native query and storage implementation files are unchanged.
The reviewer has not run builds or benchmark timings. No actionable finding
remains in the inspected implementation. This independent review and the tests
provide evidence, not an exhaustive security proof.
