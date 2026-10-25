# Indexed joins, ordered reads and small-query execution review

Independent review against `ae58e56`. The reviewer inspects implementation and
local DataFusion 55.0 session and 55.2 physical-plan contracts without running
competing builds or benchmarks. Source review and final correctness verification
are complete. Performance measurements are reported separately.

## Reviewed scope

- Native scan ordering, projection, filter barriers, offsets and fetch limits.
- Bounded join-driven probes, typed key equality, duplicate preservation,
  physical execution state, memory/source budgets and live authority.
- Compiled small-query execution and parameter rebinding, including coercion
  fallback, deadlines, result limits and authorization.

Native query/storage source files remain unchanged in the reviewed diff. Existing native shared
row/cursor methods remain the authority-enforcing read boundary.

## Findings addressed during implementation

The first cardinality helper requested 1,024 rows from `page_shared`, whose
contract accepts at most 1,000. It would silently disable equality-input join
optimization. The helper was corrected to use a valid page size; the final
bounded-input sampler requests only 129 rows to establish the 128-row cutoff.

Indexed joins initially reserved memory for Arrow input buffers but retained
additional owned key copies in both the deduplication set and probe vector.
The implementation now reserves for those keys before retaining them and drops
the deduplication set before execution. The focused small-memory-pool regression
passed and verified that the error path releases its reservation.

The initial direct-read panic path returned before clearing the generic plan
cache. It now clears the cache for both panics and ordinary execution errors.

## Ordering and source semantics

Native family/equality pages emit occurrence IDs in ascending order. Numeric
ranges emit `(typed value, occurrence ID)`, reversing both components when
descending. Canonical fixed-width UUID text preserves the native ID order.
The provider advertises only the lexicographic prefix surviving projection.
Filtering preserves that order; projecting away a varying range value cannot
make the remaining IDs globally sorted.

Fetch applies after the provider's exact predicates; residual filters and offsets
remain DataFusion barriers. Reverse-sort pushdown accepts only a numeric-range
role followed optionally by ID, with matching directions and non-null fields.
Already-limited sources and mixed-direction/unrelated sorts remain unsupported.
Range cache keys already include direction and continuation.

## Bounded inner joins

The optimizer limits substitution to single-partition inner
equality joins with a bounded left input and a native right source. Unsupported
joins retain DataFusion's ordinary execution. Probes deduplicate keys only for
source retrieval; a fresh built-in HashJoin still sees every left row and
retains SQL duplicates, filters, projection and fetch behavior. No mutable join
state is reused across executions.

Ordered joins restore the original advertised output ordering with an explicit
sort around the indexed join. Ordered joins with pushed fetch are excluded,
since sorting after truncation would choose the wrong subset. Reviewed upstream
`EquivalenceProperties::output_ordering`: it concatenates every advertised
ordering and normalizes equal expressions/uniform constants, rather than
selecting only the first ordering. Sorting that combined ordering preserves all
simultaneously valid orders, including tie-break keys and equivalent join-key
references. Reversed relationship/target identities and ordered LIMIT/OFFSET
regressions cover this path. Both children remain single-partition.

Input bounds are conservative, checked again while buffering, and capped at 128
rows. All original source predicates remain on the probed source. Probe keys
must have the exact Arrow type, and noncanonical identity strings do not match
canonical stored IDs. New source executions share the query's source budget and
retain native row policy/reference-visibility checks. Replacing children
re-evaluates eligibility instead of retaining a stale cardinality assumption.

The initial post-planning rewrite ran after plain `EXPLAIN` captured its text,
so diagnostics omitted the indexed join substitution. It now runs as an
appended `IndexJoinRule` with schema checking enabled. The session builder
preserves existing rules and appends this rule after them, keeping the same
eligibility assumptions while running before diagnostic text is captured.
Both plain `EXPLAIN` and `EXPLAIN ANALYZE` explicitly show the custom operator
in the passing four-test join integration target.

## Compiled reads

The specialization consumes DataFusion's validated logical plan and accepts a
single equality lookup or same-role numeric range conjunction, with plain
column projections/aliases and literal
limits/offsets. A filter or reordered sort above a prior limit is rejected.
Allowed ordering follows the native index's actual order or is vacuous for an
identity lookup. It does not parse SQL text independently.

An immutable compiled descriptor handles new exact typed parameter values;
coercions and unsupported shapes fall back to DataFusion. Native shared-row
methods preserve visibility checks. Source bytes are checked before output
allocation, output limits apply per call, and live authorization is checked
before reading, during iteration, and before returning. Panic and ordinary
error paths discard generic cached plans.

The synchronous specialization checks deadlines cooperatively around native
reads and rows. It cannot abandon a blocked native call as the asynchronous
worker's receiver timeout can. The API already describes cooperative budgets
and also performs synchronous authority checks before generic execution, but
this execution-path distinction should remain documented; a hard cancellation
guarantee would require cancellation-aware native reads.

Numeric range descriptors accept exact integer, decimal-storage and timestamp
values and require a finite SQL LIMIT; broad unbounded reads retain DataFusion's
bounded Arrow-page cache. Comparisons with coercions remain in DataFusion. Binding intersects
inclusive/exclusive endpoints without arithmetic overflow, handles reversed
operands, and checks all parameters before short-circuiting a NULL range.
Native range paging preserves ties and reference visibility in either direction.

## Physical template reuse

Indexed joins and ordinary sorts without fetch now join the narrow reusable
physical-operator set. Every execution rebuilds children, join futures/dynamic
filter state/metrics, sort state and metrics. Bounds remain tied to the same
immutable snapshot and exact parameter binding. General child replacement still
revalidates eligibility. Expression whitelisting rejects dynamic filters and
other unknown mutable expressions. Focused tests inspect distinct node identities,
binding changes, repeated reads, error eviction/retry and revocation.

The retained indexed-join descriptor also resets the original HashJoin runtime
state at construction. Its unused dynamic-filter producer initially caused the
conservative whitelist to reject an otherwise reusable template. Removing that
unused producer matches what custom execution already does; it does not suppress
inspection of dynamic-filter expressions in child plans.

Redundant scheduling yields are removed from the bounded left buffer and
terminal source poll; small probes yield periodically rather than before each
key. Continued native pages still yield, and terminal polling still calls the
source method that checks live authority. This changes cooperative scheduling
granularity, not row or authorization semantics; it is not a hard cancellation
latency guarantee.

## Analytical template extension

The subsequent analytical reuse extension admits exact built-in aggregate
implementations (COUNT, SUM, MIN, MAX and AVG), plus the static checked-SUM
factory identified by inner-implementation pointer identity. An immutable UDF
with a matching name or signature is insufficient. Aggregate limit hints,
grouping sets and dynamic-filter producers remain excluded. All aggregate
arguments, order expressions, grouping expressions and FILTER predicates still
pass the conservative physical-expression whitelist. The audited upstream
reconstruction creates fresh metrics; accumulator and DISTINCT state belongs to
each execution stream.

Generic HashJoin and top-k Sort templates are accepted only when a recursive
inspection finds no dynamic-filter consumer. The inspection skips only known
producer fields while independently checking actual join keys, join filters,
sort expressions and all descendants. Unknown child operators remain excluded
by the surrounding operator whitelist. Fresh HashJoin construction resets build
futures, producer state and metrics; fresh Sort construction creates a new
threshold/filter and metrics. Producer removal cannot affect row semantics when
no descendant consumes the producer.

Window templates are limited to exact built-in ROW_NUMBER within the audited
standard window descriptor. Other functions and bounded-window state observers
remain excluded. The private reconstruction contract preserves the same audited
child operators and configuration. One-child/schema checks plus that invariant
permit upstream Keep-mode replacement to preserve private order/repartition
configuration while creating fresh metrics. Audited descriptors contain immutable configuration; evaluators,
partition buffers and row-number state are created by execution.

The initial window reconstruction used upstream
`has_same_children_properties`, which compares property Arc identity rather than
semantic equality. Fresh sort descriptors therefore caused valid executions to
fail. The test exposed this overly strict guard; replacement uses the private
same-configuration reconstruction invariant and explicit child/schema checks.
The focused SQL unit run passed 33 tests with one ignored manual probe after
this correction (`/tmp/substrate-analytic-tests.log`). Inspected tests explicitly
exercise dynamic-consumer and observer rejection, unsupported window functions,
failed hash-build recovery, null-aware joins, fresh top-k thresholds, concurrent
aggregate/window executions, dropped-consumer recovery, real benchmark-shape
template retention, SQLite parity, binding changes, budget-error eviction and
live authority narrowing. No unresolved actionable correctness or authorization
issue remains in this extension. The integrated full-suite rerun passed 394 tests,
zero failures and one ignored manual probe
(`/tmp/substrate-execution-analytic-full-tests.log`); strict all-target Clippy
also passed (`/tmp/substrate-execution-analytic-clippy.log`).

## Verification status

Inspected successful focused logs: 12 indexed-provider tests, four join tests,
three initial ordered-read tests, and the join memory-error/reservation-release
unit test. The complete SQL unit target also passed 21 tests with one existing
manual performance probe ignored, including the new direct-read tests.
Additional direct-path coverage checks capability expiry and inaccessible
reference targets. The lint run completed with warnings denied.
The final range and physical-template refinements, followed by the scheduling
adjustment, passed the focused SQL verification listed below.
The join suite compares duplicates, missing/null/noncanonical keys,
join filters, offsets, fallback joins, reference visibility and revocation with
the SQLite compatibility adapter. Expanded ordered-read cases exercise tied
values across multiple pages, both directions, projection and residual-filter
barriers, offsets, nested limits and window execution.

Inspected integrated evidence before the analytical extension:

- `/tmp/substrate-execution-final-tests.log`: 387 passed, zero failed, one
  existing manual performance probe ignored.
- `/tmp/substrate-execution-final-sql-units.log`: final scheduling delta verified
  by 26 passing SQL unit tests and one ignored manual probe.
- `/tmp/substrate-execution-final-sql-integration.log`: final scheduling delta
  verified by 20 passing indexed-provider, join and ordered-read tests.
- `/tmp/substrate-execution-clippy.log`: lint completed with warnings denied.
- `/tmp/substrate-execution-example.log`: working SQL example verified indexed
  prepared reads, aggregate output, preserved native lookup and retained-handle
  denial after revocation.

The reviewer did not run competing builds or timings. Benchmark comparisons
and remaining performance limits belong in the accompanying performance report.

No unresolved actionable result-correctness or authorization issue was found
in the reviewed implementation. The synchronous cooperative-deadline
limitation above remains explicit.
