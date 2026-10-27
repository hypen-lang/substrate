# Why Substrate is currently slower than SQLite

Substrate's largest measured gaps come from missing execution optimizations in the
SQL adapter. Its SQL interface currently uses SQLite's query engine over custom
virtual tables backed by Liquid's typed, authorized relations. Ordinary SQLite
queries access its own mature table/index implementation directly.

These are prototype costs, not evidence that shared identities or conservative
schema extension inherently require poor query performance. They are also real
costs: applications using this adapter pay them today.

## Measured comparison

The cross-engine suite (historical results removed) checks every result cell, identity,
and row order before timing. SQLite and Substrate both run prepared warm reads in
one process over in-memory fixtures. The 2,000-person fixture contains 18,000 total
occurrences. Setup, query preparation, durable storage, socket transport and server
startup are outside those timed samples. This rules out log serialization, redb's
single writer, TLS and client connection overhead as explanations for these read
measurements.

The historical 2026-10-24 report (historical results removed), before
live authorization gates, measured:

| Workload | SQLite | Substrate admin | Ratio |
|---|---:|---:|---:|
| Primary-key lookup | 0.412 µs | 0.921 µs | 2.23× |
| Indexed equality | 7.642 µs | 13.967 µs | 1.83× |
| Range and ordered first 100 | 24.554 µs | 865.904 µs | 35.27× |
| Five-family point join | 2.862 µs | 7.429 µs | 2.60× |
| Grouped cross-app aggregate | 3,196.804 µs | 12,514.217 µs | 3.91× |
| Correlated EXISTS | 415.938 µs | 3,736.938 µs | 8.98× |

These are median batch means from seven batches of ten executions, not request
percentiles. A small absolute point-lookup difference should not be confused with
a large scan/ordering gap. The results are workload-specific.

## Current implementation rerun — 2026-10-25

The suite was rerun in release mode during the remote-security milestone, then
repeated with the same executable because small-query samples showed substantial
variation. Both runs verified all eight result sets. Both raw reports are retained:
first run (historical results removed) and
repeat (historical results removed). PostgreSQL was not rerun.
These runs occurred on the shared development machine while other implementation
and test work continued; they are not isolated regression measurements.

The table shows the repeat's median batch means. The first run is included as a
ratio column to show variability rather than selecting favorable results.

| Workload | SQLite | Substrate admin | Substrate reader | Admin / SQLite | First-run ratio |
|---|---:|---:|---:|---:|---:|
| Primary-key lookup | 0.329 µs | 0.729 µs | 1.121 µs | 2.21× | 2.42× |
| Indexed equality | 7.571 µs | 13.150 µs | 14.550 µs | 1.74× | 0.98× |
| Range and ordered first 100 | 24.150 µs | 813.729 µs | 973.183 µs | 33.69× | 39.93× |
| Relationship lookup | 0.992 µs | 2.896 µs | 4.133 µs | 2.92× | 7.38× |
| Five-family point join | 2.767 µs | 7.621 µs | 11.942 µs | 2.75× | 1.25× |
| Grouped cross-app aggregate | 3,164.825 µs | 12,947.429 µs | 20,120.625 µs | 4.09× | 3.91× |
| Correlated EXISTS | 402.046 µs | 3,877.729 µs | 6,279.258 µs | 9.64× | 9.54× |
| Filtered per-person top one | 2,251.546 µs | 6,132.312 µs | 8,841.812 µs | 2.72× | 2.72× |

The range gap persists at roughly 34–40× and the current plans still show a Liquid
family scan plus full sort versus SQLite's bounded index scan. This is much stronger
evidence than small percentage changes in point-query timings. Direct-principal
live gates are exercised in the `reader` column. Selected capability chains,
credential authentication, TLS and expiry-crossing operations are not measured by
this harness. Adding those paths to a dedicated security-cost benchmark remains work.

## What the code and query plans establish

**Range and ordering are the clearest missing optimization.** `LiquidTab::best_index`
in `src/sql.rs` advertises one usable equality predicate. It does not advertise
range bounds, output ordering or LIMIT consumption. For the range workload, the
saved Substrate plan is `family_scan` followed by a temporary sort; ordinary SQLite
uses `workout_duration` with both range bounds and sorts only the remaining order
term. Substrate consequently visits the whole Workout family even though the
request asks for only 100 ordered matches. The gap grows from approximately 5× at
200 people to 35× at 2,000 people in the historical runs.

**Point queries still cross an extra adapter.** Each virtual-table cursor calls the
Rust lookup/page API, obtains owned rows, and exposes values to SQLite. Snapshot
cursors read 64-row pages; they are not full-family eager materializations, but each
returned row is cloned. UUIDs become SQL text, and reference arguments are parsed
back into typed UUIDs. Each requested column converts its Rust value into a SQLite
value. These costs are visible in the implementation and are plausible optimization
targets; no CPU profile has yet attributed an exact fraction of total time to each.

**Authorization does additional work.** Restricted reads verify grants and the
reference closure of each candidate. The current traversal creates temporary
visited/dependency sets and can revisit shared targets for different occurrences.
In the historical report, restricted reader time was about 3% above admin for a
point lookup, 29% for the five-way point join and 44% for the grouped aggregate.
Ordinary SQLite was not configured with equivalent row policies. Reader-versus-admin
isolates the aggregate observed policy overhead in this fixture, not a claim that
SQLite provides those policies for free. Live revocation/expiry introduce further
authorization gates; measurements that predate those gates do not include them.

**Planner information is basic.** Equality probes currently advertise fixed row
estimates (one for identity, ten for a role equality); family scans use a fixed
cost. There are no distribution statistics, compound-index choices, multiple
predicate intersections or projection pushdown. Broad joins and correlated
subqueries can amplify both scan and per-row adapter costs. The existing plans
show chosen access paths, but a CPU profile or controlled change is still needed
to quantify each contribution.

## Transport is a separate issue

The nested process report (historical results removed) measured Unix-client request
latencies in milliseconds. The local server polls an idle listener every 5 ms and
the client opens a connection per request. That explains much of the repeated-request
penalty in its N+1 baseline; it is unrelated to embedded SQL microsecond results.
TLS adds another connection setup cost. Persistent connections/event-driven accept
and batching are relevant here; batching is already implemented for nested reads.
Neither the old process results nor embedded SQL results measure remote TLS latency.

## Next performance work

1. Implement typed range cursor bounds and communicate supported ordering to SQLite.
   Add result-equivalence tests for coercion, mixed constraints, LIMIT and hidden rows.
   Only stop early when ordering and authorization make it semantically safe.
2. Add selectivity statistics and better access-path choices, then recheck the broader
   joins and correlated workloads. Do not tune against elapsed time alone; retain plans.
3. Profile allocations and SQL value conversions; reduce row copying where ownership
   permits. Any authorization memoization must be snapshot-bound and retain live
   revocation/expiry checks.
4. Measure authenticated scoped readers separately from admin and ordinary SQLite;
   add concurrent writes, skew, memory usage and cold/larger-than-memory behavior.
5. Address connection reuse/accept wakeups independently from engine query execution.

No backend replacement is justified by these read numbers alone. Switching redb
would not add range pushdown to this adapter. Similarly, changing declaration JSON
would not remove row traversal, sorting or SQL value conversions.

## Other unfinished work

Separate from performance, the implementation still needs broader operational and
semantic work: replication/sharding and another storage backend; larger-than-memory
storage; incremental/background checkpoints; permanent view definitions; richer
constraints and incompatible schema-evolution adapters; a Bend reference model;
published/versioned SDK packages; and production service operations such as identity
provider integration, certificate lifecycle automation, audit export, comprehensive
resource quotas and stronger hostile-client isolation. Scope-bound credentials,
rotation, expiry and TLS are implemented in the accompanying security milestone;
they should not be mistaken for completion of that operational list.
