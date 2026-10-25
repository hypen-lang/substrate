# Tiny-join execution benchmark

Date: 2026-10-25. Previous adapter: commit `41c7472`.

The current adapter compiles supported bounded inner equijoins into typed native
index probes. It avoids Arrow and executor setup, accepts changing exact typed
bindings, preserves bags, and checks current authority. Unsupported shapes,
coercions and fanouts above 128 tuples use DataFusion with the remaining shared
budget and deadline. Native authorization can inspect more denied candidates;
materialized limits are not strict CPU limits.

## Method

Three alternating serialized process pairs at 2,000 and 20,000 people (18,000 and
180,000 occurrences). No builds, tests or other task benchmarks ran during timing.
The table reports medians of three process medians, in microseconds. Each process
uses seven batches of ten executions after preparation and warm-up. Results and
repeated executions are checked against SQLite, with cross-build SHA-256 digests.
Preparation, first execution, fresh handles, covering-index controls, projection
and execution diagnostics are recorded separately in the raw JSON.

These are local embedded reads, not network or durable-write latency. Both app
families and restricted-reader grants are explicit in the fixture. Direct SQLite
has equivalent tuples and indexes but does not perform Substrate authorization or
reference closure. SQLite compatibility uses Substrate's authorized virtual-table
adapter. PostgreSQL was not measured. Shared-machine variation remains possible;
small differences do not establish a universal regression or improvement.

## 20,000 people: warm prepared administrator queries

| Query | Current | Previous DataFusion | SQLite compatibility | Direct SQLite |
| --- | ---: | ---: | ---: | ---: |
| 01_primary_key | 0.450 | 0.438 | 0.667 | 0.429 |
| 02_indexed_equality | 72.696 | 71.279 | 106.283 | 78.154 |
| 03_range_top_100 | 23.321 | 23.050 | 2,464.792 | 58.804 |
| 04_relationship_lookup | 1.475 | 54.221 | 2.317 | 1.317 |
| 05_cross_app_five_way | 4.221 | 151.421 | 7.408 | 3.188 |
| 06_cross_app_group_top_25 | 14,155.658 | 15,095.567 | 134,900.300 | 42,021.725 |
| 07_correlated_exists | 1,036.525 | 1,095.408 | 33,508.521 | 476.938 |
| 08_filtered_nested_top_1 | 9,079.604 | 9,702.987 | 117,409.812 | 26,475.746 |
| 09_full_scan_aggregate | 163.892 | 171.242 | 6,090.946 | 1,041.725 |

## 2,000 people: warm prepared administrator queries

| Query | Current | Previous DataFusion | SQLite compatibility | Direct SQLite |
| --- | ---: | ---: | ---: | ---: |
| 01_primary_key | 0.408 | 0.400 | 0.600 | 0.329 |
| 02_indexed_equality | 6.604 | 6.542 | 9.625 | 7.304 |
| 03_range_top_100 | 18.863 | 19.692 | 125.129 | 25.242 |
| 04_relationship_lookup | 1.312 | 53.554 | 2.150 | 1.079 |
| 05_cross_app_five_way | 3.846 | 140.833 | 6.321 | 2.858 |
| 06_cross_app_group_top_25 | 1,341.508 | 1,271.758 | 10,684.788 | 3,681.438 |
| 07_correlated_exists | 118.575 | 124.933 | 2,731.421 | 429.762 |
| 08_filtered_nested_top_1 | 1,742.979 | 1,691.929 | 6,176.029 | 2,412.017 |
| 09_full_scan_aggregate | 38.958 | 41.212 | 436.425 | 102.562 |

## Restricted readers and changing bindings

| People | Join | Previous reader | Current reader |
| ---: | --- | ---: | ---: |
| 2,000 | 04_relationship_lookup | 56.458 | 1.900 |
| 2,000 | 05_cross_app_five_way | 142.396 | 5.396 |
| 20,000 | 04_relationship_lookup | 55.425 | 2.046 |
| 20,000 | 05_cross_app_five_way | 152.700 | 6.125 |

New parameter sets at 20,000 people, including 56 present and 14 missing bindings:

| Query | Current admin | Current reader | SQLite compatibility | Direct SQLite |
| --- | ---: | ---: | ---: | ---: |
| unseen_relationship_joins | 1.538 | 2.158 | 2.079 | 3.421 |
| unseen_five_way_joins | 3.633 | 5.896 | 5.483 | 11.454 |

The preceding frozen harness does not contain the two new unseen-join workloads;
no previous-adapter timing is claimed for those cases. All bindings use an
independent SQLite oracle and are checked on first and repeated execution.

## Remaining overhead and regressions

Visibility adds policy lookup and a live visibility-generation fence to unscoped
restricted reads. Grant-first evaluation removes an avoidable lookup but does
not eliminate all new authorization work. The final restricted-reader medians at
20,000 people include these slower non-join cases:

| Query | Previous reader µs | Current reader µs |
| --- | ---: | ---: |
| Primary key | 0.671 | 0.796 |
| Indexed equality | 83.267 | 93.508 |
| Range/top 100 | 24.312 | 26.996 |
| Unseen numeric equality | 182.637 | 215.196 |
| Unseen numeric range | 3.300 | 3.871 |

These are whole-build comparisons; they do not isolate a phase cost. At 2,000
people the grouped join and window medians also rose about 5.5% and 3.0%; at
20,000 both fell about 6%. No analytical speedup is attributed to tiny-join
execution. Correlated EXISTS remains slower than direct SQLite at 20,000 people.
Fresh handles still incur planning; large scans still convert rows to Arrow.
The result is a focused tiny-join improvement, not elimination of every query
performance gap. Raw per-process samples remain available for assessing noise.

## Reproduction and provenance

Build `cargo bench --bench comparison --no-run`, freeze its executable, and use
`experiments/datafusion/run_tiny.py --binary CURRENT --baseline PREVIOUS --output NEW_DIR`.
Use the preceding `41c7472` comparison binary for PREVIOUS. The runner refuses to
overwrite results and records executable hashes, start times and elapsed times.
Raw final data: [tiny-join-final](tiny-join-final/).
The earlier [iteration](tiny-join-followup/) exposed avoidable ambient-policy
lookups on granted reads. Final results use grant-first checks, with tests proving
filtered grants still fall through to public/authenticated visibility.

current SHA-256: `4c30af761b40faabb5354b4cb6cb8611f20f5e6ae1ec7f5c9ca2be8788766168`.

baseline SHA-256: `47242dc2450ccba99c9d62929ea3d33789cd7d15252ddfcca35ef7eb5ea3bb9c`.
