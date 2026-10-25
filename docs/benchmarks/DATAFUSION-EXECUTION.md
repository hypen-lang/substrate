# DataFusion indexed execution follow-up

This report compares the current SQL implementation with the range/EXISTS adapter
from `ae58e56` and eager DataFusion adapter from `772968f`. Every current process
also measures the explicit SQLite compatibility adapter and direct indexed
SQLite. The [previous report](DATAFUSION-RANGE-EXISTS.md) and its raw measurements
remain unchanged.

See the [final analytical follow-up results](#analytical-planning-follow-up).

The final analytical follow-up improves all nine measured warm admin workloads
against both preserved DataFusion adapters at both tested sizes. At 20,000
people, point reads are 0.45 µs, selective joins 54.50/134.25 µs and range/top-100
23.75 µs. Small joins and covering-index comparisons still favor SQLite; machine
variation limits causal speedup claims. Final results appear first, followed by
the preserved first round and its unresolved findings at that stage.

## Method

The nine established query shapes, fixture and measurement boundaries remain
unchanged. Each process checks exact ordered results against SQLite before and
after timing. Cross-process digests verify the same output across all three
binaries, and the runner also verifies parameter rebinding and restricted
projection outputs where supported by the preserved harness.

Each size ran three repetitions at 2,000 and 20,000 people (nine occurrences per
person), serially, rotating binary order across repetitions. Warm results are the median
of process medians; each process median summarizes seven samples of ten query
executions. These are sample means, not request latency percentiles. Setup,
preparation, first execution and fresh-handle execution are separate metrics.
Fresh handles have empty SQL conversion caches but reuse a loaded native database
and warm operating-system/CPU caches. They do not measure cold disk access.
Fresh-handle EXPLAIN ANALYZE counters for the range and selective joins are
collected after all timed workloads; these diagnose the DataFusion plan, including
when ordinary execution can use the compiled small-query path instead.
The analytical follow-up additionally records fresh and source-cache-warm global
aggregate diagnostics. EXPLAIN ANALYZE itself repeats planning; its total wall
time is not a measurement of the ordinary prepared query's physical-template
cache hit. Operator elapsed times can overlap and are not subtracted from totals.
No PostgreSQL service, durable writes, network or transport serialization is
included. This is a shared development machine, so unchanged engine controls
and per-run spread matter when interpreting small differences.

The existing 16-binding cycles retain their initial correctness pass and may
therefore hit converted pages during timing. New `unseen_parameters` cases use
70 distinct parameter sets, each executed only once during timing by its
prepared query. A separate, distinct binding warms the executor. A separate
SQLite statement computes expected rows beforehand; assertions for the measured
queries happen after each timed sample, then every binding is checked again.
Data pages are warm, and the earlier workloads can have populated compatible
Substrate source pages. These cases measure first use of bindings, not guaranteed
cold conversion. They appear only in the current harness, so their comparison
is against current SQLite and compatibility measurements, not a fabricated
before/after baseline.

The established SQLite control keeps its original single-column numeric index.
After all established timings, an additional `(durationMinutes, id)` covering
index is created and SQLite statistics are analyzed. Equality, top-100 and unseen
range queries are checked and timed again. This control matches the native
index's value/ID ordering more closely; its plans and index-build time are
reported separately. Comparing only the original index would overstate
Substrate's advantage on ordered numeric reads.

## Changes

- Selective inner joins can probe native indexes with the small input's keys
  instead of converting every row of the larger relation. Unsupported join
  shapes retain DataFusion execution.
- Eligible ordered numeric range/top-k reads push ordering and row limits into
  native indexed scans; residual filters, authorization and offsets retain the
  correctness barriers required before truncation.
- Narrow validated point/equality and finite-LIMIT ordered numeric-range queries
  use a compiled synchronous path, reusing typed bindings across changing parameters and avoiding Arrow/runtime
  overhead. More complex expressions continue through DataFusion.
- Eligible indexed-join physical templates are reused with fresh execution
  state, avoiding repeated physical planning for the same bound query.
- New regression coverage checks unsupported shapes, budgets, duplicate join
  keys, ordering, authorization and fallback behavior.

Arbitrary new parameter bindings reuse a compiled shape only for the narrow
point/equality and bounded ordered-range descriptors. More complex parameterized
joins and expressions still use the existing last-binding cache and can replan
when values change. This is not a general parameter-independent DataFusion plan
cache.

Native query behavior remains unchanged. See the separate
[beyond-SQL investigation](../PERFORMANCE-BEYOND-SQL.md) for write, checkpoint,
transport and native-read opportunities.

## Iteration before final measurement

The first diagnostic exposed a real eligibility gap: advertising native ID
ordering made the selective relationship and five-family joins retain ordinary
HashJoin plans. The final optimization restores any ordering expected by the
parent with a sort over the small probed result, and regression tests explicitly
require the indexed operators for both benchmark shapes. Eligibility sampling
is capped at 129 authorized candidates to establish the 128-row bound.

Broader first-run slowdowns also appeared in unchanged direct SQLite and
compatibility controls. The group, EXISTS, window and aggregate physical plans
were identical to the previous adapter. Final paired repetitions, rather than
that one diagnostic run, determine the results below.

## Analytical planning follow-up

The first round left broad-query slowdowns unresolved. The follow-up retains
physical templates for audited immutable aggregate descriptors, hash joins,
sorts/top-k and builtin ROW_NUMBER windows. Each execution reconstructs fresh
join build state, accumulators, window evaluators, top-k thresholds and metrics.
Templates with dynamic-filter consumers or unsupported expressions/operators
remain excluded. This avoids repeated physical planning without retaining a
previous query's execution state or authority decision.

This source passed 394 Rust tests across 33 targets (one existing manual probe
ignored), strict all-target Clippy and formatting checks. The independent review
also covers state reset and conservative cache eligibility. A second paired
round uses the same range/EXISTS and eager binaries, with all existing workloads
and both SQLite controls, in `execution-analytic-followup/`. No first-round
measurements are overwritten.

All 18 follow-up processes completed and verified their output digests. Every
warm admin workload has a lower measured median than both preserved adapters
at both sizes. At 20,000 people, point reads take 0.45 µs, the relationship join
54.50 µs, the five-family join 134.25 µs and range/top-100 23.75 µs. The analytical
follow-up also lowers the observed grouped, EXISTS, window and aggregate medians.

These are fixture measurements, not a claim of universal wins. Direct indexed
SQLite still takes only 1.15 µs for the relationship and 3.26 µs for the
five-family join: Substrate remains about 47× and 41× slower on those very small
joins, despite avoiding full-family conversion. The remaining DataFusion
operator/Arrow/runtime work is a further optimization opportunity; this report
does not assign exact percentages without a component profile.

Units in the warm tables are microseconds. "Previous" is `ae58e56`; compatibility
is explicit `sqlite_compat()`.

### 2,000 people / 18,000 occurrences

| Query | Current admin | Current reader | Previous admin | Eager admin | Compatibility admin | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point identity | 0.41 | 0.58 | 17.11 | 74.58 | 0.57 | 0.32 |
| Numeric equality | 6.28 | 7.36 | 68.00 | 110.25 | 9.73 | 7.23 |
| Range/top-100 | 18.84 | 21.18 | 90.88 | 198.70 | 123.92 | 24.83 |
| Relationship join | 51.31 | 51.60 | 182.47 | 276.67 | 2.17 | 1.00 |
| Selective five-family join | 133.65 | 136.58 | 517.25 | 945.13 | 6.00 | 2.87 |
| Grouped five-family join | 1,290.92 | 1,284.04 | 1,795.53 | 1,944.67 | 11,078.78 | 3,583.61 |
| Correlated EXISTS | 119.83 | 119.68 | 250.20 | 421.06 | 2,580.84 | 426.62 |
| Nested window/top-1 | 1,612.09 | 1,631.90 | 2,478.62 | 2,438.08 | 5,491.11 | 2,413.38 |
| Full aggregate | 39.51 | 39.13 | 123.12 | 176.39 | 444.85 | 107.17 |

### 20,000 people / 180,000 occurrences

| Query | Current admin | Current reader | Previous admin | Eager admin | Compatibility admin | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point identity | 0.45 | 0.67 | 44.76 | 114.95 | 0.62 | 0.38 |
| Numeric equality | 75.79 | 93.98 | 387.29 | 179.32 | 113.26 | 86.29 |
| Range/top-100 | 23.75 | 26.20 | 256.78 | 343.98 | 1,786.81 | 62.10 |
| Relationship join | 54.50 | 52.45 | 1,060.28 | 563.52 | 2.33 | 1.15 |
| Selective five-family join | 134.25 | 136.90 | 2,303.85 | 2,173.38 | 6.53 | 3.26 |
| Grouped five-family join | 12,304.91 | 14,768.58 | 19,167.78 | 14,229.51 | 124,853.33 | 40,094.38 |
| Correlated EXISTS | 930.76 | 926.04 | 1,846.00 | 1,289.55 | 32,735.85 | 461.41 |
| Nested window/top-1 | 7,158.01 | 7,183.88 | 9,173.90 | 8,282.90 | 91,789.32 | 24,354.14 |
| Full aggregate | 161.80 | 164.08 | 254.03 | 273.49 | 5,598.71 | 1,128.89 |

### Fresh SQL handles, execution milliseconds (20,000 people)

| Query | Current admin | Previous admin | Current reader | Previous reader |
| --- | ---: | ---: | ---: | ---: |
| Point identity | 0.004 | 0.151 | 0.003 | 0.234 |
| Numeric equality | 0.109 | 0.459 | 0.198 | 0.890 |
| Range/top-100 | 0.091 | 4.501 | 0.092 | 9.923 |
| Relationship join | 0.399 | 20.187 | 0.399 | 41.200 |
| Selective five-family join | 1.412 | 37.883 | 1.053 | 75.108 |
| Grouped five-family join | 50.827 | 62.490 | 116.190 | 145.850 |
| Correlated EXISTS | 13.628 | 13.876 | 52.827 | 50.941 |
| Nested window/top-1 | 28.513 | 28.220 | 70.629 | 71.298 |
| Full aggregate | 5.067 | 5.611 | 18.984 | 19.162 |

### Parameter workloads, microseconds (20,000 people)

| Workload | Current admin | Current reader | Previous admin | Compatibility admin | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: |
| id_same_value | 0.44 | 0.66 | 21.45 | 0.63 | 0.37 |
| id_16_values | 0.51 | 0.71 | 62.77 | 0.76 | 0.47 |
| numeric_equality_16_values | 90.28 | 107.32 | 189.90 | 115.33 | 99.65 |
| numeric_range_16_values | 2.78 | 3.55 | 136.54 | 410.62 | 56.80 |
| unseen_id_values | 0.47 | 0.67 | — | 0.67 | 0.39 |
| unseen_numeric_equalities | 113.09 | 174.37 | — | 142.12 | 95.75 |
| unseen_numeric_ranges | 3.27 | 3.45 | — | 264.70 | 65.11 |

### Covering-index comparison after analytical follow-up

| People | Query | Substrate admin (µs) | Covering SQLite (µs) |
| ---: | --- | ---: | ---: |
| 2,000 | Numeric equality | 6.28 | 4.18 |
| 2,000 | Range/top-100 | 18.84 | 11.56 |
| 2,000 | Unseen ranges | 2.35 | 4.24 |
| 20,000 | Numeric equality | 75.79 | 37.15 |
| 20,000 | Range/top-100 | 23.75 | 11.61 |
| 20,000 | Unseen ranges | 3.27 | 4.05 |

Thus point execution is close to direct SQLite, short unseen ranges are
competitive on this fixture, and covering SQLite remains about twice as fast
for the larger equality and top-100 outputs. SQL joins still have a much larger
gap. Authorization/reference work is present in Substrate and absent from the
direct SQLite control.

### Follow-up controls and remaining uncertainty

At 2,000 people, unchanged SQLite medians are within roughly 9% of the previous
adapter's process controls, and every workload improves. At 20,000 people,
several previous-adapter processes run slower even in their unchanged SQLite
controls. The measured speedup ratios therefore include machine variation;
absolute final latencies and the row/probe counts are more useful than treating
every ratio as an implementation-only gain.

The following ratios are medians of the three paired process ratios, rather
than ratios of the displayed median latencies. A value below one means the
current process was faster. The last column divides each paired query ratio by
its SQLite-control ratio before taking the median; it is only a noise diagnostic.

| People | Workload | Paired query ratio | Paired SQLite-control ratio | Paired ratio/control |
| ---: | --- | ---: | ---: | ---: |
| 2,000 | Grouped join | 0.72 | 1.03 | 0.70 |
| 2,000 | EXISTS | 0.48 | 1.01 | 0.48 |
| 2,000 | Window | 0.69 | 0.94 | 0.76 |
| 2,000 | Aggregate | 0.33 | 0.93 | 0.32 |
| 20,000 | Grouped join | 0.63 | 0.55 | 1.33 |
| 20,000 | EXISTS | 0.44 | 0.82 | 0.64 |
| 20,000 | Window | 0.87 | 0.97 | 0.90 |
| 20,000 | Aggregate | 0.64 | 0.98 | 0.67 |

The grouped 20,000-person residual exceeds one even though its raw median
improves against both adapters. This comparison cannot establish a causal
regression or rule one out; it is not silently normalized away. The first
round's broad raw slowdowns do not recur in this final round. No fresh-handle
execution regression above 10% versus the range adapter appeared for either
principal at either size. The largest observed increase was 6.4% for the
2,000-person admin aggregate. Other small increases include reader EXISTS
at 20,000 (52.83 vs 50.94 ms) and admin windows (28.51 vs 28.22 ms).
No parameter or projection median regression above 10% appeared at either size.
Three runs on a shared development machine are insufficient for a universal
no-regression guarantee.

### Aggregate execution diagnostics

Fresh diagnostic handles have no converted pages; their second EXPLAIN ANALYZE
call reuses converted pages but repeats diagnostic planning. These totals include
plan rendering and are distinct from normal prepared-query execution above.
The source-row metric charges candidate rows even on cache hits; it is not a
count of newly decoded rows. Both principals report four cached pages at 2,000
people and 40 at 20,000, with 4,000 and 40,000 candidate rows respectively.

| People | Principal | Fresh diagnostic wall (ms) | Warm diagnostic wall (ms) | Warm cached pages |
| ---: | --- | ---: | ---: | ---: |
| 2,000 | Admin | 0.821 | 0.405 | 4 |
| 2,000 | Reader | 2.027 | 0.370 | 4 |
| 20,000 | Admin | 5.518 | 0.786 | 40 |
| 20,000 | Reader | 19.341 | 0.619 | 40 |

Operator compute times are retained in raw plans. They are not subtracted from
wall time or added together because execution can overlap. Range and join
source/probe counts remain 100, four and nine respectively at both sizes, with
the interpretation described in [Source work](#source-work).

### Follow-up raw files and reproduction

```sh
python3 experiments/datafusion/run_execution.py \
  --binary /tmp/substrate-execution-analytic-candidate \
  --range-baseline /tmp/substrate-range-ae58e56 \
  --eager-baseline /tmp/substrate-eager-datafusion-772968f \
  --output /tmp/substrate-execution-repeat
```

The checked-in directory is preserved. The command uses a fresh output location;
choose another new path if that location already exists.
Current follow-up executable SHA-256:
`47242dc2450ccba99c9d62929ea3d33789cd7d15252ddfcca35ef7eb5ea3bb9c`.
The range and eager hashes are unchanged from the first round.

| People | Final current | Previous range/EXISTS | Eager |
| ---: | --- | --- | --- |
| 2,000 | [1](execution-analytic-followup/datafusion-execution-2000-1.json), [2](execution-analytic-followup/datafusion-execution-2000-2.json), [3](execution-analytic-followup/datafusion-execution-2000-3.json) | [1](execution-analytic-followup/datafusion-execution-range-baseline-2000-1.json), [2](execution-analytic-followup/datafusion-execution-range-baseline-2000-2.json), [3](execution-analytic-followup/datafusion-execution-range-baseline-2000-3.json) | [1](execution-analytic-followup/datafusion-execution-eager-baseline-2000-1.json), [2](execution-analytic-followup/datafusion-execution-eager-baseline-2000-2.json), [3](execution-analytic-followup/datafusion-execution-eager-baseline-2000-3.json) |
| 20,000 | [1](execution-analytic-followup/datafusion-execution-20000-1.json), [2](execution-analytic-followup/datafusion-execution-20000-2.json), [3](execution-analytic-followup/datafusion-execution-20000-3.json) | [1](execution-analytic-followup/datafusion-execution-range-baseline-20000-1.json), [2](execution-analytic-followup/datafusion-execution-range-baseline-20000-2.json), [3](execution-analytic-followup/datafusion-execution-range-baseline-20000-3.json) | [1](execution-analytic-followup/datafusion-execution-eager-baseline-20000-1.json), [2](execution-analytic-followup/datafusion-execution-eager-baseline-20000-2.json), [3](execution-analytic-followup/datafusion-execution-eager-baseline-20000-3.json) |

## First paired round: measured results

This round records the completed initial execution changes. The broad-query physical planning follow-up above builds on these changes;
these measurements are preserved as the first round rather than silently replaced.

All 18 processes completed successfully on 2026-10-25. Ordered result digests
matched all three adapter builds; parameter, restricted projection and covering
index checks also passed. Units are microseconds unless stated otherwise.
"Previous" means `ae58e56`; "compatibility" means explicit `sqlite_compat()`.
SQLite controls do not perform Substrate's authority/reference checks.

At 20,000 people, the observed admin medians improve about 46× for identity
reads, 7.4× for both selective join shapes, 4.9× for range/top-100 and 42× for
changing range bindings versus the previous adapter. Fresh selective join
execution improves about 12–14× for admin and 34–48× for the reader. These are
ratios of measured medians on this fixture, not universal throughput promises.
The broad-query slowdowns below prevent a blanket improvement claim.

### 2,000 people / 18,000 occurrences

| Query | Current admin | Current reader | Previous admin | Eager admin | Compatibility admin | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point identity | 0.93 | 0.63 | 18.96 | 212.98 | 0.70 | 0.79 |
| Numeric equality | 7.00 | 12.99 | 178.64 | 373.90 | 11.87 | 8.83 |
| Range/top-100 | 21.57 | 23.95 | 217.74 | 571.05 | 294.38 | 38.11 |
| Relationship join | 116.89 | 69.14 | 537.20 | 784.56 | 4.75 | 2.55 |
| Selective five-family join | 370.34 | 211.43 | 1,679.12 | 2,678.15 | 7.45 | 3.28 |
| Grouped five-family join | 3,113.43 | 3,871.00 | 4,410.23 | 4,968.13 | 19,131.49 | 4,790.68 |
| Correlated EXISTS | 848.65 | 728.68 | 582.79 | 1,697.75 | 5,769.85 | 961.92 |
| Nested window/top-1 | 5,803.60 | 5,675.14 | 2,774.43 | 6,164.66 | 12,790.67 | 5,093.15 |
| Full aggregate | 363.01 | 307.68 | 294.36 | 563.87 | 883.27 | 148.17 |

### 20,000 people / 180,000 occurrences

| Query | Current admin | Current reader | Previous admin | Eager admin | Compatibility admin | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point identity | 0.45 | 0.63 | 20.51 | 150.47 | 0.68 | 0.38 |
| Numeric equality | 82.70 | 110.39 | 187.02 | 204.25 | 118.80 | 87.89 |
| Range/top-100 | 23.80 | 24.37 | 115.96 | 516.56 | 2,177.30 | 61.46 |
| Relationship join | 50.58 | 54.93 | 373.25 | 690.54 | 2.53 | 1.17 |
| Selective five-family join | 142.70 | 139.28 | 1,059.86 | 2,466.80 | 14.31 | 3.31 |
| Grouped five-family join | 17,989.71 | 16,817.67 | 14,965.73 | 13,559.29 | 142,812.70 | 46,093.04 |
| Correlated EXISTS | 1,210.06 | 2,003.55 | 3,065.04 | 1,151.39 | 37,934.76 | 515.34 |
| Nested window/top-1 | 10,187.55 | 12,446.87 | 8,818.98 | 11,102.38 | 129,468.76 | 29,927.40 |
| Full aggregate | 473.62 | 344.45 | 253.26 | 270.05 | 8,374.87 | 1,555.72 |

### Fresh SQL handles, execution milliseconds (20,000 people)

| Query | Current admin | Previous admin | Current reader | Previous reader |
| --- | ---: | ---: | ---: | ---: |
| Point identity | 0.005 | 0.101 | 0.003 | 0.223 |
| Numeric equality | 0.211 | 0.294 | 0.214 | 0.325 |
| Range/top-100 | 0.133 | 2.514 | 0.149 | 4.790 |
| Relationship join | 0.483 | 6.956 | 0.440 | 21.286 |
| Selective five-family join | 1.301 | 15.999 | 1.376 | 46.546 |
| Grouped five-family join | 51.318 | 50.025 | 131.858 | 123.198 |
| Correlated EXISTS | 16.738 | 14.452 | 58.886 | 51.544 |
| Nested window/top-1 | 39.146 | 34.435 | 105.401 | 76.099 |
| Full aggregate | 10.958 | 5.978 | 36.149 | 20.084 |

### Parameter workloads, microseconds (20,000 people)

| Workload | Current admin | Current reader | Previous admin | Compatibility admin | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: |
| id_same_value | 0.57 | 0.83 | 15.33 | 0.87 | 0.53 |
| id_16_values | 0.64 | 0.82 | 60.74 | 0.91 | 1.01 |
| numeric_equality_16_values | 235.27 | 247.40 | 197.87 | 168.99 | 219.14 |
| numeric_range_16_values | 3.33 | 3.85 | 139.93 | 792.01 | 70.01 |
| unseen_id_values | 0.51 | 0.75 | — | 0.73 | 0.42 |
| unseen_numeric_equalities | 158.15 | 298.82 | — | 232.45 | 99.22 |
| unseen_numeric_ranges | 4.51 | 3.61 | — | 395.55 | 85.82 |

### SQLite with equivalent numeric ordering

These controls run after the established workloads. Plans confirm a covering
index search and no temporary ordering step for these queries.

| People | Query | Substrate admin (µs) | Covering SQLite (µs) |
| ---: | --- | ---: | ---: |
| 2,000 | Numeric equality | 7.00 | 4.44 |
| 2,000 | Range/top-100 | 21.57 | 22.08 |
| 2,000 | Unseen ranges | 3.22 | 4.90 |
| 20,000 | Numeric equality | 82.70 | 39.27 |
| 20,000 | Range/top-100 | 23.80 | 11.87 |
| 20,000 | Unseen ranges | 4.51 | 4.40 |

At 20,000 people, well-indexed SQLite is about twice as fast for equality and
range/top-100, while the unseen short ranges are close. The original SQLite
range control has extra ordering work; it must not be used alone to claim that
Substrate beats SQLite. Creating the extra index and running ANALYZE took a
median 2.91 ms at 2,000 people and 35.64 ms at 20,000, excluded from read timings.

### Remaining slowdowns and uncertainty

Several broad workloads have worse raw medians than the previous adapter.
At 20,000 people, grouped joins are 1.20×, windows 1.16× and aggregates 1.87×
slower for admin; at 2,000 people the window median is 2.09× slower. These
results are retained, not omitted as outliers. Reader results differ materially.

| People | Workload | Current/previous admin | Unchanged SQLite ratio | Unchanged compatibility-admin ratio |
| ---: | --- | ---: | ---: | ---: |
| 2,000 | Grouped join | 0.71 | 0.73 | 1.01 |
| 2,000 | EXISTS | 1.46 | 1.05 | 1.22 |
| 2,000 | Window | 2.09 | 1.67 | 1.33 |
| 2,000 | Aggregate | 1.23 | 1.28 | 1.19 |
| 20,000 | Grouped join | 1.20 | 1.04 | 0.93 |
| 20,000 | EXISTS | 0.39 | 0.58 | 1.10 |
| 20,000 | Window | 1.16 | 1.09 | 1.16 |
| 20,000 | Aggregate | 1.87 | 1.46 | 1.52 |

Machine variation is substantial even though this task ran no concurrent builds
or benchmarks. For example, the 20,000-person current five-family join measured
142, 748 and 143 µs across processes; current windows measured 10.19, 14.71 and
9.56 ms. Across the paired 20,000-person group runs, current/previous ratios
were 0.66, 1.85 and 1.29, while unchanged SQLite ratios were 0.45, 1.95 and 1.07.
This also explains why some 2,000-person timings exceed 20,000-person timings;
these measurements do not establish cross-size latency scaling.

Dividing each paired admin ratio by its corresponding SQLite-control ratio gives
median residual ratios of 1.21 for the 20,000-person group, 1.13 for its window,
and 1.06 for its aggregate; the 2,000-person window residual is 1.12. This is a
noise diagnostic, not a valid correction that proves the regression disappeared.
The broad plans match the previous adapter, but these measurements cannot rule
out a smaller implementation regression. More isolated alternating runs and
component profiles remain necessary before declaring those paths unchanged.
Changing numeric-equality bindings also remain slower in this run (235 vs
198 µs admin), and unseen equalities are 158 µs admin / 299 µs reader. Returning
hundreds of rows and policy checks remain a different problem from point-read
planning overhead.

## Source work

Fresh EXPLAIN ANALYZE reports 100 source rows for the generic range/top-100
plan, two Performed source rows plus two probed Workout rows for the relationship
query, and one Person source row plus eight probed rows across four joins for the
five-family query. These counts are the same at both measured fixture sizes. They measure
execution-provider/probe rows, not every native read: eligibility sampling and
authorization/reference traversal are additional work. Original family scans
remain visible as templates under indexed joins but have no execution metrics.

The range diagnostic runs DataFusion and proves its provider pushdown; the timed
plain range query uses the compiled descriptor. It is not a counter sample from
that direct execution. Both paths are covered by result and authorization tests.

## Reproduction

Build the comparison release benchmark, then run:

```sh
python3 experiments/datafusion/run_execution.py \
  --binary target/release/deps/comparison-<build-hash> \
  --range-baseline /tmp/substrate-range-ae58e56 \
  --eager-baseline /tmp/substrate-eager-datafusion-772968f \
  --output /tmp/substrate-execution-repeat
```

The runner refuses to overwrite output, disables optional PostgreSQL access,
checks result digests and records executable SHA-256 hashes in every raw file.

## Raw measurements

Every JSON file includes sample means, plans, preparation/catalog times,
fresh-handle execution, principal comparisons, executable hashes and process
start/elapsed times. Current files additionally include unseen bindings,
covering-index controls and fresh execution counters.

| People | Current | Previous range/EXISTS | Eager |
| ---: | --- | --- | --- |
| 2,000 | [1](datafusion-execution-2000-1.json), [2](datafusion-execution-2000-2.json), [3](datafusion-execution-2000-3.json) | [1](datafusion-execution-range-baseline-2000-1.json), [2](datafusion-execution-range-baseline-2000-2.json), [3](datafusion-execution-range-baseline-2000-3.json) | [1](datafusion-execution-eager-baseline-2000-1.json), [2](datafusion-execution-eager-baseline-2000-2.json), [3](datafusion-execution-eager-baseline-2000-3.json) |
| 20,000 | [1](datafusion-execution-20000-1.json), [2](datafusion-execution-20000-2.json), [3](datafusion-execution-20000-3.json) | [1](datafusion-execution-range-baseline-20000-1.json), [2](datafusion-execution-range-baseline-20000-2.json), [3](datafusion-execution-range-baseline-20000-3.json) | [1](datafusion-execution-eager-baseline-20000-1.json), [2](datafusion-execution-eager-baseline-20000-2.json), [3](datafusion-execution-eager-baseline-20000-3.json) |

Executable SHA-256:

- Current: `44a929189bc73d957717a80ec24c31e1e3db4a0aa61b946ca70b168c0b10655e`
- Previous `ae58e56`: `dfdbb7733f28c08d67afa46f5c0a3d242122cdb5f5a592440837835b5e090584`
- Eager `772968f`: `ea991198ad0172a22c1a52ca7134c4de8349f6439c2768d9632b5d1f8f7c34d3`

The [independent review](../reviews/DATAFUSION-EXECUTION.md) describes correctness,
authorization, resource/error handling and the cooperative deadline limitation.
Package publication and Bend reference verification remain separate milestones.
