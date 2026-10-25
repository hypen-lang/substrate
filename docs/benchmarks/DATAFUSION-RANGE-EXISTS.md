# DataFusion range, EXISTS and window follow-up

This report compares the revised SQL adapter with both preserved release builds:
the eager DataFusion adapter from `772968f` and the first indexed adapter from
`94baeae`. Each current process also measures explicit `sqlite_compat()` and
direct indexed SQLite. The previous [indexed report](DATAFUSION-INDEXED.md) and
all its raw measurements remain unchanged.

## Measurement method

The deterministic fixture, nine SQL query shapes, SQL text, result conversion,
and timing boundaries remain unchanged from the indexed report. Each process
checks exact ordered results against SQLite before and after repetitions. The
runner verifies matching result digests between all three adapter binaries.
The indexed baseline and current binary also verify parameter-rebinding and
row-policy projection outputs across processes.

For each size (2,000 and 20,000 people, nine occurrences per person), all three
binaries run serially for three repetitions. Execution order rotates so that
each binary occupies each position once. There are no concurrent task builds or
benchmarks during measurements. This is a shared macOS development machine,
not an isolated performance lab. No PostgreSQL, network, durable-write or
transport serialization timings are included.

Warm numbers are the median of three process medians, each computed from seven
samples of ten executions after warm-up. They are averages per query within a
sample, not single-request p50/p99. Setup, preparation and first execution are
separate fields. Fresh SQL handles have unprimed authorized Arrow caches but
reuse the loaded native database and warm CPU/OS caches; they are not cold disk
measurements. Shared-handle first executions follow workload order and may
inherit compatible cached pages. Initial process/runtime overhead is retained
in the raw data and distinguished from later preparation costs.

The same-ID prepared workload measures plan-cache hits. Cycles of 16 IDs,
numeric equalities and numeric ranges measure rebinding and cache behavior.
Changing values miss the one-entry bound-plan cache, while the initial correctness
pass may populate all source pages; timed rebinding can therefore combine plan
misses with source-page hits. Every parameter set is checked before and after timing. The payload projection
fixture checks admin and restricted reader outputs independently, with direct
SQLite using an equivalent predicate for the reader.

## Previous plan diagnosis

The first indexed adapter's range/top-100, EXISTS and window workloads all
use `LiquidScanExec access=numeric_range`, which reconverts qualifying rows on
every execution. The range plan has `limit=None`, followed by DataFusion TopK.
EXISTS uses two hash semi-joins and a final TopK; the nested window performs
joins and sorting before its partitioned row number. The eager adapter instead
scans previously converted Arrow pages. These plans identify repeated conversion
as a plausible shared cost; they do not apportion the entire regression without
component profiling.

Point reads already have a minimal identity scan. Their remaining gap against
SQLite includes Substrate's facade, authorization, DataFusion execution and
result conversion. The selective five-family join pushes its person constant
into Person, Performed and CustomerDeal, but still scans Workout and Deal.
Dynamic join-time index probes remain a separate opportunity.

## Changes under measurement

Numeric ranges without a pushed LIMIT can now reuse authorized projected Arrow
pages in the same bounded per-handle cache used for full scans. Range cache keys
include typed bounds, direction, every pushed predicate, projection and cursor.
Hits still recheck live authority and recharge candidate-row and projected-byte
budgets; retained key memory counts toward the cache budget. Identity/equality
lookups remain direct native reads.

Deterministic EXISTS, IN and scalar subqueries can now retain optimized logical
plans after recursive inspection of their original subquery and view plans.
Stable/volatile functions remain excluded. Joins still build fresh physical
execution state. This removes avoidable repeated logical optimization without
retaining mutable join or top-k state.

## Reproduction

Preserve the prior binaries before rebuilding, then run:

```sh
cargo build --offline --release --bench comparison -j 3
python3 experiments/datafusion/run_range_exists.py \
  --binary target/release/deps/comparison-<hash> \
  --indexed-baseline /tmp/substrate-indexed-94baeae \
  --eager-baseline /tmp/substrate-eager-datafusion-772968f
```

The runner requires three distinct binaries, at least three repetitions,
removes the optional PostgreSQL environment setting, records executable hashes,
and refuses to overwrite existing result files. To repeat the experiment, use
`--output` with a fresh directory. The eager binary's slower script-based fixture
loading is excluded from read measurements. Rebuild baselines in separate
checkouts of their named commits if the preserved binaries are unavailable.

Preserved indexed binary SHA-256:
`461529237dbbad67054c46eed91a269acae3d22e223de8dca2e793aa8be1dd5f`.
Preserved eager binary SHA-256:
`ea991198ad0172a22c1a52ca7134c4de8349f6439c2768d9632b5d1f8f7c34d3`.

## Point-read dispatch diagnostic

An isolated optimized unit probe used a one-row fixture, 100 warm-up calls per
path, then nine samples of 1,000 calls. These are median sample means:

| Call path | Microseconds per call |
|---|---:|
| Executor no-op round trip | 10.433 |
| Prepared constant SQL | 19.766 |
| Prepared identity SQL | 25.010 |
| Native identity read | 0.055 |

The no-op still traverses the runtime dispatch/timeout path. This exposes a
substantial fixed call-path cost even before relation access. It is a descriptive
comparison, **not additive component profiling**: subtracting rows does not
isolate the cost of individual phases. It also does not replace the 2k/20k
same-fixture SQL comparisons. Native reads do not parse or execute SQL, and
bypassing the SQL facade's authorization or budgets is not proposed.

Reproduce independently, with no concurrent builds or measurements:

```sh
cargo test --offline --release --lib sql_execution_overhead_probe -- --ignored --nocapture
```

[Raw probe output](sql-overhead-range-exists.txt). Measured release test binary
SHA-256: `be61251e436b76c790060b5836c1f9fdafd631cf9d4eadd4f7e8123748f7bb6d`.

## Results

All 18 processes completed successfully. Every common ordered result matched
across all three builds and SQLite; repeated-query, parameter-rebinding and
restricted-row checks passed. The environment was macOS/aarch64, DataFusion
facade 55.0.0 / execution crates 55.2.0, imbl 7.0.2 and SQLite 3.46.0.

At 20k, warm range execution improves **13.18×** over the first indexed adapter;
EXISTS improves **2.47×** and the nested window **2.18×**. Against fresh eager
DataFusion measurements, the respective improvements are **2.37×, 1.09× and
1.06×**. The latter two are small margins on a variable shared machine: window
wins in two of three runs, with the third 4.9% slower. This recovers the prior
large regressions, but does not demonstrate a robust universal speedup over
eager DataFusion.

### Warm query execution

All values are **microseconds per query**, median of three process medians.
Every baseline was rerun in this experiment; historical reports are not the
source of the comparisons below. Compatibility means the explicit SQLite-backed
Substrate adapter, not direct SQLite.

#### 2,000 people / 18,000 occurrences

| Workload | Current DF admin | Indexed 94baeae admin | Eager 772968f admin | Current DF reader | Compatibility admin | Direct SQLite |
|---|---:|---:|---:|---:|---:|---:|
| Primary key | 22.39 | 30.81 | 82.14 | 22.15 | 0.69 | 0.37 |
| Typed numeric equality | 141.76 | 98.76 | 167.93 | 74.89 | 10.43 | 8.01 |
| Range / top 100 | 104.23 | 191.88 | 277.10 | 95.95 | 140.91 | 28.02 |
| Relationship lookup | 210.28 | 218.85 | 539.76 | 188.61 | 2.32 | 1.16 |
| Selective five-family join | 655.41 | 641.18 | 1,228.68 | 639.83 | 6.48 | 3.22 |
| Grouped join / top 25 | 2,559.09 | 2,335.13 | 2,879.23 | 2,699.40 | 13,312.61 | 4,061.94 |
| Correlated EXISTS | 298.16 | 791.70 | 1,010.42 | 305.93 | 3,407.18 | 485.80 |
| Nested window / top 1 | 3,085.82 | 4,084.31 | 3,488.21 | 3,155.91 | 7,731.28 | 2,901.77 |
| Full-scan aggregate | 151.77 | 147.19 | 225.83 | 175.00 | 589.52 | 138.92 |

#### 20,000 people / 180,000 occurrences

| Workload | Current DF admin | Indexed 94baeae admin | Eager 772968f admin | Current DF reader | Compatibility admin | Direct SQLite |
|---|---:|---:|---:|---:|---:|---:|
| Primary key | 32.13 | 26.38 | 182.98 | 30.97 | 1.46 | 0.42 |
| Typed numeric equality | 213.77 | 171.40 | 281.63 | 357.62 | 132.12 | 87.73 |
| Range / top 100 | 194.95 | 2,569.08 | 462.13 | 154.38 | 3,284.96 | 66.02 |
| Relationship lookup | 481.90 | 449.33 | 659.43 | 456.51 | 2.58 | 2.61 |
| Selective five-family join | 1,221.38 | 1,200.48 | 2,889.38 | 1,107.58 | 7.43 | 7.53 |
| Grouped join / top 25 | 17,260.21 | 17,576.07 | 17,161.85 | 17,955.20 | 154,147.41 | 49,589.86 |
| Correlated EXISTS | 1,384.22 | 3,423.49 | 1,505.57 | 1,356.65 | 38,996.29 | 527.43 |
| Nested window / top 1 | 10,891.78 | 23,779.55 | 11,550.28 | 10,894.72 | 121,911.81 | 31,292.45 |
| Full-scan aggregate | 321.26 | 419.95 | 343.55 | 310.68 | 6,369.02 | 1,310.68 |

Point and selective-join costs have not been fixed by these changes. At 20k,
current point reads are 32.13 µs versus 26.38 µs in the prior indexed adapter;
selective joins are 1.221 versus 1.200 ms. Numeric equality also regresses in the
observed admin medians, 213.77 versus 171.40 µs, while the repeated parameterized
equality workload below is essentially unchanged. These paths do not use the
new range cache. The shared-machine variability prevents assigning these changes
to a particular cause without further isolation/profiling. They are reported
rather than omitted. Direct SQLite still wins point, equality, range, relationship,
selective-join and EXISTS reads in this fixture. Current DataFusion wins the
larger grouped join, window and full-scan aggregate.

The three 20k repetitions expose the variability behind the summary. Values
are **milliseconds**, listed current / prior indexed / eager:

| Repetition | Range / top 100 | EXISTS | Nested window |
|---|---|---|---|
| 1 | 0.143 / 2.510 / 0.425 | 1.338 / 3.506 / 2.049 | 10.446 / 25.125 / 11.550 |
| 2 | 0.195 / 2.691 / 0.462 | 1.518 / 3.423 / 1.506 | 11.785 / 23.780 / 12.471 |
| 3 | 0.224 / 2.569 / 0.568 | 1.384 / 3.209 / 1.457 | 10.892 / 21.120 / 10.379 |

For example, current point samples across processes span 21.53–34.04 µs and
eager point samples span 135.64–250.42 µs. Small percentage differences should
not be treated as statistically established improvements. Large range gains
appear in all repetitions. The runner rotates execution order, but cannot
remove external system load or thermal/frequency variability.

### Catalog, preparation and first execution

Catalog medians below are **milliseconds**. Admin construction is first in its
process and includes startup effects; reader construction comes afterward.

| People | Current admin | Prior indexed admin | Eager admin | Current reader | Prior indexed reader | Eager reader |
|---|---:|---:|---:|---:|---:|---:|
| 2,000 | 0.4388 | 2.6822 | 4.6172 | 0.0610 | 0.0890 | 13.3596 |
| 20,000 | 18.1109 | 21.2795 | 71.5690 | 0.1656 | 0.1250 | 133.1428 |

Current first admin catalog samples at 20k are 0.785, 18.111 and 20.994 ms;
first point preparation samples are 0.723, 21.497 and 24.742 ms. Those are
preserved initial outliers, not steady-state prepare latencies. The first
point execution median is 16.607 ms; later fresh-handle point preparation and
execution are 0.124 and 0.229 ms. This change does not claim that initial runtime
startup or all first-time SQL preparation became faster.

Each workload also creates a fresh SQL handle, verifies outputs, and records
catalog/prepare/execute separately. The following **20k millisecond** values
show the cost when authorized source pages have not been populated:

| Workload | Admin catalog | Admin prepare | Admin execution | Reader execution |
|---|---:|---:|---:|---:|
| Primary key | 0.1039 | 0.1237 | 0.2290 | 0.1354 |
| Typed numeric equality | 0.1093 | 0.1606 | 0.5285 | 0.3870 |
| Range / top 100 | 0.1019 | 0.1510 | 2.7754 | 6.1063 |
| Relationship lookup | 0.1115 | 0.1913 | 8.8397 | 26.1804 |
| Selective five-family join | 0.0940 | 0.4201 | 18.3627 | 52.7140 |
| Grouped join / top 25 | 0.1425 | 0.5427 | 59.5222 | 145.8496 |
| Correlated EXISTS | 0.0947 | 0.1948 | 17.4405 | 71.8830 |
| Nested window / top 1 | 0.1150 | 0.5703 | 37.9728 | 89.6884 |
| Full-scan aggregate | 0.1245 | 0.2639 | 8.2065 | 23.2500 |

Warm range execution is 0.195 ms, but a fresh admin handle needs 2.775 ms;
EXISTS goes from 1.384 to 17.440 ms, and the window from 10.892 to 37.973 ms.
The restricted reader's first-use costs are higher because uncached source
materialization checks policy per row. Cache reuse therefore helps repeated
queries on a pinned authorized SQL handle, not every one-shot query equally.
The new cache remains bounded to 64 MiB estimated retained memory / 4,096 FIFO
entries shared with full-scan pages. Larger working sets, eviction, changing
projections and short-lived handles require separate measurements.

### Rebinding and source-cache reuse

Prepared parameter executions at 20k, **microseconds per query**:

| Parameters | Current admin | Prior indexed admin | Current reader | Compatibility admin | Direct SQLite |
|---|---:|---:|---:|---:|---:|
| Same ID repeatedly | 28.83 | 25.63 | 28.90 | 0.74 | 0.86 |
| 16 distinct IDs | 65.84 | 82.82 | 75.78 | 0.75 | 0.47 |
| 16 numeric equality values | 290.43 | 290.45 | 280.30 | 141.27 | 102.51 |
| 16 numeric ranges | 187.20 | 1034.38 | 195.09 | 609.24 | 62.17 |

The 16-range sequence improves 5.53× over the prior indexed adapter. Changed
parameters still miss the one-entry bound-plan cache, but the initial correctness
pass has populated source pages for the 16 ranges, allowing source-cache hits.
This does not demonstrate performance for endlessly changing unseen ranges or a
working set larger than the cache. The 256-row payload projection/row-policy
fixture also passed every admin/reader result check; its raw timings are retained.

### Remaining work

Ordered-range/top-k pushdown could avoid visiting and sorting all qualifying
rows when only a small prefix is needed. Dynamic join-driven probes could avoid
scanning Workout and Deal for the selective join. The runtime diagnostic above
also identifies a fixed SQL dispatch floor worth profiling. Neither native query
semantics nor the SQL authorization/budget checks were bypassed for these results.

## Raw measurements

All reports include complete timing samples, exact output digests and EXPLAIN
plans. Current and prior indexed reports additionally include compatibility,
fresh-handle, parameter-rebinding and projection timings.

| People | Current | Prior indexed | Eager |
|---|---|---|---|
| 2,000 | [1](datafusion-range-exists-2000-1.json), [2](datafusion-range-exists-2000-2.json), [3](datafusion-range-exists-2000-3.json) | [1](datafusion-range-exists-indexed-baseline-2000-1.json), [2](datafusion-range-exists-indexed-baseline-2000-2.json), [3](datafusion-range-exists-indexed-baseline-2000-3.json) | [1](datafusion-range-exists-eager-baseline-2000-1.json), [2](datafusion-range-exists-eager-baseline-2000-2.json), [3](datafusion-range-exists-eager-baseline-2000-3.json) |
| 20,000 | [1](datafusion-range-exists-20000-1.json), [2](datafusion-range-exists-20000-2.json), [3](datafusion-range-exists-20000-3.json) | [1](datafusion-range-exists-indexed-baseline-20000-1.json), [2](datafusion-range-exists-indexed-baseline-20000-2.json), [3](datafusion-range-exists-indexed-baseline-20000-3.json) | [1](datafusion-range-exists-eager-baseline-20000-1.json), [2](datafusion-range-exists-eager-baseline-20000-2.json), [3](datafusion-range-exists-eager-baseline-20000-3.json) |

Current comparison binary SHA-256:
`dfdbb7733f28c08d67afa46f5c0a3d242122cdb5f5a592440837835b5e090584`.
Baseline hashes are recorded above and in every raw file. All prior reports and
raw files remain unchanged.
