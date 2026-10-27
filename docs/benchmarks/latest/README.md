# Paged reads and loading: one million records

Filtered grouping now takes **28.03 ms**, versus **33.72 ms** for the intermediate
baseline and **31.00 ms** for the borrowed-iterator control: reductions of **16.9%**
and **9.6%**. The 1% range aggregate falls from **396.05 to 278.93 µs** (**29.6%**),
or **17.9%** against the control. The remaining PostgreSQL gaps are explicit:
filtered grouping is **21.5% slower** than PostgreSQL's **23.06 ms**; the 1% range
is **9.5% slower** than its **254.78 µs**. This closes part of the gap, not all of it.

Loading's median is **29.78 s**, versus **30.95 s** for the intermediate baseline
and **30.40 s** for the control. Their ranges overlap. Both comparison binaries
already contain the earlier loading and small-read optimizations, and this run
does not establish their total improvement over the original implementation.
The surviving baseline is an intermediate build, not the start-of-task version.

Small reads remain mixed. Against the borrowed control, warm point and equality
medians are **14.2% and 13.3% slower**; against the intermediate baseline, selective
joins are **14.4% slower**. Their execution paths are unchanged by compiled index
validation, and round ranges overlap, so these are measured regressions without
an established cause. The final warm changing-parameter median improves, but one
round rises to **3.28 µs**. Cold changing-parameter results vary substantially;
no reliable cold-storage speedup is claimed. All observations remain below.

## Builds and provenance

All three builds use dense LPG3 rows and boxed backend page payloads:

- **Intermediate baseline (v4):** `e7e07b6da92fef23a1fed508ffdac3d710c7b7687588858e17d5f57482d88c4f`.
  It already includes earlier loading, cache, authorization-fence, packed-range
  and scan changes. Earlier temporary binaries/results were lost in an environment
  reset; its original source manifest is unavailable. The preserved binary hash
  is valid, but this is not a reproducible initial-task source baseline.
- **Borrowed control:** `8b24bdb2fd6645ad2359b4d8dc7305ee4313ed36f79b7df09f22574e26bf207c`.
  The index range visitor borrows immutable leaf entries, avoiding per-entry
  owning access guards while preserving iterator bounds and failure behavior.
- **Final:** `0ed70904c5b6ab6f9bc922148a684f2db26a5b0bee80ef9d5cc788e6922246ec`.
  It additionally compiles the fixed index prefix/type/width once and compares
  canonical payloads directly with validated packed values, including exact key
  length, tuple terminators and occurrence identities.

The [final source manifest](comparison/final-source-sha256.json) and
[borrowed source manifest](comparison/borrowed-source-sha256.json) contain 250
entries and differ only in `src/paged/packed_range.rs`. The
[compiled-index patch](comparison/compiled-index-validation.patch), SHA-256
`4c364e94499a16ddc3187cd285c73424a3c87b3addb0f3f3f7f241992844cbb9`,
transforms that borrowed source into the final source; reversing it reconstructs
the control on the recorded manifest. The patch includes its differential tests.
[The baseline manifest](comparison/baseline-manifest.json) records the earlier
baseline's limited provenance. This comparison does not isolate page allocation.

The earlier loading changes reuse validated block metadata, avoid duplicate
index-version encoding, skip redundant dependency reads when the pinned view is
exactly current, and omit an unused final tentative commit map. Small-read changes
reuse validated packed lengths, compiled relations and stack keys. Live authority
publication uses a generation mirror with final fences and poison checks. These
changes are documented and tested, but their aggregate speedup is not recoverable
from the surviving before/after pair.

## Method

There are **500,000 Person + 500,000 Score records**, **two relation families in
two application namespaces**, 32 groups, and ordered one-to-one references.
Matching schema identities, values, indexes and all eleven query outputs are
verified in **three sequential rotated rounds**, 15 observations, on one ARM64
macOS runner with 10 logical CPUs. Each observation has a fresh process/database.
No builds, tests or other benchmarks overlapped; ordinary workstation activity
was not eliminated. [Plan](comparison/plan.json), [raw results](comparison/results.json),
[summary](comparison/summary.json) and [environments](comparison/environments.json)
retain hashes, settings, execution order, plans and resources.

Query times are **microseconds per query, the median of three round means**,
not request p50/p99. Preparation and warmup are excluded. The full aggregate has
one timed iteration per round. OS caches are not flushed: “cold” describes the
first changing-parameter pass outside the decoded working set, not a cold SSD.
Repeated full comparisons are separated experiments; none are pooled or silently
substituted for the primary result.

Substrate uses Immediate durability; SQLite uses WAL/FULL/fullfsync; PostgreSQL
18.3 uses synchronous commit/fsync_writethrough. Backend cache targets are 8 MiB;
Substrate also has a 1 MiB decoded cache. PostgreSQL uses prepared statements over
a local Unix socket, 4 MiB work memory and no parallel query workers. Its latency
includes IPC; Substrate and SQLite are embedded. Substrate enforces reference
visibility that the comparison SQL schemas do not model. PostgreSQL process
sampling every 250 ms adds overhead only to its cases. Loading uses matching
transaction batches; PostgreSQL uses per-row prepared protocol calls, not COPY.

Reproduce with `experiments/paged_sql/compare_paged.py --before BASELINE_BINARY
--control BORROWED_BINARY --after FINAL_BINARY --rows 500000 --rounds 3
--output FRESH_DIRECTORY --pg-config PATH_TO_PG_CONFIG`. Binaries must use LPG3.
The subsequent I/O, native/API and wide-group runs are separate diagnostics.
This is one two-family workload, not a general engine ranking or capacity limit.

## All SQL queries and resources

Positive deltas mean slower. All round means, including outliers, are retained.
CPU/RSS cover the whole client, including loading and untimed diagnostics. CPU is
the median of each process's user plus system seconds; RSS is the maximum across
three processes. Loading records/second is **not TPS, network bandwidth or a
concurrent-writer limit**.

| Query (µs) | Intermediate baseline | Borrowed control | Final | SQLite | PostgreSQL | Final vs Intermediate baseline | Final vs Borrowed control |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm point | 0.420 | 0.368 | 0.421 | 1.413 | 9.238 | +0.1% | +14.2% |
| Warm equality | 0.483 | 0.440 | 0.498 | 1.126 | 9.121 | +3.1% | +13.3% |
| Changing parameters, cold | 25.592 | 7.297 | 9.274 | 37.913 | 14.023 | -63.8% | +27.1% |
| Changing parameters, warm | 1.861 | 1.961 | 1.763 | 2.177 | 13.177 | -5.3% | -10.1% |
| Selective join | 0.818 | 0.921 | 0.936 | 0.678 | 11.237 | +14.4% | +1.6% |
| Full aggregate | 32,780.333 | 33,984.500 | 32,346.708 | 7,068.958 | 28,127.291 | -1.3% | -4.8% |
| Equality aggregate | 1.158 | 1.194 | 1.145 | 0.255 | 11.174 | -1.1% | -4.1% |
| Range aggregate (1%) | 396.052 | 339.929 | 278.929 | 82.883 | 254.779 | -29.6% | -17.9% |
| Broad filter (90%) | 32,993.167 | 33,318.222 | 33,190.819 | 6,249.028 | 30,450.944 | +0.6% | -0.4% |
| Grouped aggregate | 33,606.458 | 34,153.264 | 33,309.889 | 310,529.236 | 43,288.542 | -0.9% | -2.5% |
| Filtered grouping (50%) | 33,720.458 | 31,004.139 | 28,026.681 | 306,371.444 | 23,059.417 | -16.9% | -9.6% |

Round-mean ranges (µs), preserving all observations:

| Query | Intermediate baseline | Borrowed control | Final | SQLite | PostgreSQL |
| --- | ---: | ---: | ---: | ---: | ---: |
| Warm point | 0.389–0.427 | 0.360–0.420 | 0.392–0.474 | 1.259–1.526 | 9.043–9.553 |
| Warm equality | 0.432–0.484 | 0.406–0.500 | 0.448–0.559 | 1.061–1.136 | 9.076–10.026 |
| Changing parameters, cold | 9.247–25.885 | 5.108–23.293 | 4.798–24.357 | 7.021–57.010 | 13.547–19.383 |
| Changing parameters, warm | 1.505–1.862 | 1.712–2.152 | 1.534–3.280 | 2.061–5.454 | 12.579–14.196 |
| Selective join | 0.803–0.896 | 0.834–0.984 | 0.812–0.960 | 0.665–1.150 | 11.068–11.798 |
| Full aggregate | 30,890.083–34,832.417 | 31,889.583–41,160.583 | 32,074.459–40,321.500 | 6,970.750–8,947.792 | 26,930.708–28,833.750 |
| Equality aggregate | 1.141–1.213 | 1.127–1.216 | 1.113–1.181 | 0.252–0.317 | 10.880–11.264 |
| Range aggregate (1%) | 396.042–419.394 | 339.448–340.012 | 278.152–288.040 | 81.650–103.454 | 250.915–295.360 |
| Broad filter (90%) | 31,867.444–35,051.847 | 32,838.528–33,663.306 | 32,859.764–36,794.097 | 6,179.528–6,586.847 | 28,279.347–31,774.736 |
| Grouped aggregate | 32,357.472–33,677.139 | 33,079.222–34,203.417 | 33,243.514–34,077.833 | 302,407.139–318,305.111 | 41,964.458–43,641.347 |
| Filtered grouping (50%) | 33,712.569–35,252.070 | 29,753.653–31,677.528 | 27,055.750–32,131.792 | 299,787.403–321,440.306 | 22,525.736–23,641.556 |

| Resource metric | Intermediate baseline | Borrowed control | Final | SQLite | PostgreSQL |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load seconds | 30.95 | 30.40 | 29.78 | 26.30 | 29.23 |
| Load records/second | 32,312.63 | 32,895.95 | 33,581.78 | 38,019.86 | 34,211.24 |
| Client CPU seconds | 12.88 | 12.80 | 12.56 | 10.41 | 4.13 |
| Peak client RSS MiB | 37.42 | 44.59 | 44.66 | 21.88 | 12.12 |

Intermediate baseline load range: 28.903–32.117 seconds.

Borrowed control load range: 29.659–30.496 seconds.

Final load range: 29.171–31.955 seconds.

SQLite load range: 25.003–27.954 seconds.

PostgreSQL load range: 29.053–30.000 seconds.

PostgreSQL table/index MiB: 154.10; database excluding WAL MiB: 161.76; maximum sampled sum of server RSS MiB (shared pages double counted): 82.41.

Final peak client RSS is **44.66 MiB**, versus **44.59 MiB** for the borrowed
control and **37.42 MiB** for the intermediate baseline in this run. The final
increase against that baseline is retained rather than hidden. The comparison
keeps the same bounded page/decoded caches and fixed scan cursor state; process
RSS also includes allocator behavior, mappings and other runtime costs. This
measurement does not establish a new hard RSS cap or a causal explanation for
the different high-water marks. PostgreSQL's server RSS sum double counts shared
pages and is not directly comparable to embedded-client RSS.

Storage is unchanged for the three Substrate builds: **682.38 MiB allocated**,
**1,256 MiB logical**, or **715.53 allocated bytes per record**. SQLite's open
main/WAL/SHM footprint is **197.82 MiB allocated** and **181.51 MiB logical**.
PostgreSQL's tables and indexes occupy **154.10 MiB**, with **161.76 MiB** for the
whole database excluding WAL. These scopes differ; sparse logical size and
allocated size are separate measures. Dense payload blocks are only part of
Substrate's footprint: role, unique, incoming-reference, identity and conflict
version trees remain substantial.

## Rejected inline experiment

A separate completed three-round, 15-observation comparison added `#[inline]`
to three small backend accessors. It was rejected and the attributes removed.
Against its same-run borrowed control, filtered grouping rose from **30.91 to
31.55 ms**, warm point from **0.393 to 0.433 µs**, equality from **0.463 to
0.507 µs**, and loading from **28.11 to 29.09 s**. Other metrics were mixed,
including a lower reported RSS and cold-read median; those do not establish a
causal benefit. The primary final build does not contain these hints.

[Raw rejected observations](rejected-inline/results.json),
[summary](rejected-inline/summary.json), [plan](rejected-inline/plan.json), and
[source manifest](rejected-inline/source-sha256.json) preserve that experiment.
Its results are not pooled into the headline comparison. Single-round pilots
were used for screening, not as the final performance evidence.

## Authorization, bounds and validation

The packed path still checks every selected index key, complete typed root and
terminal-reference record, snapshot revision, credential scope, visibility policy,
work/page bounds and live authority. Denied referenced records cannot disclose
private participants. Unsupported schemas and policy shapes retain the general
executor. Borrowed slices cannot escape their callback; owning guards and pinned
snapshots retain their lifetimes. No additional row cache, table-sized scan
materialization, weaker durability or parallel writer was introduced.

The final feature-enabled engine suite passed **794 tests**, with **11 ignored**
in 51 test groups. The backend library passed 41 tests, focused iterator checks
passed 10, the borrowed lifetime compile-fail test passed, and strict Clippy passed.
These suites overlap in coverage and must not be added into a new unique-test
count. The backend checks were also run on the earlier code-identical borrowed
backend; the final compiled-index change is confined to the engine.
[Validation](validation.json) retains exact commands/log scopes. Unchanged SDK
and Python harness suites were not rerun in this iteration; historical successes
are not counted as fresh validation. Independent subagents reviewed lifetimes,
errors, memory limits, index equivalence and authorization; [review](review.md)
records scope and limitations.

## Separate I/O and memory diagnostics

One separate instrumented observation per intermediate baseline/final binary
counts requests to redb’s FileBackend, including OS-cache hits. These are **not
physical SSD traffic, syscall counts or network bandwidth**. Query counts cover
timed iterations only; instrumented latency is not pooled into the headline.
[Raw observations](io/results.json) and [plan](io/plan.json) retain the complete data.

| Query | Baseline calls/query | Final calls/query | Baseline MiB/query | Final MiB/query |
| --- | ---: | ---: | ---: | ---: |
| Warm point | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| Warm equality | 0.0005 | 0.0005 | 0.0000 | 0.0000 |
| Changing parameters, cold | 2.8660 | 2.8660 | 0.0112 | 0.0112 |
| Changing parameters, warm | 1.0955 | 1.0955 | 0.0043 | 0.0043 |
| Selective join | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| Full aggregate | 20,622.0000 | 20,622.0000 | 80.5547 | 80.5547 |
| Equality aggregate | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| Range aggregate (1%) | 35.0000 | 35.0000 | 0.1367 | 0.1367 |
| Broad filter (90%) | 20,631.0000 | 20,631.0000 | 80.5898 | 80.5898 |
| Grouped aggregate | 20,622.0000 | 20,622.0000 | 80.5547 | 80.5547 |
| Filtered grouping (50%) | 17,160.0000 | 17,160.0000 | 67.0312 | 67.0312 |

Read calls and requested bytes are identical for every case. The improvements
therefore come from CPU-side work in these measurements, not fewer backend
requests. Filtered grouping’s instrumented CPU drops from **35.30 to 28.10 ms/query**
and the range aggregate from **404.65 to 280.90 µs/query**. Broad/full scans still
request approximately **80.6 MiB/query**, including validation of referenced rows.

Loading also issues identical requests: **177,857 reads / 728,498,496 bytes**,
**647,458 writes / 2,637,204,928 bytes**, and **3,915 backend sync requests**.
There are **3,908 engine durable groups**, each with one accepted transaction,
including setup. Sync requests are not identical to engine groups or guaranteed
physical synchronization syscalls. This serial load cannot demonstrate concurrent
group-commit amortization. The last read-path optimizations do not reduce its I/O.

Allocator telemetry reports identical live heap in both binaries at every
recorded phase: **8,487,328 bytes after load** and **9,390,160 bytes after diagnostics**.
Reserved allocator memory finishes at **24 MiB baseline versus 32 MiB final**.
The extra final RSS appears during changing-parameter reads, before the new packed
range path executes. That is consistent with allocator reservation/layout effects
rather than a larger logical cache, but it does not prove the root cause. The higher
RSS is real and remains in the results. Allocator reservation, live heap and lifetime
RSS high-water marks are distinct measures.

## Native and SQL, resident and paged

A separate sequential supplement ran **three rotated rounds** of resident SQL,
resident native and paged native using the final binary and the same verified
one-million-record fixture. The paged SQL/SQLite/PostgreSQL columns are carried
from the primary comparison; they were not pooled with this supplement.
[Raw observations](matrix/results.json), [summary](matrix/summary.json) and
[environment](matrix/environment.json) preserve that distinction.

**Resident means durable storage with memory-resident current data and indexes**,
not a nondurable in-memory database. Its heap can grow with data size. Paged mode
keeps current data/indexes on disk with bounded caches. Both use Immediate durability.

Native get/lookup operations use the public API; joins assemble indexed lookups
in the client. Native aggregates are public scans and client folds: resident scans
eagerly copy rows, while paged scans return **256-row pages**. Native range/broad
folds scan and filter; SQL can push down filters and aggregates or use indexes.
These columns compare usable API execution paths, **not identical aggregate
kernels or SQL-parser overhead**. The native scan costs show a remaining API
pushdown/copying opportunity; they do not invalidate the bounded SQL executor.

| Query (µs) | Resident native | Resident SQL | Paged native | Paged SQL | SQLite | PostgreSQL |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm point | 0.462 | 0.901 | 0.293 | 0.421 | 1.413 | 9.238 |
| Warm equality | 0.603 | 0.870 | 0.750 | 0.498 | 1.126 | 9.121 |
| Changing parameters, cold | 5.620 | 5.780 | 32.203 | 9.274 | 37.913 | 14.023 |
| Changing parameters, warm | 1.399 | 1.508 | 1.952 | 1.763 | 2.177 | 13.177 |
| Selective join | 0.865 | 1.235 | 1.019 | 0.936 | 0.678 | 11.237 |
| Full aggregate | 49,356.125 | 1,675.709 | 433,967.125 | 32,346.708 | 7,068.958 | 28,127.291 |
| Equality aggregate | 0.282 | 22.242 | 0.782 | 1.145 | 0.255 | 11.174 |
| Range aggregate (1%) | 46,156.892 | 42.142 | 444,960.219 | 278.929 | 82.883 | 254.779 |
| Broad filter (90%) | 45,661.222 | 1,347.847 | 460,374.972 | 33,190.819 | 6,249.028 | 30,450.944 |
| Grouped aggregate | 46,957.222 | 4,335.278 | 434,825.833 | 33,309.889 | 310,529.236 | 43,288.542 |
| Filtered grouping (50%) | 45,941.139 | 2,208.681 | 434,369.847 | 28,026.681 | 306,371.444 | 23,059.417 |

<details>
<summary>Native/resident round-mean ranges (µs)</summary>

| Query | Resident native | Resident SQL | Paged native |
| --- | ---: | ---: | ---: |
| Warm point | 0.453–0.483 | 0.554–0.904 | 0.290–0.320 |
| Warm equality | 0.600–0.624 | 0.584–0.941 | 0.674–0.762 |
| Changing parameters, cold | 5.007–6.883 | 3.975–10.342 | 8.025–176.714 |
| Changing parameters, warm | 1.118–1.475 | 0.915–1.965 | 1.890–2.177 |
| Selective join | 0.851–0.879 | 0.987–1.304 | 0.999–1.198 |
| Full aggregate | 48,150.250–51,604.791 | 1,579.416–1,680.666 | 432,043.541–443,038.667 |
| Equality aggregate | 0.281–0.289 | 21.495–22.363 | 0.769–0.801 |
| Range aggregate (1%) | 45,231.463–47,072.742 | 40.173–42.892 | 437,319.138–466,734.140 |
| Broad filter (90%) | 45,614.264–47,744.042 | 1,276.208–1,363.375 | 436,520.542–461,188.194 |
| Grouped aggregate | 46,915.694–49,652.514 | 4,306.805–4,336.042 | 431,931.778–465,907.458 |
| Filtered grouping (50%) | 45,466.764–48,004.514 | 2,205.306–2,239.653 | 432,079.514–454,624.958 |

</details>

The main comparison already lists the ranges for its other three columns.

| Resource | Resident native | Resident SQL | Paged native |
| --- | ---: | ---: | ---: |
| Load seconds | 29.08 | 30.54 | 34.60 |
| Client CPU seconds | 14.43 | 13.43 | 31.18 |
| Peak client RSS MiB | 2,210.22 | 1,671.86 | 33.11 |

RSS includes loading, queried result materialization and untimed diagnostics;
the resident-native peak especially includes copied scan results. These are
whole-process high-water marks rather than the engine’s persistent data alone.
The paged-native cold-parameter round means range from **8.02 to 176.71 µs**;
the large outlier remains present rather than being discarded.

## Remaining bottlenecks

- **Filtered grouping:** still 21.5% behind PostgreSQL in the 32-group fixture.
  Every selected row and referenced target is validated; PostgreSQL’s fixture
  has no equivalent reference-visibility work. The index path also traverses
  separate index, root and target trees.
- **Broad/full scans:** approximately 80.6 MiB of backend requests per query
  remain unchanged, and final medians trail PostgreSQL by 9.0%/15.0%. Reducing
  CPU overhead alone does not remove that data-access requirement.
- **Native aggregate APIs:** public-page decoding/copying and client folds cost
  much more than SQL pushdown. Native query builders need equivalent authorized,
  bounded aggregate execution to share these benefits.
- **Small reads and memory:** measured tiny-query regressions and broad cold-read
  variance remain unresolved. The larger final RSS reservation is real despite
  unchanged logical cache limits and identical sampled live heap.
- **Loading:** serial Immediate commits still synchronize separately; these last
  changes do not reduce backend write traffic or sync requests. Existing concurrent
  group commit needs multiple outstanding writes to amortize synchronization.
  Separate index/version trees also contribute to write amplification and disk size.

## Wider grouping diagnostic

A separate **single observation per build/engine** changes the same one-million
records to **1,024 groups**, beyond the 64-slot numeric group cache. It is a
screening diagnostic, not a repeated non-regression guarantee. The intermediate
baseline ran first, followed by final paged SQL, SQLite and PostgreSQL.
[Baseline observations](wide/before/results.json), [final/comparator observations](wide/after/results.json)
and their environment files retain all cases and settings. This uses the final
binary and preserves authorization checks; it is not pooled with the 32-group
headline results.

| Query (µs) | Intermediate baseline | Final paged SQL | SQLite | PostgreSQL |
| --- | ---: | ---: | ---: | ---: |
| Warm point | 0.328 | 0.360 | 0.646 | 9.238 |
| Warm equality | 0.384 | 0.422 | 0.550 | 9.036 |
| Changing parameters, cold | 4.135 | 10.478 | 2.699 | 14.838 |
| Changing parameters, warm | 1.663 | 1.546 | 2.081 | 12.486 |
| Selective join | 0.810 | 0.804 | 0.778 | 11.146 |
| Full aggregate | 31,992.125 | 30,393.125 | 7,380.041 | 26,090.875 |
| Equality aggregate | 1.105 | 1.096 | 0.276 | 11.247 |
| Range aggregate (1%) | 390.010 | 263.673 | 87.252 | 252.787 |
| Broad filter (90%) | 32,959.236 | 31,382.625 | 6,127.458 | 26,674.208 |
| Grouped aggregate | 46,123.708 | 45,060.986 | 87,574.264 | 40,774.111 |
| Filtered grouping (50%) | 41,731.945 | 34,757.083 | 87,788.167 | 22,759.264 |

Filtered grouping improves **16.7%**, from **41.73 to 34.76 ms**, but remains
**52.7% slower** than PostgreSQL’s **22.76 ms**. Full grouping is **45.06 ms**
versus PostgreSQL’s **40.77 ms**. More groups expose additional map/accumulator
overhead; the small-group headline advantage does not generalize to this case.

In this single diagnostic, loading is **39.23 s baseline / 42.08 s final**,
with peak client RSS **37.88 / 45.91 MiB**. SQLite loads in **35.38 s** and
PostgreSQL in **31.04 s**. These observations change group cardinality and run
later than the primary comparison; they must not be substituted for its medians.
