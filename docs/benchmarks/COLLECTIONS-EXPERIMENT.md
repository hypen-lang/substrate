# Persistent collections: isolated engine experiment

The experiment keeps the normal engine on `im 15.1.0` and builds a frozen copy
with only its dependency alias changed to `imbl 7.0.2`. No engine source shim was
needed. The full candidate Rust suite passed **316 tests including the doctest**,
covering snapshots, indexed queries, optimistic conflicts, authorization, durable
replay, checkpoints, group commit, transport, replication and disk snapshot leases.
This is compatibility evidence for the tested source, not a proof of every
concurrent execution or power-loss case.

## Scope and reproduction

[The helper](../../experiments/collections/README.md) prepares independent source
copies and documents the build commands. The [frozen source hashes](collections-source-sha256.json)
identify the measured engine; the recorded candidate lockfile pins transitive
dependencies. The root Cargo manifest and lockfile remain unchanged.

`examples/collections_probe.rs` runs the actual public engine API at 1,000 and
10,000 records with three roles: a unique integer, a 100-way bucket and a 256-byte
payload. All roles retain the engine's normal indexes. It measures bulk setup,
snapshot capture/clone, point reads, equality lookup, full materialized scans,
512 one-row updates with and without retained snapshots, 512 insertions in
transactions of 1 or 32 rows, and eight concurrent independent writers. The retained
case keeps the initial snapshot and one snapshot after each 32 updates. Index
verification, row-count/digest checks and retained-snapshot assertions run outside
timed loops. The concurrent case includes thread startup and join.

The storage mode is memory-only. Timings therefore include normal engine
validation, persistent-state updates, queue-size encoding and publication, but no
disk synchronization. They do not predict how much faster durable group commits
would become. Native reads return owned rows, so row copying/allocation is part of
the read measurement. Bulk setup is reported separately from operations on an
existing database.

The platform is arm64 macOS 26.2 with rustc 1.94.0. Both variants use the same
default optimized Cargo release profile and compiler flags. The harness executes
separate prebuilt processes serially, alternating backend order over three runs,
and checks deterministic final-state digests across all runs and variants. This
is a local development-machine experiment, not an isolated production load test.

## Dependency and build observations

The [upstream imbl README](https://github.com/jneem/imbl) describes improvements to
ordered maps/sets; those are upstream claims, separate from this engine experiment.
Its [package metadata](https://github.com/jneem/imbl/blob/main/Cargo.toml) declares
Rust 1.85 and MPL-2.0+. The pinned `im 15.1.0` package also declares MPL-2.0+.
The project's Rust floor is 1.89; this experiment used 1.94 rather than testing that
minimum toolchain.

The optimized probe executable grew from **2,341,504 to 2,376,912 bytes** (+1.5%).
That is this linked probe's size, not a measurement of the complete server or of
resident memory. The candidate lock replaces the old bitmap/chunk dependencies
with archery, imbl-sized-chunks, wide/safe_arch and related dependencies. Optional
`triomphe` and `small-chunks` variants were not tested. No production dependency
switch has been made.

## Results

[Raw native engine runs](collections-2026-10-25.jsonl) and their
[summary with individual samples and binary hashes](collections-2026-10-25-summary.json)
retain all results. The table shows medians of three batch means, in microseconds
per operation. Insert/update/setup values are per row; scan values are per complete
scan; snapshot values are per capture/clone. The ratio is im time / imbl time:
above 1 favors imbl. These are not commit p50/p99 measurements.

### 1,000 initial records

| Operation | im µs | imbl µs | im / imbl |
| --- | ---: | ---: | ---: |
| bulk_fixture | 5.266 | 3.672 | 1.43× |
| full_scan | 94.792 | 122.204 | 0.78× |
| independent_writers_8 | 29.052 | 23.392 | 1.24× |
| indexed_equality | 1.296 | 2.029 | 0.64× |
| insert_batch_1 | 32.521 | 13.436 | 2.42× |
| insert_batch_32 | 6.029 | 3.749 | 1.61× |
| point_read | 0.100 | 0.122 | 0.82× |
| snapshot_capture | 0.030 | 0.037 | 0.81× |
| snapshot_clone | 0.009 | 0.010 | 0.92× |
| updates_retained_snapshots | 28.221 | 13.795 | 2.05× |
| updates_unretained | 28.780 | 14.121 | 2.04× |

### 10,000 initial records

| Operation | im µs | imbl µs | im / imbl |
| --- | ---: | ---: | ---: |
| bulk_fixture | 6.512 | 4.488 | 1.45× |
| full_scan | 1172.354 | 1244.388 | 0.94× |
| independent_writers_8 | 37.001 | 28.495 | 1.30× |
| indexed_equality | 14.981 | 14.393 | 1.04× |
| insert_batch_1 | 41.792 | 18.226 | 2.29× |
| insert_batch_32 | 8.328 | 4.761 | 1.75× |
| point_read | 0.125 | 0.154 | 0.81× |
| snapshot_capture | 0.030 | 0.037 | 0.80× |
| snapshot_clone | 0.010 | 0.010 | 0.98× |
| updates_retained_snapshots | 38.079 | 18.409 | 2.07× |
| updates_unretained | 39.218 | 18.055 | 2.17× |

### Native engine interpretation

The write improvement is substantial and consistent across both sizes. At 10,000
records, one-row insertions were 2.29× faster, updates 2.17× faster, updates retaining
old snapshots 2.07× faster, and 32-row batches 1.75× faster per inserted row. Eight
independent writers improved 1.30×; contention/coordination overhead does not scale
in proportion to an individual map update.

The result is not an across-the-board read improvement. At 10,000 records point
reads rose from 0.125 to 0.154 µs, while full materialized scans rose from 1,172 to
1,244 µs. Indexed equality was roughly similar. The 1,000-record read cases were
noisier: imbl equality samples were 2.167, 2.029 and 1.231 µs, against im samples
1.724, 1.296 and 1.256 µs. Their median suggests regression, but its magnitude is
not stable enough to generalize. Snapshot capture/clone differences are only a few
nanoseconds; they do not establish a meaningful snapshot performance benefit.

## Prepared SQL comparison

The same frozen variants also ran the existing eight-workload SQL ladder with
2,000 people and 18,000 total occurrences across five relation families. This is
the current SQLite virtual-table adapter over Substrate, not a new SQL engine.
Both variants used identical fixtures and queries. Every returned cell and ordering
matched the equivalent SQLite representation, including the authorized reader
session. Cross-variant row counts and SHA-256 result hashes also matched.

[Raw SQL runs](collections-sql-2026-10-25.json) retain SQLite controls, setup times,
plans and all run summaries. The [paired summary](collections-sql-2026-10-25-summary.json)
includes all samples and executable hashes. Each table value is the median of
three process-run medians; each process measured seven batches of ten warm
prepared executions. Preparation and fixture setup are excluded; returned-row
materialization/normalization is included. These are local API timings rather
than network request latencies or individual-query p50/p99.

| Workload | Admin im µs | Admin imbl µs | im / imbl | Reader im µs | Reader imbl µs | im / imbl |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 01_primary_key | 0.621 | 0.613 | 1.01× | 0.904 | 0.762 | 1.19× |
| 02_indexed_equality | 10.071 | 9.654 | 1.04× | 9.738 | 9.588 | 1.02× |
| 03_range_top_100 | 129.775 | 121.079 | 1.07× | 133.975 | 128.779 | 1.04× |
| 04_relationship_lookup | 2.329 | 2.158 | 1.08× | 2.783 | 2.425 | 1.15× |
| 05_cross_app_five_way | 6.658 | 6.171 | 1.08× | 8.583 | 7.021 | 1.22× |
| 06_cross_app_group_top_25 | 10406.025 | 10079.538 | 1.03× | 13457.629 | 11967.300 | 1.12× |
| 07_correlated_exists | 2734.033 | 2598.133 | 1.05× | 3892.488 | 3535.142 | 1.10× |
| 08_filtered_nested_top_1 | 5120.242 | 5383.708 | 0.95× | 6394.062 | 6335.783 | 1.01× |

The SQL ladder mostly improved modestly: 3–8% for several admin queries and
10–22% for several authorized-reader queries. Primary-key admin timing and indexed
reader timing were close. The filtered nested query's admin median regressed 5%,
with overlapping run ranges (im 5.09–5.33 ms; imbl 4.80–5.48 ms); that result is
noisy rather than a demonstrated general nested-read regression. None of these
results erase the native point-read slowdown or establish an analytical-engine
breakthrough. The current adapter still does substantial SQLite execution,
authorization and row conversion work independent of the collection crate.

## Decision and remaining evidence

**Keep im as the current default; prioritize imbl as the next controlled
write-performance candidate.** It is the strongest collection replacement tested:
roughly twofold faster isolated writes at both state sizes, no candidate suite
failures, and mostly modest gains in the SQL ladder. It is reasonable to evaluate
an opt-in engine build or follow-up adoption change for write-heavy deployments.
A blanket swap is not justified solely by these runs because native point reads
regressed about 23% at 10,000 records and memory behavior remains unmeasured.

Before default adoption, measure retained-snapshot peak RSS/allocation at larger
states (100,000 to 1,000,000+ occurrences), mixed read/write traffic, authorization
with scoped policies, and durable grouped throughput. This run used ordinary
read-only grants in the SQL ladder, not a policy-complexity stress fixture. More
repetitions and isolated hardware would help separate small read differences from
scheduling/frequency noise. Retained-snapshot correctness was tested, but the
experiment does not quantify retained-history memory cost. No durable-file
compatibility change was needed for the alias experiment; the normal engine's
manifest and lockfile still select im.
