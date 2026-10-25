# Restricted reads, owned scans, and contention

Date: 2026-10-25. Baseline: `2dac848`. All times are microseconds unless stated otherwise.

## Method

SQL: three alternating, serialized process pairs at 2,000 and 20,000 people (18,000 and 180,000 relation occurrences). Each warm result is the median of seven batches of ten executions; tables below report the median of three process medians. The harness checks exact result digests across both builds, DataFusion principals, the SQLite compatibility adapter, and direct SQLite, including distinct parameter bindings. Setup, catalog construction, and preparation are excluded from warm execution and reported separately in the raw files.

Native: nine alternating process pairs using exactly the same updated harness against both builds. Each owned-read process p50 uses 24 individual timed operations after three warmups. Writes use 128 memory or 24 durable samples. Result disposal is timed; setup, verification, and reporting are excluded. Both native binaries use the same allocation counter; its atomic updates add instrumentation overhead, especially under contention. Allocation figures count requested bytes, not RSS. No builds, tests, or other task benchmarks ran during timing.

SQLite has equivalent indexed data and output but does not enforce Substrate visibility, reference closure, credential scope, or live revocation. The native SQLite BLOB control produces the same owned Rust rows. There is no directly equivalent SQLite capability/contention control.

## Restricted SQL reads: 2,000 people

| Query | Previous DataFusion | Current DataFusion | Current SQLite compatibility | Direct SQLite |
| --- | ---: | ---: | ---: | ---: |
| Primary key | 0.700 | 0.375 | 0.654 | 0.321 |
| Indexed equality | 8.208 | 5.008 | 8.404 | 7.250 |
| Range / top 100 | 22.933 | 14.767 | 98.513 | 24.742 |
| Relationship lookup | 1.842 | 1.204 | 2.104 | 1.017 |
| Five-family join | 5.221 | 3.521 | 6.171 | 2.833 |
| Grouped join / top 25 | 1,217.871 | 1,201.850 | 10,282.508 | 3,311.946 |
| Correlated EXISTS | 124.421 | 121.171 | 2,962.904 | 408.188 |
| Nested window / top 1 | 1,563.762 | 1,547.946 | 5,563.933 | 2,321.429 |
| Full scan aggregate | 40.092 | 40.062 | 316.462 | 99.429 |

## Restricted SQL reads: 20,000 people

| Query | Previous DataFusion | Current DataFusion | Current SQLite compatibility | Direct SQLite |
| --- | ---: | ---: | ---: | ---: |
| Primary key | 0.783 | 0.404 | 0.671 | 0.362 |
| Indexed equality | 91.112 | 57.521 | 96.775 | 81.217 |
| Range / top 100 | 25.646 | 18.054 | 1,252.046 | 59.579 |
| Relationship lookup | 2.092 | 1.312 | 2.346 | 1.133 |
| Five-family join | 5.871 | 3.783 | 6.675 | 3.346 |
| Grouped join / top 25 | 12,866.562 | 12,842.858 | 130,534.033 | 40,864.262 |
| Correlated EXISTS | 875.733 | 872.825 | 33,845.704 | 453.717 |
| Nested window / top 1 | 8,009.433 | 7,724.529 | 90,808.742 | 25,384.533 |
| Full scan aggregate | 167.225 | 162.192 | 3,725.262 | 1,057.942 |

## Admin SQL control: 20,000 people

| Query | Previous DataFusion | Current DataFusion | Direct SQLite |
| --- | ---: | ---: | ---: |
| Primary key | 0.417 | 0.383 | 0.362 |
| Indexed equality | 71.425 | 56.217 | 81.217 |
| Range / top 100 | 22.167 | 16.887 | 59.579 |
| Relationship lookup | 1.462 | 1.221 | 1.133 |
| Five-family join | 4.142 | 3.404 | 3.346 |
| Grouped join / top 25 | 13,018.796 | 12,809.258 | 40,864.262 |
| Correlated EXISTS | 869.517 | 854.858 | 453.717 |
| Nested window / top 1 | 7,895.825 | 7,702.425 | 25,384.533 |
| Full scan aggregate | 165.792 | 162.600 | 1,057.942 |

## Previously unseen bindings: 20,000 people

Seventy distinct parameter sets per case, including missing matches. Every first binding execution and repeated execution is checked; this is not a result-cache comparison.

| Query | Previous restricted | Current restricted | Direct SQLite |
| --- | ---: | ---: | ---: |
| unseen_id_values | 0.808 | 0.408 | 0.379 |
| unseen_numeric_equalities | 175.242 | 94.642 | 92.458 |
| unseen_numeric_ranges | 3.837 | 1.950 | 60.188 |
| unseen_relationship_joins | 1.892 | 1.225 | 3.300 |
| unseen_five_way_joins | 5.371 | 3.325 | 11.004 |

The affected restricted point, equality and range reads improved by approximately 48%, 37% and 30%. Broader aggregate and EXISTS execution is mostly unchanged; SQLite remains faster on the 20k correlated EXISTS case. Cold setup remains meaningful: a fresh restricted point-query handle (catalog + prepare + execution) is 129.875 µs, versus 160.000 µs previously. The 0.404 µs point result is warm execution, not end-to-end remote latency.

## Native owned reads

256 rows with one integer and one 64 KiB payload each: 16 MiB of requested payload. Projection selects only identity and integer.

| Operation | Previous p50 | Current p50 | Previous allocation bytes | Current allocation bytes |
| --- | ---: | ---: | ---: | ---: |
| legacy_table_project | 647.125 | 625.583 | 16,836,998 | 16,830,950 |
| projected_table | 18.916 | 6.334 | 29,038 | 22,990 |
| owned_scan | 648.792 | 641.709 | 16,809,894 | 16,801,830 |
| sqlite_owned_scan | 888.959 | 881.417 | 16,801,792 | 16,801,792 |

Full owned scans are effectively flat in this final measurement: 648.792 → 641.709 µs. The per-process medians span 488–796 µs before and 479–779 µs after. This spread is too large to claim a substantial full-payload speedup or statistical elimination of the earlier 9.6% timing regression. The implementation removes duplicate row lookups, repeated memo locking, and redundant policy work, but still copies all 16 MiB. Projection shows the execution-overhead reduction much more clearly (18.916 → 6.334 µs). Exact reservation is used only when every candidate is proven visible; filtered/reference results cannot expose hidden counts through capacity.

## Four-reader contention

The original finite workload has four readers each taking 256 scoped snapshots/reads while one writer commits 256 updates. Faster readers can finish before later writes. The sustained workload first verifies a read on every reader, then keeps all four active until the writer finishes. Per-write active-reader counts and individual reader progress are recorded and verified. These are whole-operation latencies, not isolated mutex timings.

| Metric | Previous | Current |
| --- | ---: | ---: |
| Finite: writer p50 | 23.250 | 4.375 |
| Finite: writer p99 | 61.167 | 56.334 |
| Sustained: writer p50 | 25.666 | 24.334 |
| Sustained: writer p99 | 64.708 | 65.666 |
| Sustained: reader p50 | 20.375 | 4.417 |
| Sustained: reader p99 | 58.667 | 33.208 |
| Finite: writer mean | 24.799 | 11.758 |
| Sustained: writer mean | 28.028 | 28.308 |
| Scoped snapshot construction p50 | 3.208 | 0.083 |

The finite writer median improves 81%. Under continuously active readers, writer latency is roughly unchanged (p99 is 1.5% higher, within overlapping run ranges), while reader median/tail improve substantially. Readers complete a median of 3,460 operations during the 256 writes, versus 1,328 previously; each reader makes progress in every run. This is not a claim of uniformly faster writers under unlimited read load.

An earlier blocking overlay cache improved writers but worsened reader p99. Nonblocking cache access removed that convoy; the final generation fence also removes repeated publication-mutex acquisition when state is unchanged. The [initial cache results](read-regressions-native/) and [nonblocking-only results](read-regressions-native-nonblocking/) are retained separately and are not final results.

## Write regression controls

| Integer roles | Payload bytes | Retained snapshot | Changed fields | Durability | Previous p50 | Current p50 |
| ---: | ---: | --- | --- | --- | ---: | ---: |
| 2 | 4,096 | False | one integer | memory | 5.541 | 5.708 |
| 2 | 4,096 | True | one integer | memory | 5.584 | 5.625 |
| 8 | 4,096 | False | one integer | memory | 6.000 | 5.958 |
| 8 | 4,096 | True | one integer | memory | 5.875 | 6.042 |
| 32 | 4,096 | False | one integer | memory | 8.958 | 9.083 |
| 32 | 4,096 | True | one integer | memory | 9.000 | 9.083 |
| 32 | 4,096 | True | all integers | memory | 39.792 | 40.000 |
| 8 | 65,536 | True | one integer | memory | 22.167 | 22.584 |
| 8 | 65,536 | True | one integer | Immediate redb | 4,016.084 | 4,022.042 |

The previous partial-update improvement is retained; sequential durable commits remain near 4 ms. Small memory-write changes are around a few percent, not a new write-throughput improvement.

## Verification

- 446 Rust tests passed, including crash recovery, visibility, credential rotation/expiry, live revocation, SQL/native parity and reference dependency checks; one manual performance probe remains intentionally ignored.
- 53 TypeScript/SDK tests passed; strict all-target Clippy and formatting checks passed.
- Independent subagent review found and prompted a fix for physical-cardinality disclosure through restricted result capacity. Follow-up review covered nonblocking caches, publication races, failure paths, same-revision replication, historical transaction binding and generation exhaustion, with no remaining findings.
- The existing TLS expiry integration test now leaves at least one full second for its initial handshake/read; it still waits for expiry and asserts denial on the same connection.

## Reproduction and raw evidence

Archive `2dac848` and copy the current `examples/native_overheads.rs` into that archive for a matched native harness. Build and freeze the previous and current release binaries. If reusing a target directory across source checkouts, clean the engine release package between builds to prevent stale artifact reuse; executable fingerprints must differ.

```sh
python3 experiments/native/run_overheads.py \
  --baseline /tmp/substrate-read-native-before \
  --current /tmp/substrate-read-native-final \
  --output /tmp/read-native-repeat --repeats 9

python3 experiments/datafusion/run_tiny.py \
  --baseline /tmp/substrate-tiny-final2 \
  --binary /tmp/substrate-read-sql-final \
  --output /tmp/read-sql-repeat --rows 2000 20000 --repeats 3
```

Final raw results: [native](read-regressions-native-final/) and [SQL](read-regressions-sql/). Runners refuse overwrites, record SHA-256, process order and start times, and verify outputs. PostgreSQL was not rerun.

| Binary | SHA-256 |
| --- | --- |
| SQL baseline | `4c30af761b40faabb5354b4cb6cb8611f20f5e6ae1ec7f5c9ca2be8788766168` |
| SQL current | `834ae428e540cf3b768fec203b2c511d976972a2157bdf58d755e19c7449902b` |
| Native baseline | `19db94ada9869686b90a98289ff44007ab3df827dd4014ca782853db923f23f0` |
| Native current | `27542ab541b45a741cd371f59689cb5ce66c03f6b7a983f9873164dc0624c704` |
