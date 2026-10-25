# Native overhead benchmark

Date: 2026-10-25. Baseline: commit `41c7472`.

Three alternating serialized process pairs. Both binaries use the same operation
harness and allocation instrumentation; baseline projection substitutes the old
`table(...).project(...)` API. Final current schemas are explicitly private,
matching baseline grant-only reads. New visibility metadata is created during
untimed setup. This is a whole-engine comparison, not isolated attribution to
individual patches. Exact output, retained snapshots, indexes and durable reopen
are asserted before a successful process emits JSON.

Timings include operation/result disposal, excluding setup and reporting.
Allocation bytes are requested allocation traffic, not live memory or RSS; both
binaries pay the instrumentation cost. See [implementation details](../NATIVE-OVERHEADS.md).

## Updates

Numbers are medians of three process p50s, in microseconds. Each row has the listed
integer roles plus one payload role; only one integer changes unless marked all.

| Integer roles | Payload | Retained snapshot | Changes | Durability | Before p50 | After p50 | Before allocated bytes | After allocated bytes |
| ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: |
| 2 | 4,096 | False | one integer | memory | 9.542 | 5.833 | 109,245 | 68,778 |
| 2 | 4,096 | True | one integer | memory | 9.667 | 5.625 | 109,245 | 66,568 |
| 8 | 4,096 | False | one integer | memory | 12.959 | 6.083 | 116,781 | 69,138 |
| 8 | 4,096 | True | one integer | memory | 13.542 | 6.084 | 116,781 | 69,138 |
| 32 | 4,096 | False | one integer | memory | 34.208 | 9.083 | 207,917 | 72,914 |
| 32 | 4,096 | True | one integer | memory | 34.167 | 9.000 | 207,917 | 72,904 |
| 32 | 4,096 | True | all integers | memory | 43.417 | 39.125 | 276,589 | 243,538 |
| 8 | 65,536 | True | one integer | memory | 42.791 | 22.583 | 976,941 | 619,730 |
| 8 | 65,536 | True | one integer | Immediate redb | 4,026.541 | 4,001.167 | 1,376,182 | 1,021,339 |

## Native reads

256 rows, each with a 64 KiB payload. Projection selects identity and integer.

| API | Before p50 µs | After p50 µs | Before allocated bytes | After allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| legacy_table_project | 540.125 | 555.291 | 16,869,702 | 16,836,998 |
| projected_table | 570.917 | 16.875 | 16,869,702 | 29,038 |
| owned_scan | 506.666 | 555.125 | 16,813,990 | 16,809,894 |

The baseline `projected_table` row is the original table-then-project operation,
not an API present in the old engine. Full owned scans still copy all payloads.

## Four-reader contention

Each reader repeatedly builds a 32-family scoped snapshot and reads one row while
a writer performs 256 commits. These measure whole commit latency, not isolated
mutex wait. Snapshot construction now resolves scope outside the publication lock.

| Metric | Before µs | After µs |
| --- | ---: | ---: |
| Writer p50 | 3.333 | 21.833 |
| Writer p99 | 238.000 | 89.333 |
| Scoped snapshot p50 | 3.166 | 3.292 |
| Writer mean | 38.826 | 25.385 |

Both iterations showed lower mean/tail commit latency but a worse median;
shorter publication locking is not a universal per-call latency improvement.
Durable sequential writes remain dominated by synchronization. These limits and
unchanged full-payload copy work should not be hidden by the projection speedup.
The final full owned scan was about 9.6% slower (507 → 555 µs), despite slightly
less allocation traffic. The projection API avoids copying unwanted payloads;
it does not make callers requesting all owned payloads allocation-free.

## Reproduction

Build/freeze `cargo build --release --example native_overheads`, and run
`experiments/native/run_overheads.py --baseline BEFORE --current AFTER --output NEW_DIR`.
The baseline harness is copied into a checkout of `41c7472`, with private installs
using its original `install` method and projected reads using the legacy operation.
The runner refuses overwrites and checks identical workload descriptions.

Final raw data: [native-overheads-final](native-overheads-final/).
Earlier iteration: [native-overheads-followup](native-overheads-followup/).

before SHA-256: `8471a981bd2034982e93260c941839a4ab2d86084b98c4d2c6b6ef6c7555003e`.

after SHA-256: `8d3a3746616f1207d7c030be66fc1e110ff74e6c19a291919158505e7701820e`.
