# Checkpoint recovery benchmark — 2026-10-24

Run `cargo bench --offline --bench recovery`. See
[raw measurements](2026-10-24-checkpoint-run.txt) and
[benchmark source](../../benches/recovery.rs).

Environment: arm64 macOS, Rust 1.94.0, release profile, pinned dependencies. Same
machine as the original SQLite baseline. OS caches were warm, not explicitly flushed;
this is a development workstation, not an isolated benchmark host.

Each workload starts with 1,000 current rows, two scalar roles and a unique label
constraint. Every subsequent commit updates 100 rows, rotating through the same
1,000 identities. We compare opening the **same file and same exact logical state**
before and after a manual checkpoint. Automatic checkpoints are explicitly disabled
for the unbounded-history baseline. Each phase has one warmup and seven timed opens;
all rows and indexes are checked outside timing after every open. Open time includes
loading the state, validation, index reconstruction, and opening redb; it excludes
verification and closing the handle. These are medians of individual warm opens.

| Update history | Full replay | Checkpoint, zero tail | Speedup | Checkpoint + 10 new commits |
| --- | ---: | ---: | ---: | ---: |
| 100 commits / 10,000 updates | 38.020 ms | 10.983 ms | 3.46× | 15.561 ms |
| 500 commits / 50,000 updates | 174.107 ms | 11.135 ms | 15.64× | 16.803 ms |

The full-replay run includes the original two catalog/data commits: 102 and 502
records respectively. Checkpointing eliminated 600,662 and 2,722,662 redo bytes.
Both checkpoints held the same number of live rows and 86,146 payload bytes
(excluding keys/manifest/backend overhead). The ten-commit tails held 53,050 bytes.

Full-replay ranges were 37.617–42.625 ms and 149.289–305.895 ms; checkpoint ranges
were 10.443–12.259 ms and 10.729–11.919 ms. The longer-history baseline had visible
scheduling/cache variability. These measurements demonstrate avoided replay work;
they do not establish a universal 15× speedup or startup latency bound.

A single checkpoint took 5.625 ms and 7.526 ms respectively. These single samples
are maintenance costs, not latency percentiles. Automatic checkpoints do this work
on the foreground commit that reaches the budget. They can cause commit stalls;
background/incremental checkpointing is not implemented.

File lengths after checkpoint were 2,174,976 and 4,304,896 bytes. Pruning releases
covered log pages for reuse but does not promise file shrinkage. Allocated identity
tombstones remain in the current state. Larger current datasets still cost more to
load and index even with a zero-length redo tail.

The existing [SQLite query baseline](README.md) compares indexed native/SQL queries
against ordinary SQLite tables. This recovery experiment isolates the effect of
Liquid checkpointing; it does not equate rebuilding Liquid's in-memory indexes with
SQLite opening persistent indexes. Controlled subprocess/I/O crash tests are covered
in the verification report, outside benchmark timings.
