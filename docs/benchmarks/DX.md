# DX and SQL measurements — 2026-10-24

Run `npm run build:ts && node tools/bench-typescript.mjs`. The fixture uses 501
relations across 11 modules, 500 typed insert consumers, scalar/reference/decimal
fields and generated runtime validators. The generated TS has shallow per-record
interfaces and descriptors; no global relationship union or recursive query generic.
All generation goes through the Rust schema compiler. This is a compiler benchmark;
the fixture does not open a SQL snapshot across 11 namespaces (SQLite limits those
to ten).

One development-machine run, Node 22.22.1 / TypeScript 5.9.3, arm64 macOS:

| Measurement | Result |
|---|---:|
| Rust validation + TS/Rust source generation | 225 ms |
| Cold TypeScript process wall time, build cache removed | 528 ms |
| Incremental TypeScript process wall time, unchanged inputs | 200 ms |
| Cold compiler reported memory | 127,519 KiB (124.5 MiB) |
| Generated TypeScript source | 384,311 bytes |
| Transport JS source | approximately 5.5 KiB |

`target/dx-type-bench/results.json` and `compiler.txt` retain machine-readable results
and TypeScript extended diagnostics. Checked-in copies are
[results](2026-10-24-dx-typescript-run.json) and [diagnostics](2026-10-24-dx-typescript-compiler.txt). Cold means compiler cache cold, not OS disk cache
cold. Memory is the compiler's reported memory, not measured process peak RSS. Timings
include process startup; this is a single run, not latency percentiles. The script
checks generous regression ceilings (15s cold, 10s incremental, 350,000 KiB compiler
memory, 2 MB generated TS); these are development checks, not product SLAs.

Editor responsiveness, declaration-only package size, optimized bundle/tree-shaking
size and browser startup have **not** been measured. SDK source bytes are not bundle
bytes. Module selection excludes unrelated apps; per-module artifacts and watch mode
are still future work. Run the fixture on CI hardware before enforcing tighter budgets.

The existing equivalent-SQLite harness is rerun for this milestone. It checks
query results before timing and distinguishes preparation costs. See the raw
`2026-10-24-dx-sqlite-run.txt` file and the final section below. The earlier checkpoint
recovery improvement remains documented separately in CHECKPOINTS.md; current DX work
does not alter the durable checkpoint format or claim parallel redb writers.


## Equivalent SQLite rerun

2,000 people / 10,000 occurrences, warm cache, seven batches; median batch mean:

| Workload | Liquid | Ordinary indexed SQLite |
|---|---:|---:|
| Indexed lookup | 3.053 µs | 2.275 µs |
| Prepared cross-app point join | 2.746 µs | 1.114 µs |
| Point join including prepare | 7.110 µs | not measured |
| Additive install, empty database initially | 16.381 µs | not applicable |
| Additive install, 10,000 existing occurrences | 20.502 µs | not applicable |

This run uses current paging/compiled metadata and the same query correctness
checks as the original harness. Lookup is about 1.34× and prepared join 2.46× SQLite.
Small differences from prior runs are not isolated optimization evidence. This
fixture stays below checkpoint thresholds: its 46.029 ms reopen is full replay of
two commits, not the bounded-recovery comparison. No parallel write throughput,
authorization overhead or larger-than-memory workload was measured here.
