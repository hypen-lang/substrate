# Manifest ordering and schema compiler scaling

Measured 2026-10-25 on the development machine, release build. Reproduce:

```sh
cargo run --release --offline --example manifest_performance
```

The example compares the frozen previous ordering routine with the current implementation in the **same process**, asserts identical results, warms each path once and reports the median of five runs. Inputs are reverse dependency chains: namespace `n00000` imports `n00001`, and so on. Module contents are empty to isolate ordering. No disk IO or installed-catalog validation is included. Other development work shared the machine; these are local algorithm probes, not production installation latencies.

| Modules | Previous median | Graph/ready queue median |
| ---: | ---: | ---: |
| 1,000 | 4.093 ms | 0.228 ms |
| 2,000 | 17.743 ms | 0.478 ms |
| 4,000 | 77.787 ms | 1.015 ms |
| 8,000 | 334.927 ms | 2.037 ms |

Raw output: [2026-10-25-manifest-ordering.csv](2026-10-25-manifest-ordering.csv).

## Changes

`SchemaManifest::canonical` now builds explicit import and prior-version dependencies, then emits the smallest ready module from a deterministic ordered queue. It retains the previous lexicographically-first-ready order, sorted imports/relations and declaration contents. Ordering work is O((V+E) log V), plus unchanged payload cloning and import/relation sorting. Missing versions/imports, cycles, zero versions and duplicate module versions retain their existing error messages. Repeated imports are one graph dependency but remain in the original declaration for the semantic validator to reject.

Adjacent lookups in re-locking, selected-module traversal and combined-manifest planning now use keyed maps/sets rather than repeated linear searches. New-module internal reference checks use one precomputed exact-name set, preserving case sensitivity and allowing same-module forward/cyclic reference definitions as before. These changes do not introduce a new schema format or alter IDs, role layouts or fingerprints.

## Verification and limits

Five new tests include a 128-case random-graph differential property against the frozen old routine, ordering/error regressions, a fixed existing-manifest fingerprint, exact declaration preservation and forward-reference/case-sensitive validation. All 18 existing DX tests pass; targeted Clippy passes with warnings denied.

This does **not** make the whole schema pipeline linear. Full semantic validation still installs definitions into a temporary database; installed namespace/name collision checks and repeated validation across authoring/selection/generation can still dominate large real catalogs. Benchmark those paths separately before claiming end-to-end installation improvements. Richer constraints and schema evolution are separate workstreams.
