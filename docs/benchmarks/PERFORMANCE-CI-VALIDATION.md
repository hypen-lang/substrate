# PR performance gate validation — 2026-10-25

Validated locally on macOS / Apple M5 (10 cores, 32 GiB), Rust 1.94.0. This is
validation of the CI harness, not a GitHub-hosted Linux run or a cross-database
ranking. The baseline is committed `76ebf345f326aabddde4d354038434137b6da365`;
the candidate includes the pre-existing uncommitted engine changes in this
workspace. Consequently both build records have the same Git HEAD, but distinct
binary/dependency fingerprints. The CI workflow itself checks out distinct exact
base and proposed-merge revisions.

## Checks and real comparisons

- 17 controller tests pass, including regression direction, noise, outliers,
  incorrect results, missing data, process failures, timeout and descendant
  cleanup, balanced ordering, and fixed-work memory selection.
- Four kernel benchmark tests pass; kernel Clippy with warnings denied passes.
- Both revisions build with the common kernel and SQL sources. Invalid SQL
  sampling declarations fail before loading a fixture.
- `actionlint` 1.7.7 validates the workflow; formatting and whitespace checks pass.
- An independent code review found no outstanding blockers.

Every comparison has one discarded warmup pair and six measured pairs with
alternating order. Builds and tests did not overlap timed measurements.

| Run | Outcome | Interpretation |
| --- | --- | --- |
| [Kernel](perf-ci-2026-10-25-kernel/summary.md) | All 16 metrics pass | Covers native reads, durable writers, load/recovery, fixed-work memory and size. |
| [Original SQL sampling](perf-ci-2026-10-25-sql/summary.md) | Two regressions and several inconclusive metrics | Existing seven samples of ten queries were too short for reliable tiny-query comparisons in this run. Kept as evidence. |
| [Original identical-binary control](perf-ci-2026-10-25-sql-identical/summary.md) | All 19 metrics pass | Both sides execute the same candidate binary. |
| [Final SQL sampling](perf-ci-2026-10-25-sql-stable/summary.md) | 17 pass, two regressions, none inconclusive | Longer fixed per-query repetition counts; no budget changes. |
| [Final identical-binary control](perf-ci-2026-10-25-sql-stable-identical/summary.md) | All 19 metrics pass | Confirms the final sampling configuration accepts identical executables on this host. |

The final SQL gate **correctly fails** for the current working tree's correlated
`EXISTS` case: admin median 119.1 → 430.6 µs and authorized-reader median
118.2 → 555.1 µs. All six pairs exceed the budget for both access modes. This
pre-existing engine slowdown remains unresolved by the CI implementation; no
query engine behavior or threshold was changed to make it pass. All query result
hashes and row counts agree. Other SQL cases pass, including point reads,
selective joins, ranges, grouped reads and full scans. The nested-query case
improves substantially.

Each directory contains the captured policy, per-process JSON/stderr/CPU/RSS,
normalized pairs, and the decision. Original failed/noisy runs are preserved;
the final run changes the sampling method and is not a retry until green.

## Activation and limits

The workflow has not been pushed or executed on GitHub, and branch rules have not
been modified. After merging, require **Performance gate** on `develop` and
require an up-to-date branch. Run identical-binary controls on the actual Linux
runner fleet before tightening budgets. The initial checks screen meaningful
15–25% regressions; they cannot prove zero slowdown or cover every workload.

See the [CI guide](../../tools/perf/README.md) for the base-owned policy,
reproduction commands, activation steps, fairness boundaries and E2B discussion.
