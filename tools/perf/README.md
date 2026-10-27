# Pull request performance checks

`.github/workflows/performance.yml` runs on **every pull request**, with
no branch or path filters, including reopened, updated, and retargeted PRs, plus manual dispatch. It benchmarks the exact PR base SHA and the
proposed merge commit. It does not compare against an old result from another VM.

## What blocks the check

Two isolated GitHub-hosted `ubuntu-24.04` jobs cover:

- **Kernel:** 10,000 rows in one private typed relation, authorized fresh/retained
  point reads, indexed lookup, owned scans, load, checkpoint, usable reopen, and
  1/8 independent durable writers with commit p50/p99 and throughput.
- **SQL:** the existing comparison harness, 2,000 people / 18,000 occurrences,
  five populated relations across core/fitness/crm, nine query shapes, both admin
  and authorized-reader execution. SQLite and the old adapter verify results and
  remain in raw output; their speed does not set Substrate's regression budget.

Both revisions build in release mode with Rust **1.94.0**, their own locked
Cargo dependencies, identical copied benchmark source and no native CPU tuning.
Dependency artifacts may be cached; engine artifacts are cleaned before each
build. Both builds finish before any timed process starts. Each revision uses
fresh processes and database files. One complete warmup pair is discarded;
**six measured pairs alternate base/PR and PR/base**, on the same VM for each suite.
SQL uses seven samples per query, with fixed per-query iteration counts from
`policy.json` (50,000 for point reads, fewer for expensive queries). Both
revisions and all in-process adapters execute the same counts. This lengthens
tiny-query samples without giving faster engines more work in the RSS comparison.
The ordinary comparison benchmark keeps its existing defaults outside CI.
Every SQL result hash and row count must agree across revisions and repetitions.
The kernel checks values, index consistency, private access, retained snapshots,
and exact acknowledged counters after close/reopen. It is not a power-failure test.

The initial budgets in `policy.json` are deliberately screening budgets:

- 15% SQL latency or native read-throughput regression (SQL also needs >0.1 µs).
- 20% load/checkpoint/reopen time, durable TPS, file size or memory regression.
- 25% durable p50/p99 latency regression, also exceeding 100 µs.

Five of six pairs must exceed both applicable budgets, and the median must also
exceed them, to label a **regression**. A median beyond budget without agreement,
or paired-change median absolute deviation above 20 percentage points, is
**inconclusive** and also fails. A single outlier is not sufficient. This is a
transparent engineering rule, not a formal confidence interval or a claim that
smaller regressions are impossible. Calibrate with repeated identical-binary
runs on the actual CI fleet before tightening thresholds. Rerun the complete job
for inconclusive results; do not select favorable samples or automatically retry
until green.

Kernel peak RSS comes from a **separate fixed-work process**: the same row count
and exactly 128 updates with a retained snapshot. Timed writers do variable work;
their process RSS is retained only as diagnostic data. SQL executes fixed counts,
so its process peak is compared directly. Linux reports RSS in KiB and macOS in
bytes; the driver normalizes to bytes. CPU time and wall time are retained, but
CPU time of a throughput test is not treated as equal-work cost. All latency and
TPS measurements remain local API measurements, not remote service throughput.

Missing/extra metrics, non-finite values, changed units, incomplete pairs,
incorrect results, crashes and timeouts fail closed. Each process runs in a fresh
process group; descendants are killed after success, failure or timeout. Raw
JSON, stderr, per-process CPU/RSS, binary/lock/harness fingerprints, CPU details,
runner image and the decision table are uploaded for 30 days. GitHub job summaries
show the metric table. Saved metadata and captured logs replace local workspace and personal home paths with placeholders. Build-log paths remain real until the executable has been copied; execution arguments are never redacted. There is no PR-comment bot or write token.

## Activation and maintaining the baseline

1. Open a PR containing the workflow and harness to run its first paired check.
   Draft PRs are included. Manual dispatch becomes available after the workflow
   exists on the default branch. The initial manual baseline is the published
   `codex/performance-baseline-76ebf34` engine revision because remote `main`
   currently has no Rust project. Select the desired engine baseline explicitly
   once development branches are published; never compare against an empty tree.
   Existing open PRs run on their next update/reopen or manual dispatch after activation.
2. In the target branch ruleset/protection, require **Performance gate** and
   require the branch to be up to date before merging. The workflow fails a check;
   it cannot itself prevent a maintainer from merging without a required check.
3. If a merge queue is enabled later, add equivalent `merge_group` triggering and
   base/merge SHA selection before making it the queue's required check.

The first introduction uses the new harness for both engines if the base lacks
`tools/perf/policy.json`; the workflow emits a bootstrap notice and still compares
both engines. Afterward **the base owns the harness, driver and budgets**. PR edits
to them are unit-tested, but only become measurement policy after being merged.
An API change that cannot compile the common harness fails visibly: review a
compatible harness migration instead of silently skipping its baseline. Changes
to workflows, benchmark source and policies need human review/appropriate
CODEOWNERS protection. This is not adversarial proof against candidate code that
intentionally falsifies its work. Do not use persistent privileged self-hosted
runners for untrusted PR code.

## Local reproduction

The driver uses only Python's standard library and works on Linux/macOS. Supply
**two disposable checkouts**: build copies the common harness into both trees.
Do not use a checkout containing benchmark edits you need to preserve.

```sh
python3 -m unittest discover -s tools/perf -p 'test_*.py'
python3 tools/perf/guard.py build --suite kernel \
  --base /tmp/base-checkout --candidate /tmp/pr-checkout \
  --harness-root /tmp/base-checkout --target /tmp/perf-target \
  --output /tmp/perf-kernel-binaries
python3 tools/perf/guard.py run --suite kernel \
  --binaries /tmp/perf-kernel-binaries --output /tmp/perf-kernel-results
```

Use `--suite sql` with separate output directories for SQL. During bootstrap use
the new harness checkout as `--harness-root`. Use `--offline` only when Cargo
already has both lockfiles' dependencies. To test the noise floor, copy one built
executable to both `base` and `candidate` in a fresh binary directory, then run the
same driver. No benchmarking should overlap compilation, tests or other workloads.

## Fairness and E2B

Our Mac results are fair **within their stated workload, machine, API and durability
boundaries**. They do not establish universal superiority to SQLite/PostgreSQL:
PostgreSQL includes local IPC, Substrate's native API is in-process, authorization
work differs, and macOS full-sync behavior does not predict Linux/cloud disks.
Keep workload definitions, indexes, exact outputs, durability, transaction
isolation, hardware and timings with every cross-engine result.

For PR regressions, compare both versions serially on the same fresh runner.
Independent VMs for base and PR add hardware and neighbor-load differences.
[E2B sandboxes](https://docs.e2b.dev/sandbox) provide isolated Linux VMs and
configurable resources, but that documentation does not promise dedicated,
frequency-stable CPU or equivalent storage latency. E2B is useful for isolation;
it is not inherently a more accurate benchmark host. If used, run a complete
base/PR pair inside one sandbox, not one unrelated sandbox per revision.
[GitHub-hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
already provide fresh VMs for the chosen runner class. For reliable smaller
regression budgets, evaluate a fixed dedicated/ephemeral runner configuration and
repeat across x86 Linux and ARM macOS. Keep the larger 100k/1M scale and external
PostgreSQL comparisons as separately controlled capacity experiments; this PR
suite does not claim complete scale, transport or production-load coverage.

## Local validation

See the 2026-10-25 validation report (historical results removed)
for successful controls and the existing SQL regression detected by the gate.
