# Independent review — 2026-10-27

Subagents reviewed the loading, small-read, packed-range, compiled-index and
borrowed backend changes. No remaining actionable correctness finding was
identified in the reviewed scope. This is source-review evidence, not a claim
that all performance regressions are resolved. Execution evidence is separate
in [validation](validation.json) and [the benchmark report](README.md).

## Checked contracts

- Borrowed redb range visitation uses higher-ranked callback lifetimes; page
  slices cannot escape the callback. The range retains its transaction guard.
  Ordinary owning guards still pin pages across commits. Forward, reverse,
  mixed-direction, break/resume and panic-consumption behavior share the original
  bound/crossing state machine. Storage errors and callback errors keep their
  ordering. There is no new unsafe lifetime extension.
- Compiled fixed-index comparison checks exact key length, family/index prefix,
  type tag, ordered scalar/reference payload, and unique terminator or occurrence
  identity. Differential properties compare it with the canonical encoder and
  malformed-key tests cover initial and later-leaf entries. Every selected root
  and terminal target still undergoes complete typed validation and revision checks.
- Query paths preserve discovery, row policy, capability scope, referenced-target
  visibility, expiry and live-revocation fences. Final fences precede release of
  successful results and data-dependent errors. Unsupported schemas or filtered
  policies fall back before consuming any source; partial results never switch
  executors.
- Compiled metadata and borrowed cursors have fixed per-query size. Root and target
  cursors retain bounded blocks, not all candidates. Page, source, reference,
  transaction/group and aggregate-state budgets remain enforced. Cache accounting
  includes retained packed lengths; these lengths share immutable revision keys.
- Exact-view commit validation skips only dependencies captured from the identical
  immutable current view. Earlier accepted group members' version overlay takes
  priority. Stale views retain durable dependency checks. Omitting the last unused
  overlay does not omit durable versions or conflict tracking.
- Authorization publication stores the generation under the head write lock before
  swapping the published view. Readers use Acquire generation checks; new snapshots
  acquire the same lock. Failure flags and poisoned writer/head locks fail closed.
  Persistence remains Immediate and precedes publication/acknowledgment.

## Tests and limitations

Tests cover arbitrary/corrupt index keys, canonical fixed scalar encodings,
reverse/random occurrence order, callback and storage-error precedence, source
and visibility limits, denied targets, credential changes, old snapshots,
transaction conflicts, integrity checks and crash boundaries. Exact test counts
and log scopes are in validation.json; overlapping suites are not added together.

The iterator refactor was compared with the locally available upstream redb 3.1.3
implementation. Maintaining this local backend API remains an upgrade obligation.
Three extra inline hints were measured and rejected; they are absent from final
source. The final/control source manifests differ only in packed_range.rs, and
the retained patch reconstructs that boundary.

Review does not establish a hard RSS limit, eliminate allocator reservation,
verify physical-device bandwidth, or show a maximum number of writers. The
benchmark retains tiny-query regressions, higher RSS, noisy cold/load results,
and the remaining PostgreSQL filtered-grouping gap. Native client folds and SQL
pushdown are different execution paths. Wide-group evidence has one observation
per engine and is diagnostic only.
