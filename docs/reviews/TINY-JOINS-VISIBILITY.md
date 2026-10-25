# Tiny joins, native overheads and schema visibility review

Date: 2026-10-25. Baseline: `41c7472`.

Two independent subagents reviewed the implementation. One reviewed SQL semantics,
resource/error paths, encoding, differential indexes, OCC, lock scope and native
projection. The other reviewed visibility, namespaces, credential scopes,
anonymous transport, metadata disclosure, persistence and live revocation.

## Findings fixed before measurement

- Tiny joins checked projected payload size before residual predicates. A row
  rejected by an exact predicate could raise an oversized-cell error while generic
  DataFusion returned an empty result. Predicate acceptance now precedes payload
  checks; spent probe work remains charged and fallback keeps the shared budget.
- `Checked` requests could compare a full fingerprint while catalog discovery
  redacted a private reference domain. Contract checks now require complete
  reference-domain discovery before comparing fingerprints, closing that oracle.
- A restricted principal equal to the internal host-owner marker could gain
  ambient private reads. The marker is excluded from application ownership;
  administrator access and explicitly granted reads retain their meanings.
- Explicit host visibility updates on legacy families now resolve an existing
  namespace owner instead of creating host-owned policy metadata. Merely opening
  or exporting an old database still does not create policies.

## Added evidence

SQL review added randomized bag/alias/reversed-join/USING comparisons against
SQLite and generic DataFusion, plus the rejected-large-payload regression.
Visibility review added anonymous and authenticated fingerprint-oracle cases,
reference-only discovery, reserved-principal access, concurrent policy conflicts,
retained disk-lease invalidation and legacy namespace ownership.

The visibility reviewer reported 24 focused tests plus two legacy recovery tests
passing. The SQL reviewer reported 42 SQL tests passing, one ignored, and no additional
correctness findings in exact admission sizing, differential index/version
updates, snapshot locking or native projection. Final coordinated test and
benchmark evidence is recorded with the benchmark reports.

The first paired measurement revealed extra visibility lookup work on already
granted reads. The visibility reviewer subsequently reordered successful explicit
grants before ambient lookup and added a union/filter regression. Filter failures
still fall through to ambient visibility; anonymous grants and scoped ceilings
retain their restrictions. Final measurements use this revised build.

## Remaining limits

Tiny-join limits bound materialized tuples, not all work in native authorization
traversal. Deadlines are cooperative between native calls. Large or unsupported
joins retain DataFusion. Visibility changes conservatively invalidate all
unscoped retained readers; narrower dependency generations remain future work.
A code review and test suite are not a formal proof or production security audit.
