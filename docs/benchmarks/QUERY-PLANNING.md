# SQL planner and cursor fixes — 2026-10-25

The focused probes reproduce the predicate-order and unused-payload problems from the supplied review. They use an in-memory Substrate database, trusted administrator snapshots, release compilation and warm query handles; population and SQL catalog setup are excluded except in the explicitly named catalog probes. These are local synthetic measurements on the shared development machine, not service latency or throughput estimates. No durable writes, TLS, credentials or Effect overhead are measured.

Run `cargo run --offline --release --example query_performance`. Raw [before](2026-10-25-query-planning-before.csv) and [after](2026-10-25-query-planning-after.csv) CSV files retain each workload. Times are mean nanoseconds over three iterations, or ten for payload counts. `size` is row count for joins/ranges, bytes per row for `blob_count`, and family count for catalog setup. `returned_result_rows` counts result rows across repetitions, not a COUNT aggregate's value. Correctness is checked separately by the regression/differential suite. These small samples have timing variation; an intermediate after run produced 1.36 ms instead of 1.10 ms for the 2,000-row correlated query.

| Probe | Before | After |
| --- | ---: | ---: |
| Broad equality first, correlated EXISTS, 500 rows | 46.32 ms | 0.225 ms |
| Broad equality first, 1,000 rows | 134.64 ms | 0.715 ms |
| Broad equality first, 2,000 rows | 552.86 ms | 1.100 ms |
| UUID equality first, same query, 2,000 rows | 1.223 ms | 1.082 ms |
| Numeric range + descending top 100, 2,000 rows | 440 µs | 50 µs |
| COUNT, 1,000 empty-payload rows | 182 µs | 78 µs |
| COUNT, 1,000 rows with unused 64 KiB blobs | 2,613 µs | 64 µs |

The correlated query has one broad equality bucket containing every row. The former planner selected the first usable equality; this repeatedly walked that bucket for each outer row. The new planner prioritizes UUID equality regardless of predicate order. The two equivalent queries now use the same identity lookup plan.

Immutable SQL cursors retain the engine's existing `Arc<Row>` values rather than cloning whole rows per page or UUID lookup. Only requested columns are converted to SQLite results, and requested text/blob values are borrowed until SQLite copies the result. Public owned-row APIs retain their existing behavior. The internal pointer-identity test proves that cursors share stored rows; it does not rely on timing thresholds. Transactional SQL still uses owned, dependency-tracked mutation/read paths.

Numeric range cursors traverse bounded role-index buckets and resume by `(value, occurrence ID)`, preserving duplicate values across pages. Supported ascending/descending order is reported only for a single integer-like role (integer, boolean, decimal minor units or timestamp). SQLite retains all predicate rechecks and the LIMIT operator. Authorization happens before a row becomes a visible candidate. Unsupported bound coercions use a wider ordered index scan and let SQLite recheck, avoiding false negatives and preserving promised ordering. Mutable SQL keeps its tracked family-scan fallback so phantom detection is unchanged. Text/ref/blob ranges, multiple-column order consumption and general cost/selectivity planning remain unimplemented.

## Explicit catalog selection

Unrestricted raw SQL still registers all readable families. No regex or speculative SQL parsing was added. Native callers can now use `snapshot.sql_selected(&["core.Person"])`; TypeScript callers can use:

```ts
const rows = await client.querySelected(
  "SELECT name FROM core.Person WHERE name = ?1",
  [{Text: "Ada"}],
  [Person]
);
```

This uses the existing checked-contract protocol. Only selected families are registered; authorization and reference closure still use the full immutable engine snapshot. Omitted families are unavailable to the SQL handle. Unknown/unauthorized families are denied, not implicitly fetched. The existing typed/nested query paths already select contracts.

The after-build comparison includes fresh connection/catalog construction plus a tiny count query. Both modes use the same build and database:

| Readable families | Full catalog | Explicit one-family catalog |
| --- | ---: | ---: |
| 10 | 136 µs | 36 µs |
| 100 | 914 µs | 28 µs |
| 1,000 | 20.16 ms | 46 µs |

The selected path still filters the in-memory relation catalog; it avoids constructing hundreds of unused SQLite virtual tables. Automatic raw-SQL family discovery and reusable correctly rebound SQL connections remain future work.

## Verification

`tests/sql_planning.rs` covers predicate order, EXPLAIN index selection, ascending/descending numeric ranges, exclusive/inclusive and contradictory bounds, duplicates spanning pages, limits around the 64-row page boundary, i64 extrema, REAL/TEXT/BLOB/NULL coercion fallbacks, equality row policies, live revocation, explicit registration and mutable phantom dependencies. Its numeric range test compares 432 query/bound/ordering/limit combinations with equivalent SQLite tables. Existing SQL write, SQL security and nested-read suites also pass. `dx::cursor_tests` verifies shared payload identity; `tools/query-selected.test.mjs` verifies explicit contracts and exact bigint decoding. Focused all-warning Clippy checks pass.
