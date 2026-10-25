# DataFusion SQL reads

The default `Snapshot::sql()`, `sql_selected()`, prepared SQL reads, remote SQL
reads, typed SQL builder and nested SQL loader now use DataFusion. Native
`scan`, `lookup`, `Table` projection/join, transaction validation, packed storage
and redb durability are unchanged. Persistent collections now use imbl 7.0.2.

The Rust dependency is pinned to DataFusion 55.0.0 with its execution crates
locked to 55.2.0. The Python 55.0.0 benchmark wheel reports the same 55.2.0 native
engine. Rust 1.94 is now required. The comparison and its boundaries are in
[the DataFusion experiment](benchmarks/DATAFUSION-EXPERIMENT.md).

## Execution and authority

Each SQL handle registers discoverable schemas and lazy native table providers
against one authorized Liquid snapshot. Creating the handle does not copy rows.
Providers use native identity, typed role-equality and numeric-range indexes when
predicates have exact matching semantics; unsupported comparisons remain DataFusion
filters. Both range bounds are intersected. Only requested columns are converted
into Arrow batches, in pages of at most 1,000 authorized candidates. Row policies
and transitive reference visibility still run through the native read APIs before
any row enters Arrow. LIMIT is pushed only when residual filters cannot discard
needed rows. `sql_selected()` limits the catalog to explicitly selected families.
No raw DataFusion context, detached Arrow batch or table provider escapes.

EXPLAIN exposes `LiquidScanExec` with identity, role-equality, numeric-range or
family-scan access. EXPLAIN ANALYZE's `source_rows` counts authorized candidates
served by the provider, including cached pages, not all underlying index entries
or hidden rows traversed during authorization. `cache_hits` counts reused pages.
Neither metric is a CPU-work counter.

The handle retains its live authorization guard, including capability and
credential dependencies. Creation, preparation, execution and final result release
check that authority. Revocation, expiry, rotation and source-host loss invalidate
retained handles. Data remains pinned to the original revision; authority remains
live. Already delivered values cannot be recalled.

Each prepared query caches one exact typed parameter binding. Deterministic
queries reuse an optimized logical plan. A conservative whitelist of simple
physical operators can additionally reuse an unexecuted template, rebuilding fresh
operator instances and streams on every call, including the first. Joins, sorts,
dynamic filters and unknown operators do not reuse physical templates. Stable or
volatile functions and context variables bypass the cache. EXISTS, IN and scalar
subqueries can reuse logical planning after recursive inspection of their raw
plans and view sources; this prevents caching folded execution-time functions.
Correlated references retain their meaning for the pinned snapshot and binding.
Changed bindings replace the entry; execution errors discard
it. The planning cache retains no result rows, cursors, task contexts or source budgets. This avoids
the repeated top-k errors observed when mutable physical plans were reused.
Parameters remain typed values;
`?`, `?1`, and `$1` are normalized using SQL tokens, never string interpolation.
Exact signed integers, decimal minor units and timestamps use Arrow Int64;
references remain canonical UUID strings and byte values remain binary.

The synchronous API schedules work on a separate shared Tokio runtime, so calling
it inside an application's Tokio runtime does not nest `block_on`. Every query
uses a separate execution future and either returns a complete result or an error.

## Bounds and SQL surface

Each execution shares a source budget across all its scans: one million authorized
candidate rows and approximately 128 MiB of projected column payload, with a 1 MiB
limit per projected source cell. A large unselected field no longer prevents SQL
handle creation or COUNT. Parameter payloads are also bounded. Native reads retain
their existing value limits.
Each SQL context has a 256 MiB DataFusion
execution memory pool, with disk spilling disabled. These are accounting budgets,
not a hard process RSS limit; Arrow metadata, source state and some temporary
allocations are additional. Reusing handles and prepared queries amortizes schema
registration and planning. Predicate-free full scans and numeric ranges without
a pushed LIMIT can reuse immutable projected Arrow pages in a cache
shared only by that SQL handle's pinned snapshot and authority. The FIFO cache is
capped at 64 MiB of estimated retained memory and 4,096 entries; keys include the
relation identity, ordered projection and page cursor. Range keys also contain
typed bounds, direction and every pushed predicate. Cache hits recheck live
authority and recharge the original candidate-row count and projected bytes,
including candidates discarded by additional predicates. Key storage is included
in the cache accounting. Identity/equality lookups and reads with a pushed LIMIT
continue to use native reads. First execution still pays conversion cost. Evicted pages
held by active queries can outlive the cache entry, so the cache cap is not a hard
process-memory cap.

`QueryLimits` bounds returned rows, bytes, and execution time (`timeout_ms`). Its
legacy `vm_steps` field counts returned cells in DataFusion; it is not an opcode or
CPU budget. In the explicit SQLite compatibility adapter it still counts SQLite
VM instructions. Deadlines are cooperative cancellation, not process isolation.

Only one read query or EXPLAIN is accepted. DDL, DML, session configuration,
external table registration, file/network table functions and protected system
catalogs are unavailable through this API. Unbounded expansion functions and
collection/string aggregation are outside this initial SQL surface. Invoker views use the same catalog and
guard. Use `EXPLAIN SELECT ...` for the DataFusion plan. Profiling reports include
these plans, which can contain literal and bound values.

## Compatibility

DataFusion is a different SQL dialect. It uses stronger typing than SQLite;
SQLite affinity/coercion tricks, `COLLATE NOCASE`, `PRAGMA`, and SQLite-specific
functions are not a compatibility promise. The output API remains `SqlTable`
with primitive SQL values, including NULL. Unsupported result types and integers
outside the signed 64-bit output range return errors rather than silently losing
precision. Signed integer arithmetic is rewritten before optimization into checked
operations, including constants, predicates, mixed widths and bound parameters.
Integer SUM uses a checked widened accumulator, including sliding windows; an
out-of-range final result returns an error. SUM DISTINCT is currently unsupported.
Unsigned/decimal arithmetic requires an explicit BIGINT or DOUBLE cast. Floating
arithmetic is approximate; non-finite returned values are rejected. Use explicit
casts and aliases for portable queries.

`Snapshot::sqlite_compat()` and `sqlite_compat_selected()` retain the previous
adapter explicitly for applications that need its dialect. There is no silent
fallback after a DataFusion failure. The native query API is unchanged.

`SqlTransaction::execute()` still uses SQLite's mutation adapter to call Liquid's
typed transactional insert/update/delete operations. SQLite is not canonical
storage. Transactional SELECTs use DataFusion over the transaction's staged state;
the adapter conservatively records reads of all visible families before executing
the query against that pinned state. This preserves conflict detection but can produce
more conflicts than a future provider that records precise read dependencies.


[Initial index-backed benchmark](benchmarks/DATAFUSION-INDEXED.md) compares that milestone
with the previous eager DataFusion adapter, the explicit SQLite compatibility
adapter and ordinary indexed SQLite. Native read/write APIs remain unchanged.

The [range/EXISTS/window follow-up](benchmarks/DATAFUSION-RANGE-EXISTS.md) compares
both previous DataFusion adapters with the current implementation and SQLite.
Ordered-range/top-k pushdown and join-driven index probes remain unimplemented;
cached source pages do not remove DataFusion's sorting and hash-join work.
