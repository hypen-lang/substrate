# 007 — Typed queries, batched relationship reads and scoped discovery

Status: accepted for prototype, 2026-10-24.

Use generated field-domain metadata to compile a small typed TypeScript query AST
to parameterized SQL. Reuse the existing SQLite virtual-table provider over Liquid
snapshots and its compiled authorization; do not introduce a second semantic store
or a client-side join engine. Query contract checks and execution share a snapshot,
and the connection registers only explicitly selected families.

Nested reads use a typed Rust ReadPlan and a single snapshot/SQL connection. Each
selected relationship branch issues at most one statement containing distinct parent
IDs. Reverse references use a window rank partitioned by their parent role, with
filters/authorization applied before per-parent limits. Forward references fetch
distinct target IDs. Grouped maps assemble the result tree and preserve occurrence
multiplicity. See SQLite's [window function semantics](https://www.sqlite.org/windowfunctions.html)
and [virtual-table interface](https://www.sqlite.org/vtab.html).

This avoids request/statement N+1 while retaining the existing SQL planner and native
indexes. It does not promise constant physical work or make SQLite's equality-only
Liquid pushdown a range index. Small embedded workloads can be slower because plan
validation, window sorting and output assembly cost more than repeated prepared
lookups. Bounded tree/output budgets make high-fanout failure explicit.

Use a separate catalog DTO for authorized discovery. Installable manifests contain
full imports and reference domains, which would disclose hidden metadata if returned
unchanged. Discovery gives granted family contracts, role IDs and operation flags;
inaccessible reference targets are opaque. This response never grants authority or
executes schema changes. Revisions pin multi-page enumeration, with existing snapshot
policy semantics. Lifecycle and untrusted proposal/installation control remain separate
milestones. Neither query DX nor discovery changes packed rows or durable events.
