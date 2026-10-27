# Typed queries and nested reads

The Node/TypeScript client can now build typed projections and inner joins, or load
an explicit relationship tree in one request. Rust exposes the same nested executor
through `Snapshot::read_nested(&ReadPlan)`; a Rust fluent, compile-time query builder
is not implemented. No storage layout or durable event format changed.

Run `npm run demo:processes` for the complete two-application example. It validates
schema discovery, named writes, typed joins, nested reads, authorization, and the
same results after killing/reopening the host. See `examples/dx/app.ts` and the
negative compilation tests in `examples/dx/query-types.test.ts`.

## Projection and joins

```typescript
import {table, select, eq, gt, asc} from "./sdk/typescript/query.js";
import {core_Person, fitness_Performed, fitness_Workout} from "./generated/schema.js";

const p = table(core_Person, "p");
const f = table(fitness_Performed, "f");
const w = table(fitness_Workout, "w");
const query = select({name: p.c.name, minutes: w.c.durationMinutes})
  .from(p)
  .innerJoin(f, eq(f.c.athlete, p.c.id))
  .innerJoin(w, eq(f.c.workout, w.c.id))
  .where(gt(w.c.durationMinutes, 30n))
  .orderBy(asc(p.c.id))
  .limit(20);
const rows = await query.execute(client); // {name: string; minutes: bigint}[]
const plan = await query.explain(client);
```

Fields, reference domains, and projection results are typed. Aliases and binding
scope are checked at runtime; same-family aliases still have distinct table objects.
Comparison literals and limits become SQL parameters. Names are quoted. Exact i64,
decimal minor units and timestamps stay bigint; bytes stay Uint8Array. Results are
validated against descriptor scalar domains. `compile()` exposes SQL, parameters,
and selected relation contracts; `explain()` submits a checked EXPLAIN query.

Predicates support `eq/ne/gt/ge/lt/le`, `and/or/not`; sorting supports `asc/desc`.
Each `where`, `orderBy`, or `include` call replaces that component; use `and` for
multiple conditions. Queries are constructed immutably. Default limit is 100,
maximum 1,000, with 16 tables, 128 projection fields and eight ordering fields.
Supply an explicit order when using limits and requiring repeatable row selection.

## Nested relationships

```typescript
import {read, many, one} from "./sdk/typescript/query.js";

const result = await read(p).orderBy(asc(p.c.id)).limit(20).include({
  performed: many(f, f.c.athlete).limit(2).include({
    workout: one(w, f.c.workout).where(gt(w.c.durationMinutes, 30n))
  })
}).execute(client);

result.rows[0].related.performed[0].related.workout?.data.durationMinutes;
result.revision; // exact revision string
result.stats;    // sql_statements, fetched_rows, returned_nodes
```

`many(child, childReference)` traverses a reverse reference. Its limit is **per
parent**, after row authorization and filtering. `one(target, parentReference)`
traverses a forward reference; a target that fails the branch filter becomes null.
An empty reverse branch is an empty array. Branch filters do not filter the parent.
There is no implicit global Person shape or inferred relationship merging.

Each returned node has `{id, data, related}`. Relationship occurrences retain their
own IDs and multiplicity, even when they reference the same target. A target shared
by several parents is fetched once for that branch, then included in each output
location. Identity ascending breaks ordering ties; it is also the default order.

One Read request captures one data **and policy** snapshot. The executor validates
all selected contracts, reference roles and read permissions before any data query,
including branches under an empty root. It performs one SQL statement per nonempty
plan node: Person → Performed → Workout takes three statements for one or hundreds
of parents. Distinct parent IDs are batched with IN; reverse reads use partitioned
ROW_NUMBER to apply per-parent limits. Assembly uses maps, not per-parent scans.
The SQL engine can perform multiple internal index probes for an IN predicate; the statement
counter is not an index-probe count or constant-work claim.

The authorized DataFusion providers filter inputs and enforces reference
visibility **before** joins, windows and limits. A link to an invisible participant
is itself hidden. Selecting an unauthorized family returns denied, not a silently
empty branch. Typed SQL contract validation and execution also share one snapshot.
Only selected families are registered for execution, avoiding failures caused by
unrelated readable namespaces. The current service uses DataFusion; the attached
namespace ceiling of the legacy embedded SQLite adapter does not apply here.

## Budgets and current limits

Nested reads allow root plus 16 branches, depth four edges, 256 total predicate
nodes (depth 16), up to eight ordering fields per node, and limits 1–1,000 per node.
Forward limits are exactly one. Fetched rows and expanded output each have a
5,000-node and 2 MiB logical scalar-size budget. Each statement also has a
configured source/output work budget; this is not a SQLite VM instruction counter. The transport additionally caps serialized responses at
4 MiB. Budget failures return an error, never a partial tree. These limits do not
bound every allocation, recursive policy check or unit of CPU work.

Nested reads return the requested first N rows; they do not advertise hasMore or
continuation cursors. Use the existing revision-guarded `page` API for keyset scans.
Nested field projection, relation-existence filters on parents, flattened through
relations, outer-join typing, aggregation/grouping builders and upsert remain
pending. Persistent view management is not exposed through this nested/remote API;
resident durable views have a separate native API. Raw parameterized SQL supports
its richer SELECT surface. Remote `query` remains read-only; mutations use atomic
typed batches or the bounded [`execute` SQL API](SQL-WRITES.md).

Retained native snapshots preserve data but fail closed when their authority is
revoked. Nested and SQL reads check before releasing results and enforce live
authorization and execution-budget checks during work. See [SECURITY.md](SECURITY.md) for scoped delegation,
read linearization and the existing local-host trust boundary.

See measurements (historical results removed), [discovery](CATALOG.md), and
[execution decision](adr/007-nested-queries.md).
