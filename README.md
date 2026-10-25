# Substrate — Liquid SQL

Substrate is a transactional relational database for **independent applications and agents that share stable identities**. One app can add a fitness record to a person while another adds a CRM relationship, each using its own typed schema and permissions. Neither app has to rewrite the person or migrate the other app's records.

The Liquid SQL model represents data as typed relationships, including ordinary entity records, properties, links and relationships with multiple participants. Additive schema modules compose around existing identities. Rust and TypeScript APIs provide typed writes, queries and nested reads; SQL offers a familiar interface over the same data. Sharing an identity does not grant access to everything attached to it.

Use it to prototype personal data platforms, extensible business applications, plugin ecosystems and agent tools that need to discover or add narrowly authorized schemas while other applications continue running. It is currently a **single-node research/development prototype**. Production operations, large working sets, distributed infrastructure and query performance still need work; see the implemented/planned table below.

## SQL + DX example

```sh
npm ci
npm run test:ts
cargo run --example dx
npm run demo:processes
npm run demo:schema
npm run demo:security
npm run demo:remote
npm run demo:effect
```

The Unix/Node example starts a private database host and two independent applications
from a TypeScript-authored, locked schema. It regenerates clients from the installed
catalog, discovers authorized schemas, checks cross-app isolation, runs typed joins
and batched nested reads, kills/reopens the host, and
checks durable results. The Rust example uses generated records and retry helpers.
See the [DX guide](docs/DX.md), [implementation matrix](docs/DX-MATRIX.md), and
[typed query guide](docs/QUERY-DX.md), [schema discovery](docs/CATALOG.md),
[scoped schema control](docs/SCHEMA-CONTROL.md),
and [cross-engine benchmarks](docs/benchmarks/COMPARISON.md).
Durable read-only views are demonstrated by `cargo run --example durable_views`;
see [the invoker-view contract](docs/DURABLE-VIEWS.md).

`demo:schema` demonstrates host-provisioned namespaces, automatic and manual
approval, online installation, runtime descriptors without code generation, aliases
and lifecycle metadata. Concurrent readers continue, and checkpoint/reopen preserves
the result. The host alone provisions ownership and approves pending proposals.

`demo:security` adds principal-bound capability delegation, narrowed row filters,
authorized schema installation and cascading live revocation. See the
[security API and exact revocation semantics](docs/SECURITY.md).

`demo:remote` runs a TLS 1.3 Rust host and Node client with verified certificates,
scoped credentials, atomic credential rotation, live revocation and capability
expiry. OpenSSL is required to create disposable development certificates. See
[remote access and its security limits](docs/REMOTE-SECURITY.md).
Opt-in [persistent sessions](docs/PERSISTENT-TRANSPORT.md) reuse connections while
checking current credentials and permissions on every operation.

`demo:effect` uses the [Effect 3 integration](docs/EFFECT.md): a scoped service with
typed errors, generated records, nested reads and atomic batches. It accepts both
local and remote clients. SDKs are currently repository packages, not published
npm/crates.io releases.

## TypeScript API

```ts
import { RemoteClient } from "./sdk/typescript/client.mjs";
import { Person } from "./generated/schema.js";

const client = new RemoteClient({
  host: "db.internal", port: 7443, servername: "db.internal", ca: trustedCaPem
}, credential);
const id = await client.insert(Person, { name: "Ada" });
const people = await client.page(Person);
```

Generated TypeScript exports use `Person`, `PersonInsert`, `PersonUpdate` and
`PersonRead` when those names are unambiguous. Qualified exports such as
`core_Person` remain available for compatibility and collisions. Regenerating with
another `Person` definition can remove the short alias; canonical schema names and
UUIDs remain stable. Rust already groups types as `core::Person`.

## Run it

Requires Rust 1.94 or newer. The checked-in lockfile pins dependencies.
Run `cargo run --example datafusion_sql` for a SQL join/aggregate, native lookup
and live-revocation example.

```sh
cargo run -- demo
# Or persist the demonstration in a new file:
cargo run -- demo /tmp/substrate-demo.redb
cargo run -- inspect /tmp/substrate-demo.redb
cargo run -- checkpoint /tmp/substrate-demo.redb
cargo run -- query /tmp/substrate-demo.redb fitness 'SELECT name FROM core.Person'
cargo test
cargo clippy --all-targets -- -D warnings
cargo bench --bench sqlite
cargo bench --bench recovery
cargo bench --bench nested
cargo bench --bench schema_install
# Includes PostgreSQL when local binaries are available:
tools/bench-comparison-postgres.sh
```

Use a fresh filename for the durable demo. `demo` rejects a populated database.
Rust verification can run with `--offline` when locked dependencies are cached.
The CLI is a local administrator tool: choosing a principal is **not authentication**.
Only trusted host code should hold `Database`; give apps their restricted `Session`.

To benchmark a SQL file and record an interactive flamegraph, install Samply once
(`cargo install --locked samply`), then run
`npm run profile:query -- /path/to/database.redb fitness query.sql`.
Use `--profiler none` for benchmarks only, `--profiler flamegraph` for an SVG,
or `--mode request` to include SQL setup on every iteration. See the
[query profiling guide](docs/QUERY-PROFILING.md) for parameters, timing boundaries,
platform setup, and reading the results.

The demo installs `core.Person`, inserts Alice, installs Fitness and CRM independently,
attaches a workout and a deal to Alice's existing identity, and executes:

```sql
SELECT p.name, w.durationMinutes, d.amount
FROM core.Person p
JOIN fitness.Performed f ON f.athlete = p.id
JOIN fitness.Workout w ON w.id = f.workout
JOIN crm.CustomerDeal c ON c.customer = p.id
JOIN crm.Deal d ON d.id = c.deal;
```

Its first result is `Alice | 45 | 125050`. The `amount` role has decimal scale 2:
SQL exposes exact minor units, so `125050` represents `1250.50`. The demo then commits
independent app writes and confirms that its retained snapshot stays unchanged.
Distinct relationship occurrences may contain identical tuples; SQL preserves bags.

Fitness cannot query CRM's private relations. Admin can explicitly grant a reporting
principal access to both. Authorization filters inputs and preserves reference
visibility before either native joins or SQL aggregates execute.

## Rust API

```rust
use substrate_liquid::{demo, Database, Grant, Result};

fn main() -> Result<()> {
    let db = Database::memory(); // Database::open("example.redb") for durability
    let mut tx = db.admin().transaction();
    tx.install(demo::core())?;
    let alice = tx.insert("core.Person", vec!["Alice".into()])?;
    tx.grant_schema("fitness", "fitness")?;
    tx.grant("fitness", "core.Person", Grant {
        reference: true, ..Grant::read_only()
    })?;
    tx.commit()?;

    let fitness = db.session("fitness");
    let mut tx = fitness.transaction();
    tx.install(demo::fitness())?; // authorized app owns its new definitions
    let workout = tx.insert("fitness.Workout", vec![45.into()])?;
    tx.insert("fitness.Performed", vec![alice.into(), workout.into()])?;
    tx.commit()?;

    let snapshot = fitness.snapshot();
    let links = snapshot.lookup("fitness.Performed", "athlete", &alice.into())?;
    assert_eq!(links.len(), 1);
    let sql = snapshot.sql()?;
    let mut query = sql.prepare("SELECT durationMinutes FROM fitness.Workout")?;
    println!("{:?}", query.query(&[])?.rows);
    Ok(())
}
```

Inputs are validated by the typed engine. Generated `Record` implementations and
`TypedId<T>` add compile-time safety; generated contract IDs/hashes detect mismatches.
`insert_named` and `update_named` accept field names; generated `insert_record` and
`replace_record` avoid positional arguments. Every mutation error poisons its
transaction. Drop rolls back. `Session::transact` retries only optimistic conflicts
from a fresh snapshot; closures must not perform external side effects.

## Schema tooling

TypeScript, Rust builders and JSON declarations enter the same validated typed model.
Use locked manifests for reproducible packages:

```sh
cargo run -- schema-lock examples/dx/schema.json examples/dx/manifest.json
cargo run -- manifest-install /tmp/schema-demo.redb examples/dx/manifest.json
cargo run -- manifest-plan /tmp/schema-demo.redb examples/dx/manifest.json
cargo run -- manifest-export /tmp/schema-demo.redb installed.json
cargo run -- generate installed.json generated fitness@1
```

Locking preserves IDs and immutable history. Imports pin integer module versions;
versions add new families. An optional property is a separate relation with a unique
owner role; it does not become a column on the old entity. Installed role slots,
constraints and layouts cannot be replaced implicitly. JSON is declaration/wire
syntax: runtime uses compiled typed role offsets, constraints, indexes and policies.
[Range and length checks](docs/CONSTRAINTS.md) use manifest v2 and explicit LQ02
metadata when present; existing unconstrained manifests and packed row bytes retain
their previous format.

Generated TS types include strict runtime insert/patch/read validators supporting
Standard Schema V1. Exact integers, decimal minor units and timestamp microseconds
use bigint; wire values use tagged decimal strings. The Node client supports named
writes, atomic batches, bounded pages, parameterized SQL and checked SQL results.
Rust uses generated typed records/references and the same schema compiler. Full
API examples and supported limitations are in [DX.md](docs/DX.md).

## Checkpoints and recovery

Durable files now recover from a current-state checkpoint plus a bounded redo tail.
By default a commit reaching **128 tail commits or 8 MiB** creates an atomic checkpoint
including that commit, leaving an empty tail. `Database::checkpoint()` runs maintenance
explicitly; `Database::open_with_options` configures or disables automatic limits.
`storage_info()` and `recovery_info()` expose persisted and startup counters.
An opt-in [background checkpointer](docs/BACKGROUND-CHECKPOINTS.md) coalesces early
maintenance requests; synchronous hard tail limits remain the fallback. Maintenance
serializes with writes but does not hold the published-snapshot lock during disk I/O.

Checkpoint replacement, head publication, and log pruning commit atomically. Startup
validates the catalog, typed records, counts, head, and tail sequence; it rebuilds
indexes and compiled policies. The first open of an old unbounded file may replay its
history once before compacting. Current state, allocated identity tombstones and
indexes still fit in memory. [Disk-backed historical read leases](docs/DISK-SNAPSHOTS.md)
can release retained snapshot payloads while preserving live authorization; they
do not make the mutable write engine larger than RAM. Version 2 checkpoints update only changed row and
metadata entries; old version 1 checkpoints upgrade once. This maintenance is
distinct from schema migration or backfill. Log pruning
permits space reuse but does not promise file shrinkage or secure erasure.

See [the checkpoint decision](docs/adr/005-checkpoints.md) and
[measured recovery improvement](docs/benchmarks/CHECKPOINTS.md), and
[incremental checkpoint format and measurements](docs/INCREMENTAL-CHECKPOINTS.md).

## Implemented and planned

| Area | Working now | Not implemented |
| --- | --- | --- |
| Kernel | UUID identities and schema IDs, explicit memberships, typed n-ary occurrences, references to occurrences | General schema morphisms, automatic identity merging |
| Values | Bool, i64, UTF-8 text, references, fixed-scale decimal minor units, UTC microsecond timestamps, bytes | Native NULL, floats, arbitrary precision decimal |
| Constraints | Required roles, types, composite uniqueness, compiled integer-range/byte-length checks, functional properties, typed references, RESTRICT deletion | General cardinality bounds, deferred constraints, cascades |
| Transactions | Atomic installs/writes/grants, retained snapshots, optimistic serializable dependency validation, bounded durable group commit | Parallel physical writes per file, distributed transactions |
| Storage | In-memory persistent B-trees; packed redo, incremental atomic checkpoints, automatic tail bounds, bounded background scheduling, disk-backed historical read leases | On-disk serving of the mutable working set |
| Queries | Indexed equality and numeric ranges/order, native selection/projection/equijoin/distinct, snapshot SQL, embedded SQL DML, prepared queries, bounded pages, typed TS joins and batched nested reads | Wider range domains, projection/limit pushdown, cost-based native planning |
| Views | Snapshot-local and durable read-only invoker views with source contracts | Remote view API, published disclosures, materialized views |
| Security | Verified TLS 1.3, scope-bound durable credentials, atomic rotation, capability expiry, equality row policies, reference closure, live read/commit gates, cascading revocation, bounded per-peer admission and host audit events | Enrollment/OIDC, certificate automation, durable audit trail, field policies, general ReBAC, durable trusted time |
| Tooling | Locked manifests, TS/Rust generation with clean aliases, Effect 3 service, validators, authorized catalog, dynamic descriptors, scoped proposals/approval/online apply, namespace quotas and lifecycle metadata | Published SDK packages, operator approval UI, ownership transfer, destructive retirement, derive macros, graphical studio |
| Verification | Rust reference properties, concurrency, SQL differential, corruption, subprocess crash and I/O fault tests | Bend 2 oracle, formal proofs, exhaustive power-loss testing |

SQL reads use **DataFusion with lazy, authorized native-index providers**. Native indexed queries,
relational algebra and redb storage retain their existing semantics. SQL handles
pin data while checking live permissions before releasing results. Typed queries
and nested reads register only selected families. The default SQL catalog has no
SQLite attached-schema limit and does not copy rows at creation. Execution source
and output budgets apply instead. Prepared queries reuse safe planning work while
creating fresh execution state.
Explicit `sqlite_compat()` handles preserve the old dialect, and embedded SQL
mutations retain the SQLite adapter into Liquid transactions. There is no silent
query fallback. See [DataFusion SQL](docs/DATAFUSION-SQL.md) for dialect differences,
resource bounds, prepared execution, and migration details.

redb retains one active writer per file. Concurrent independent transactions can
share one durable redb commit while retaining separate revisions and conflict outcomes.
The default coordinator drains queued work without a deliberate collection delay;
publication and acknowledgements follow successful persistence. See
[group commit semantics and benchmarks](docs/group-commit.md). Readers access the
previous durable snapshot during disk I/O. A sequential write/await client still
needs larger atomic batches to amortize synchronization. The local
host supports multiple client processes and independent logical writes; this does
not demonstrate parallel durable write execution. Remote SQL is read-only; use
atomic typed batches for remote writes. Working sets and indexes still fit in RAM.

[Fixed local partition routing](docs/PARTITION-ROUTING.md) can bind separately
provisioned database files. It rejects cross-partition references and transactions;
it provides no global snapshot, rebalancing, or distributed transaction guarantee.
[Trusted-host replication](docs/REPLICATION.md) provides typed seeds, ordered
packets and read-only followers with atomic durable cursors. It has no consensus
or failover, and follower permissions remain eventual until revocations arrive.

## Performance and next priorities

Performance depends on query shape, authorization, result size and durability.
The [latest SQL comparison](docs/benchmarks/DATAFUSION-EXECUTION.md) measures both
prior DataFusion adapters, explicit SQLite compatibility and direct indexed SQLite
at 2,000 and 20,000 people. It separates preparation, warm queries, fresh handles
and previously unseen parameters; PostgreSQL was not rerun. These are local
embedded read measurements, excluding transport and writes.

At 20,000 people / 180,000 occurrences, warm prepared admin reads measured these
medians in microseconds across three processes:

| Query | Current | Previous adapter | Direct SQLite |
| --- | ---: | ---: | ---: |
| Identity lookup | 0.45 | 44.76 | 0.38 |
| Relationship lookup | 54.50 | 1,060.28 | 1.15 |
| Five-family selective join | 134.25 | 2,303.85 | 3.26 |
| Grouped join / top 25 | 12,304.91 | 19,167.78 | 40,094.38 |
| Nested window / top 1 | 7,158.01 | 9,173.90 | 24,354.14 |

All nine established admin query shapes improved over both previous DataFusion adapters
in this final run. The shared machine still produces timing variation; these are
workload results, not universal speedups. SQLite does not perform Substrate's
authority/reference checks. Tiny joins remain substantially faster in SQLite;
the report also includes an equivalent covering-index control for numeric reads.

Separate native and durability measurements remain useful context:

- Shared-reference visibility at 1,000 roots: **248.497 ms → 1.128 ms**, using snapshot-local completed-result caching.
- Default durable group commit at 32 concurrent writers: **274 → 4,103 tx/s** (three-run local medians); sequential writers were essentially unchanged.
- Incremental checkpoints: **17,408 → 2,048 row writes** over 2,048 inserts. Total insertion time was essentially unchanged; individual durable commits still dominated.

These historical focused measurements have different boundaries from the SQL
comparison and are not universal speedups. See [visibility](docs/VISIBILITY-PERFORMANCE.md),
[group commit](docs/group-commit.md), [checkpoint measurements](docs/benchmarks/INCREMENTAL-CHECKPOINTS.md),
and [operational hardening](docs/OPERATIONAL-HARDENING.md).

The [candidate experiments](docs/benchmarks/CANDIDATES.md) informed adoption of
`imbl` for persistent collections and DataFusion for SQL reads. `imbl` improved
the tested memory-only inserts and updates by roughly twofold, with mixed native
read results. [DataFusion versus DuckDB](docs/benchmarks/DATAFUSION-EXPERIMENT.md)
favored DataFusion for grouped joins, windows and correlated EXISTS; point queries
were slower. These are workload-specific results, with preparation costs reported
separately. JSON parsing and durability-matched storage tests did not justify
replacing serde_json or redb. Reports include raw results and reproduction commands.

SQL now uses join-driven native index probes for eligible selective inner joins,
ordered numeric range/top-k pushdown, and compiled identity/equality/range reads that
reuse typed bindings without per-value planning or Arrow conversion. Each path
retains native visibility checks, live revocation and per-query budgets. Both
`EXPLAIN` and `EXPLAIN ANALYZE` show the physical join plan.

General analytical queries still use DataFusion and its bounded source-page cache.
Complex queries retain a one-binding plan cache; audited joins, aggregates, sorts
and `ROW_NUMBER` windows reuse templates with fresh execution state. Larger joins
use ordinary DataFusion execution. Fresh full scans still pay conversion costs. Native indexed
APIs and storage retain their existing behavior. See the
[SQL guide](docs/DATAFUSION-SQL.md) for precise eligibility and cancellation limits,
and the [beyond-SQL investigation](docs/PERFORMANCE-BEYOND-SQL.md) for the next
write, native-read and transport opportunities.

The implementation queue and remaining limitations are tracked in [missing.md](missing.md).
Package publication and Bend verification are excluded from this run. Additive schema
installation works today; arbitrary destructive changes are not migration-free.

Read the [architecture](docs/ARCHITECTURE.md), [decisions](docs/adr/README.md),
[verification report](docs/IMPLEMENTATION.md), and
[measured baseline](docs/benchmarks/README.md) for guarantees and limits.
