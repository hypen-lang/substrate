# Substrate — Liquid SQL

Substrate is a transactional relational database for **independent applications and agents that share stable identities**. One app can add a fitness record to a person while another adds a CRM relationship, each using its own typed schema and permissions. Neither app has to rewrite the person or migrate the other app's records.

The Liquid SQL model represents data as typed relationships, including ordinary entity records, properties, links and relationships with multiple participants. Additive schema modules compose around existing identities. Rust and TypeScript APIs provide typed writes, queries and nested reads; SQL offers a familiar interface over the same data. Sharing an identity does not grant access to everything attached to it.

Use it to prototype personal data platforms, extensible business applications, plugin ecosystems and agent tools that need to discover or add narrowly authorized schemas while other applications continue running. It is currently a **research/development prototype with a single-node default**. The opt-in paged engine serves working sets larger than RAM with bounded caches. Production operations, distributed infrastructure, storage footprint and broad analytical scans still need work; see the implemented/planned table below.

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
and cross-engine benchmarks (historical results removed).
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

New relation families default to **authenticated** discovery and reads. Use
**private** for owner/grant-only data, or **public** for anonymous reads. Visibility
never grants writes or permission to reference rows, and credential scopes remain
hard limits. Existing databases retain their prior grants. See the
[visibility guide](docs/SCHEMA-VISIBILITY.md) and run
`cargo run --example schema_visibility` for `GamerProfile` and private
`GamingMessages`, including live visibility changes.

Native NULL and finite floats work in typed rows, indexes, SQL and generated clients.
References can declare authorized cascades and explicit transaction-level deferral.
Run `cargo run --example nullable_references` and `cargo run --example sql_writes`;
see [types and reference constraints](docs/NULLABLE-AND-REFERENCES.md).

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
history once before compacting. In resident `Database` mode, current state, allocated identity tombstones and
indexes must still fit in memory. The opt-in [paged engine](docs/PAGED-STORAGE.md)
stores current rows, indexes, identities and conflict versions on disk with bounded
caches; it uses a distinct file format and supports SQL and the existing local/TLS SDK protocol. [Disk-backed historical read leases](docs/DISK-SNAPSHOTS.md)
can release retained snapshot payloads while preserving live authorization; they
do not make the resident mutable write engine larger than RAM. Version 3 checkpoints buffer
up to 4,096 latest row overrides or 8 MiB, partially merging them into base rows
inside the same durable transaction. V1/V2 files remain readable and upgrade at
their next checkpoint; older binaries cannot read V3 files. This maintenance is
distinct from schema migration or backfill. Log pruning
permits space reuse but does not promise file shrinkage or secure erasure.

See [the checkpoint decision](docs/adr/005-checkpoints.md) and
measured recovery improvement (historical results removed), and
[incremental checkpoint format and measurements](docs/INCREMENTAL-CHECKPOINTS.md).

## Implemented and planned

| Area | Working now | Not implemented |
| --- | --- | --- |
| Kernel | UUID identities and schema IDs, explicit memberships, typed n-ary occurrences, references to occurrences | General schema morphisms, automatic identity merging |
| Values | Bool, i64, UTF-8 text, references, fixed-scale decimal minor units, UTC microsecond timestamps, bytes, explicit nullable types, finite binary64 floats | Arbitrary precision decimal, nonfinite floats |
| Constraints | Required/nullable roles, types, NULL-distinct uniqueness, compiled checks, typed references, authorized RESTRICT/CASCADE deletion, explicitly deferred foreign keys | General cardinality bounds, deferred uniqueness/CHECK constraints |
| Transactions | Atomic installs/writes/grants, retained snapshots, optimistic serializable dependency validation, bounded durable group commit | Parallel physical writes per file, distributed transactions |
| Storage | Resident persistent B-trees and checkpointed redo; opt-in paged current rows/indexes/identities/versions on disk with bounded caches and dense family blocks (LPG3), SQL and local/TLS access; historical read leases; explicit paged/resident-file import, paged group commit and replication | Paged named-view management and partition routing; automatic repacking of legacy paged files |
| Queries | Indexed equality and numeric/decimal/timestamp ranges and order, native selection/projection/equijoin/distinct, snapshot SQL with projection and LIMIT pushdown, join-driven index probes, ordered top-k, embedded SQL DML, bounded remote SQL INSERT/UPDATE/DELETE, prepared queries, bounded pages, typed TS joins and batched nested reads | Text and other non-numeric range domains, general cost-based planning (paged mode has only bounded range-selectivity estimates) |
| Views | Resident snapshot-local and durable read-only invoker views with source contracts | Remote view API, published disclosures, materialized views |
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

redb retains one active writer per file. In resident and paged modes, concurrent independent transactions can
share one durable redb commit while retaining separate revisions and conflict outcomes.
The default coordinator drains queued work without a deliberate collection delay;
publication and acknowledgements follow successful persistence. See
[group commit semantics and benchmarks](docs/group-commit.md). Readers access the
previous durable snapshot during disk I/O. A sequential write/await client still
needs larger atomic batches to amortize synchronization. The local
host supports multiple client processes and independent logical writes; this does
not demonstrate parallel durable write execution. Remote clients can use explicit
[`execute(sql, params)`](docs/SQL-WRITES.md) for bounded transactional INSERT/UPDATE/DELETE
or atomic typed batches. The query endpoint remains read-only. Resident working sets and indexes still need
to fit in RAM; paged storage serves them from disk.

[Fixed local partition routing](docs/PARTITION-ROUTING.md) can bind separately
provisioned database files. It rejects cross-partition references and transactions;
it provides no global snapshot, rebalancing, or distributed transaction guarantee.
[Trusted-host replication](docs/REPLICATION.md) provides typed seeds, ordered
packets and read-only followers with atomic durable cursors; follower permissions
remain eventual until revocations arrive. [Paged replication](docs/PAGED-REPLICATION.md)
adds bounded on-disk streams and atomic apply. The optional [`consensus` feature](docs/CONSENSUS.md)
adds OpenRaft quorum commits, elections, failover and fenced leader reads. It is a
trusted embedding API with static membership, a host-supplied authenticated peer
transport and a hard log cap; production peer serving and snapshot compaction remain.
Run `cargo run --features consensus --example consensus_failover` for quorum writes
and failover under an isolated leader.

## Disk-backed current data

Use `paged::PagedDatabase::open(path, options)` when current data should live on
disk with bounded caches. It now supports DataFusion SQL, the existing local/TLS
servers, generated Rust records and TypeScript SDKs, named writes, nested reads,
and online schema proposal/install. Credentials retain scoped permissions, expiry,
rotation, delegation and live revocation across these operations.

Current rows, indexes, identity history and conflict records stay on disk. Startup
compiles bounded schema/policy metadata instead of rebuilding every row. Existing
resident `Database` remains available; storage selection is explicit and existing
files are never converted automatically. LPG1/LPG2/LPG3 files can be converted
explicitly with `PagedDatabase::import_paged_file`.
`PagedDatabase::import_resident_file` streams LDB1/LDB2/LDB3 checkpoints and redo,
preserving typed rows and supported policies. Unsupported control metadata or
ownership mappings that would broaden access are rejected; see
[the import guide](docs/PAGED-REPLICATION.md).

Run `npm run demo:paged` for a complete TypeScript/TLS example, or
`cargo run --example paged_sql` for SQL joins and aggregates. Existing CLI commands
accept `--paged`, including `serve`, `serve-tls`, `query` and manifest installation.
See [paged storage](docs/PAGED-STORAGE.md) and the
integration benchmarks (historical results removed) for cache bounds,
authorization semantics, remaining limitations and measured performance.
Scalar aggregates, typed filters and direct-field grouping use bounded execution
with live authorization and reference checks. Prepared singleton reads and joins
reuse shared rows without copying unselected fields. See the
small-read and aggregate benchmarks (historical results removed)
and broad-scan follow-up (historical results removed) for the same-runner
SQLite/PostgreSQL results and remaining gaps.
The one-million-record overall comparison (historical results removed)
covers all eleven SQL cases, loading, CPU, memory, storage footprints and query plans
for both Substrate storage modes, SQLite and PostgreSQL on the same runner.
The newer native/SQL × resident/paged matrix (historical results removed)
adds all four Substrate paths alongside SQLite and PostgreSQL at one million
records, with cache/I/O diagnostics and a table-by-table storage breakdown (historical results removed).
Native aggregate figures include client folds over public APIs; they are not
isolated SQL parsing-overhead measurements.
The packed-scan follow-up (historical results removed) tests direct typed
field access and singleton setup, including unsuccessful batching variants and
another complete million-record SQL comparison. A separate
dense-layout experiment (historical results removed) measures
scan speed, space and update write amplification. New paged databases now use
[production dense family blocks](docs/PAGED-STORAGE.md#dense-family-blocks-lpg3),
shared by native reads, SQL and SDK services. Existing files retain their format;
authorization and logical transaction conflicts remain independent of block layout.
The [production dense comparison](docs/benchmarks/latest/README.md) covers
the same million-record workload against the previous adapter, SQLite and PostgreSQL.

## Performance and next priorities

The scale and concurrency report (historical results removed) measures 100 through
10 million total rows, up to 256 clients, CPU, memory, disk size, logical/disk
throughput, latency, and recovery against SQLite and PostgreSQL. That report predates
paged storage: the **resident** engine completed one million rows, and its
ten-million-row load exceeded the experiment’s 5 GiB process footprint budget after
2.619 million confirmed rows. Paged mode removes that structural limit.

The latest [one-million-record paged comparison](docs/benchmarks/latest/README.md)
contains all eleven SQL cases, CPU, memory, loading, storage and separate I/O
measurements against SQLite and PostgreSQL on the same runner. Broad and grouped
scans take 33.19/33.31 ms; filtered grouping takes 28.03 ms, 16.9% below an
intermediate baseline and 9.6% below the isolated borrowed-iterator control.
The 1% range aggregate falls 29.6% to 278.93 µs. Substrate remains 21.5% slower
than PostgreSQL on filtered grouping; the range gap is 9.5%. Loading is 29.78 s
with overlapping before/after ranges. Small reads are
mixed, with point/equality regressions against the control and a selective-join
regression against the intermediate baseline. Peak client RSS is 44.66 MiB versus
44.59 MiB control and 37.42 MiB baseline; identical sampled live heap but larger
allocator reservation does not establish the cause. The baseline already includes
earlier fixes, so these are not total start-of-task improvements.

A separate three-round matrix includes resident native/SQL and paged native/SQL.
Resident storage is durable; peak client RSS is 2,210/1,672 MiB for native/SQL,
versus 33.1/44.7 MiB for paged native/SQL. Native client folds and pushed-down SQL
aggregates do different work. A single-observation 1,024-group diagnostic leaves
filtered grouping 52.7% slower than PostgreSQL. This workload does not measure SQL
writes, imports, concurrent group commit or consensus. See all results, ranges,
provenance and scope limits before treating these figures as a general ranking.

The subsequent capacity tuning report (historical results removed) records
atomic snapshot publication, compact index postings, a configurable 64 MiB backend
cache, cheaper checkpoint replacements, adaptive commit grouping, and indexed SQL
EXISTS execution. At one million records and 32 concurrent writers, a 60-second
run improved durable throughput from **3,240 to 4,778 TPS**, with median latency
falling from **8.21 to 5.11 ms** and p99 from **17.09 to 14.30 ms**. At 128 writers,
throughput rose from 8,389 to 10,859 TPS, with p99 slightly higher at 22.11 versus
21.25 ms. The PostgreSQL SERIALIZABLE control at 32 writers reached 3,850 TPS with a
13.89 ms p99. These are workload-specific local API/IPC comparisons of the resident
engine, whose mutable data and indexes reside in RAM; disk amplification remains a priority.

The ownership and query follow-up (historical results removed)
profiles shared reference-count contention and adds `Session::get(relation, id)`
for fresh point reads. Warm scalar reads borrow session-owned handles while retaining
live authorization checks. The subsequent follow-up also caches compiled capability
scope for scalar reads; reference-bearing reads keep the snapshot path. Use
`Snapshot` when several operations must share a revision.
The same milestone adds bounded indexed top-one-per-partition SQL and removes
temporary checkpoint row/operation copies. The report includes before/after
million-record runs, rejected experiments, SQLite results, and exact query parity.
The storage I/O probe (historical results removed) separately measures backend
write amplification; it remains a storage-layout problem, not an ownership fix.

The storage, memory and scoped-read follow-up (historical results removed)
adds bounded checkpoint overrides, compact conflict-version maps, allocation-free
compiled grant lookups, and
`Session::get_shared` for authorized immutable rows without payload copies.
The [working example](examples/shared_reads.rs) demonstrates filtered capability
reads, stable old results, and live credential revocation. Shared handles retain
their payload until released; they do not turn the engine into disk-backed mutable
storage. The report separates isolated allocation/read results from complete
engine measurements and SQLite/PostgreSQL controls.

Performance depends on query shape, authorization, result size and durability.
Open PRs have paired SQL and kernel performance checks using the
same runner for the base and proposed merge revisions. See the
[performance CI guide](tools/perf/README.md) for coverage, regression budgets,
raw artifacts, fairness limits, and the required **Performance gate** branch rule.
The tiny-join milestone comparison (historical results removed) measures the
preceding DataFusion adapter, SQLite compatibility and direct indexed SQLite at
2,000 and 20,000 people. These are local embedded reads, excluding transport and
writes, with three alternating process pairs and exact result checks.

At 20,000 people / 180,000 occurrences, warm prepared admin reads measured these
medians in microseconds:

| Query | Current | Previous adapter | Direct SQLite |
| --- | ---: | ---: | ---: |
| Identity lookup | 0.450 | 0.438 | 0.429 |
| Relationship lookup | 1.475 | 54.221 | 1.317 |
| Five-family selective join | 4.221 | 151.421 | 3.188 |
| Grouped join / top 25 | 14,155.658 | 15,095.567 | 42,021.725 |
| Nested window / top 1 | 9,079.604 | 9,702.987 | 26,475.746 |

The tiny joins improved roughly 36×. That milestone also introduced restricted
non-join read overhead, addressed in the read and contention follow-up (historical results removed).
Against `2dac848`, restricted point reads improved from 0.783 to 0.404 µs,
indexed equality from 91.112 to 57.521 µs, and range/top-100 from 25.646 to
18.054 µs at 20,000 people. Direct SQLite measured 0.362, 81.217 and 59.579 µs
respectively. These are warm embedded executions; a fresh restricted point-query
handle still costs about 130 µs including catalog construction and preparation.
SQLite does not perform Substrate's authorization or reference-closure checks.
Fresh handles, unseen bindings, all query shapes and regressions are included in
the report; the preceding execution report (historical results removed)
retains the earlier adapters' evidence.

Native measurements (historical results removed) show a 32-field partial
update improving from 34.167 to 9.000 µs and projection of 256 payload-heavy rows
from 570.917 to 16.875 µs. Projection allocation traffic fell from 16.87 MB to
29 KB. The follow-up removes repeated scan lookups and policy work; projection
improves further from 18.916 to 6.334 µs in its matched runs. Full owned scans
remain copy-bound and effectively flat (649 → 642 µs with substantial run
variation), while durable sequential updates remain near 4 ms. The original
four-reader workload's writer median improves from 23.250 to 4.375 µs. A new
sustained-reader control shows roughly unchanged writer latency and improved
reader p99 (58.667 → 33.208 µs). These are workload results, not universal speedups.

Separate native and durability measurements remain useful context:

- Shared-reference visibility at 1,000 roots: **248.497 ms → 1.128 ms**, using snapshot-local completed-result caching.
- Default durable group commit at 32 concurrent writers: **274 → 4,103 tx/s** (three-run local medians); sequential writers were essentially unchanged.
- Incremental checkpoints: **17,408 → 2,048 row writes** over 2,048 inserts. Total insertion time was essentially unchanged; individual durable commits still dominated.

These historical focused measurements have different boundaries from the SQL
comparison and are not universal speedups. See [visibility](docs/VISIBILITY-PERFORMANCE.md),
[group commit](docs/group-commit.md), checkpoint measurements (historical results removed),
and [operational hardening](docs/OPERATIONAL-HARDENING.md).

The candidate experiments (historical results removed) informed adoption of
`imbl` for persistent collections and DataFusion for SQL reads. `imbl` improved
the tested memory-only inserts and updates by roughly twofold, with mixed native
read results. DataFusion versus DuckDB (historical results removed)
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
use ordinary DataFusion execution. Fresh full scans still pay conversion costs.
Native writes now avoid duplicate packed encoding and unchanged index mutations;
scoped snapshot construction holds the publication lock briefly, and
`projected_table` copies only selected result fields. Transaction and authorization
semantics are preserved; see [native overheads](docs/NATIVE-OVERHEADS.md). See the
[SQL guide](docs/DATAFUSION-SQL.md) for precise eligibility and cancellation limits,
and the [beyond-SQL investigation](docs/PERFORMANCE-BEYOND-SQL.md) for the next
write, native-read and transport opportunities.

The implementation queue and remaining limitations are tracked in [missing.md](missing.md).
Package publication and Bend verification are excluded from this run. Additive schema
installation works today; arbitrary destructive changes are not migration-free.

Read the [architecture](docs/ARCHITECTURE.md), [decisions](docs/adr/README.md),
[verification report](docs/IMPLEMENTATION.md), and
[measured baseline](docs/benchmarks/README.md) for guarantees and limits.
