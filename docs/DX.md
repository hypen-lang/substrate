# SQL and developer experience milestone

The complete initial pipeline is implemented: TypeScript declarations → versioned
manifest → existing Rust compiler → transactional installation → installed catalog
export → generated Rust/TypeScript records and runtime validators → typed writes,
paged reads and parameterized SQL. The storage model remains unchanged.

## Run the complete example

Requirements: Rust 1.89+, Node 20+ (verified with 22.22.1), npm. TypeScript 5.9.3 is
pinned in package-lock.json. The process example requires a Unix system.

```sh
npm ci
npm run test:ts
cargo run --example dx
npm run demo:processes
```

`demo:processes` creates its own temporary directory with private credentials. It
compiles the TypeScript declaration, reuses the committed manifest IDs, installs
through a trusted Rust host, exports the catalog, regenerates and compiles the
client, then launches independent Fitness and CRM Node processes. Each attaches
facts to the same Person UUID, and each is denied access to the other app's data.
A reporting principal joins both through SQL. The amount `9007199254740993` survives
without JavaScript number rounding. The script kills the server, reopens the same
file and verifies the join again, then cleans up only its own fixture.

`cargo run --example dx` demonstrates the embedded Rust equivalent: generated
records, typed references, retryable transactions, a SQL update, a SQL join and
native typed readback. Both examples are applications of the same engine.

## Declare once

See `examples/dx/schema.ts`:

```ts
import {defineModule, entity, relation, ref, s, optionalProperty}
  from "../../sdk/typescript/schema.js";

const fitness = defineModule({
  namespace: "fitness", version: 1, imports: {core: 1},
  entities: {Workout: entity({durationMinutes: s.int64()})},
  relations: {
    Performed: relation({
      athlete: ref("core.Person"), workout: ref("fitness.Workout")
    }).index("athlete"),
    TrainingGoal: optionalProperty("core.Person", s.string())
  }
});
```

`compileModules([core, fitness, crm])` returns the declaration envelope. Write it as
JSON, then run the explicit deployment workflow:

```sh
cargo run -- schema-lock declarations.json manifest.json
cargo run -- manifest-plan existing.redb manifest.json
cargo run -- manifest-install database.redb manifest.json
cargo run -- manifest-export database.redb installed.json
cargo run -- generate installed.json generated fitness@1
```

`schema-lock` validates through the authoritative Rust compiler, allocates IDs only
for new relations, and preserves all prior module versions. Commit the manifest.
Repeated locking with that manifest produces identical IDs and canonical JSON.
Generating from a local manifest needs no running database. `manifest-plan` requires
an existing database and drops the staging transaction; `manifest-install` can
create a new database. Ordinary client construction never installs schemas.

The installed catalog is authoritative. `generate … fitness@1` includes Fitness,
its exact imports and necessary prior module revisions; it excludes unrelated CRM
schemas. A supplied bundle can generate both SDKs without selecting roots. Identical
output files are left untouched. Separate files per module and watch mode are deferred.

There is no global type containing every installed application's Person extensions.
`TrainingGoal` is an independent `fitness.TrainingGoal(person, value)` family with a
unique `person` role. No occurrence means absent optional property. This API does
not add a nullable column, change Person's layout, or synthesize nested reads.
Deleting its occurrence removes the extension value. Packed Person bytes remain
identical after extension; periodic checkpoints may still copy unchanged bytes.

## Canonical contract and compatibility

The v1 IR is the typed Rust `SchemaManifest / Module / Relation / Role / DataType`
model. JSON object order has no semantics. Canonicalization sorts modules by their
dependencies/name/version and sorts relation names/imports. **Role arrays retain
order**, because `RoleId { relation UUID, slot }` is a persistent layout identity.
The TS authoring compiler emits alphabetically ordered role arrays; Rust/JSON
builders can specify arrays explicitly and must preserve their locked order.

Versions are positive consecutive integers and imports pin exact versions. A new
version is an additive delta of new families. Changing an old role, type, ID,
constraint or import is rejected, including changes to previously authored versions
when re-locking. Renames/aliases, constraint tightening and automatic backfills have
no implicit implementation. Use a new family/version and explicit application logic.

The manifest fingerprint is SHA-256 over `LiquidManifest/1\0` followed by compact
canonical serde JSON. Relation fingerprints use `LiquidRelation/1\0` plus the typed
relation declaration (ID, name, ordered roles/codecs and unique keys). These are
versioned contract hashes, not signatures or credentials. A relation hash covers
that relation's declaration, not recursively the definitions of referenced families.
The selected manifest hash covers the full imported contract.

Generated Rust records and TS descriptors carry stable IDs and relation hashes.
Typed reads/writes compare them with compiled installed metadata and reject drift,
even if a different database reuses an ID with a changed layout. TS requests include
a contract check; the kernel still validates all data and authorization. Handwritten
native `Record` implementations may omit their hash; dynamic/raw protocol operations
validate against the installed schema and do not claim a statically checked contract.

Rust authoring uses `Module::new`, `Relation::new` and `Role::new`. Assign checked-in
manifest IDs when reconstructing a contract; allocating a new UUID each build is not
stable authoring. Tests construct a Rust-authored core module, install/export it,
compare fingerprints and generated source with TS, and use generated typed records.
No derive macro, DSL interpreter, network build step or new storage compiler exists.

## Types and validation

Generated TS exposes `core_PersonInsert`, `core_PersonUpdate`, `core_PersonRead`,
`core_Person` (descriptor), and branded `EntityId<"core.Person">`. Insert fields are
required; patch fields are optional but explicit undefined is invalid. IDs are
allocated by the engine. Schema authoring provides `InferEntity`, `InferInsert`,
`InferUpdate`, `insertSchema` and `updateSchema` with shallow mapped types.

```ts
const checked = core_Person.insertSchema.safeParse(externalInput);
if (!checked.success) console.log(checked.issues);
const person = await client.insert(core_Person, {name: "Ada"});
await client.update(core_Person, person, {name: "Ada Lovelace"});
const page = await client.page(core_Person, {limit: 20});
```

Validators reject missing fields, unknown fields, incorrect types, integer overflow,
invalid references and malformed wire values. Errors have codes, field paths,
expected domains and messages. `.parse` throws `ValidationError`;
`.safeParse` returns a discriminated success/failure result. The descriptor's
`readSchema` validates `{id, data}` and prefixes nested issue paths with `data`.
All generated validators implement the structural **Standard Schema V1** interface
(`~standard.version = 1`, vendor, validate and inferred output metadata). No Effect,
Zod or Valibot dependency is required for generated validators. An optional
[Effect 3 service adapter](EFFECT.md) is available.

Client-side reference validation checks UUID shape; existence, membership, unique
keys, row policy and reference capabilities remain authoritative server checks.
Batch encoders additionally allow explicit `link<"fitness.Workout">("workout")`
references to a preceding insertion alias. Stored-read validators reject aliases.
No arbitrary JS closures, regex constraints, range refinements or permissions are
serialized as engine constraints. Unsupported declaration options fail instead of
being silently treated as database-enforced behavior.

| Logical scalar | Rust | TS decoded | Application wire | SQL |
|---|---|---|---|---|
| Int64 | i64 | bigint | tagged decimal string | INTEGER |
| Decimal(scale) | i64 minor units | bigint minor units | tagged decimal string | INTEGER minor units |
| Timestamp | i64 UTC microseconds | bigint UTC microseconds | tagged decimal string | INTEGER |
| Bool | bool | boolean | tagged boolean | 0 / 1 |
| Text | UTF-8 String | string without unpaired surrogates | tagged string | TEXT |
| Ref | Id / TypedId | branded UUID string | tagged UUID | lowercase UUID TEXT |
| Bytes | Vec<u8> | Uint8Array | tagged byte array | BLOB |

Timestamps exclude leap seconds; Date/ISO coercion is not automatic. Decimal scale
is fixed at the role (0..18), not carried in each value. SQL real arithmetic can
lose exactness; expose minor units and use deliberate arithmetic. Native rows have
no null roles; SQL outer joins can yield NULL. Runtime validators are portable
standard TypeScript; the current transport is Node/Unix only, not browser/edge.

## Queries and transactions

The Node client has `insert`, partial `update`, `delete`, `page`, atomic `batch`,
`query`, `queryChecked` and `capabilities`. `page` uses native identity-order keyset
pagination, with optional typed-wire equality filtering. A page materializes at most
limit+1 visible rows, up to a limit of 1,000. To reject cross-request changes, pass
its revision back as `expectedRevision`; retry the whole read on conflict. This is
a revision guard, not a server-pinned snapshot or consistent multi-request stream.

`query(sql, params)` executes server-side parameterized SQL; it returns columns and
positional scalar rows, without pretending to infer arbitrary SQL. For checked
results use `queryChecked(sql, params, resultSchema)`, supplying a Standard Schema
validator of the returned object rows. Duplicate result names require aliases.
Joins happen in the server's SQLite query engine over Liquid virtual tables; the
client never downloads all families to join them. SQL EXPLAIN QUERY PLAN exposes
identity/equality access paths. A composable TypeScript projection/inner-join builder and explicit batched nested
reads now work: see [QUERY-DX.md](QUERY-DX.md). Nested reads use one request and
one snapshot, with one statement per selected nonempty branch. Upsert, typed
outer joins and aggregation builders remain deferred.

Rust `Session::transact(RetryPolicy, closure)` retries only `Error::Conflict` using
a fresh transaction, with bounded attempts/delay. The closure may execute repeatedly:
external side effects belong after successful commit. Schema/permission/constraint
errors and ambiguous storage errors are never retried. Native named writes reject
unknown/duplicate/missing fields and validate old and new values against row policy.

Remote `batch` runs 1..100 named mutations atomically with server-side conflict
retries; aliases refer only to earlier inserts in that batch. No open transaction
handle crosses the transport. The SDK **does not retry disconnected mutations**:
acknowledgement loss leaves an uncertain outcome; reconcile before retrying. Durable
idempotency keys for data batches and interactive remote transactions are not implemented.
Schema proposals/apply have their own durable receipt-based reconciliation.

Embedded Rust `Session::sql_transaction()` supports INSERT/UPDATE/DELETE, reads its
staged writes and commits through Liquid's OCC checks. SQL inserts require an explicit
UUID `id`. Any SQL failure aborts the entire transaction, including earlier statements;
drop rolls back. Identity changes, RETURNING, DDL, ATTACH, PRAGMA and SQL-managed
BEGIN/COMMIT are rejected. `SqlSnapshot` remains read-only. Remote SQL is read-only;
remote mutations use the typed batch protocol.

Snapshot SQL cursors fetch 64-row pages. Transactional SQL scans retain statement
candidate rows for stable mutation semantics. The trusted embedded query API can
collect unrestricted results; remote SQL uses output row/byte and VM-step budgets.
These are not comprehensive CPU/memory isolation: policy traversals, sorting,
schema registration and native operations need broader resource accounting.

## Local host and capability boundary

`serve database.redb private-dir/socket credentials.json` is a local single-owner
host. Credential files must be private, the socket parent mode 0700 and socket mode
0600. Tokens map to fixed principals; a caller cannot submit an administrator flag
or choose a different principal. Even a principal named `admin` remains restricted.
The local CLI/Database handle is the trusted administrative boundary. Keep all
credentials and raw database files out of application processes' access; same-OS-user
malicious processes with filesystem access are not isolated by Unix permissions.

Authorized paged [catalog discovery](CATALOG.md) exposes stable IDs, role domains,
constraints and operation flags for explicitly granted families. Hidden reference
targets are opaque; imports and row-filter values are omitted. Legacy capability
listing omits families with hidden reference targets. Neither API grants authority. Existing
reference-closed visibility and write checks govern native and SQL operations.
Scoped delegation and attenuation now work through `withCapability`, with live
checks on retained handles and before result release/commit. See [SECURITY.md](SECURITY.md).
[Remote TLS authentication, credential rotation and expiry](REMOTE-SECURITY.md)
now work. Field policies, arbitrary ReBAC and public-service hardening remain deferred.

The server limits workers (32), request/response frames (1 MiB / 4 MiB), read/write
frame time (5 seconds), page size, batch size and remote SQL results/work. The Node
client uses a 10-second overall request timeout. A stale socket causes startup to
fail; operators must verify its owner is dead before removing it. The example only
removes its own socket after awaiting its killed child. This is not an HA service.

Multiple processes submit requests concurrently. **redb still has one active write
transaction per file**, and Liquid publication serializes. This demonstrates logical
independence and a shared host, not parallel durable write execution. Engine/storage
semantics stay separate from the protocol and SDK; redb types remain inside the
durable module. Evaluating FoundationDB later requires preserving transaction,
snapshot, conflict and durability semantics, not merely replacing a key/value API.

## Evidence and remaining work

See DX-MATRIX.md for the feature audit, IMPLEMENTATION.md for verification, and
benchmarks/DX.md for compiler/SQL measurements. No derive macros, rich ORM surface,
semver resolution, schema registry, distributed service, subscriptions, persistent
views, larger-than-memory execution or Bend reference model are claimed complete.

Typed/nested query measurements and the expanded TypeScript consumer fixture are
in [NESTED.md](benchmarks/NESTED.md). The [cross-engine suite](benchmarks/COMPARISON.md)
now runs eight verified workloads on SQLite, PostgreSQL and Substrate.

## Runtime schema workflow

Run `npm run demo:schema` for a separate Node application that proposes and installs
new families while readers continue, then discovers and uses them without generated
source. The [schema-control guide](SCHEMA-CONTROL.md) documents host provisioning,
manual/automatic approval, exact plans, retries/reconciliation, namespace quotas,
aliases, lifecycle and Rust/TypeScript runtime descriptors. These are scoped local
operations; no remote administrative provisioning or approval API is exposed.
Retirement preserves existing reads and writes and is not access revocation.

Run `npm run demo:security` for delegated runtime operations, nested reads, approved
schema installation, cascading revocation and durable recovery.
