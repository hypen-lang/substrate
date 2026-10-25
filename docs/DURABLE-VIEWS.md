# Durable invoker views

Substrate can persist immutable, named SQL read queries and execute them using the caller's current source permissions. A view never grants access to its underlying rows. Host code creates definitions; applications need a separate view grant and their own source grants.

Run the restart and revocation example:

```sh
cargo run --offline --example durable_views
```

## Native API

```rust
let id = Id::new();
db.define_view(
    id,
    "People",
    "SELECT name FROM core.Person ORDER BY name",
    &["core.Person"],
)?;
db.grant_view(id, "app")?;
let snapshot = db.session("app").snapshot();
let definition = snapshot.view_definition(id)?;
let result = snapshot.query_view(id)?;
```

`Database::define_view`, `grant_view`, and `revoke_view` are trusted host operations. `Snapshot::view_definitions` returns only definitions the caller can inspect. `Snapshot::open_view` returns a reusable `DurableView` with `query` and `definition` methods. The handle pins data, while authorization remains subject to the existing live revocation and expiry gates. Source revocation, view revocation, credential rotation, and capability or credential expiry fence retained handles. Results already copied to a caller cannot be recalled.

The definition ID is a stable schema UUID supplied by the host. Names are canonicalized to `liquid_views.<Name>`. Definitions are immutable: a replacement requires a new ID and name. SQL must be a single read query accepted by SQLite's view grammar. Mutation, additional statements, attachment, and bound parameters are rejected. Constant queries need no source permissions. Source names are explicit; omitted families cannot be accessed, and schema-control aliases are not resolved. The source list may deliberately include extra dependencies; their permissions are still required.

Each execution builds a read-only SQL catalog containing exactly those source families. Row filters and reference visibility use the invoker's snapshot. Inspection also requires access to each source's schema metadata, including reference-domain metadata. Stored SQL is revalidated whenever a handle is opened, even if trusted host code has modified the private ledger directly. Handles run a fixed read query; this API exposes no write-through operation.

## Persistence and protected metadata

Definitions use a versioned, validated `ViewDefinition` Rust representation with explicit source schema IDs, canonical names, structural fingerprints, and result column labels. JSON is the serialized declaration envelope. A compiled typed cache and the ordinary role index locate definitions at runtime; permission evaluation does not interpret JSON. Result labels are recorded, but expression result domains are not generated static Rust types.

An empty relation in the reserved `liquid_views` namespace provides each view's grant/capability identity. The actual definition lives in the host-only `liquid_view_catalog.Definition` ledger. Reading a marker through ordinary scans or SQL returns no definition SQL or source contracts. Marker permission alone cannot inspect a view. Capability scopes cannot include the private ledger; agent namespace ownership and schema installation cannot claim either reserved namespace. The trusted host can still inspect or mutate internal records using its administrator API, as with other internal ledgers.

Marker installation and definition insertion commit atomically. Both use existing typed packed families, transaction events, recovery validation, and checkpoints. This adds no new storage-event ABI and rewrites no old application records. Replay/checkpoint loading validates the envelope version and source contracts; unknown versions and invalid contracts fail closed. Query planning remains an execution-time validation step. Recovery does not eagerly prepare every stored SQL query.

## Bounds and current limits

Definitions are limited to 256 per database, 64 declared sources, 64-character names, 64 KiB SQL, and 256 result columns. Queries use the standard defaults of 1,000 result rows, 1 MiB result bytes, and 1,000,000 SQL VM steps. These are query execution bounds, not a wall-clock deadline for preparing arbitrary SQL.

This first slice creates one empty relation family and one additive `liquid_views` module version per view, so catalog size grows with definitions. There is no deletion or replacement API to reclaim the quota. It provides native Rust host/snapshot APIs, not remote service commands, generated TypeScript view clients, parameterized definitions, materialized results, view-on-view composition, definer rights, or permission to disclose otherwise protected source data. The marker family is a permission object; ordinary `SELECT` against that marker is empty, and callers execute the stored query through `query_view`/`open_view`.

Tests cover source row filters, separate view grants, private catalog protection, hidden reference-domain metadata, read-only validation, immutable names/IDs, reserved namespaces, retained-handle revocation and expiry, credential rotation, malformed version recovery, and restart through both redo and checkpoints.
