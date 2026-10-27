# SQL mutations through the service

`LocalClient::execute` and `RemoteClient::execute` in Rust, and `client.execute` in TypeScript, execute one bounded, atomic SQL mutation against either resident or paged storage. Run `cargo run --example sql_writes` for a working local service example. The existing query API remains read-only.

Supported statements:

```sql
INSERT INTO core.Person (name) VALUES ($1), ($2);
UPDATE core.Person SET name = $1 WHERE id = $2;
DELETE FROM core.Person WHERE id IN ($1, $2);
```

The wire operation is `{"op":"execute","sql":"...","params":[...]}`. Parameters use the same exact tagged scalar representation as queries: `{"Integer":"9223372036854775807"}`, `{"Real":1.25}`, `{"Text":"Ada"}`, `{"Blob":[1,2]}`, or `"Null"`. Bind values rather than interpolating SQL. `$N`, `?N`, and anonymous `?` placeholders are accepted. Parameter arity is checked. UUID reference and identity values use text parameters. Decimal and timestamp values use exact integer units. Float values must be finite; nullable roles accept SQL `NULL` or the bound null value.

Insert generates stable identities. Update/delete require an explicit `id = value` or `id IN (...)` predicate. Missing identities and existing rows outside the caller’s write filter both affect zero rows, preventing an existence oracle; repeated identities are changed once. Insert values and identity lists are limited to 100 entries. The returned `affected_rows` and `ids` describe explicitly targeted occurrences, excluding any implicit cascade effects. Rust returns `SqlWriteResult`; TypeScript returns `{affectedRows, ids}`. The capabilities response advertises `sql_writes` and the exact `sql_write_features` list.

Every statement compiles to existing native transaction operations. Write authorization, row filters on both previous and replacement values, credential scopes, reference checks, unique/check constraints, and live revocation remain authoritative. Write-only grants work without read privileges. Compilation against the authorized schema repeats on transaction conflict, and no partial multirow change is committed. SQL text and parameters are each bounded to 1 MiB, with at most 1,024 parameters. Repeated bindings are budgeted before allocation against a 4 MiB materialized mutation limit. Reads, nested reads and native mutations retain their existing behavior.

This is a deliberate subset: no insert-select, expression assignments, arbitrary update/delete filters, joins, returning clauses, upserts, explicit identity insertion, DDL, or multiple statements. DataFusion's general DML remains disabled; the controlled compiler routes accepted operations to native transactions. A transport disconnect after dispatch can have an uncertain commit outcome. Clients do not automatically resend mutations; reconcile the result before retrying.

TypeScript schema declarations support `s.float()` and `s.nullable(s.float())` (likewise other scalar types). Generated fields use `number` and `T | null`, with finite-number validation. Native wire null is the string `"Null"`, preserving Rust's tagged value format. Nullable fields still require an explicit key in generated full-insert validation; patches may omit keys.

Reference declarations also accept `ref("tree.Node", {onDelete: "cascade", deferrable: true})`, including inside `s.nullable(...)`. Defaults are restrict and immediate validation. Only reference fields may carry these options. Authorized catalog discovery and generated/runtime descriptors preserve this metadata; neither declarations nor metadata grant extra read, write or reference permissions.
