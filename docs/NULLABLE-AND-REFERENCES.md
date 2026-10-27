# Nullable values, finite floats, and reference actions

Schema declarations compile into the existing typed Rust representation. `DataType::Float` is a finite IEEE-754 binary64 scalar; `DataType::Nullable(Box<DataType>)` adds explicit nullability to any base type. JSON declarations use `"Float"` and `{"Nullable":"Float"}`. Nullable reference declarations use `{"Nullable":{"Ref":"app.Person"}}`.

`FiniteFloat::new(value)` rejects NaN and infinities. Negative zero is normalized to positive zero, so equality, ordering, hash keys, JSON and indexes agree. Native values use `Value::Float(FiniteFloat)` or `Value::Null`. Exact decimals retain their existing fixed-scale representation; a float is not an exact money type.

All declared fields remain present in named writes. Nullability permits an explicit NULL; it does not invent defaults or make omitted fields optional. Generated Rust types use `Option<T>` and `substrate_liquid::FiniteFloat`. Generated TypeScript types use `T | null` and `number`, with finite-number validation. Integer-only SQL fast paths exclude nullable/float shapes; general authorized execution preserves NULL semantics.

A UNIQUE tuple containing any NULL does not conflict with another tuple, while NULL values remain queryable through native equality indexes. CHECK constraints permit UNKNOWN for a nullable input and still reject a false predicate. A NULL reference creates no incoming edge and discloses no referenced row.

## Explicit reference constraints

Reference roles accept `.on_delete(OnDelete::Cascade)` and `.deferrable()`. Defaults are RESTRICT and immediate validation. These fields are omitted from serialized declarations at their defaults, preserving existing declaration fingerprints. They are part of an immutable relation definition: changing an existing declaration still requires a conservative new relation family.

```rust
Role::new("person", DataType::Ref("core.Person".into()))
    .on_delete(OnDelete::Cascade)
    .deferrable()
```

A cascade computes the full dependent-row closure before staging changes. Every deleted row requires write permission, including rows in another application. An unauthorized dependent aborts the transaction. Cycles are handled once, and traversal has explicit row and edge budgets. Incoming-edge read dependencies prevent a concurrent insertion from slipping past a deletion. Stable identities are retained after memberships are deleted.

`transaction.defer_constraints()?` defers **only foreign-key constraints explicitly marked deferrable**. It does not defer UNIQUE or CHECK constraints. Final target existence and reference permissions are checked before commit, with read dependencies retained for conflict detection. This permits forward references and cyclic inserts, or deleting a target before repairing its dependent references in the same transaction. Any unrepaired reference rejects the whole transaction. Deferral does not grant reference permission to protected target rows.

## Durable compatibility

Legacy LQ01/LQ02 redo records retain their existing validation and encoding. LQ03 explicitly admits the new type/constraint vocabulary. Nullable packed fields add one presence byte, followed by the base payload only for non-NULL values. Resident redo and spill rows encode finite floats in eight little-endian bytes; nonfinite values and noncanonical negative zero are rejected during replay. Versioned redo validates deferred references and cascade outcomes against the complete transaction state. Checkpoint and disk-spill decoding apply the same scalar validation.

Old relation families retain their layouts and records; adding an application family with nullable/float roles does not rewrite old rows. New spill rows use LDS2 while legacy rows keep LDS1. Paged storage uses its separately versioned PG02 codec, with big-endian float payloads; existing PG01 families keep their encoding.

Run `cargo run --example nullable_references` for explicit forward-reference deferral, a finite float and NULL attached to one stable person identity, followed by an authorized atomic cascade.
