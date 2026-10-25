# Immutable typed checks and conservative evolution

New relation families can declare inclusive `IntRange` and `ByteLength` checks.
`IntRange` accepts only an `Int` role; decimal and timestamp bounds are not
implicitly treated as integer constraints. `ByteLength` accepts `Text` or `Bytes`.
For text it counts UTF-8 bytes: `🦀` takes four bytes. There are at most 64 checks
per relation. Unknown roles, incompatible types, inverted bounds and duplicate
checks are rejected before installation.

```rust
let profile = Relation::new("app.Profile", vec![
    Role::new("age", DataType::Int),
    Role::new("name", DataType::Text),
])
.check(Check::int_range("age", 0, 150))
.check(Check::byte_length("name", 1, 80));
```

```typescript
const Profile = entity({ age: s.int64(), name: s.string() })
  .intRange("age", 0n, 150n)
  .byteLength("name", 1, 80);
```

The Rust schema compiler resolves check roles to tuple offsets once. Every typed,
named, dynamic, generated or SQL write then evaluates those compiled checks without
interpreting JSON. Failed mutations abort their transaction. Existing authorization,
uniqueness and reference checks still apply. Recovery validates checks on every
decoded row before publishing recovered state.

Generated TypeScript clients validate insert, patch, batch encoding and decoded
records. Runtime descriptors discover the same checks through authorized catalog
introspection and apply them locally. The database remains the authority; client
validation alone cannot grant access or bypass a constraint. Generated Rust records
retain their ordinary field types, with checks enforced by the database write API.

## Declaration and storage compatibility

Manifest format **2** is required for nonempty checks. An integer range is represented
as `{"IntRange":{"role":"age","min":"0","max":"150"}}`: integer bounds are
canonical decimal strings so TypeScript preserves every signed 64-bit value.
Byte lengths use unsigned 32-bit JSON numbers. Rust builders use `i64`/`u32` directly.
Unknown variants/fields and noncanonical integer bounds fail closed.

Existing format 1 manifests, schema fingerprints and unconstrained generated
contracts are preserved. Empty checks are omitted when serializing declarations.
Selecting only an old unconstrained module still produces its format 1 manifest,
even when another application installed a constrained extension. Nonempty checks
are part of a version 2 relation fingerprint, so stale generated contracts fail.

Redo records containing constrained installs use an explicit **LQ02** envelope and
frozen metadata event format. The decoder still supports frozen **LQ01** records;
old schema events convert to an empty check list. The new field is never silently
interpreted as an LQ01 extension. Ordinary packed rows keep their existing encoding.
Version 2 incremental checkpoints store constrained metadata with LQ02 and unchanged
application rows with the same row bytes. Old binaries reject new metadata rather
than ignoring its constraints; downgrade compatibility is not promised.

Checks are immutable parts of a relation definition. They cannot be attached to
an existing family in place. Add a new independently named relation/version instead;
referencing existing person identities does not rewrite old person rows. Manifest
relocking and installation reject edits to prior versions. This milestone does not
automatically copy data into a replacement relation or claim arbitrary destructive
schema changes are migration-free.

## Run and verify

```sh
cargo run --offline --example constraints
cargo test --offline --test constraints
cargo test --offline --lib codec::compatibility_tests
npm run build:ts
node --test tools/constraints.test.mjs
```

The native example installs a constrained health extension, writes through a runtime
descriptor, rejects an invalid age, checkpoints and reopens, and confirms the original
person record is unchanged. Tests cover 128 randomized reference comparisons,
inclusive boundaries, multibyte text, insert/update/SQL/dynamic writes, permission and
reference failures, redo/checkpoint recovery, manifest compatibility, old packed row
bytes, invalid LQ02 metadata, and executable generated TypeScript validation.

Still missing: cross-row cardinality bounds, deferred constraints, expression checks,
cascading deletion, automatic backfills/conversion plans, safe destructive lifecycle,
ownership transfer, and new generated Rust scalar wrapper types. Per-row checks add
no new indexes or background work; no performance improvement is claimed here.
