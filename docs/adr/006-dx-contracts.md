# ADR 006 — One typed contract, generated SDKs and a local application host

Status: accepted for the initial SQL + DX slice.

Preserve the Rust compiler and immutable relation families. The TypeScript frontend
only emits the v1 declaration envelope. A durable lock manifest owns IDs; generators
never invent identities. Canonicalization preserves ordered role slots and exact
version imports. Versioned SHA-256 fingerprints distinguish installed contracts from
client assumptions. Selected-module generation prevents unrelated app extensions
from changing static client types.

The TypeScript builder takes the code-first and inference approach demonstrated in
[Drizzle's schema declarations](https://orm.drizzle.team/docs/sql-schema-declaration).
It does not adopt Drizzle's physical table/migration model. Separating authored
contracts, emitted artifacts and explicit deployment is also a useful pattern from
[Prisma's contract authoring](https://www.prisma.io/docs/orm/contract-authoring/psl-syntax).
Here, additive installation is validated by Liquid and preserves old packed layouts.

Generate strict runtime validators alongside types. Implement the structural
[Standard Schema V1 interface](https://standardschema.dev/schema) without requiring
an external validation library. Keep encoded wire and decoded application values
explicit, consistent with the conceptual separation in
[Effect Schema transformations](https://effect.website/docs/v4/schema/transformations).
[Effect's Standard Schema integration](https://effect.website/docs/v4/schema/standard-schema)
provides an interoperability path; this milestone does not ship a dedicated Effect
adapter or test an external Effect version. Portable constraints are limited to what
the Rust engine actually enforces; application functions are never authoritative
serialized database validators.

[Gel's relationship-oriented model](https://docs.geldata.com/) is useful context for
future ergonomic traversal APIs. Current generated references and n-ary records
already derive from the single declaration. Nested loading, computed links and query
builders remain deferred. [DataFusion](https://datafusion.apache.org/user-guide/introduction.html)
remains a possible analytical provider; SQLite virtual tables already provide the
required parameterized SQL/join surface without changing canonical storage.

Expose a small versioned native protocol through a local Unix host. A host owns the
redb file and maps credentials to restricted principals. No application command can
request administrative authority. Batches preserve native transaction semantics;
ambiguous network/storage outcomes are not retried automatically. Read pages are
bounded and remote SQL is budgeted, but this is not full resource isolation or a
public network service.

The API intentionally stops before nested ORM operations, derive macros, implicit
schema updates, global entity unions, remote SQL writes and distributed transactions.
Measure the compiler with hundreds of relations before adding deeper generics.
See DX.md and DX-MATRIX.md for supported behavior and the deferred feature inventory.
