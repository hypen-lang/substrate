# Authorized runtime schema discovery

`Snapshot::catalog(&CatalogRequest)` and the local protocol's `catalog` command
provide a versioned, bounded discovery view over compiled schema and policy state.
This is the first milestone in the agent-facing schema workflow. It is deliberately
a discovery DTO, not an installable manifest or an authorization token.

```typescript
import {introspect} from "./sdk/typescript/catalog.js";

const first = await introspect(client, {limit: 50});
const person = await introspect(client, {relation: "core.Person"});
const next = first.next === null ? null : await introspect(client, {
  after: first.next, expectedRevision: first.revision, limit: 50
});
```

The helper validates the returned scalar domains, role identities, constraints,
revision and pagination metadata. The working process example checks that Fitness
sees only Core/Fitness and CRM sees only Core/CRM. Discovery needs no generated
application types. `describe(client, nameOrAlias)` now compiles a complete response
into a runtime descriptor; see [schema control](SCHEMA-CONTROL.md).

## Visibility contract

A relation is discoverable only with an explicit read, write or reference grant.
An all-false grant and a namespace-install grant do not reveal existing families.
The response includes relation UUID/name/fingerprint, owning module and exact
revision, role IDs and scalar domains, required roles, equality indexes, composite
unique keys and read/insert/update/delete/reference operation flags. The owning
module revision is not the latest namespace revision. Types, properties and
relationships use the same relation primitive; no artificial entity/edge distinction
is invented by discovery.

An inaccessible reference target becomes `{kind:"ref",target:null}`. Its name and
ID are omitted. A discoverable target includes just its name and ID; discovery does
not recursively expand its schema or its imports. No sibling lists, module imports,
row counts, row-policy values, principals, namespace grants or hidden constraints
are returned. A complete descriptor includes the installed contract fingerprint. A descriptor
with an opaque reference domain returns null for its fingerprint too: publishing
the full-contract hash would permit offline guesses of hidden target names. Such
a partial descriptor is not usable as a checked generated-client contract.
Names of granted roles themselves are part of that granted relation's public contract.

`row_restricted` says a row predicate exists without revealing the predicate. Flags
are grant capabilities, not promises that every row or reference will be accepted.
Subsequent reads/writes still enforce row policy, reference capabilities, uniqueness
and transaction checks. Reference-only metadata access does not permit data reads.

The legacy `capabilities` endpoint returns full Relation declarations and cannot
encode opaque targets. It now omits families whose reference targets are hidden,
and all-false grants. Use `catalog` for complete, safely redacted discovery.

Direct lookup of a missing or inaccessible relation returns the same denied error.
Listing is UUID-ordered and pages only discoverable IDs, with 1–100 relations (50
by default), no global count, and a 1 MiB encoded-relation budget. A single oversized
relation fails explicitly; it cannot be truncated into a misleading descriptor.
The entire catalog remains memory-resident, and paging may scan hidden entries;
this is not a constant-time side-channel-resistant service.

One request uses one immutable data/policy snapshot. Every request gets fresh policy.
`expectedRevision` prevents mixed-revision pagination, including after data writes
or revocations; restart enumeration on conflict. The revision is the existing global
revision token and can reveal that some database activity occurred. No historical
snapshot handle is exposed remotely. Native retained snapshots pin data but now fail closed on authority revocation.
See [live authorization semantics](SECURITY.md).

## Scoped installation and remaining security work

The [schema-control workflow](SCHEMA-CONTROL.md) now adds host-provisioned namespace
ownership and quotas, concrete proposals, host approval or bounded automatic
approval, transactional online apply, runtime descriptors, aliases and lifecycle
metadata. Catalog lookup resolves aliases and reports lifecycle; it still confers
no authority. The local protocol exposes scoped application operations, with no
administrative provisioning or approval endpoint.

Scoped delegation and live read/commit gates are now implemented. Arbitrary ReBAC,
authenticated remote transport and stronger resource isolation remain future work.
The private Unix host's fixed credentials do not isolate processes that share
OS-user access to its files. Retirement is metadata plus a restriction on new
managed references; it preserves existing data access and is not revocation.
