# Scoped schema control and runtime descriptors

The local host now supports a complete flow: provision a namespace, propose a
bounded additive module, approve its exact plan, install it online, discover its
descriptor, and immediately query/write it. No generated application source or
agent restart is needed. This remains a private local service with trusted host
administration; it is not an internet-facing hostile-agent sandbox.

Run the working example:

```sh
npm run demo:schema
```

A Rust host provisions one automatically approved namespace and one requiring host
review. A separate Node process submits both modules, installs them, discovers
runtime descriptors, writes exact bigint values, performs nested reads, verifies
cross-principal denial, and creates an alias/deprecation/retirement. The host reviews
the stored plan through its Database handle while a concurrent reader continues.
Checkpoint/reopen verifies the installed schemas, data and metadata. Source:
`examples/schema_host.rs` and `examples/dx/dynamic-app.ts`.

## Host provisioning and approval

Only trusted code holding `Database` can provision ownership/quotas or approve a
pending proposal. No corresponding administrative command exists in the public
protocol. Ordinary clients cannot elevate themselves by sending an owner, approver,
admin flag or arbitrary grants.

```rust
use substrate_liquid::schema_control::{NamespacePolicy, SchemaQuotas};

db.configure_namespace(NamespacePolicy {
    namespace: "fitness".into(),
    owner: "fitness_agent".into(),
    auto_approve: false,
    enabled: true,
    quotas: SchemaQuotas::default(),
})?;

// While the server is running, in trusted host application code:
let proposal = db.review_schema_proposal(proposal_id)?;
// Present proposal.plan to the host's approval UI/process, then:
db.decide_schema_proposal(proposal_id, &proposal.plan.digest, true)?;
```

Approval is bound to the owner, canonical module (including every relation UUID),
and policy generation. The digest uses a versioned SHA-256 domain. It is not a
bearer credential: another principal cannot inspect or apply the proposal even if
it knows its UUID and digest. Changing quotas, enabled state or approval mode
advances policy generation and makes outstanding plans stale. Ownership cannot be
reassigned. Namespace case collisions are rejected.

`auto_approve: true` is explicit host authorization for bounded additive modules in
that namespace, subject to current reference grants and quotas. It is off unless
the host supplies it. `enabled: false` stops new planning/apply/alias/lifecycle
changes, without revoking existing data grants. An already installed receipt can
still be read/reconciled. The host API is usable from an embedding application;
an operator approval dashboard or second administrative network service is absent.

## Application flow

```typescript
import {randomUUID} from "node:crypto";
import {proposeSchema, schemaProposal, applySchema} from "./sdk/typescript/schema-control.js";
import {describe} from "./sdk/typescript/runtime.js";

const proposalId = randomUUID();
const module = {
  name: "fitness", version: 1,
  imports: [["core", 1]] as [string, number][],
  relations: [{
    id: randomUUID(), name: "fitness.Score",
    roles: [
      {name: "person", data_type: {Ref: "core.Person"} as const},
      {name: "value", data_type: "Int" as const}
    ],
    unique: [["person"]]
  }]
};
const proposal = await proposeSchema(client, proposalId, module);
// If pending, have the host review it. schemaProposal(client, proposalId)
// retrieves the latest status; the SDK does not automatically poll or retry.
await applySchema(client, proposal);

const score = await describe(client, "fitness.Score");
const id = await client.insert(score, {person: existingPersonId, value: 42n});
await client.update(score, id, {value: 43n});
const page = await client.page(score);
```

Persist/reuse the exact module and proposal UUID when reconciling a lost reply.
Wire proposals require explicit relation UUIDs; omitted IDs are rejected, unlike
some offline authoring conveniences. Repeating submission with the same ID and
module returns the existing receipt. Reusing that ID for another module conflicts.
Repeating successful apply returns Installed without another commit or regranting
permissions. Disconnected mutations are not silently retried by the client.

States are Pending → Approved → Installed, with Rejected or Cancelled as terminal
alternatives. A pending/approved proposal may be cancelled by its owner. Host
rejection may withdraw an approval before installation. Approval/apply/cancel
races conflict through the same transactional row dependencies.

## Plan and installation semantics

A plan reports the normalized module, exact IDs, compatibility through the existing
compiler, reference requirements, grants to the owner, projected namespace usage,
per-family index counts and minimum packed Put-event bytes. Reference domains must
already be reference-authorized; unknown and inaccessible targets return the same
denied error. Referenced-family fingerprints are redacted if their own reference
domains are hidden. Plans do not expose other owners' schemas, imports or policies.

Imports must be the exact minimal namespace/revision set derived from authorized
external references: for each namespace, the maximum owning revision of the
referenced families. Unused imports and guesses at hidden revisions are rejected.
Same-namespace prior families need no import entry. This deliberately narrower
proposal contract does not add semver ranges or automatic dependency fetching.

Each proposal adds one successive module version with new family names/IDs. Existing
roles, constraints and layouts cannot be changed. No installed data is rewritten or
backfilled. Minimum Put-event bytes exclude identity/framing/checksum/index-tree
costs and variable text/blob contents. Schema-byte quotas count encoded definitions,
not total disk size, row data, checkpoint size or allocator overhead. Automatic
checkpoints may still copy unchanged records as maintenance.

Apply revalidates current policy generation, reference permissions, lifecycle,
imports, version availability, alias reservations and quotas in one transaction.
The new compiled schemas, read/write/reference grants for **only the new families**,
and Installed receipt commit atomically. No preexisting grant is broadened.
A concurrent winning install of the same namespace version returns Conflict for
the loser. Reconcile, cancel the stale proposal, and submit a new ID/version/plan;
approval is never silently transplanted to another definition.

Namespace/catalog, quota buckets, proposal rows, reference policy, retirement and
alias reads participate in OCC. Revocation between validation and commit conflicts.
Enrollment racing a legacy install also conflicts. Managed namespaces reject
restricted `Transaction::install`, even if a legacy broad `grant_schema` exists;
those callers must use proposals. Host/raw administrative Database APIs remain
trusted and can deliberately bypass control-plane policy. Do not hand those handles
to applications or mutate the private control tables directly.

## Quotas, versions and lifecycle

Default per-namespace limits are 64 installed relations, 512 total roles, 32 module
versions, 256 KiB encoded definitions and 128 lifetime proposal records. Hard
configuration ceilings are 4,096 relations, 32,768 roles, 1,024 versions, 16 MiB
definitions and 4,096 proposals. A single proposal is at most 64 KiB, 64 families,
128 roles per family and 16 unique constraints per family. Alias count is bounded
by the relation quota. Failed validation consumes no proposal entry. Accepted then
cancelled/rejected proposals remain counted; retirement does not reclaim IDs or
quota. This prevents an unbounded submit/cancel loop. Hosts may raise quotas within
the hard ceilings. Proposal archival/garbage collection is not implemented.

`schemaNamespaces(client)` lists only the principal's owned policies and usage.
Existing authorized catalog discovery supplies reusable families and references;
there is no global package registry exposing other applications' private metadata.
Versions remain exact additive integers. TypeScript submission rejects unsafe JS
integer versions; revision/generation counters in responses are decimal strings.

`createSchemaAlias(client, "fitness.Rating", "fitness.Score")` reserves an immutable
same-namespace name. Discovery resolves it to the canonical descriptor; it does not
create a SQL view or rename a physical table. Aliases cannot shadow real families,
be rebound, or be taken by subsequent managed additions. Authorization still checks
the target family. Existing SQL uses canonical names.

`setSchemaLifecycle(client, relation, state)` allows monotonic Active → Deprecated
→ Retired transitions (including directly retiring). Discovery returns lifecycle
and aliases. Deprecation is advisory. Retirement blocks new managed schema references
and new aliases to that family, **but preserves existing reads, writes, references,
grants and data**. It is not deletion, secure erasure, a write ban or live revocation.
Explicit data-access and capability revocation remain separate authorization operations.

## Dynamic typed operations

TypeScript `describe(client, nameOrAlias)` creates a Descriptor from authorized
catalog metadata using the same exact scalar codecs, object validators, named-field
mutations and contract checks as generated records. Opaque reference domains cannot
produce a complete descriptor. Cached descriptors confer no authority: each request
checks current policy and the installed contract. `table(descriptor, alias)` works
with the query builder. `referenceColumn(table, roleName)` checks a runtime-discovered
reference role for nested `many`/`one` plans. No client-side data joins are added.

Unknown schemas cannot acquire compile-time field names by magic. Dynamic records
are `Record<string, boolean | bigint | string | Uint8Array>` with runtime validation.
Use generated records when compile-time field/domain precision is needed.

Rust uses `Snapshot::describe`, `RuntimeDescriptor`, `Transaction::insert_dynamic`,
`update_dynamic`, `delete_dynamic` and `Snapshot::get_dynamic`. Values are the typed
Rust Value enum; named fields enter the existing compiled validation/index path.
A mismatched descriptor aborts the write transaction. JSON is not interpreted by
normal runtime data operations.

## Persistence and security limits

The host bootstraps four private, fixed-ID relation families in `liquid_control`:
namespace policies, proposal receipts, lifecycle and aliases. Policies use typed
bool/integer/text columns, not executable JSON. Proposal plans have a versioned
JSON declaration envelope with required stable IDs. These control records use the
existing typed binary row/log/checkpoint machinery; no new durable event ABI or old
application backfill is needed. The current receipt state persists across replay
and checkpoints; this is not a complete append-only audit trail of approvers and
historical decisions.

The host retains fixed principal credentials and private Unix transport. Same-OS-user
filesystem access is not isolated. Scoped delegation and live revocation gates are described in [SECURITY.md](SECURITY.md).
Arbitrary ReBAC, remote authenticated TLS, general CPU/memory budgets,
data-volume quotas, ownership transfer and destructive schema retirement remain
unimplemented. This explicitly provisioned local workflow is not a claim that those
prerequisites for hostile public self-service have been solved.

See [verification](IMPLEMENTATION.md), [measured installation costs](benchmarks/SCHEMA-CONTROL.md)
and [design decision](adr/008-schema-control.md).


Delegated namespace operations use the selected reference scope, keep host approval
requirements and grant new families only to the namespace owner. Delegation does not
include lifecycle or alias management. Existing capability relation lists do not
expand after installation. Read [the security workflow](SECURITY.md) before exposing
these operations to a delegated principal.
