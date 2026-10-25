# Scoped delegation and live revocation

TLS remote authentication, durable credential rotation and capability expiry are now
implemented. See [remote security](REMOTE-SECURITY.md) and `npm run demo:remote`.

Run `npm run demo:security`. A trusted Rust host starts a private Unix service.
Authenticated Node clients delegate a narrowed capability, discover runtime
schemas, write exact integers, load nested relationships, obtain host approval for
an additive schema, and verify cascading revocation. Native retained snapshots,
prepared SQL and staged writes are checked too, followed by checkpoint/reopen.
Sources: `examples/security_host.rs` and `examples/dx/security-app.ts`.

## Authority and attenuation

Host-only `Database::issue_capability(id, owner, scope)` issues a root bounded by
that owner's **existing** relation grants and namespace ownership. Ordinary
sessions cannot issue roots or approve schema proposals. Root issuance does not
create a login credential. The recipient must already authenticate as the named
principal through the existing host credential mechanism.

```rust
use substrate_liquid::{capability::*, Grant, Id};

let root = db.issue_capability(Id::new(), "owner", CapabilityScope {
    relations: vec![RelationCapability {
        relation: person_schema_id,
        grant: Grant::read_only(),
    }],
    schema_namespace: None,
    delegations: 1,
})?;
let child = db.session("owner").delegate_capability(
    root.id, Id::new(), "worker",
    CapabilityScope { delegations: 0, ..root.scope.clone() },
)?;
let worker = db.session("worker").with_capability(child.id)?;
let people = worker.snapshot().scan("core.Person")?;
db.session("owner").revoke_capability(child.id)?;
assert!(worker.snapshot().scan("core.Person").is_err());
```

A scope contains at most 64 exact relation UUIDs, independent read/write/reference
bits, and an `All` or typed equality row filter for each relation. A child may
remove a relation/right, narrow All to equality, retain the same equality, remove
schema authority, and reduce delegation depth. It cannot change an equality to a
different predicate, broaden it, add a relation/right/namespace or replenish depth.
The same row predicate applies to both old and new write values. References and
reference-closed read visibility use the selected scope too.

Capabilities are **principal-bound records, not bearer secrets**. Knowing another
principal's capability UUID does not authorize use. `with_capability` explicitly
selects one chain; its permissions replace the session's ambient grants for that
operation. They never union with ambient grants, another capability, or the root
owner's other permissions. A selected session cannot switch to another chain or
return to its ambient session. Code that separately possesses an ordinary session
or credential still possesses that independent authority.

Delegation depth is at most eight edges. The ledger is bounded to 8,192 records
per database and 256 issued records per issuer, including revoked records. Each
encoded record is limited to 64 KiB. There is no ledger reclamation yet. Allocation
uses caller-supplied unique IDs; duplicate issuance/delegation fails instead of
replacing authority. The SDK does not retry a disconnected mutation automatically.

An issuer can revoke its issued capability, a subject can relinquish its own, and
the root owner or host can revoke the chain. A selected session can revoke itself
or a directly issued child, with the usual issuer/subject checks. Revoking any
ancestor disables descendants without rewriting them. Revocation is irreversible;
a new grant requires a new root/child identity. Repeated revocation is safe.

## TypeScript and protocol

```typescript
import {delegateCapability, revokeCapability} from "./sdk/typescript/capability.js";
import {describe} from "./sdk/typescript/runtime.js";

const child = await delegateCapability(ownerClient, rootId, newId, "worker", {
  relations: [{relation: personSchemaId, grant: {
    read: true, write: false, reference: true,
    filter: {Equals: {role: "name", value: {Text: "Ada"}}}
  }}],
  schema_namespace: null,
  delegations: 0
});
const scoped = workerClient.withCapability(child.id);
const person = await describe(scoped, "core.Person");
await scoped.page(person);
await revokeCapability(ownerClient, child.id);
```

`LocalClient.withCapability` preserves the existing typed client interface and
wraps every request in `using_capability`. Native protocol clients can send that
command explicitly. Nested selection wrappers and attempts to change the selected
chain fail. `delegate_capability` and `revoke_capability` are public authenticated
operations; root issuance remains host-only. There is no public administrator flag.

Equality policy values use the same tagged exact scalar format as data mutations:
`{Int:"9007199254740993"}`, decimal minor units and timestamp microseconds remain
strings on the wire. Numbers in those positions are rejected, preventing JavaScript
rounding from broadening/changing a policy. Predicates compile into typed role slots.

## Schema control under delegation

An optional `schema_namespace` delegates proposal, receipt inspection, cancellation
and approved apply in **one namespace owned by the root authority**. It does not
permit provisioning, host approval, quota changes, ownership transfer, lifecycle
changes or alias creation. A child can retain this namespace or remove it.

Proposals remain owned by the namespace owner. This delegates namespace-level
workflow authority, not just access to proposals created by that child. Reference
requirements still use the selected relation/reference grants, not the owner's
broader permissions. Stored receipts with out-of-scope references are denied;
reference fingerprints are redacted when their own targets are hidden. Full native
`schema`/`relations` APIs also deny/omit declarations with hidden targets; use the
catalog for a safely partial descriptor. Host review
can validate a redacted requirement without publishing its hidden hash.

Manual host approval or the namespace's explicit automatic-approval policy remains
required. Apply revalidates the capability chain, namespace policy generation,
reference permissions, quotas, compatibility and approved digest in the installation
transaction. Revocation racing validation/commit aborts publication. Schema, owner
grants and Installed receipt stay atomic. Lifecycle and alias APIs reject delegated
sessions until those operations have explicit capability scopes of their own.

**New-family grants go only to the namespace owner.** An installed family is outside
an existing capability's fixed relation list. To use it, the host issues an updated
root and the owner delegates a new scope. Schema authority does not silently become
unbounded data authority. Revocation stops future use/apply, including receipt
reconciliation through that revoked capability; it does not undo an installed
schema or erase acknowledged data.

Changing namespace policy increments its durable generation and permanently
invalidates roots bound to the earlier generation. Restoring the previous settings
does not restore the old generation. Root issuance and each delegated operation
check that the namespace remains enabled and owned by the same principal.

## Live revocation semantics

Data stays snapshot-consistent. Authorization is checked against the current host
at operation entry, at read-result release, and under the publication lock at write
commit. Prepared SQL, native scans/lookups/pages, runtime descriptors, catalog
introspection and nested reads share this boundary. SQL also checks cooperatively
through its VM progress hook; native policy traversal has a final result gate, not
preemptive thread cancellation.

A direct principal's retained handle is invalidated by replacement/removal of any
of that principal's grants. Adding a new grant or additive schema does not invalidate
existing data readers or retroactively expand their pinned access. For selected
capabilities, changing/revoking an ancestor or its namespace policy invalidates
retained handles. Replacing/removing a root owner's referenced relation grant
atomically revokes affected roots, so restoring that grant cannot resurrect old
capabilities. Unrelated sibling capabilities and other principals remain usable.

A stale handle fails closed; it does not combine old data with newly evaluated row
predicates. Obtain a fresh snapshot/session when still authorized. Mutations that
fail poison their transaction. Staged transactions may report Denied or an OCC
Conflict; a fresh retry must pass current authority. The host does not keep a stale
snapshot alive merely to retain authorization: restricted handles fail after their
original database host has closed, even if a new host opens the same file.

The linearization point of a read is its final authorization gate; that of revocation
is durable commit/publication. An operation overlapping revocation can finish if
its final gate preceded revocation. If revocation publishes before that gate, no
successful result is released. This is **not a network-delivery barrier**: buffers
already authorized/sent may arrive later. Returned rows, descriptors and materialized
Tables are ordinary caller-owned copies and cannot be recalled. Trusted administrator
snapshots are outside the restricted-principal revocation boundary.

## Storage, validation and limits

A reserved fixed-ID `liquid_security.Capability` family stores version-2 typed
capability declarations (with version-1 compatibility) and an irreversible revoked flag through existing packed
rows, atomic redo and checkpoints. No old application layout or log ABI changes.
Malformed serialized scope/scalar declarations fail recovery. Issuance validates
rights against typed schema/grants; chain use checks parent/issuer/subject/authority,
strict attenuation, depth, revocation, expiry and namespace generation.

On publication/recovery, the engine builds capability-ID and root-by-grant indexes
and compiles equality predicates once. Runtime snapshots use at most the selected
scope's grants and a bounded ancestor chain. They do not parse JSON or scan the log.
Per-handle live dependency versions are ephemeral; durable revocation flags and
namespace generations preserve authority across recovery. Checkpoint reconstruction
of indexes cannot reactivate a revoked record.

This milestone does not implement general ReBAC, arbitrary predicate conjunctions,
cryptographically portable/offline bearer tokens,
distributed revocation, field policies, exhaustive
side-channel defense or comprehensive resource isolation. The private Unix host
still uses fixed credentials; same-OS-user raw filesystem access is not isolated.
Database/admin handles and raw storage remain trusted. A public hostile-agent
service still needs enrollment, operational hardening and isolation work.
