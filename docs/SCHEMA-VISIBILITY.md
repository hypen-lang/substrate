# Schema visibility

Visibility controls discovery and reading of each relation family. It does not
grant insertion, update, deletion, reference, or schema administration rights.

| Visibility | Discover and read |
| --- | --- |
| `public` | Anyone, including anonymous callers |
| `authenticated` | Any authenticated application in this database; default for new application schemas |
| `private` | The owning application and explicitly granted recipients |

Run `cargo run --example schema_visibility` for authenticated profiles, private
messages, anonymous SQL, and live revocation. The example creates `GamerProfile`
with the default and explicitly makes `GamingMessages` private.

## Creation and ownership

Trusted host transactions can call `install(module)` for the default,
`install_with_visibility(module, Visibility::Private)` for a uniform policy, or
`install_with_visibilities(module, map)` for per-relation overrides. Map keys are
fully qualified relation names from the new module; omitted keys default to
authenticated and unknown keys are rejected. `install_manifest_with_visibility`
sets a uniform policy for new modules only. Reinstalling an existing manifest
never changes its policies.

An application uses `Session::propose_schema_with_visibility` with the same map,
then the normal approval/install flow. The normalized initial policy is included
in the proposal digest, so approval binds both the immutable module and its
initial visibility. Installation and metadata creation share one transaction.

The host assigns namespace ownership through `configure_namespace`. New host
installs respect an existing namespace owner. Provisioning a previously unowned
namespace adopts its existing visibility policies. Namespace ownership cannot be
reassigned through this API. A host-created private schema with no namespace
owner remains host/grant accessible until ownership is provisioned.

Enabled namespace owners can call `Session::set_schema_visibility(relation,
visibility)`. Scoped sessions require the exact namespace schema capability;
ordinary data credentials cannot administer visibility. The host can use
`Transaction::set_schema_visibility` to combine policy changes with other writes.
Credential expiry/revocation and namespace policy changes are checked during
these operations and at commit.

## Authorization boundaries

Unscoped authenticated sessions combine ambient visibility with explicit grants.
An explicit row filter does not narrow a public or authenticated baseline. Use
private visibility with filtered recipient grants when row-level privacy is
required. Private owners can read their own family; writes and references still
require their separate grants.

Credential and delegated-capability scopes remain hard limits. Their effective
grants and row filters cap reads even for public families or the owning app.
Namespace-only capabilities grant schema operations without granting data reads.
Public rows referring to private rows remain subject to recursive reference
visibility: an inaccessible referenced row suppresses the outer row. Catalogs
redact inaccessible reference targets and fingerprints. SQL omits families whose
complete schemas cannot be exposed, so querying such a family may return an
unknown-table error. Joins and aggregates use the same authorized source rows.

`Database::session(principal)` is a trusted embedding API, not authentication of
an arbitrary remote name. `Database::anonymous()` creates a reserved, explicitly
unauthenticated session. It cannot write or reference even if a host mistakenly
grants that reserved identity permissions. Remote clients use authenticated
credentials or an explicit anonymous constructor; invalid credentials never fall
back to anonymous access.

Changing an existing visibility policy invalidates retained unscoped readers,
prepared SQL handles, catalog cursors, and dependent transactions. Clients must
obtain a fresh snapshot. New additive installs leave old readers operating.
Scoped readers do not gain ambient visibility, and retain their existing grant
and credential revocation fences.

## TypeScript and anonymous clients

```ts
import {proposeSchema, applySchema, setSchemaVisibility} from "./sdk/typescript/schema-control.js";
import {RemoteClient} from "./sdk/typescript/client.mjs";

// module contains gaming.GamerProfile and gaming.GamingMessages with stable IDs.
const proposal = await proposeSchema(owner, proposalId, module, {
  "gaming.GamingMessages": "private",
}); // GamerProfile defaults to authenticated.
// Apply after automatic approval, or after the host approves this exact digest.
await applySchema(owner, proposal);
await setSchemaVisibility(owner, "gaming.GamerProfile", "public");
const anonymous = RemoteClient.anonymous({host, port, servername, ca});
const profiles = await anonymous.query("SELECT name FROM gaming.GamerProfile");
```

`LocalClient.anonymous(socketPath)` supports the same reads over a Unix socket.
TLS verification and request limits still apply to anonymous remote calls.
The version-one request envelope retains its required `token` field: an empty
string explicitly selects anonymous access; a nonempty invalid token is denied.
Catalog descriptors include `visibility`; `null` means legacy grant-only policy.
The immutable manifest and generated record types exclude visibility policies,
so changing visibility does not require regenerating clients.

## Persistence and compatibility

Immutable relation definitions and packed application rows do not contain
visibility. Policies are separate typed rows in the protected
`liquid_visibility.Policy` family, keyed by stable relation identity. Writes
validate metadata and compile an indexed policy map; reads do not parse JSON or
scan log entries. The normal atomic redo and checkpoint paths persist these rows.
Visibility changes do not rewrite application records or relation definitions.
Internal security/control families never receive ambient visibility.

Old logs and checkpoints lacking visibility metadata retain their original
grant-only behavior. The authenticated default applies only to newly installed
application schemas. Existing version-one schema proposals retain their original
approval digest and install private policies; new version-two proposals bind
their normalized visibility map. Recovery tests exercise both pre-visibility
redo/checkpoints and pending version-one proposals.
