# Authenticated remote access, rotation and expiry

Run `npm run demo:remote` (Node, Rust and OpenSSL required). The example creates a disposable CA and localhost certificate, starts a TLS host, and runs a separate Node application. It verifies scoped runtime reads/writes, immediate credential rotation, retained-handle denial, parent revocation, deadline expiry, and checkpoint recovery. Development certificates are temporary; supply your own certificate/key and distribute a trusted CA for deployment.

## Host APIs

```rust,ignore
// Deadline is exclusive UTC Unix seconds, not application Timestamp microseconds.
let cap = db.issue_capability_until(id, "agent", scope, Some(deadline))?;
let login = db.issue_credential("agent", cap.id, deadline)?;
let server = RemoteServer::bind(db.clone(), address, &certificate_pem, &key_pem)?;
// Deliver login.token securely, once. Never log it.
let replacement = db.rotate_credential(login.id, new_deadline)?;
db.revoke_credential(replacement.id)?;
```

Only trusted code holding `Database` can issue, rotate or revoke credentials. There is no remote administration or login-enrollment endpoint. Every credential binds one principal and one selected capability. An authenticated session cannot remove that scope, switch chains, borrow ambient grants or name a different principal. Rotation preserves its principal/capability and atomically inserts a new credential and revokes the old one. No grace overlap is implemented. Replacement credentials cannot outlive the capability deadline. Host code can renew an expired credential while its capability remains live; revoked credentials cannot be rotated. To change scope, issue a new capability and credential explicitly.

Tokens contain a version, random UUID locator and 256 cryptographically random secret bits. The reserved typed `liquid_auth.Credential` v1 family stores only a domain-separated SHA-256 digest, principal, capability identity, deadline and revoked flag. Comparison uses constant-time digest equality. High-entropy generated tokens are not human passwords and do not need password hashing. Authentication looks up the compiled credential by ID; it does not scan log entries or parse JSON. Returned `IssuedCredential` deliberately lacks Debug and Serialize. Application code is responsible for secure secret delivery/storage and keeping request bodies out of logs.

Credential creation/rotation/revocation uses the existing atomic log and checkpoints. Credential rows carry normal dependency versions; retaining a `Session`, native snapshot, prepared SQL statement or pending transaction cannot bypass later revocation. The ledger is currently bounded to 8,192 historical credentials; reclamation is not implemented. Capability limits remain separately enforced.

## Transport and TypeScript

```ts
import { RemoteClient } from "./sdk/typescript/client.mjs";
import { Person } from "./generated/schema.js";

const client = new RemoteClient({
  host: "db.internal", port: 7443, servername: "db.internal", ca: trustedCaPem
}, token);
const people = await client.page(Person);
```

The Rust `RemoteClient` uses a socket address, verified server name, CA PEM and token. Both clients verify certificate trust and hostname and require TLS 1.3. There is no plaintext fallback, insecure verifier, or TLS early-data mutation path. Node sends the request only after `secureConnect` and copies only supported connection options, so extra settings cannot disable verification. Each connection carries one bounded JSON request/response using the same typed protocol as the Unix service. The query endpoint remains read-only. Explicit `execute(sql, params)` supports bounded transactional INSERT/UPDATE/DELETE under the same credentials and scopes; typed batches remain available. See [SQL write syntax and limits](SQL-WRITES.md). SQL/native/runtime/nested/schema operations use the same engine authorization gates. Effect's `layer(client)` accepts this client unchanged.

The Rust server allows at most 32 active connections, a 1 MiB request and 4 MiB response. The absolute ten-second IO budget includes the handshake and slow-trickle reads/writes. Completed workers are reaped. The budget does not preempt all kernel CPU work; SQL retains its existing statement limits and progress checks. There is no per-source rate limiter, connection pool, HTTP/PostgreSQL protocol or server-side cancellation. Clients never retry a disconnected mutation automatically because its commit outcome can be uncertain.

The older Unix `LocalServer` static `ServerConfig` remains supported for trusted local deployment. It is a separate credential mode: static tokens do not participate in the new credential registry/rotation. TLS never falls back to those tokens. `Database::authenticate` is available to other trusted transports that need the durable registry.

## Capability expiry and live semantics

New capability records use format 2 with optional `expires_at`. Format 1 records without expiry still load; unknown versions, a format-1 expiry, invalid deadlines and unknown fields are rejected. Legacy `issue_capability` deliberately creates a non-expiring root. `issue_capability_until` adds a deadline. New children inherit their parent's deadline by default. `delegate_capability_until` (or TypeScript's optional final `expiresAt` argument) can shorten it, never extend it. Schema definitions and packed application rows remain unchanged.

Deadlines are positive, integral Unix seconds bounded to year 9999, so JSON and JavaScript represent them exactly. At `now >= expires_at` authority is denied. Checks run at authorization entry, read-result release, SQL progress gates and transaction publication, including schema installation and delegation. The effective session deadline is the earlier of credential and capability expiry. Manual-clock tests cover exact boundaries without sleeps.

Revocation/expiry cannot retract copied results or bytes already authorized for delivery. Reads linearize at their final engine gate and mutations at commit; a successful self-revocation is acknowledged. The transport does not add a second gate that could report a committed mutation as an ordinary authorization failure. Socket failures after commit remain uncertain outcomes. Revoking a credential invalidates sessions authenticated with that credential; it does not revoke separately issued capabilities/credentials. Revoke an ancestor capability to stop its whole delegated subtree.

Wall time must be trusted. A process-local nondecreasing watermark prevents time moving backwards during normal operation. The watermark is not durable: after restart the host trusts its wall clock. This is not a secure-time system or distributed revocation protocol.

## Verification and remaining security work

Tests cover narrowing, subject binding, malformed/non-ASCII tokens, secret non-persistence, exact expiry boundaries, inherited deadlines, shortening/extension rejection, retained SQL/runtime/native handles, blocked staged writes and schema installs, credential and ancestor revocation, legacy capability compatibility, log/checkpoint recovery, and subprocess crashes immediately before/after rotation publication and checkpoint commit. TLS integration tests reject wrong CAs, wrong names and plaintext, and exercise rotation/revocation through real sockets. The Rust/Node example exercises the real SDK against this server.

This milestone supplies authenticated remote access, not a fully operated public service. Still missing: enrollment/OIDC or mTLS identity mapping, credential delivery and secret-manager integration, certificate reload/rotation automation, durable audit trails, per-client admission/rate limits, stronger resource isolation, secure time across restarts, ledger cleanup, general ReBAC/field policies and independent security review. Raw database-file access remains trusted; these permissions do not isolate hostile processes with filesystem access as the database's OS user.
