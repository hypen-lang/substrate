# ADR 010: TLS, durable credentials and capability deadlines

Status: implemented, 2026-10-25.

The network boundary is TLS 1.3 over the existing bounded command protocol. Rustls verifies certificates in the Rust client; Node's TLS client verifies the supplied CA and hostname. There is no plaintext downgrade and no new database execution engine. Only the trusted host owns the listener configuration, certificate private key and credential administration APIs.

Remote credentials bind to a principal and one capability rather than unrestricted ambient authority. Generated 256-bit secrets are delivered once; their digests live in a reserved typed relation, rebuilt into an ID lookup cache on recovery. Atomic rotation creates a fresh identity/secret and irreversibly revokes the old record. Immediate cutover is simpler to reason about than overlapping grace credentials. Credential revocation is independent of capability-subtree revocation.

Capability format 2 introduces exclusive Unix-second expiry; format 1 remains readable without a deadline. Delegation inherits or shortens its parent's lifetime. Credential deadlines cannot exceed the selected capability's deadline. Engine result and publication gates use the minimum deadline and current dependency versions, so snapshots and in-flight transactions cannot retain expired or rotated authority. A process-local nondecreasing wall-clock watermark prevents local rollback; trusted wall time after restart is an explicit assumption.

The transport respects operation linearization. It does not check again after a committed operation and falsely report a successful self-revocation as denied. Mutation responses lost to network failure still have uncertain outcome. Neither TLS nor expiry can recall results already released by the engine.

The existing durable row/log/checkpoint ABI is retained. Credential schema v1 and capability payload v2 provide explicit format boundaries; applications' packed layouts are unchanged. We do not add portable signed tokens, a distributed verifier, public enrollment, OIDC, certificate reload, credential garbage collection or general ReBAC in this milestone. See [remote security](../REMOTE-SECURITY.md) for operational limits and tests.
