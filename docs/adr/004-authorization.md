# ADR 004 — Explicit capabilities and reference-closed reads

Status: row/reference semantics accepted. Retained-policy revocation behavior below
is superseded by [ADR 009](009-delegation-revocation.md); general policies remain deferred.

The primitive read is Q(Visible_p(X_t)). A relation row participates only when its
own grant and equality predicate pass and all reachable referenced participants are
readable. Hidden references suppress the ordinary relationship rather than leaking
participant IDs or presenting redaction as NULL. Reference permission independently
allows creating links; it does not grant read access.

For writes, validate old and proposed new rows. Applications cannot update their own
capabilities. Schema ownership grants apply to namespace installation, not existing
people referenced by that namespace. Principals are trusted host-assigned labels in
this slice; they are not yet stored as first-class protected identities.

[PostgreSQL row security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
is a useful precedent for policies on input rows and write checks. The prototype has
one small equality predicate shared across actions, not PostgreSQL's full policy system.
[Zanzibar](https://www.usenix.org/conference/atc19/presentation/pang) motivates keeping
policy/data consistency explicit; its relationship authorization system is not
implemented here.

Restricted snapshots now pin data with live authorization gates at entry, result
release and commit. A changed authority invalidates affected retained handles.
Scoped principal-bound delegation and cascading revocation are implemented in
[ADR 009](009-delegation-revocation.md). Copied results cannot be recalled.

There is no published/definer view delegation, field redaction, arbitrary recursive
policy language, authenticated network endpoint, or exhaustive timing-channel defense.
All raw storage and Database handles must remain in a trusted host boundary. This
restriction is part of the API contract, not an authentication implementation.
