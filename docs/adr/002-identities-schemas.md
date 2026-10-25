# ADR 002 — Stable UUIDs and immutable definition contracts

Status: accepted semantics; UUID and declaration encodings provisional.

Use UUID v4 for both data and relation-definition identities. Reject collisions
against both namespaces; optimistic allocation dependencies also reject conflicting
concurrent allocations. IDs are not location- or application-derived. Persist data
allocation tombstones and never silently assign an old identity to a new object.

Membership is (type ID, identity), separate from allocation. One identity may have
multiple explicitly asserted memberships. Reference roles name an immutable target
definition; installation resolves that name into the fixed catalog contract. Physical
indexes use stable type UUIDs and immutable positional role IDs.

A module release is a new positive integer version with additive definitions only.
Imports pin exact installed version numbers; version n exports all earlier additions
in that module. Replacing a name, role shape, type meaning or constraint is rejected.
Optional attributes become new sparse families with a unique owner key when functional.
Only new rows have required roles. Existing-family transformations and backfills need
separate future APIs and cannot be mislabeled conservative installation.

JSON declarations and the native builder API are sufficient to exercise the developer
workflow now. UUIDs may be omitted in JSON and are allocated on load. Generated clients,
semantic version ranges, stable module UUIDs, and explicit incompatible-version adapters
are deferred. Relation definitions have stable IDs now; module identity is currently
the immutable namespace/version contract, a deliberate limit of this first catalog.
