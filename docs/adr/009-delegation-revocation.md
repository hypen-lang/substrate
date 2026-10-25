# 009 — Principal-bound attenuation and live result gates

Accepted for the local single-host engine. Supersedes ADR 004's retained-policy
revocation limitation; its row/reference semantics remain in force.

Use explicit host-issued roots and principal-bound child records. Selection replaces
ambient permissions; do not infer delegation from ordinary grants or union multiple
sources. Rights, exact relation IDs, equality predicates, one namespace and strictly
decreasing depth define a small decidable attenuation relation. Host provisioning
and approval remain outside the public protocol. New schema grants belong to the
owner, never a wildcard expansion of existing delegated scopes.

Store immutable versioned scope declarations and revoked flags in a reserved typed
family. Compile/cache them at publication/recovery; index roots by source grant.
Keep ancestor revocation as a bounded chain check instead of a descendant rewrite.
Invalidate affected roots atomically on direct grant replacement/revocation, so a
later regrant cannot resurrect a prior capability. Namespace generations provide
the same permanent invalidation for namespace authority.

Keep snapshot data and pinned effective grants together. Use live dependency gates
to invalidate stale authority instead of combining historical data with current
row predicates. Check at read entry/result release and write commit. SQL progress
hooks provide cooperative cancellation; native traversal uses result suppression.
Read authorization linearizes at the final gate, not client packet receipt. Already
returned copies and buffers authorized before concurrent revocation cannot be
recalled. Administrator snapshots remain privileged. Weak host references avoid
keeping database files open solely because an old snapshot exists.

This trades some handle availability for simple, auditable semantics: any direct
principal grant replacement/removal invalidates its retained handles, even if a
particular query used another grant. New grants do not invalidate old readers.
Selected sibling capabilities remain independent unless their common ancestor or
source grant is revoked. Cached descriptors remain data, not authorization.

Scope syntax remains independent of the semantic model. Exact integer policy values
use the existing tagged string wire codec. No new redo ABI, mandatory backfill,
distributed infrastructure or remote authentication protocol is introduced.
