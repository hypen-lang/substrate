# 008 — Durable scoped proposals and runtime schema clients

Accepted for the private local prototype, 2026-10-24.

Keep administrative authority at the host Database boundary. A namespace policy
assigns an immutable owner, quotas, enabled state and explicit automatic-approval
mode. Ordinary authenticated Sessions can propose, inspect their own receipts,
cancel and apply; they cannot provision policies or approve pending requests.

Represent governance as four reserved typed relation families with fixed schema IDs.
This uses existing atomic transactions, indexes, compiled validation, versioned redo
and checkpoints rather than introducing a parallel metadata database or rewriting
old log formats. Private grants prevent application access to those families. A
versioned declaration plan is stored for review; grants and quotas are never executed
from arbitrary JSON. Metadata lookups use relation indexes, not log scans.

Approval binds principal, policy generation and canonical module. Applying stages
schemas, grants for new families and a durable receipt in one transaction. Repeated
requests use the same proposal and schema IDs; installed receipts reconcile lost
acknowledgements. They are not bearer capabilities and cannot expand authorization.
All relevant catalog/policy/quota/lifecycle/alias reads participate in OCC. Managed
namespaces cannot bypass this path using older broad native namespace grants, and
namespace enrollment conflicts with an already-staged legacy install.

Do not implicitly migrate versions or rename tables. Exact successive module
versions add independent families. Aliases resolve only at descriptor discovery;
lifecycle is monotonic metadata with non-destructive retirement. Existing data and
queries retain their semantics. Retired families cannot gain new references through
the managed proposal API. Host raw administrative APIs remain privileged escape
hatches and require trusted use.

Compile discovered scalar metadata into the existing Rust/TypeScript descriptor
paths. Runtime clients validate unknown field shapes; they cannot offer the static
field-name inference of generated types. Both interfaces use the same core schema,
constraints, reference closure, authorization, SQL provider and transaction machinery.

No new public administrative service, delegation language or distributed machinery
is introduced. The current local trust boundary remains explicit. Hard schema and
proposal bounds limit proliferation, but are not comprehensive data/CPU/memory quotas.
Benchmarks separate the three durable control commits from raw SQLite DDL because
they do not implement equivalent security workflows.
