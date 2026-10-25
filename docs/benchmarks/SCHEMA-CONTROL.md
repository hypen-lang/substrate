# Online additive schema-control measurements

Run:

```sh
cargo bench --offline --bench schema_install > schema-control.json
```

This focused benchmark measures the new durable schema-control workflow against
10 and 2,000 existing `core.Person` records. Each fixture installs eight fresh
single-family versions in one owned namespace, excluding the first as warmup.
The seven measured operations are additive installs, not repeated idempotent
application of an existing receipt. Every version references an existing Person,
has a unique `person` role and an integer `value` role, and receives one new row.
A concurrent thread continuously queries one pinned pre-install snapshot.

The Substrate phases are measured separately:

1. Submit and durably record a validated, quota-checked proposal.
2. Record explicit host approval of its exact digest.
3. Apply the approved module with owner grants and installed receipt atomically.
4. Discover its runtime descriptor, with no code generation or restart.
5. Commit the first named-field runtime-descriptor write.
6. Read and validate that row through the descriptor.

Proposal, approval, and apply each commit separately. Their sum is the reported
pipeline time. Schema installation, grants, and the installation receipt share
the final commit. The fixture uses explicit approval rather than auto-approval.
Host namespace provisioning and population are outside the timed samples.

The SQLite reference uses a durable WAL database, `synchronous=FULL`, foreign keys,
primary and unique keys, and a scalar index. It creates an independent table and
index in one transaction; then reads metadata with `PRAGMA table_info`, inserts one
row, and reads it. Another connection holds a pre-DDL read transaction throughout.
This is an additive-storage reference, **not an equivalent security workflow**:
SQLite does not submit proposals, enforce namespace ownership/quotas, approve a
plan, generate grants, or persist an installation receipt in this harness.

## Correctness checks

Both implementations compare all original IDs/names, sorted by ID, against a
shared deterministic reference before and after the entire run. Their SHA-256
query digests must match. Each new occurrence's reference and value are checked.
Pinned readers must continue returning the original person and must not see new
schema definitions. Every run asserts at least one successful concurrent query;
actual counts are recorded. Substrate additionally verifies its indexes, confirms
the pinned original relation still matches the fresh snapshot, and requires each
plan to report zero existing-record rewrites.

The public benchmark verifies logical records and query outputs. It does **not**
inspect private packed buffers. The separate kernel test
`approved_install_preserves_old_arcs_and_exact_packed_bytes` checks byte-for-byte
packed encoding and shared row/schema allocations. Crash recovery, conflict races,
capability denial, and revocation are tested separately by the parent suite; this
benchmark is not a replacement for those tests.

## Results — 2026-10-24

One development-machine run, arm64 macOS, SQLite 3.46.0, release Rust build.
[Raw measurements](2026-10-24-schema-control-run.json) include every sample, setup
costs, concurrent-reader observations, and original-query digests.

Median of seven fresh installs, **milliseconds**:

| Existing people | Substrate proposal | Host approval | Atomic apply | Complete pipeline | SQLite table + index DDL |
|---:|---:|---:|---:|---:|---:|
| 10 | 8.062 | 7.990 | 8.021 | 24.071 | 0.169 |
| 2,000 | 7.962 | 7.023 | 7.940 | 20.156 | 0.283 |

Descriptor and first-operation medians, **microseconds**:

| Existing people | Substrate describe | First durable write | First read | SQLite metadata | First durable write | First read |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 7.083 | 7,979.875 | 3.541 | 7.958 | 3,236.291 | 11.375 |
| 2,000 | 6.583 | 7,117.791 | 2.292 | 6.584 | 161.333 | 13.708 |

Observed successful pinned-reader queries over the **whole fixture**, including
warmup and first writes:

| Existing people | Substrate reader queries | Maximum observed query, µs | SQLite reader queries | Maximum observed query, µs |
|---:|---:|---:|---:|---:|
| 10 | 347,901 | 52.167 | 100,206 | 24.833 |
| 2,000 | 337,872 | 47.416 | 69,895 | 15.083 |

The reader loops run for different total durations, so counts are evidence of
progress, **not comparable throughput**. Maximum observed timings include OS
scheduling and are not latency bounds. Each reader is pinned to the original
snapshot; current-snapshot visibility and online conflicts are covered separately.

## Interpretation and boundaries

The observed installs preserved all 2,000 preexisting records and query results,
and the concurrent readers continued to operate. Increasing this small fixture
from 10 to 2,000 records did not produce a proportional installation cost. This is
consistent with additive relation families; it is not proof of constant-time
installation at large schema/catalog sizes. Across a fixture the catalog grows
from one to eight extension families, and the control ledger grows too.

These runs include real persistence: Substrate uses redb immediate durability;
SQLite uses WAL with FULL synchronous mode. The implementations have different
commit counts, WAL/page behavior, and platform flush paths. No attempt was made to
equate power-loss guarantees or filesystem cache conditions. Write timings vary
substantially (see raw min/max); the small run's slower median does not mean larger
databases are faster. Neither fixture crossed Substrate's default checkpoint
threshold: the final log contained 34 commits and checkpoint revision remained 0.
Consequently this run measures durable commits without checkpoint rewriting cost.

Setup is reported independently and includes each engine's database creation,
population, and metadata initialization; it is not normalized ingestion throughput.
Runtime first writes require no generated code, but this embedded Rust measurement
does not include a socket request, TypeScript validation, or an agent approval UI.
It does not measure competing installers, parallel write execution, arbitrary
ReBAC, delegation, or strict live revocation.

PostgreSQL installation was **not measured in this focused benchmark**. PostgreSQL
was run in the separate [three-engine query suite](COMPARISON.md); those read
measurements say nothing about its DDL or a hypothetical equivalent control plane.
The present baseline is deliberately limited to Substrate's completed workflow
and SQLite's additive physical operation, with their differing semantics explicit.

These recorded timings precede the scoped delegation/live-gate milestone. They
do not measure its authorization overhead; see [security semantics](../SECURITY.md).
