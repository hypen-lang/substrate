# Fixed local partition routing

`partition::PartitionRouter` is an executable, host-provisioned routing layer over
multiple single-node databases. It supports partition-affine applications whose
entities and relationship occurrences can be kept together. It is not a distributed
database, consensus system, or a global transaction layer.

Run the example with `cargo run --offline --example partitioned`. It provisions two
redb files, installs the same schema and local grants, commits an application write,
rejects a cross-partition transaction, then reopens the original identity using the
same routing manifest. `cargo test --offline --test partition` checks the contract.

## Routing and identities

A validated format-1 `RoutingMap` has a stable UUID and an ordered list of 1–256
unique partition UUIDs. The count must be a power of two. Low bits of the first UUID
byte select the slot; this algorithm does not depend on Rust hashing or process
state. Generated local identities remain UUID v4: allocation sets the routing bits
in byte zero and leaves the version and variant bits untouched. UUIDs retain at
least 114 random bits with 256 partitions. Existing identities are never rewritten;
their existing UUID chooses their partition. Schema IDs are not routed.

`to_json` and `from_json` persist and validate the declaration. `fingerprint` hashes
its canonical representation; `check_fingerprint` checks a trusted host pin. The
host **must store and check the pin before binding** and retain the original slot
order and count. The router does not persist topology in database files or prevent
a host from intentionally using a different map. Changing slots, count, or ownership
requires a future migration protocol; it is not supported online.

## Operations and authorization

A trusted host binds exactly one already-authorized `Session` per partition and
hands applications only the router or its partition sessions. The host provisions
schemas and grants separately, preferably using identical schema declarations and
IDs across files. Binding does not authenticate a user or grant access. Capability-
and credential-bound sessions retain their existing attenuation, expiry, rotation,
and live-revocation behavior because every operation delegates to those sessions.

A partition transaction has one immutable destination and one atomic local outcome.
Insert, explicit-ID insert, membership extension, update, delete, point read, scan,
and equality lookup are supported. IDs and all `Value::Ref` values must route to the
selected partition. The engine still checks reference existence/type, constraints,
row permissions, and OCC. A routing or engine operation error poisons the wrapper:
ignoring an error cannot commit earlier staged mutations. `transaction_for` rejects
an empty identity set or identities assigned to different partitions.

Wrapped reads check returned row identities and references, rejecting a result if
trusted host code previously placed data in the wrong partition. They do not scan
or repair the whole database. A snapshot pins only its local database revision;
there is no combined snapshot or comparable revision across databases. No raw
session/transaction escape or arbitrary SQL endpoint is exposed by the wrapper.

The binding is a trusted host boundary. Use distinct physical database files per
partition; this API validates binding names and completeness, but cannot identify
two Session values backed by the same file. Keep raw host database handles private.
A shared identity must have all directly referencing relation occurrences in its
partition. Global lookups or cross-partition references are explicitly unsupported.

## Remaining work

There is no automatic tenant assignment, remote shard transport, persisted routing
ownership, placement discovery, rebalancing, global uniqueness/indexes, global
snapshot, cross-partition join/transaction/reference, failover, or distributed live
revocation. Tenant isolation is still enforced by normal grants and row policies;
a partition is a placement decision, not itself a permission grant. Independent
partitions can commit concurrently to separate files, but each database retains its
own redb writer serialization and durability semantics. This slice does not claim
a measured distributed throughput improvement.
