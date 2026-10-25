# Bounded trusted-host replication

`replication::Source` and `Replica` implement an initial single-leader transfer
protocol. A trusted host can seed a read-only follower, send ordered incremental
packed redo, retry a packet after a lost acknowledgement, and reopen the follower.
Run `cargo run --offline --example replicated` for the complete local example.

This is an executable protocol slice, not a distributed service. Transfer
connections, authenticated peers, leader election, failover, consensus, online
replacement and distributed authorization remain unimplemented. The source and
follower working states and indexes still fit in RAM.

## Transfer contract

A source captures a consistent durable snapshot and assigns a fresh stream UUID.
The typed seed includes all schemas, allocated identity tombstones, records, grants,
schema grants, capabilities, credentials and durable view definitions. These bytes
are sensitive host data; never hand a seed or packet to an application merely
because it can read one relation. Export requires the privileged `Database` handle.

The source holds its stream cursor in memory. `next_packet` exports a retained
prefix of the actual committed redo records, preserving each transaction's source
revision. No application records are rewritten to export them. Calls are serialized
through `&mut Source`; retain returned bytes until the follower acknowledges them.
Export advances the source cursor, so retransmission uses the retained packet.
`None` means caught up at that capture, not that future writes have stopped.

The initial implementation limits seeds to 64 MiB and packets to 8 MiB and 128
transactions. A transaction that cannot fit a packet requires reseeding within the
seed bound. Capture briefly uses the existing writer coordinator to obtain a
consistent head or redo prefix. The published-state mutex is not held during disk
reads or seed encoding. Captured snapshots are immutable.

A source does not retain redo on behalf of followers. Automatic or manual leader
checkpoints can prune required history. Export then fails explicitly and the host
creates a fresh source and seeds a new follower file. Losing/restarting the Source
handle also requires a fresh stream and seed; durable leader stream identities,
resumable export acknowledgements, retention leases and incremental reseeding are
future work. Existing follower files are never silently reset or overwritten.

## Formats and validation

The envelope versions are explicit: LRS1 seeds, LRP1 packets and LRF1 follower
files. Seeds contain individually keyed existing checkpoint metadata/row records;
packets contain existing LQ01/LQ02 redo records. Adding check constraints therefore
uses the same versioned engine decoder as normal recovery. Counts and lengths are
checked before use, unsupported versions and trailing bytes fail, and seeds reject
duplicate keys. Schema, typed value, uniqueness, reference and private policy/view
validation all run before follower publication.

A cursor contains stream UUID, source revision and SHA-256 lineage digest. The seed
checksum anchors the stream. Every incremental record hashes its prior cursor,
revision and packed bytes; packet head digests must match the complete chain.
These checks detect corruption and divergence, not malicious authorship: hashes
are not signatures, and the transfer peer must be trusted and authenticated by the
host. Receivers reject wrong streams, gaps, out-of-order packets and divergent
bases. An intact packet ending at the current cursor is an idempotent duplicate,
including after restart; older historical packets are rejected once the follower
has advanced past them. There is no unbounded duplicate-history table.

## Persistence and recovery

The follower uses a separate redb file and exposes no writable `Session` or
`Database`. Each packet is decoded and replayed into a tentative state first. All
its accepted source revisions, packet bytes and final cursor are then written in
one Immediate redb transaction. Only after that succeeds is the new engine state
published and success returned. Packet members remain distinct source revisions,
but readers see only the previous or complete packet head. A malformed late record
cannot publish a valid prefix.

At 128 accumulated records or 8 MiB of packet bytes, the same atomic transaction
writes a complete follower checkpoint and prunes covered packets. Recovery verifies
the checkpoint/cursor binding and a strictly ordered bounded tail. This first
follower checkpoint implementation rewrites its full bounded seed; it does not yet
reuse the primary database's incremental checkpoint storage. A checkpoint exceeding
the seed bound is rejected before persistence and requires an operator decision.

A persistence failure has uncertain outcome: apply stops, retained restricted
reads fail closed, and the follower must reopen. Opening validates its durable
cursor against the recovered state. Crash tests exit before and after redb commit,
including compaction: reopening yields either the old packet head or the complete
new packet head, with the matching cursor. The API never acknowledges before
persistence. There is no automatic retransmission or recovery after a storage error.

## Read and authorization semantics

`Replica::snapshot_eventual(principal)` is a trusted host principal selection,
not authentication. `authenticate_eventual(token)` uses the replicated credential
registry, selected capabilities, expiry and current follower policy. Both return
read-only snapshots. Applied grant removal, capability revocation and credential
rotation invalidate retained follower snapshots/SQL through the existing live
result-release guards. Credentials and scopes never gain rights through replication.

**A lagging follower can still authorize a credential revoked on the leader.**
The eventual API names make this deliberate limitation visible. Expiry uses the
follower's existing wall-clock assumptions; no secure distributed clock is added.
Do not expose eventual reads to untrusted users when strict leader-current
revocation is required.

`snapshot_at_cursor(principal, cursor)` requires exact equality of stream,
revision and digest with the applied follower cursor. Older or newer barriers fail;
obtain and apply a fresh trusted cursor. This avoids claiming an ancestry proof
without retaining historical digests. The host obtains the barrier
from the current source after exporting through the desired leader revision.
This proves only that barrier, not that the leader has made no later revocations.
A strict distributed live-revocation protocol would need a leader authorization
lease/fence that spans result release; that protocol is not implemented here.

## Verification

The focused unit suite covers seed/index/query/identity parity, restart and duplicate
handling, gaps/divergence/wrong streams, malformed versions/checksums, atomic rejection
after a valid record prefix, checkpoint/tail bounds, pruned leader history, explicit
revocation lag and applied retained-read invalidation, scoped credentials/rotation,
subprocess crashes before/after persistence, and injected sync failure fail-stop.
The runnable example additionally demonstrates restart and a trusted read barrier.
No distributed throughput or high-availability claim is made from these tests.
