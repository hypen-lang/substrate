# Optional paged consensus

The `consensus` Cargo feature integrates pinned OpenRaft 0.9.25 with the paged engine. OpenRaft supplies terms, elections, quorum replication and committed membership. This is separate from the asynchronous paged follower API.

A leader obtains a quorum read barrier, stages an authorized paged transaction and prepares a versioned event **without publishing or committing it locally**. OpenRaft persists and replicates that proposal. Only a committed entry reaches the state machine. Applying it writes rows, indexes, catalog, replication cursor and the Raft applied-log/membership marker in one Immediate redb transaction. The caller receives success only after quorum replication and local application. Rejected stale proposals still advance the applied marker without changing database state.

Each node also has a separate durable redb Raft file containing its votes, log and committed pointer. Node identity, the complete bootstrap cursor (source UUID, revision and digest), and trusted peer map are pinned to that file. Opening audits table identity, contiguous log keys and byte accounting before accepting the store. A leader read runs through a restricted callback returning owned values, between OpenRaft linearizable barriers, followed by a local authorization fence. The API does not expose retained snapshots or executable plans. An isolated former leader cannot acknowledge a write or serve a new barrier read.

The Rust `ConsensusNode` API accepts trusted embedding principals. It preserves schema/row permissions and ordered revocation. `transact_admin` is a host operation; it must not be exposed to an unauthenticated caller. Application-token transport, distributed credential expiry decisions and deployment management are not implemented by this API.

Hosts provide `AuthenticatedPeerTransport`. It must authenticate the target node and deliver a verified peer identity to `handle_rpc`; the endpoint additionally checks the pinned peer set, cluster lineage, claimed voting/leader identity and byte/admission bounds. The test transport is an in-process identity-bound router with partition controls, not a production authenticated remote transport.

Current limits are deliberately explicit:

- Static configured membership, at most 16 nodes. Bootstrap one fresh node with the identical peer map used by every member.
- Transactions at most 256 KiB before Raft JSON encoding; bounded append and RPC batches. OpenRaft recovery batches are capped at 64 entries (64 MiB serialized-log budget and about 16 MiB raw application payload).
- Configured durable log byte cap. Snapshot transfer/compaction is not implemented; reaching the cap stops the node instead of silently growing storage. Logs are disk-backed; startup verifies them in a streaming audit and replays bounded OpenRaft recovery batches.
- No automatic retry after a quorum timeout. A timed-out proposal may still commit; the caller must resolve its outcome before retrying non-idempotent work.
- Existing standalone and asynchronous follower APIs remain the default. Enabling the Cargo feature alone does not start a cluster.

`shutdown().await` waits for the Raft state-machine and blocking storage workers
to release their handles. Before reopening the files, the host must also drain its
own request tasks and drop every node handle. This lifecycle boundary is separate
from resolving uncertain outcomes of requests interrupted during shutdown.

The integration follows the upstream [storage contracts](https://docs.rs/openraft/0.9.25/openraft/docs/getting_started/index.html) and [linearizable read protocol](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/read/index.html). It does not implement a separate homegrown election protocol.

Run `cargo run --features consensus --example consensus_failover` for a local three-node demonstration. It commits through a quorum, partitions the leader, rejects that former leader's write, elects a majority leader, and updates the same stable identity. `cargo test --features consensus --test consensus` also covers abrupt process exit after acknowledgement, durable restart, majority loss, ordered revocation, bootstrap mismatch, log-cap exhaustion, and recovery of a large committed backlog.
