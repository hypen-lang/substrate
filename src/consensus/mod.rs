//! Optional OpenRaft consensus over the paged state machine. Success follows
//! durable quorum replication AND local state application. Hosts supply an
//! authenticated, bounded peer transport; this module is not a remote server.
//! No snapshot compaction yet: the durable log has a hard configured byte cap.
mod network;
mod state;
mod storage;
use crate::{
    Id,
    paged::{PagedReplica, PagedSnapshot, PagedTransaction},
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

const MAX_COMMAND_BYTES: usize = 256 * 1024;
const MAX_ENTRY_BYTES: usize = 1024 * 1024;
const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;
// OpenRaft 0.9.25 startup recovery requests fixed batches of 64 log entries.
const MAX_RECOVERY_ENTRIES: usize = 64;
const MAX_RECOVERY_RAW_BYTES: usize = MAX_RECOVERY_ENTRIES * (MAX_COMMAND_BYTES + 1024);
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    event: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Outcome {
    Applied(u64),
    Conflict,
}
openraft::declare_raft_types!(pub Types: D = Command, R = Outcome, NodeId = u64, Node = BasicNode, SnapshotData = std::io::Cursor<Vec<u8>>);
type Entry = openraft::Entry<Types>;
fn error(message: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage(format!("consensus: {message}"))
}

/// Each implementation is bound to one authenticated local node identity. It
/// MUST authenticate the remote peer as `target`, bound received bodies before
/// allocation, and deliver its own authenticated identity to `handle_rpc`.
/// Application credentials must never be accepted as cluster peer credentials.
#[async_trait::async_trait]
pub trait AuthenticatedPeerTransport: Send + Sync + 'static {
    async fn request(&self, target: u64, request: Vec<u8>) -> Result<Vec<u8>, String>;
}
#[derive(Clone, Copy, Debug)]
pub struct ConsensusOptions {
    pub max_log_bytes: u64,
    pub heartbeat_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub request_timeout: Duration,
}
impl Default for ConsensusOptions {
    fn default() -> Self {
        Self {
            max_log_bytes: 256 * 1024 * 1024,
            heartbeat_ms: 100,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            request_timeout: Duration::from_secs(5),
        }
    }
}
/// Trusted embedding API. Static peer identities are pinned when opening; dynamic
/// membership, authenticated application-token writes, and snapshots are not yet
/// exposed. Direct writes through the underlying replica remain disabled.
pub struct ConsensusNode {
    id: u64,
    cluster: crate::paged::PagedReplicationCursor,
    peers: BTreeMap<u64, BasicNode>,
    raft: openraft::Raft<Types>,
    replica: Arc<PagedReplica>,
    proposal: tokio::sync::Mutex<()>,
    inbound: tokio::sync::Semaphore,
    storage_drained: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    options: ConsensusOptions,
}
impl ConsensusNode {
    pub async fn open(
        id: u64,
        replica: PagedReplica,
        log_path: impl AsRef<Path>,
        peers: BTreeMap<u64, BasicNode>,
        transport: Arc<dyn AuthenticatedPeerTransport>,
        options: ConsensusOptions,
    ) -> crate::Result<Self> {
        if peers.is_empty()
            || peers.len() > 16
            || !peers.contains_key(&id)
            || peers.values().any(|peer| peer.addr.len() > 2048)
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&options.max_log_bytes)
            || options.request_timeout.is_zero()
            || options.request_timeout > Duration::from_secs(60)
        {
            return Err(error("invalid consensus limits or peer set"));
        }
        let cursor = replica.cursor()?;
        let (mut log, cluster) =
            storage::LogStore::open(log_path.as_ref(), id, cursor, &peers, options.max_log_bytes)?;
        let (drained, storage_drained) = tokio::sync::oneshot::channel();
        let drained = Arc::new(drained);
        log.drained = Some(drained.clone());
        let replica = Arc::new(replica);
        let machine = state::Machine {
            replica: replica.clone(),
            _drained: drained,
        };
        let marker = machine.marker().map_err(error)?;
        if marker.last_applied.is_none() && cursor != cluster {
            return Err(error("replica state differs from genesis"));
        }
        log.check_applied(marker.last_applied)?;
        let config = openraft::Config {
            cluster_name: format!("{}:{}", cluster.source, cluster.revision),
            heartbeat_interval: options.heartbeat_ms,
            election_timeout_min: options.election_timeout_min_ms,
            election_timeout_max: options.election_timeout_max_ms,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_payload_entries: 4,
            ..Default::default()
        }
        .validate()
        .map_err(error)?;
        let raft = openraft::Raft::new(
            id,
            Arc::new(config),
            network::Network { transport, cluster },
            log,
            machine,
        )
        .await
        .map_err(error)?;
        Ok(Self {
            id,
            cluster,
            peers,
            raft,
            replica,
            proposal: tokio::sync::Mutex::new(()),
            inbound: tokio::sync::Semaphore::new(32),
            storage_drained: tokio::sync::Mutex::new(Some(storage_drained)),
            options,
        })
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }
    /// Host diagnostic for local state-machine progress. This is not a quorum
    /// read barrier and must not be used to authorize application reads.
    pub fn last_applied_log_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_applied.map(|id| id.index)
    }
    /// Call on one fresh node only, using the identical peer set on all nodes.
    pub async fn initialize(&self) -> crate::Result<()> {
        self.raft
            .initialize(self.peers.clone())
            .await
            .map_err(error)
    }
    /// Stop Raft and wait for its state-machine and blocking storage workers.
    /// Before reopening the files, hosts must also drain their own request
    /// tasks and drop all node handles (including this one).
    pub async fn shutdown(&self) -> crate::Result<()> {
        self.raft.shutdown().await.map_err(error)?;
        // OpenRaft waits for its core and ticker, but the state-machine worker
        // and blocking storage jobs can outlive that core. Await their actual
        // handle drops before callers close/reopen these database files.
        let mut drained = self.storage_drained.lock().await;
        if let Some(receiver) = drained.as_mut() {
            let _ = receiver.await;
            *drained = None;
        }
        Ok(())
    }
    /// Execute an owned-result read between quorum barriers. The wrapper cannot
    /// yield retained snapshot/plan handles. Keep the closure free of side effects:
    /// a final authority/leadership failure discards its result.
    pub async fn read<T>(
        &self,
        principal: &str,
        query: impl FnOnce(&ConsensusRead) -> crate::Result<T>,
    ) -> crate::Result<T> {
        self.barrier().await?;
        let context = ConsensusRead {
            snapshot: self.replica.snapshot_eventual(principal)?,
        };
        let result = query(&context)?;
        self.barrier().await?;
        context.snapshot.fence()?;
        Ok(result)
    }
    async fn barrier(&self) -> crate::Result<()> {
        tokio::time::timeout(
            self.options.request_timeout,
            self.raft.ensure_linearizable(),
        )
        .await
        .map_err(|_| error("read quorum timeout"))?
        .map_err(error)?;
        Ok(())
    }
    /// Closure runs only after a quorum read barrier, with the caller's existing
    /// principal permissions. A timeout means the outcome may be unknown: the
    /// proposed transaction can still commit; do not blindly retry side effects.
    pub async fn transact<T>(
        &self,
        principal: &str,
        build: impl FnOnce(&mut PagedTransaction) -> crate::Result<T>,
    ) -> crate::Result<(T, u64)> {
        self.write(Some(principal), build).await
    }
    /// Host-only schema/bootstrap/control operation; never expose directly to an
    /// unauthenticated remote caller.
    pub async fn transact_admin<T>(
        &self,
        build: impl FnOnce(&mut PagedTransaction) -> crate::Result<T>,
    ) -> crate::Result<(T, u64)> {
        self.write(None, build).await
    }
    async fn write<T>(
        &self,
        principal: Option<&str>,
        build: impl FnOnce(&mut PagedTransaction) -> crate::Result<T>,
    ) -> crate::Result<(T, u64)> {
        let _admission = self
            .proposal
            .try_lock()
            .map_err(|_| crate::Error::Conflict)?;
        tokio::time::timeout(
            self.options.request_timeout,
            self.raft.ensure_linearizable(),
        )
        .await
        .map_err(|_| error("proposal quorum timeout"))?
        .map_err(error)?;
        let mut tx = match principal {
            Some(p) => self.replica.consensus_transaction(p)?,
            None => self.replica.consensus_admin_transaction()?,
        };
        let value = build(&mut tx)?;
        let event = tx.replication_proposal()?;
        if event.len() > MAX_COMMAND_BYTES {
            return Err(crate::Error::Constraint(
                "consensus transaction exceeds 256 KiB".into(),
            ));
        }
        let response = tokio::time::timeout(
            self.options.request_timeout,
            self.raft.client_write(Command { event }),
        )
        .await
        .map_err(|_| error("commit quorum timeout; transaction outcome unknown"))?
        .map_err(error)?;
        match response.data {
            Outcome::Applied(revision) => Ok((value, revision)),
            Outcome::Conflict => Err(crate::Error::Conflict),
        }
    }
    /// Host peer endpoint. `authenticated_peer` must come from the transport's
    /// authenticated session, never a caller-supplied JSON header.
    pub async fn handle_rpc(&self, authenticated_peer: u64, body: &[u8]) -> crate::Result<Vec<u8>> {
        if !self.peers.contains_key(&authenticated_peer) || body.len() > MAX_RPC_BYTES {
            return Err(crate::Error::Denied);
        }
        let _admission = self
            .inbound
            .try_acquire()
            .map_err(|_| error("peer RPC admission full"))?;
        let envelope: network::Envelope = serde_json::from_slice(body).map_err(error)?;
        if envelope.cluster != self.cluster {
            return Err(crate::Error::Denied);
        }
        match envelope.request {
            network::Request::Append(request) => {
                if request.vote.leader_id.voted_for() != Some(authenticated_peer)
                    || request.entries.len() > 4
                {
                    return Err(crate::Error::Denied);
                }
                for entry in &request.entries {
                    if let openraft::EntryPayload::Normal(command) = &entry.payload
                        && command.event.len() > MAX_COMMAND_BYTES
                    {
                        return Err(crate::Error::Denied);
                    }
                }
                serde_json::to_vec(&self.raft.append_entries(request).await).map_err(error)
            }
            network::Request::Vote(request) => {
                if request.vote.leader_id.voted_for() != Some(authenticated_peer) {
                    return Err(crate::Error::Denied);
                }
                serde_json::to_vec(&self.raft.vote(request).await).map_err(error)
            }
        }
    }
}

/// Restricted synchronous view used only inside a quorum-fenced read closure.
/// Methods return owned values; no local snapshot or executable SQL plan escapes.
pub struct ConsensusRead {
    snapshot: PagedSnapshot,
}
impl ConsensusRead {
    pub fn get(&self, relation: &str, id: Id) -> crate::Result<Option<crate::Row>> {
        self.snapshot.get(relation, id)
    }
    pub fn scan_page(
        &self,
        relation: &str,
        after: Option<Id>,
        limit: usize,
    ) -> crate::Result<crate::paged::PagedPage> {
        self.snapshot.scan_page(relation, after, limit)
    }
    pub fn lookup_page(
        &self,
        relation: &str,
        role: &str,
        value: &crate::Value,
        after: Option<Id>,
        limit: usize,
    ) -> crate::Result<crate::paged::PagedPage> {
        self.snapshot
            .lookup_page(relation, role, value, after, limit)
    }
}
