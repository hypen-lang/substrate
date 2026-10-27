use super::*;
use openraft::{
    RaftNetwork, RaftNetworkFactory,
    error::{RPCError, RaftError, RemoteError, Unreachable},
    network::RPCOption,
    raft::*,
};

#[derive(Serialize, Deserialize)]
pub(super) enum Request {
    Append(AppendEntriesRequest<Types>),
    Vote(VoteRequest<u64>),
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Envelope {
    pub cluster: crate::paged::PagedReplicationCursor,
    pub request: Request,
}
#[derive(Clone)]
pub(super) struct Network {
    pub transport: Arc<dyn AuthenticatedPeerTransport>,
    pub cluster: crate::paged::PagedReplicationCursor,
}
pub(super) struct Connection {
    transport: Arc<dyn AuthenticatedPeerTransport>,
    cluster: crate::paged::PagedReplicationCursor,
    target: u64,
}
impl RaftNetworkFactory<Types> for Network {
    type Network = Connection;
    async fn new_client(&mut self, target: u64, _node: &openraft::BasicNode) -> Self::Network {
        Connection {
            transport: self.transport.clone(),
            cluster: self.cluster,
            target,
        }
    }
}
fn unreachable<E: std::error::Error>(
    message: impl std::fmt::Display,
) -> RPCError<u64, openraft::BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
        message.to_string(),
    )))
}
impl Connection {
    async fn send<T: serde::de::DeserializeOwned>(
        &self,
        request: Request,
        ttl: Duration,
    ) -> Result<T, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        let bytes = serde_json::to_vec(&Envelope {
            cluster: self.cluster,
            request,
        })
        .map_err(unreachable)?;
        if bytes.len() > MAX_RPC_BYTES {
            return Err(unreachable("Raft RPC exceeds byte budget"));
        }
        let response = tokio::time::timeout(ttl, self.transport.request(self.target, bytes))
            .await
            .map_err(unreachable)?
            .map_err(unreachable)?;
        if response.len() > MAX_RPC_BYTES {
            return Err(unreachable("Raft response exceeds byte budget"));
        }
        let result: Result<T, RaftError<u64>> =
            serde_json::from_slice(&response).map_err(unreachable)?;
        result.map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}
impl RaftNetwork<Types> for Connection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Types>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
        self.send(Request::Append(rpc), option.hard_ttl()).await
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        self.send(Request::Vote(rpc), option.hard_ttl()).await
    }
    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<Types>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        Err(unreachable(
            "consensus snapshot transport is not implemented",
        ))
    }
}
