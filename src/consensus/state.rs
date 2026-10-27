// OpenRaft storage traits require its concrete StorageError; boxing here would
// change that upstream contract and add conversions throughout the adapter.
#![allow(clippy::result_large_err)]
use super::*;
use openraft::{
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership,
    storage::RaftStateMachine,
};
use storage::{blocking, io};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Marker {
    version: u8,
    pub last_applied: Option<openraft::LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
}
#[derive(Clone)]
pub(super) struct Machine {
    pub replica: Arc<PagedReplica>,
    // Declared after replica: the last storage handle drops before shutdown is
    // notified, including clones moved into blocking application jobs.
    pub _drained: Arc<tokio::sync::oneshot::Sender<()>>,
}
impl Machine {
    pub fn marker(&self) -> Result<Marker, StorageError<u64>> {
        match self.replica.cluster_marker().map_err(io)? {
            None => Ok(Marker {
                version: 1,
                ..Marker::default()
            }),
            Some(bytes) => {
                let marker: Marker = serde_json::from_slice(&bytes).map_err(io)?;
                if marker.version != 1 || marker.membership.log_id() > &marker.last_applied {
                    return Err(io("invalid Raft applied marker"));
                }
                Ok(marker)
            }
        }
    }
}
impl RaftSnapshotBuilder<Types> for Machine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<Types>, StorageError<u64>> {
        Err(io(
            "consensus snapshot export is not implemented; compaction is disabled",
        ))
    }
}
impl RaftStateMachine<Types> for Machine {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let machine = self.clone();
        blocking(move || {
            let marker = machine.marker()?;
            Ok((marker.last_applied, marker.membership))
        })
        .await
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Outcome>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut batch = Vec::new();
        let mut bytes = 0;
        for entry in entries {
            bytes += match &entry.payload {
                openraft::EntryPayload::Normal(command) => command.event.len(),
                _ => 256,
            };
            if bytes > MAX_RECOVERY_RAW_BYTES || batch.len() >= MAX_RECOVERY_ENTRIES {
                return Err(io("Raft apply batch exceeds memory budget"));
            }
            batch.push(entry);
        }
        let machine = self.clone();
        blocking(move || {
            let mut marker = machine.marker()?;
            let mut results = Vec::with_capacity(batch.len());
            for entry in batch {
                if entry.log_id.index
                    != marker
                        .last_applied
                        .map_or(0, |last| last.index.saturating_add(1))
                {
                    return Err(io("nonconsecutive Raft application"));
                }
                marker.last_applied = Some(entry.log_id);
                if let openraft::EntryPayload::Membership(membership) = &entry.payload {
                    marker.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                }
                let encoded = serde_json::to_vec(&marker).map_err(io)?;
                let event = match &entry.payload {
                    openraft::EntryPayload::Normal(command) => Some(command.event.as_slice()),
                    _ => None,
                };
                let outcome = match machine.replica.apply_consensus(event, &encoded) {
                    Ok(()) => Outcome::Applied(machine.replica.cursor().map_err(io)?.revision),
                    Err(crate::Error::Conflict) => {
                        machine
                            .replica
                            .apply_consensus(None, &encoded)
                            .map_err(io)?;
                        Outcome::Conflict
                    }
                    Err(error) => return Err(io(error)),
                };
                results.push(outcome);
            }
            Ok(results)
        })
        .await
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        Err(io("consensus snapshot installation is not implemented"))
    }
    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, openraft::BasicNode>,
        _snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        Err(io("consensus snapshot installation is not implemented"))
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<Types>>, StorageError<u64>> {
        Ok(None)
    }
}
