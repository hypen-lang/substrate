// OpenRaft storage traits require its concrete StorageError; boxing here would
// change that upstream contract and add conversions throughout the adapter.
#![allow(clippy::result_large_err)]
use super::*;
use openraft::{
    LogState, RaftLogReader, StorageError, StorageIOError,
    storage::{LogFlushed, RaftLogStorage},
};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use std::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log_v1");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta_v1");
const READ_BUDGET: usize = MAX_RECOVERY_ENTRIES * MAX_ENTRY_BYTES;
pub(super) fn io(error: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::write(openraft::AnyError::error(std::io::Error::other(
        error.to_string(),
    )))
    .into()
}
pub(super) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, StorageError<u64>> + Send + 'static,
) -> Result<T, StorageError<u64>> {
    tokio::task::spawn_blocking(f).await.map_err(io)?
}
#[derive(Clone)]
pub(super) struct LogStore {
    db: Arc<redb::Database>,
    max_bytes: u64,
    pub drained: Option<Arc<tokio::sync::oneshot::Sender<()>>>,
}
impl LogStore {
    pub fn open(
        path: &Path,
        id: u64,
        cursor: crate::paged::PagedReplicationCursor,
        peers: &BTreeMap<u64, BasicNode>,
        max_bytes: u64,
    ) -> crate::Result<(Self, crate::paged::PagedReplicationCursor)> {
        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Identity {
            version: u8,
            node: u64,
            genesis: crate::paged::PagedReplicationCursor,
            peers: BTreeMap<u64, BasicNode>,
        }
        let db = redb::Database::builder()
            .set_cache_size(4 * 1024 * 1024)
            .create(path)
            .map_err(error)?;
        let read = db.begin_read().map_err(error)?;
        let tables = read
            .list_tables()
            .map_err(error)?
            .map(|t| t.name().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        if read.list_multimap_tables().map_err(error)?.next().is_some() {
            return Err(error("foreign multimap tables in Raft file"));
        }
        let expected = ["raft_log_v1".to_owned(), "raft_meta_v1".to_owned()]
            .into_iter()
            .collect();
        let genesis;
        if tables.is_empty() {
            drop(read);
            let identity = Identity {
                version: 1,
                node: id,
                genesis: cursor,
                peers: peers.clone(),
            };
            let bytes = serde_json::to_vec(&identity).map_err(error)?;
            let tx = db.begin_write().map_err(error)?;
            {
                let mut meta = tx.open_table(META).map_err(error)?;
                meta.insert("identity", bytes.as_slice()).map_err(error)?;
                meta.insert("bytes", b"0".as_slice()).map_err(error)?;
                tx.open_table(LOG).map_err(error)?;
            }
            tx.commit().map_err(error)?;
            genesis = cursor;
        } else {
            if tables != expected {
                return Err(error("unexpected Raft file tables"));
            }
            let meta = read.open_table(META).map_err(error)?;
            let bytes = meta
                .get("identity")
                .map_err(error)?
                .ok_or_else(|| error("missing Raft identity"))?;
            if bytes.value().len() > 64 * 1024 {
                return Err(error("oversized Raft identity"));
            }
            let identity: Identity = serde_json::from_slice(bytes.value()).map_err(error)?;
            if identity.version != 1
                || identity.node != id
                || identity.genesis.source != cursor.source
                || &identity.peers != peers
            {
                return Err(error("Raft identity, lineage or trusted peers differ"));
            }
            genesis = identity.genesis;
            let expected_bytes: u64 = serde_json::from_slice(
                meta.get("bytes")
                    .map_err(error)?
                    .ok_or_else(|| error("missing Raft byte accounting"))?
                    .value(),
            )
            .map_err(error)?;
            if expected_bytes > max_bytes {
                return Err(error("existing Raft log exceeds configured byte budget"));
            }
            let log = read.open_table(LOG).map_err(error)?;
            let mut total = 0u64;
            let mut expected_index = 0u64;
            for entry in log.iter().map_err(error)? {
                let (key, bytes) = entry.map_err(error)?;
                if bytes.value().len() > MAX_ENTRY_BYTES || key.value() != expected_index {
                    return Err(error("Raft log hole or oversized entry"));
                }
                let entry: Entry = serde_json::from_slice(bytes.value()).map_err(error)?;
                if entry.log_id.index != key.value() {
                    return Err(error("Raft log key mismatch"));
                }
                total = total
                    .checked_add(bytes.value().len() as u64)
                    .ok_or_else(|| error("Raft log accounting overflow"))?;
                expected_index = expected_index
                    .checked_add(1)
                    .ok_or_else(|| error("Raft log index exhausted"))?;
            }
            if total != expected_bytes {
                return Err(error("Raft log byte accounting mismatch"));
            }
            drop(meta);
            drop(read);
        }
        Ok((
            Self {
                db: Arc::new(db),
                max_bytes,
                drained: None,
            },
            genesis,
        ))
    }
    pub fn check_applied(&self, last: Option<openraft::LogId<u64>>) -> crate::Result<()> {
        if let Some(last) = last {
            let entries = self
                .read_entries(
                    (Bound::Included(last.index), Bound::Included(last.index)),
                    false,
                )
                .map_err(error)?;
            if entries.len() != 1 || entries[0].log_id != last {
                return Err(error("applied marker has no matching durable Raft log"));
            }
        }
        Ok(())
    }
    fn read<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError<u64>> {
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(META).map_err(io)?;
        table
            .get(key)
            .map_err(io)?
            .map(|value| {
                if value.value().len() > 64 * 1024 {
                    return Err(io("oversized Raft metadata"));
                }
                serde_json::from_slice(value.value()).map_err(io)
            })
            .transpose()
    }
    fn save<T: Serialize>(&self, key: &str, value: &T) -> Result<(), StorageError<u64>> {
        let bytes = serde_json::to_vec(value).map_err(io)?;
        let tx = self.db.begin_write().map_err(io)?;
        tx.open_table(META)
            .map_err(io)?
            .insert(key, bytes.as_slice())
            .map_err(io)?;
        tx.commit().map_err(io)
    }
    fn read_entries(
        &self,
        range: (Bound<u64>, Bound<u64>),
        limited: bool,
    ) -> Result<Vec<Entry>, StorageError<u64>> {
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(LOG).map_err(io)?;
        let mut output = Vec::new();
        let mut bytes = 0usize;
        for next in table.range(range).map_err(io)? {
            let (index, value) = next.map_err(io)?;
            if limited
                && !output.is_empty()
                && (bytes + value.value().len() > MAX_RPC_BYTES / 2 || output.len() >= 4)
            {
                break;
            }
            bytes = bytes
                .checked_add(value.value().len())
                .ok_or_else(|| io("log read overflow"))?;
            if bytes > READ_BUDGET {
                return Err(io("Raft log read exceeds configured memory budget"));
            }
            let entry: Entry = serde_json::from_slice(value.value()).map_err(io)?;
            if entry.log_id.index != index.value() {
                return Err(io("Raft log key mismatch"));
            }
            output.push(entry);
        }
        Ok(output)
    }
}
impl RaftLogReader<Types> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<u64>> {
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        let store = self.clone();
        blocking(move || store.read_entries(bounds, false)).await
    }
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, StorageError<u64>> {
        let store = self.clone();
        blocking(move || store.read_entries((Bound::Included(start), Bound::Excluded(end)), true))
            .await
    }
}
impl RaftLogStorage<Types> for LogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<Types>, StorageError<u64>> {
        let store = self.clone();
        blocking(move || {
            let tx = store.db.begin_read().map_err(io)?;
            let table = tx.open_table(LOG).map_err(io)?;
            let last = table
                .last()
                .map_err(io)?
                .map(|(_, value)| {
                    serde_json::from_slice::<Entry>(value.value())
                        .map(|e| e.log_id)
                        .map_err(io)
                })
                .transpose()?;
            Ok(LogState {
                last_purged_log_id: None,
                last_log_id: last,
            })
        })
        .await
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        let store = self.clone();
        let vote = *vote;
        blocking(move || store.save("vote", &vote)).await
    }
    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        let store = self.clone();
        blocking(move || store.read("vote")).await
    }
    async fn save_committed(
        &mut self,
        committed: Option<openraft::LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let store = self.clone();
        blocking(move || store.save("committed", &committed)).await
    }
    async fn read_committed(&mut self) -> Result<Option<openraft::LogId<u64>>, StorageError<u64>> {
        let store = self.clone();
        blocking(move || {
            Ok(store
                .read::<Option<openraft::LogId<u64>>>("committed")?
                .flatten())
        })
        .await
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<Types>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut encoded = Vec::new();
        let mut admission = 0;
        for entry in entries {
            let bytes = serde_json::to_vec(&entry).map_err(io)?;
            admission += bytes.len();
            if bytes.len() > MAX_ENTRY_BYTES || admission > READ_BUDGET {
                return Err(io("Raft append exceeds memory budget"));
            }
            encoded.push((entry.log_id.index, bytes));
        }
        let store = self.clone();
        let result = blocking(move || {
            let tx = store.db.begin_write().map_err(io)?;
            { let mut log = tx.open_table(LOG).map_err(io)?; let mut meta = tx.open_table(META).map_err(io)?;
              let mut total: u64 = meta.get("bytes").map_err(io)?.map(|v| serde_json::from_slice(v.value()).map_err(io)).transpose()?.unwrap_or(0);
              for (index, bytes) in encoded {
                let last = log.last().map_err(io)?.map(|(key, _)| key.value());
                if last.is_some_and(|last| index > last.saturating_add(1)) || (last.is_none() && index != 0) { return Err(io("Raft append would create a hole")); }
                let previous = log.get(index).map_err(io)?.map(|v| v.value().len() as u64).unwrap_or(0);
                total = total.checked_sub(previous).and_then(|v| v.checked_add(bytes.len() as u64)).ok_or_else(|| io("Raft log accounting overflow"))?;
                if total > store.max_bytes { return Err(io("Raft log budget exhausted; node stopped; snapshot compaction not implemented")); }
                log.insert(index, bytes.as_slice()).map_err(io)?;
              }
              let bytes = serde_json::to_vec(&total).map_err(io)?; meta.insert("bytes", bytes.as_slice()).map_err(io)?;
            }
            tx.commit().map_err(io)
        }).await;
        callback.log_io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|e| std::io::Error::other(e.to_string())),
        );
        result
    }
    async fn truncate(&mut self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        let store = self.clone();
        blocking(move || {
            let tx = store.db.begin_write().map_err(io)?;
            {
                let mut log = tx.open_table(LOG).map_err(io)?;
                let mut removed = 0u64;
                for value in log
                    .extract_from_if(log_id.index.., |_, _| true)
                    .map_err(io)?
                {
                    removed += value.map_err(io)?.1.value().len() as u64;
                }
                let mut meta = tx.open_table(META).map_err(io)?;
                let total: u64 = meta
                    .get("bytes")
                    .map_err(io)?
                    .map(|v| serde_json::from_slice(v.value()).map_err(io))
                    .transpose()?
                    .unwrap_or(0);
                let bytes = serde_json::to_vec(
                    &total
                        .checked_sub(removed)
                        .ok_or_else(|| io("Raft log accounting underflow"))?,
                )
                .map_err(io)?;
                meta.insert("bytes", bytes.as_slice()).map_err(io)?;
            }
            tx.commit().map_err(io)
        })
        .await
    }
    async fn purge(&mut self, _log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        Err(io("Raft snapshot compaction is not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cursor() -> crate::paged::PagedReplicationCursor {
        crate::paged::PagedReplicationCursor {
            source: Id::from_u128(15),
            revision: 3,
            digest: [7; 32],
        }
    }
    fn peers() -> BTreeMap<u64, BasicNode> {
        [(1, BasicNode::new("peer-one"))].into_iter().collect()
    }
    #[test]
    fn existing_database_is_not_repurposed_as_a_raft_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("foreign");
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            tx.open_table(TableDefinition::<u64, u64>::new("foreign_data"))
                .unwrap();
            tx.commit().unwrap();
        }
        assert!(LogStore::open(&path, 1, cursor(), &peers(), 1024 * 1024).is_err());
        let db = redb::Database::open(&path).unwrap();
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.list_tables()
                .unwrap()
                .map(|t| t.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["foreign_data"]
        );
    }
    #[test]
    fn genesis_and_trusted_peers_are_pinned_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let (store, genesis) = LogStore::open(&path, 1, cursor(), &peers(), 1024 * 1024).unwrap();
        drop(store);
        let mut advanced = cursor();
        advanced.revision += 1;
        let (store, restored) = LogStore::open(&path, 1, advanced, &peers(), 1024 * 1024).unwrap();
        assert_eq!(restored, genesis);
        drop(store);
        let changed = [(1, BasicNode::new("different-trust-endpoint"))]
            .into_iter()
            .collect();
        assert!(LogStore::open(&path, 1, advanced, &changed, 1024 * 1024).is_err());
        assert!(LogStore::open(&path, 2, advanced, &peers(), 1024 * 1024).is_err());
    }
    #[test]
    fn damaged_log_accounting_cannot_bypass_the_storage_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let (store, _) = LogStore::open(&path, 1, cursor(), &peers(), 1024 * 1024).unwrap();
        {
            let tx = store.db.begin_write().unwrap();
            tx.open_table(META)
                .unwrap()
                .insert("bytes", b"1".as_slice())
                .unwrap();
            tx.commit().unwrap();
        }
        drop(store);
        assert!(LogStore::open(&path, 1, cursor(), &peers(), 1024 * 1024).is_err());
    }
}

#[cfg(test)]
mod multimap_test {
    use super::*;
    #[test]
    fn multimap_only_database_is_not_repurposed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("foreign");
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            tx.open_multimap_table(redb::MultimapTableDefinition::<u64, u64>::new("foreign"))
                .unwrap();
            tx.commit().unwrap();
        }
        let cursor = crate::paged::PagedReplicationCursor {
            source: Id::new(),
            revision: 0,
            digest: [0; 32],
        };
        assert!(LogStore::open(&path, 1, cursor, &BTreeMap::new(), 1024 * 1024).is_err());
        let db = redb::Database::open(&path).unwrap();
        assert_eq!(db.begin_read().unwrap().list_tables().unwrap().count(), 0);
    }
}
