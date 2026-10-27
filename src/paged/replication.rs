//! Explicit trusted-host paged replication. This is an ordered durable stream,
//! not leader election or consensus. Transport authentication belongs to the host.
use super::*;
use sha2::{Digest, Sha256};

pub(super) const LOG: TableDefinition<u64, &[u8]> =
    TableDefinition::new("paged_replication_log_v1");
pub(super) const CONTROL: TableDefinition<&str, &[u8]> =
    TableDefinition::new("paged_replication_control_v1");
const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_LOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_LOG_ENTRIES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagedReplicationCursor {
    pub source: Id,
    pub revision: u64,
    pub digest: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JournalState {
    version: u8,
    pub cursor: PagedReplicationCursor,
    anchor: PagedReplicationCursor,
    bytes: usize,
    entries: usize,
    pub follower: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PagedReplicationApply {
    Applied(PagedReplicationCursor),
    Duplicate(PagedReplicationCursor),
}
fn read_state(read: &redb::ReadTransaction) -> Result<Option<JournalState>> {
    let table = match read.open_table(CONTROL) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(err(error)),
    };
    let bytes = table
        .get("state")
        .map_err(err)?
        .ok_or_else(|| err("missing replication control"))?;
    let state: JournalState = serde_json::from_slice(bytes.value()).map_err(err)?;
    if state.version != 1
        || state.entries > MAX_LOG_ENTRIES
        || state.bytes > MAX_LOG_BYTES
        || state.anchor.source != state.cursor.source
        || state.anchor.revision > state.cursor.revision
    {
        return Err(err("invalid replication control"));
    }
    Ok(Some(state))
}
pub(super) fn validate(view: &View) -> Result<bool> {
    let Some(state) = read_state(&view.read)? else {
        return Ok(false);
    };
    if state.cursor.revision != view.revision {
        return Err(err("replication head differs from paged revision"));
    }
    let mut cursor = state.anchor;
    let mut bytes = 0usize;
    let mut entries = 0usize;
    let log = view.read.open_table(LOG).map_err(err)?;
    for entry in log.iter().map_err(err)? {
        let (revision, record) = entry.map_err(err)?;
        let event = Event::decode(record.value())?;
        if event.base != cursor || event.head.revision != revision.value() {
            return Err(err("replication journal gap"));
        }
        cursor = event.head;
        entries += 1;
        bytes = bytes
            .checked_add(record.value().len())
            .ok_or_else(|| err("replication size overflow"))?;
        if entries > MAX_LOG_ENTRIES || bytes > MAX_LOG_BYTES {
            return Err(err("replication journal unbounded"));
        }
    }
    if cursor != state.cursor || entries != state.entries || bytes != state.bytes {
        return Err(err("replication journal accounting mismatch"));
    }
    Ok(state.follower)
}
fn write_state(tx: &redb::WriteTransaction, state: &JournalState) -> Result<()> {
    tx.open_table(CONTROL)
        .map_err(err)?
        .insert("state", serde_json::to_vec(state).map_err(err)?.as_slice())
        .map_err(err)?;
    Ok(())
}
pub(super) fn append(
    tx: &redb::WriteTransaction,
    state: &mut JournalState,
    bytes: &[u8],
) -> Result<()> {
    let event = Event::decode(bytes)?;
    if event.base != state.cursor {
        return Err(err("replication append lineage mismatch"));
    }
    let mut log = tx.open_table(LOG).map_err(err)?;
    while state.entries != 0
        && (state.entries >= MAX_LOG_ENTRIES
            || state.bytes.saturating_add(bytes.len()) > MAX_LOG_BYTES)
    {
        let (revision, length, head) = {
            let (revision, old) = log
                .first()
                .map_err(err)?
                .ok_or_else(|| err("missing retained event"))?;
            (
                revision.value(),
                old.value().len(),
                Event::decode(old.value())?.head,
            )
        };
        state.anchor = head;
        log.remove(revision).map_err(err)?;
        state.entries -= 1;
        state.bytes -= length;
    }
    log.insert(event.head.revision, bytes).map_err(err)?;
    state.entries += 1;
    state.bytes += bytes.len();
    state.cursor = event.head;
    drop(log);
    write_state(tx, state)
}
impl PagedDatabase {
    /// Enable a bounded durable change stream for future transactions. Existing
    /// snapshots must be bootstrapped explicitly; old history is not fabricated.
    pub fn enable_replication(&self) -> Result<PagedReplicationCursor> {
        let _writer = self.inner.writer.lock().map_err(lock_err)?;
        if self.inner.failed.load(Ordering::Acquire) {
            return Err(err("reopen after uncertain persistence"));
        }
        let view = self.inner.head.read().map_err(lock_err)?.clone();
        if let Some(state) = read_state(&view.read)? {
            return Ok(state.cursor);
        }
        let cursor = PagedReplicationCursor {
            source: Id::new(),
            revision: view.revision,
            digest: [0; 32],
        };
        let state = JournalState {
            version: 1,
            cursor,
            anchor: cursor,
            bytes: 0,
            entries: 0,
            follower: false,
        };
        let tx = self.inner.db.begin_write().map_err(err)?;
        tx.open_table(LOG).map_err(err)?;
        write_state(&tx, &state)?;
        if let Err(error) = tx.commit() {
            self.inner.failed.store(true, Ordering::Release);
            return Err(err(error));
        }
        let result = (|| {
            let next = View::new(
                self.inner.db.begin_read().map_err(err)?,
                view.catalog.clone(),
                view.revision,
                view.auth_epoch,
            )?;
            self.inner.publish(next)?;
            Ok(cursor)
        })();
        if result.is_err() {
            self.inner.failed.store(true, Ordering::Release);
        }
        result
    }
    pub fn replication_cursor(&self) -> Result<PagedReplicationCursor> {
        self.admin().snapshot()?.fence()?;
        read_state(&self.inner.head.read().map_err(lock_err)?.read)?
            .map(|s| s.cursor)
            .ok_or_else(|| err("replication is not enabled"))
    }
    /// Return one exact transaction after a trusted follower cursor, bounded to
    /// 16 MiB. Retention gaps and wrong lineages require a fresh bootstrap.
    pub fn replication_next(&self, after: PagedReplicationCursor) -> Result<Option<Vec<u8>>> {
        let snapshot = self.admin().snapshot()?;
        let state =
            read_state(&snapshot.view.read)?.ok_or_else(|| err("replication is not enabled"))?;
        if after == state.cursor {
            return Ok(None);
        }
        if after.source != state.cursor.source || after.revision < state.anchor.revision {
            return Err(err(
                "replication lineage/history unavailable; bootstrap required",
            ));
        }
        let next = after
            .revision
            .checked_add(1)
            .ok_or_else(|| err("replication revision exhausted"))?;
        let log = snapshot.view.read.open_table(LOG).map_err(err)?;
        let bytes = log
            .get(next)
            .map_err(err)?
            .ok_or_else(|| err("replication cursor not in retained history"))?;
        let event = Event::decode(bytes.value())?;
        if event.base != after {
            return Err(err("replication cursor digest mismatch"));
        }
        Ok(Some(bytes.value().to_vec()))
    }
}

pub(super) fn current(view: &View) -> Result<Option<JournalState>> {
    read_state(&view.read)
}

pub(super) struct Event {
    pub base: PagedReplicationCursor,
    pub head: PagedReplicationCursor,
    pub auth_epoch: u64,
    pub catalog: Option<CatalogData>,
    pub identities: BTreeSet<Id>,
    pub rows: BTreeMap<(Id, Id), Option<Vec<u8>>>,
}
fn put(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    out.extend(u32::try_from(bytes.len()).map_err(err)?.to_le_bytes());
    out.extend(bytes);
    if out.len() > MAX_EVENT_BYTES - 32 {
        return Err(err("replicated transaction exceeds event limit"));
    }
    Ok(())
}
impl Event {
    pub(super) fn encoded_len(
        catalog: Option<&[u8]>,
        identities: &BTreeSet<Id>,
        rows: &BTreeMap<(Id, Id), Option<Row>>,
        encoded: &BTreeMap<(Id, Id), Vec<u8>>,
    ) -> Result<usize> {
        // Header (76), catalog/count lengths (12), and trailing digest (32).
        let mut size = 120usize;
        for bytes in [
            catalog.map_or(0, <[u8]>::len),
            identities.len().saturating_mul(16),
            rows.len().saturating_mul(36),
        ] {
            size = size
                .checked_add(bytes)
                .ok_or_else(|| err("replication size overflow"))?;
        }
        for (key, row) in rows {
            if row.is_some() {
                size = size
                    .checked_add(
                        encoded
                            .get(key)
                            .ok_or_else(|| err("missing encoded replica row"))?
                            .len(),
                    )
                    .ok_or_else(|| err("replication size overflow"))?;
            }
        }
        if size > MAX_EVENT_BYTES {
            return Err(err("replicated transaction exceeds event limit"));
        }
        Ok(size)
    }
    pub(super) fn encode(
        base: PagedReplicationCursor,
        revision: u64,
        auth_epoch: u64,
        catalog: Option<&[u8]>,
        identities: &BTreeSet<Id>,
        rows: &BTreeMap<(Id, Id), Option<Row>>,
        encoded: &BTreeMap<(Id, Id), Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let length = Self::encoded_len(catalog, identities, rows, encoded)?;
        let mut out = Vec::with_capacity(length);
        out.extend_from_slice(b"LPR1");
        out.extend(base.source.0.as_bytes());
        out.extend(base.revision.to_le_bytes());
        out.extend(base.digest);
        out.extend(revision.to_le_bytes());
        out.extend(auth_epoch.to_le_bytes());
        put(&mut out, catalog.unwrap_or_default())?;
        out.extend(u32::try_from(identities.len()).map_err(err)?.to_le_bytes());
        for id in identities {
            out.extend(id.0.as_bytes());
        }
        out.extend(u32::try_from(rows.len()).map_err(err)?.to_le_bytes());
        for ((tid, id), row) in rows {
            out.extend(tid.0.as_bytes());
            out.extend(id.0.as_bytes());
            put(
                &mut out,
                if row.is_some() {
                    encoded
                        .get(&(*tid, *id))
                        .ok_or_else(|| err("missing encoded replica row"))?
                        .as_slice()
                } else {
                    &[]
                },
            )?;
        }
        if out.len() > MAX_EVENT_BYTES - 32 {
            return Err(err("replicated transaction exceeds event limit"));
        }
        let hash = Sha256::digest(&out);
        out.extend(hash);
        Ok(out)
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 116 || bytes.len() > MAX_EVENT_BYTES {
            return Err(err("invalid replication event size"));
        }
        let (body, hash) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != hash {
            return Err(err("replication event checksum mismatch"));
        }
        let mut input = Input(body);
        if input.take(4)? != b"LPR1" {
            return Err(err("unknown paged replication event version"));
        }
        let source = input.id()?;
        let base = PagedReplicationCursor {
            source,
            revision: input.u64()?,
            digest: input.take(32)?.try_into().unwrap(),
        };
        let revision = input.u64()?;
        if Some(revision) != base.revision.checked_add(1) {
            return Err(err("replication revision gap"));
        }
        let auth_epoch = input.u64()?;
        let bytes = input.bytes()?;
        let catalog = if bytes.is_empty() {
            None
        } else {
            Some(serde_json::from_slice(bytes).map_err(err)?)
        };
        let mut identities = BTreeSet::new();
        for _ in 0..input.u32()? {
            if !identities.insert(input.id()?) {
                return Err(err("duplicate replicated identity"));
            }
        }
        let mut rows = BTreeMap::new();
        for _ in 0..input.u32()? {
            let key = (input.id()?, input.id()?);
            let bytes = input.bytes()?;
            let row = (!bytes.is_empty()).then(|| bytes.to_vec());
            if rows.insert(key, row).is_some() {
                return Err(err("duplicate replicated row"));
            }
        }
        if !input.0.is_empty() {
            return Err(err("trailing replication bytes"));
        }
        Ok(Self {
            base,
            head: PagedReplicationCursor {
                source,
                revision,
                digest: hash.try_into().unwrap(),
            },
            auth_epoch,
            catalog,
            identities,
            rows,
        })
    }
}
struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(err("truncated replication event"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn id(&mut self) -> Result<Id> {
        Ok(Id(uuid::Uuid::from_bytes(
            self.take(16)?.try_into().unwrap(),
        )))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let size = self.u32()? as usize;
        self.take(size)
    }
}

pub(super) fn copy_journal(view: &View, target: &redb::Database, follower: bool) -> Result<()> {
    let Some(mut state) = current(view)? else {
        if follower {
            return Err(err("replication bootstrap requires an enabled journal"));
        }
        return Ok(());
    };
    state.follower |= follower;
    // Retained history has a fixed 64 MiB cap; commit one bounded event at a
    // time so import does not turn the whole retained window into dirty pages.
    for entry in view
        .read
        .open_table(LOG)
        .map_err(err)?
        .iter()
        .map_err(err)?
    {
        let (revision, bytes) = entry.map_err(err)?;
        let tx = target.begin_write().map_err(err)?;
        tx.open_table(LOG)
            .map_err(err)?
            .insert(revision.value(), bytes.value())
            .map_err(err)?;
        tx.commit().map_err(err)?;
    }
    let tx = target.begin_write().map_err(err)?;
    tx.open_table(LOG).map_err(err)?;
    write_state(&tx, &state)?;
    tx.commit().map_err(err)?;
    Ok(())
}

/// Trusted-host follower exposing read-only snapshots. Bootstrap currently uses
/// an explicit offline file import; transfer/RPC authentication and elections
/// are not provided. Eventual reads may retain permissions until revocations
/// arrive. A supplied cursor barrier proves only that exact trusted revision.
pub struct PagedReplica {
    db: PagedDatabase,
}
impl PagedReplica {
    pub fn create_from_file(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        options: PagedOptions,
    ) -> Result<Self> {
        PagedDatabase::import_file(source, &destination, options, true)?;
        Self::open(destination, options)
    }
    pub fn open(path: impl AsRef<Path>, options: PagedOptions) -> Result<Self> {
        let db =
            PagedDatabase::open_with_factory(options, |builder| builder.open(path).map_err(err))?;
        if !db.inner.read_only.load(Ordering::Acquire) {
            return Err(err("file is not a paged follower"));
        }
        Ok(Self { db })
    }
    pub fn cursor(&self) -> Result<PagedReplicationCursor> {
        self.db.replication_cursor()
    }
    pub fn snapshot_eventual(&self, principal: &str) -> Result<PagedSnapshot> {
        self.db.session(principal)?.snapshot()
    }
    pub fn anonymous_eventual(&self) -> Result<PagedSnapshot> {
        self.db.anonymous().snapshot()
    }
    /// Authenticate against replicated credential state; revocations may lag
    /// until applied. Expiry and scoped read permissions are still enforced.
    pub fn authenticate_eventual(&self, token: &str) -> Result<PagedSnapshot> {
        self.db.authenticate(token)?.snapshot()
    }
    pub fn snapshot_at_cursor(
        &self,
        principal: &str,
        cursor: PagedReplicationCursor,
    ) -> Result<PagedSnapshot> {
        let _writer = self.db.inner.writer.lock().map_err(lock_err)?;
        if self.cursor()? != cursor {
            return Err(err("follower differs from trusted cursor barrier"));
        }
        self.snapshot_eventual(principal)
    }
    pub fn apply(&self, bytes: &[u8]) -> Result<PagedReplicationApply> {
        self.apply_inner(bytes, None)
    }
    fn apply_inner(&self, bytes: &[u8], marker: Option<&[u8]>) -> Result<PagedReplicationApply> {
        if let Some(marker) = marker {
            validate_marker(marker)?;
        }
        let event = Event::decode(bytes)?;
        let inner = &self.db.inner;
        let _writer = inner.writer.lock().map_err(lock_err)?;
        if inner.failed.load(Ordering::Acquire) {
            return Err(err("reopen after uncertain persistence"));
        }
        let current = inner.head.read().map_err(lock_err)?.clone();
        let mut journal =
            replication::current(&current)?.ok_or_else(|| err("follower journal missing"))?;
        if event.head == journal.cursor {
            if let Some(marker) = marker {
                persist_marker(inner, &current, marker)?;
            }
            return Ok(PagedReplicationApply::Duplicate(journal.cursor));
        }
        if event.base != journal.cursor {
            return Err(if marker.is_some() {
                Error::Conflict
            } else {
                err("replication gap or divergent history")
            });
        }
        let catalog = match &event.catalog {
            Some(data) => {
                if !data.modules.starts_with(&current.catalog.data.modules)
                    || current
                        .catalog
                        .data
                        .owners
                        .iter()
                        .any(|(namespace, owner)| data.owners.get(namespace) != Some(owner))
                {
                    return Err(err("nonconservative replicated catalog"));
                }
                if serde_json::to_vec(data).map_err(err)?.len() > inner.options.max_catalog_bytes {
                    return Err(err("replicated catalog too large"));
                }
                let compiled = Catalog::compile(data.clone())?;
                authority::validate_visibility_identity_extension(&current.catalog, &compiled)?;
                if compiled.data.reserved != authority::catalog_ids(&compiled.state) {
                    return Err(err("replicated catalog reservations differ"));
                }
                Arc::new(compiled)
            }
            None => current.catalog.clone(),
        };
        let auth_changed = event.auth_epoch != current.auth_epoch;
        if event.auth_epoch < current.auth_epoch
            || event.auth_epoch > current.auth_epoch.saturating_add(1)
            || (auth_changed && event.catalog.is_none())
        {
            return Err(err("invalid replicated authorization epoch"));
        }
        if authority::authority_changed(&current.catalog.data, &catalog.data) && !auth_changed {
            return Err(err("replicated authority change omitted epoch fence"));
        }
        if !current
            .catalog
            .data
            .reserved
            .is_subset(&catalog.data.reserved)
        {
            return Err(err("replication removed stable catalog identities"));
        }
        for id in catalog
            .data
            .reserved
            .difference(&current.catalog.data.reserved)
        {
            if current
                .ids
                .get(id.0.as_bytes().as_slice())
                .map_err(err)?
                .is_some()
                || event.identities.contains(id)
            {
                return Err(err(
                    "replicated catalog identity collides with allocated identity",
                ));
            }
        }
        if event.identities.len() > inner.options.max_transaction_rows {
            return Err(err("replicated identity budget exceeded"));
        }
        if event.rows.len() > inner.options.max_transaction_rows {
            return Err(err("replication row budget exceeded"));
        }
        let snapshot = self.db.admin().snapshot()?;
        let mut transaction = PagedTransaction {
            #[cfg(test)]
            before_persist_test_hook: None,
            snapshot,
            catalog: catalog.clone(),
            rows: BTreeMap::new(),
            staged_unique: BTreeMap::new(),
            identities: event.identities.clone(),
            reads: BTreeMap::new(),
            bytes: 0,
            effects: 0,
            failed: false,
            catalog_changed: event.catalog.is_some(),
            auth_changed,
            defer_foreign_keys: false,
            constraint_work: 0,
        };
        for id in &event.identities {
            if !event.rows.keys().any(|(_, occurrence)| occurrence == id) {
                return Err(err("replicated identity has no transaction membership"));
            }
            if current
                .ids
                .get(id.0.as_bytes().as_slice())
                .map_err(err)?
                .is_some()
                || catalog.state.relations.contains_key(id)
                || catalog.state.identities.contains(id)
            {
                return Err(err("replication reallocates existing or reserved identity"));
            }
        }
        for (&(tid, id), bytes) in &event.rows {
            let def = catalog
                .state
                .relations
                .get(&tid)
                .ok_or_else(|| err("unknown replicated relation"))?;
            if catalog.state.identities.contains(&id)
                || catalog.state.relations.contains_key(&id)
                || (!event.identities.contains(&id)
                    && current
                        .ids
                        .get(id.0.as_bytes().as_slice())
                        .map_err(err)?
                        .is_none())
            {
                return Err(err("unallocated replicated occurrence"));
            }
            if bytes.is_none()
                && !event.identities.contains(&id)
                && current.rows.get(&row_key(tid, id))?.is_none()
            {
                return Err(err("replication deletes absent membership"));
            }
            let row = if let Some(bytes) = bytes {
                if bytes.len() > inner.options.max_row_bytes {
                    return Err(err("replicated row exceeds limit"));
                }
                let (revision, row) = decode_row(def, id, bytes)?;
                if revision != event.head.revision {
                    return Err(err("replicated packed revision mismatch"));
                }
                Some(row)
            } else {
                None
            };
            transaction.rows.insert((tid, id), row);
        }
        // Populate the complete candidate before checking references, allowing
        // forward references and unique-value moves within the same transaction.
        let candidate = transaction.rows.clone();
        for (&(tid, id), row) in &candidate {
            if let Some(row) = row {
                let def = catalog.state.relations[&tid].clone();
                transaction.validate(&def, row)?;
            } else {
                let prefix = incoming_prefix(tid, id);
                let end = prefix_end(&prefix);
                for entry in current
                    .incoming
                    .range::<&[u8]>((
                        std::ops::Bound::Included(prefix.as_slice()),
                        end.as_deref()
                            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    ))
                    .map_err(err)?
                {
                    let (key, _) = entry.map_err(err)?;
                    let key = key.value();
                    if key.len() != 72 {
                        return Err(err("malformed incoming reference"));
                    }
                    let source = (
                        Id(uuid::Uuid::from_slice(&key[32..48]).map_err(err)?),
                        Id(uuid::Uuid::from_slice(&key[48..64]).map_err(err)?),
                    );
                    match candidate.get(&source) {
                        Some(None) => {}
                        Some(Some(row))
                            if !catalog.state.relations[&source.0]
                                .reference_slots
                                .iter()
                                .any(|(slot, target)| {
                                    catalog.state.names[target] == tid
                                        && row.values[*slot] == Value::Ref(id)
                                }) => {}
                        _ => return Err(err("replication deletes referenced occurrence")),
                    }
                }
            }
            transaction.stage(tid, id, row.clone())?;
        }
        let mut prepared = commit::Prepared::new(
            transaction,
            &current,
            event.head.revision,
            event.auth_epoch,
            inner.options,
        )?;
        let mut tx = inner.db.begin_write().map_err(err)?;
        tx.set_durability(redb::Durability::Immediate)
            .map_err(err)?;
        prepared.apply(&tx, current.format, inner.options)?;
        append(&tx, &mut journal, bytes)?;
        if let Some(marker) = marker {
            tx.open_table(CONTROL)
                .map_err(err)?
                .insert("raft_applied", marker)
                .map_err(err)?;
        }
        crate::durable::failpoint("paged_replica_before_commit");
        if let Err(error) = tx.commit() {
            inner.failed.store(true, Ordering::Release);
            return Err(err(error));
        }
        crate::durable::failpoint("paged_replica_after_commit");
        let published = (|| {
            let next = View::new(
                inner.db.begin_read().map_err(err)?,
                catalog,
                event.head.revision,
                event.auth_epoch,
            )?;
            inner.publish(next)?;
            Ok(PagedReplicationApply::Applied(event.head))
        })();
        if published.is_err() {
            inner.failed.store(true, Ordering::Release);
        }
        published
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    fn source(path: &Path) -> (PagedDatabase, Id) {
        let db = PagedDatabase::open(path, Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "replica",
                1,
                vec![],
                vec![
                    Relation::new("replica.Person", vec![Role::new("name", DataType::Text)])
                        .unique(&["name"]),
                ],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        let id = tx.insert("replica.Person", vec!["Alice".into()]).unwrap();
        tx.grant("reader", "replica.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        db.enable_replication().unwrap();
        (db, id)
    }
    #[test]
    fn paged_replication_preserves_transactions_catalog_indexes_and_live_revocation() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source");
        let follower_path = dir.path().join("follower");
        let (db, person) = source(&source_path);
        let seed = db.replication_cursor().unwrap();
        drop(db);
        let replica =
            PagedReplica::create_from_file(&source_path, &follower_path, Default::default())
                .unwrap();
        let old = replica.snapshot_eventual("reader").unwrap();
        assert!(old.get("replica.Person", person).unwrap().is_some());
        assert!(matches!(
            replica.db.admin().transaction(),
            Err(Error::Denied)
        ));
        let db = PagedDatabase::open(&source_path, Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.update("replica.Person", person, vec!["Changed".into()])
            .unwrap();
        tx.revoke("reader", "replica.Person").unwrap();
        let revision = tx.commit().unwrap();
        let packet = db.replication_next(seed).unwrap().unwrap();
        let cursor = db.replication_cursor().unwrap();
        assert_eq!(cursor.revision, revision);
        assert_eq!(
            replica.apply(&packet).unwrap(),
            PagedReplicationApply::Applied(cursor)
        );
        assert_eq!(
            replica.apply(&packet).unwrap(),
            PagedReplicationApply::Duplicate(cursor)
        );
        assert!(matches!(
            old.get("replica.Person", person),
            Err(Error::Denied)
        ));
        assert!(matches!(
            replica
                .snapshot_eventual("reader")
                .unwrap()
                .get("replica.Person", person),
            Err(Error::Denied)
        ));
        assert_eq!(
            replica
                .snapshot_at_cursor("owner", cursor)
                .unwrap()
                .lookup_page("replica.Person", "name", &"Changed".into(), None, 10)
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(replica.snapshot_at_cursor("owner", seed).is_err());
        replica.db.admin().snapshot().unwrap().verify().unwrap();
        drop(old);
        drop(replica);
        let replica = PagedReplica::open(&follower_path, Default::default()).unwrap();
        assert_eq!(replica.cursor().unwrap(), cursor);
        assert!(matches!(
            replica.db.admin().transaction(),
            Err(Error::Denied)
        ));
    }
    #[test]
    fn journal_rejects_corruption_gaps_divergence_and_retention_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source");
        let (db, person) = source(&path);
        let seed = db.replication_cursor().unwrap();
        drop(db);
        let replica =
            PagedReplica::create_from_file(&path, dir.path().join("follower"), Default::default())
                .unwrap();
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let mut packets = Vec::new();
        let mut cursor = seed;
        for n in 0..3 {
            let mut tx = db.admin().transaction().unwrap();
            tx.update(
                "replica.Person",
                person,
                vec![Value::Text(format!("row {n}"))],
            )
            .unwrap();
            tx.commit().unwrap();
            packets.push(db.replication_next(cursor).unwrap().unwrap());
            cursor = db.replication_cursor().unwrap();
        }
        assert!(replica.apply(&packets[1]).is_err());
        let mut corrupt = packets[0].clone();
        corrupt[30] ^= 1;
        assert!(replica.apply(&corrupt).is_err());
        assert_eq!(replica.cursor().unwrap(), seed);
        let mut wrong = seed;
        wrong.digest[0] ^= 1;
        assert!(db.replication_next(wrong).is_err());
        for packet in &packets {
            replica.apply(packet).unwrap();
        }
        assert!(replica.apply(&packets[0]).is_err());
        for n in 0..MAX_LOG_ENTRIES {
            let mut tx = db.admin().transaction().unwrap();
            tx.update(
                "replica.Person",
                person,
                vec![Value::Text(format!("prune {n}"))],
            )
            .unwrap();
            tx.commit().unwrap();
        }
        assert!(db.replication_next(seed).is_err());
        let head = db.replication_cursor().unwrap();
        drop(db);
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        assert_eq!(db.replication_cursor().unwrap(), head);
    }
    #[test]
    fn replica_bootstrap_requires_enabled_stream_and_never_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let target = dir.path().join("target");
        drop(PagedDatabase::open(&source, Default::default()).unwrap());
        assert!(PagedReplica::create_from_file(&source, &target, Default::default()).is_err());
        assert!(!target.exists());
        let db = PagedDatabase::open(&source, Default::default()).unwrap();
        db.enable_replication().unwrap();
        drop(db);
        fs::write(&target, b"keep").unwrap();
        assert!(PagedReplica::create_from_file(&source, &target, Default::default()).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"keep");
    }
}

const MAX_RAFT_MARKER_BYTES: usize = 64 * 1024;
fn validate_marker(marker: &[u8]) -> Result<()> {
    if marker.is_empty() || marker.len() > MAX_RAFT_MARKER_BYTES {
        return Err(err("invalid consensus applied marker size"));
    }
    Ok(())
}
fn persist_marker(inner: &Inner, current: &View, marker: &[u8]) -> Result<()> {
    validate_marker(marker)?;
    let mut tx = inner.db.begin_write().map_err(err)?;
    tx.set_durability(redb::Durability::Immediate)
        .map_err(err)?;
    tx.open_table(CONTROL)
        .map_err(err)?
        .insert("raft_applied", marker)
        .map_err(err)?;
    if let Err(error) = tx.commit() {
        inner.failed.store(true, Ordering::Release);
        return Err(err(error));
    }
    let published = (|| {
        let next = View::new(
            inner.db.begin_read().map_err(err)?,
            current.catalog.clone(),
            current.revision,
            current.auth_epoch,
        )?;
        inner.publish(next)?;
        Ok(())
    })();
    if published.is_err() {
        inner.failed.store(true, Ordering::Release);
    }
    published
}
#[cfg(any(feature = "consensus", test))]
impl PagedReplica {
    /// Internal consensus staging; ordinary commit remains denied on this file.
    #[cfg(feature = "consensus")]
    pub(crate) fn consensus_transaction(&self, principal: &str) -> Result<PagedTransaction> {
        self.db.session(principal)?.build_transaction()
    }
    #[cfg(feature = "consensus")]
    pub(crate) fn consensus_admin_transaction(&self) -> Result<PagedTransaction> {
        self.db.admin().build_transaction()
    }
    pub(crate) fn cluster_marker(&self) -> Result<Option<Vec<u8>>> {
        let snapshot = self.db.admin().snapshot()?;
        let table = snapshot.view.read.open_table(CONTROL).map_err(err)?;
        table
            .get("raft_applied")
            .map_err(err)?
            .map(|bytes| {
                validate_marker(bytes.value())?;
                Ok(bytes.value().to_vec())
            })
            .transpose()
    }
    /// The consensus layer validates marker contents and monotonic log IDs.
    /// Logical changes, durable cursor and applied marker publish atomically.
    pub(crate) fn apply_consensus(&self, event: Option<&[u8]>, marker: &[u8]) -> Result<()> {
        validate_marker(marker)?;
        if let Some(bytes) = event {
            self.apply_inner(bytes, Some(marker))?;
            return Ok(());
        }
        let inner = &self.db.inner;
        let _writer = inner.writer.lock().map_err(lock_err)?;
        if inner.failed.load(Ordering::Acquire) {
            return Err(err("reopen after uncertain persistence"));
        }
        let current = inner.head.read().map_err(lock_err)?.clone();
        persist_marker(inner, &current, marker)
    }
}

#[cfg(test)]
mod crash_tests {
    use super::*;
    #[test]
    fn applied_state_and_consensus_marker_recover_atomically() {
        for point in ["paged_replica_before_commit", "paged_replica_after_commit"] {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("source");
            let follower_path = dir.path().join("follower");
            let db = PagedDatabase::open(&source_path, Default::default()).unwrap();
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "crash",
                1,
                vec![],
                vec![Relation::new(
                    "crash.Item",
                    vec![Role::new("value", DataType::Int)],
                )],
            ))
            .unwrap();
            let id = tx.insert("crash.Item", vec![1.into()]).unwrap();
            tx.commit().unwrap();
            let seed = db.enable_replication().unwrap();
            drop(db);
            drop(
                PagedReplica::create_from_file(&source_path, &follower_path, Default::default())
                    .unwrap(),
            );
            let db = PagedDatabase::open(&source_path, Default::default()).unwrap();
            let mut tx = db.admin().transaction().unwrap();
            tx.update("crash.Item", id, vec![2.into()]).unwrap();
            tx.commit().unwrap();
            let head = db.replication_cursor().unwrap();
            let packet = dir.path().join("event");
            std::fs::write(&packet, db.replication_next(seed).unwrap().unwrap()).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "paged::replication::crash_tests::replica_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("SUBSTRATE_REPLICA_PATH", &follower_path)
                .env("SUBSTRATE_REPLICA_PACKET", &packet)
                .env("SUBSTRATE_TEST_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(91));
            let replica = PagedReplica::open(&follower_path, Default::default()).unwrap();
            let committed = point == "paged_replica_after_commit";
            assert_eq!(
                replica.cursor().unwrap(),
                if committed { head } else { seed }
            );
            assert_eq!(
                replica.cluster_marker().unwrap(),
                committed.then(|| b"{\"index\":2}".to_vec())
            );
            assert_eq!(
                replica
                    .db
                    .admin()
                    .get("crash.Item", id)
                    .unwrap()
                    .unwrap()
                    .values,
                vec![Value::Int(if committed { 2 } else { 1 })].into_boxed_slice()
            );
            replica.db.admin().snapshot().unwrap().verify().unwrap();
        }
    }
    #[test]
    #[ignore]
    fn replica_child() {
        let replica = PagedReplica::open(
            std::env::var_os("SUBSTRATE_REPLICA_PATH").unwrap(),
            Default::default(),
        )
        .unwrap();
        let packet = std::fs::read(std::env::var_os("SUBSTRATE_REPLICA_PACKET").unwrap()).unwrap();
        replica
            .apply_consensus(Some(&packet), b"{\"index\":2}")
            .unwrap();
        panic!("crash point did not execute");
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    #[test]
    fn rehashed_semantic_errors_cannot_publish_a_valid_prefix_or_remove_auth_fences() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source");
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "validate",
                1,
                vec![],
                vec![
                    Relation::new("validate.Item", vec![Role::new("name", DataType::Text)])
                        .unique(&["name"]),
                ],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        let first = tx.insert("validate.Item", vec!["a".into()]).unwrap();
        let second = tx.insert("validate.Item", vec!["b".into()]).unwrap();
        tx.grant("reader", "validate.Item", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let seed = db.enable_replication().unwrap();
        drop(db);
        let replica =
            PagedReplica::create_from_file(&path, dir.path().join("follower"), Default::default())
                .unwrap();
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.update("validate.Item", first, vec!["c".into()]).unwrap();
        tx.update("validate.Item", second, vec!["d".into()])
            .unwrap();
        tx.revoke("reader", "validate.Item").unwrap();
        tx.commit().unwrap();
        let bytes = db.replication_next(seed).unwrap().unwrap();
        let event = Event::decode(&bytes).unwrap();
        let snapshot = db.admin().snapshot().unwrap();
        let tid = snapshot.view.catalog.state.names["validate.Item"];
        let def = &snapshot.view.catalog.state.relations[&tid];
        let rows: BTreeMap<_, _> = [first, second]
            .into_iter()
            .map(|id| {
                (
                    (tid, id),
                    Some(Row {
                        id,
                        values: vec!["collision".into()].into_boxed_slice(),
                    }),
                )
            })
            .collect();
        let encoded: BTreeMap<_, _> = rows
            .iter()
            .map(|(key, row)| {
                (
                    *key,
                    encode_row(def, row.as_ref().unwrap(), event.head.revision).unwrap(),
                )
            })
            .collect();
        let catalog = event
            .catalog
            .as_ref()
            .map(|data| serde_json::to_vec(data).unwrap());
        let collision = Event::encode(
            event.base,
            event.head.revision,
            event.auth_epoch,
            catalog.as_deref(),
            &event.identities,
            &rows,
            &encoded,
        )
        .unwrap();
        assert!(replica.apply(&collision).is_err());
        assert_eq!(replica.cursor().unwrap(), seed);
        assert_eq!(
            replica
                .snapshot_eventual("reader")
                .unwrap()
                .get("validate.Item", first)
                .unwrap()
                .unwrap()
                .values[0],
            Value::Text("a".into())
        );
        // Recompute the checksum after removing an epoch increment: hashes are
        // integrity framing, so semantic authorization validation must reject it.
        let mut missing_fence = bytes.clone();
        missing_fence[68..76].copy_from_slice(&(event.auth_epoch - 1).to_le_bytes());
        let end = missing_fence.len() - 32;
        let hash = Sha256::digest(&missing_fence[..end]);
        missing_fence[end..].copy_from_slice(&hash);
        assert!(replica.apply(&missing_fence).is_err());
        assert_eq!(replica.cursor().unwrap(), seed);
        replica.db.admin().snapshot().unwrap().verify().unwrap();
        replica.apply(&bytes).unwrap();
        assert_eq!(replica.cursor().unwrap(), db.replication_cursor().unwrap());
    }
}
