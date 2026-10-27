//! Bounded caller-elected group commit with one durable publication.
use super::*;
use std::{
    sync::Condvar,
    time::{Duration, Instant},
};

struct Request {
    transaction: Mutex<Option<PagedTransaction>>,
    result: Mutex<Option<Result<u64>>>,
    bytes: usize,
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Arc<Request>>,
    bytes: usize,
    active: bool,
    stats: GroupCommitStats,
}
pub(super) struct Coordinator {
    queue: Mutex<Queue>,
    changed: Condvar,
    options: GroupCommitOptions,
}
impl Coordinator {
    pub(super) fn new(options: GroupCommitOptions) -> Self {
        Self {
            queue: Mutex::new(Queue::default()),
            changed: Condvar::new(),
            options,
        }
    }
    pub(super) fn submit(&self, transaction: PagedTransaction, inner: &Inner) -> Result<u64> {
        let bytes = transaction.bytes.max(256);
        if bytes > self.options.max_group_bytes || bytes > self.options.max_queued_bytes {
            return Err(Error::Constraint(
                "transaction exceeds paged commit queue budget".into(),
            ));
        }
        let request = Arc::new(Request {
            transaction: Mutex::new(Some(transaction)),
            result: Mutex::new(None),
            bytes,
        });
        let mut queue = self.queue.lock().map_err(lock_err)?;
        if queue.pending.len() >= self.options.max_queued_transactions
            || bytes > self.options.max_queued_bytes - queue.bytes
        {
            return Err(Error::Constraint(
                "paged commit queue full; retry with backoff".into(),
            ));
        }
        queue.bytes += bytes;
        queue.pending.push_back(request.clone());
        self.changed.notify_all();
        loop {
            if let Some(result) = request.result.lock().map_err(lock_err)?.take() {
                return result;
            }
            if queue.active {
                queue = self.changed.wait(queue).map_err(lock_err)?;
                continue;
            }
            queue.active = true;
            if !self.options.collection_window.is_zero() {
                let until = Instant::now() + self.options.collection_window;
                while queue.pending.len() < self.options.max_group_transactions
                    && queue.bytes < self.options.max_group_bytes
                {
                    let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                        break;
                    };
                    queue = self
                        .changed
                        .wait_timeout(queue, remaining)
                        .map_err(lock_err)?
                        .0;
                }
                queue.stats.collection_windows += 1;
            }
            let mut group = Vec::new();
            let mut bytes = 0;
            while let Some(next) = queue.pending.front() {
                if group.len() == self.options.max_group_transactions
                    || next.bytes > self.options.max_group_bytes - bytes
                {
                    break;
                }
                let next = queue.pending.pop_front().unwrap();
                bytes += next.bytes;
                queue.bytes -= next.bytes;
                group.push(next);
            }
            drop(queue);
            let (results, writes) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| process(inner, &group)))
                    .unwrap_or_else(|_| {
                        inner.failed.store(true, Ordering::Release);
                        (
                            group
                                .iter()
                                .map(|_| Err(err("commit coordinator panicked; reopen database")))
                                .collect(),
                            0,
                        )
                    });
            queue = self.queue.lock().map_err(lock_err)?;
            for (request, result) in group.iter().zip(results) {
                *request.result.lock().map_err(lock_err)? = Some(result);
            }
            queue.stats.committed_writes += writes as u64;
            queue.stats.durable_groups += u64::from(writes != 0);
            queue.stats.largest_group = queue.stats.largest_group.max(writes);
            queue.active = false;
            self.changed.notify_all();
        }
    }
}
impl PagedDatabase {
    /// Explicit queue bounds and optional collection window. The default open
    /// path drains already queued requests without adding a deliberate delay.
    pub fn open_with_group_commit(
        path: impl AsRef<Path>,
        options: PagedOptions,
        groups: GroupCommitOptions,
    ) -> Result<Self> {
        if groups.max_queued_transactions == 0
            || groups.max_group_transactions == 0
            || groups.max_queued_bytes == 0
            || groups.max_group_bytes == 0
            || groups.collection_window > Duration::from_secs(1)
        {
            return Err(Error::Constraint(
                "invalid paged group commit options".into(),
            ));
        }
        let mut db = Self::open(path, options)?;
        Arc::get_mut(&mut db.inner)
            .ok_or_else(|| err("new database unexpectedly shared"))?
            .commits = Coordinator::new(groups);
        Ok(db)
    }
    pub fn group_commit_stats(&self) -> Result<GroupCommitStats> {
        let queue = self.inner.commits.queue.lock().map_err(lock_err)?;
        Ok(GroupCommitStats {
            queued_transactions: queue.pending.len(),
            queued_bytes: queue.bytes,
            ..queue.stats
        })
    }
}

pub(super) struct Prepared {
    transaction: PagedTransaction,
    revision: u64,
    auth_epoch: u64,
    catalog: Arc<Catalog>,
    catalog_bytes: Option<Vec<u8>>,
    old: BTreeMap<(Id, Id), Row>,
    encoded: BTreeMap<(Id, Id), Vec<u8>>,
    allocated: Vec<Id>,
    versions: VersionChanges,
    retained_bytes: usize,
}
fn index_versions(
    catalog: &Catalog,
    tid: Id,
    row: &Row,
    changes: &mut VersionChanges,
) -> Result<()> {
    // Collect old and replacement keys once, before admission/publication.
    // Applying the physical indexes must not reconstruct these same tagged
    // keys or mutate the already admitted version set.
    let def = &catalog.state.relations[&tid];
    for i in 0..def.unique.len() {
        if !unique_applies(def, row, i) {
            continue;
        }
        changes.insert(tagged(
            b'u',
            &unique_key(tid, i, &def.unique_values(row, i)),
        ))?;
    }
    for (slot, target) in &def.reference_slots {
        if let Value::Ref(id) = row.values[*slot] {
            changes.insert(tagged(
                b'n',
                &incoming_prefix(catalog.state.names[target], id),
            ))?;
        }
    }
    Ok(())
}
/// Conservative retained-byte admission, not an allocator/RSS measurement.
/// Charge variable payloads before their owned decoding/encoding allocation.
struct PreparationBudget {
    used: usize,
    limit: usize,
}
impl PreparationBudget {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        let used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| Error::Constraint("commit preparation byte overflow".into()))?;
        if used > self.limit {
            return Err(Error::Constraint(
                "commit preparation byte budget exceeded".into(),
            ));
        }
        self.used = used;
        Ok(())
    }
    fn versions(
        &mut self,
        versions: &mut VersionChanges,
        work: impl FnOnce(&mut VersionChanges) -> Result<()>,
    ) -> Result<()> {
        let previous = versions.bytes;
        // Each version key is retained here and copied into the group overlay.
        versions.limit = previous + (self.limit - self.used) / 2;
        work(versions)?;
        self.charge((versions.bytes - previous).saturating_mul(2))
    }
}
struct JsonSize(usize);
impl std::io::Write for JsonSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("catalog length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Prepared {
    pub(super) fn new(
        transaction: PagedTransaction,
        current: &View,
        revision: u64,
        auth_epoch: u64,
        options: PagedOptions,
    ) -> Result<Self> {
        Self::bounded(
            transaction,
            current,
            revision,
            auth_epoch,
            options,
            options.max_transaction_bytes,
        )
    }
    fn bounded(
        transaction: PagedTransaction,
        current: &View,
        revision: u64,
        auth_epoch: u64,
        options: PagedOptions,
        group_remaining: usize,
    ) -> Result<Self> {
        let mut budget = PreparationBudget {
            used: 0,
            limit: options.max_transaction_bytes.min(group_remaining),
        };
        budget.charge(transaction.bytes.max(256))?;
        // BTree entries and the per-row revision keys copied into the overlay.
        budget.charge(transaction.rows.len().saturating_mul(128))?;
        let catalog = if transaction.catalog_changed {
            transaction.catalog.clone()
        } else {
            current.catalog.clone()
        };
        let catalog_bytes = if transaction.catalog_changed {
            let mut size = JsonSize(0);
            serde_json::to_writer(&mut size, &catalog.data).map_err(err)?;
            budget.charge(size.0)?;
            let mut bytes = Vec::with_capacity(size.0);
            serde_json::to_writer(&mut bytes, &catalog.data).map_err(err)?;
            Some(bytes)
        } else {
            None
        };
        let mut versions = VersionChanges::new(0);
        let mut old = BTreeMap::new();
        let mut encoded = BTreeMap::new();
        for (&(tid, id), row) in &transaction.rows {
            let def = &catalog.state.relations[&tid];
            if !transaction.identities.contains(&id)
                && let Some(bytes) = current.rows.get(&row_key(tid, id))?
            {
                if bytes.value().len() > options.max_row_bytes {
                    return Err(err("old row exceeds configured bound"));
                }
                // Packed bytes bound total variable payload; reserve every
                // Value slot plus row/map overhead before materializing it.
                budget.charge(
                    bytes
                        .value()
                        .len()
                        .saturating_add(
                            def.roles.len().saturating_mul(std::mem::size_of::<Value>()),
                        )
                        .saturating_add(128),
                )?;
                let (_, previous) = decode_row(def, id, bytes.value())?;
                budget.versions(&mut versions, |changes| {
                    index_versions(&catalog, tid, &previous, changes)
                })?;
                old.insert((tid, id), previous);
            }
            if let Some(row) = row {
                budget.charge(row_encoded_len(def, row)?.saturating_add(128))?;
                encoded.insert((tid, id), encode_row(def, row, revision)?);
                budget.versions(&mut versions, |changes| {
                    index_versions(&catalog, tid, row, changes)
                })?;
            } else {
                budget.versions(&mut versions, |changes| {
                    changes.insert(tagged(b'r', &row_key(tid, id)))
                })?;
            }
        }
        let allocation_count = transaction.identities.len().saturating_add(
            catalog
                .data
                .reserved
                .difference(&current.catalog.data.reserved)
                .count(),
        );
        budget.charge(allocation_count.saturating_mul(std::mem::size_of::<Id>()))?;
        let allocated: Vec<_> = transaction
            .identities
            .iter()
            .chain(
                catalog
                    .data
                    .reserved
                    .difference(&current.catalog.data.reserved),
            )
            .copied()
            .collect();
        for id in &allocated {
            budget.versions(&mut versions, |changes| {
                changes.insert(tagged(b'a', id.0.as_bytes()))
            })?;
        }
        if transaction.catalog_changed {
            budget.versions(&mut versions, |changes| changes.insert(b"catalog".to_vec()))?;
        }
        if transaction.auth_changed {
            budget.versions(&mut versions, |changes| changes.insert(b"auth".to_vec()))?;
        }
        if replication::current(current)?.is_some() {
            budget.charge(replication::Event::encoded_len(
                catalog_bytes.as_deref(),
                &transaction.identities,
                &transaction.rows,
                &encoded,
            )?)?;
        }
        Ok(Self {
            transaction,
            revision,
            auth_epoch,
            catalog,
            catalog_bytes,
            old,
            encoded,
            allocated,
            versions,
            retained_bytes: budget.used,
        })
    }
    pub(super) fn apply(
        &mut self,
        tx: &redb::WriteTransaction,
        format: PagedFormat,
        options: PagedOptions,
    ) -> Result<()> {
        let mut rows = RowWriter::new(
            tx.open_table(ROWS).map_err(err)?,
            format,
            options.max_row_bytes,
        );
        let mut index = tx.open_table(INDEX).map_err(err)?;
        let mut unique = tx.open_table(UNIQUE).map_err(err)?;
        let mut incoming = tx.open_table(INCOMING).map_err(err)?;
        let mut versions = tx.open_table(VERS).map_err(err)?;
        let mut ids = tx.open_table(IDS).map_err(err)?;
        for (&(tid, _), row) in &self.old {
            change_indexes(
                format,
                &self.catalog,
                tid,
                row,
                false,
                &mut index,
                &mut unique,
                &mut incoming,
            )?;
        }
        for (&(tid, id), row) in &self.transaction.rows {
            let key = row_key(tid, id);
            if let Some(row) = row {
                if !self.transaction.identities.contains(&id) {
                    versions
                        .remove(tagged(b'r', &key).as_slice())
                        .map_err(err)?;
                }
                rows.insert(
                    &key,
                    self.encoded
                        .remove(&(tid, id))
                        .ok_or_else(|| err("missing prepared row"))?,
                )?;
                change_indexes(
                    format,
                    &self.catalog,
                    tid,
                    row,
                    true,
                    &mut index,
                    &mut unique,
                    &mut incoming,
                )?;
            } else {
                rows.remove(&key)?;
            }
        }
        rows.finish()?;
        crate::durable::failpoint("paged_after_row_blocks");
        for id in &self.allocated {
            ids.insert(id.0.as_bytes().as_slice(), 0).map_err(err)?;
        }
        for key in &self.versions.keys {
            versions
                .insert(key.as_slice(), self.revision)
                .map_err(err)?;
        }
        let mut meta = tx.open_table(META).map_err(err)?;
        if let Some(bytes) = &self.catalog_bytes {
            meta.insert("catalog", bytes.as_slice()).map_err(err)?;
        }
        meta.insert("revision", self.revision.to_le_bytes().as_slice())
            .map_err(err)?;
        meta.insert("auth_epoch", self.auth_epoch.to_le_bytes().as_slice())
            .map_err(err)?;
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    // Per-thread counters make deterministic process() tests independent of
    // other parallel tests, without adding production-path instrumentation.
    static OCC_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

fn process(inner: &Inner, requests: &[Arc<Request>]) -> (Vec<Result<u64>>, usize) {
    let fail_all = |message: String| (requests.iter().map(|_| Err(err(&message))).collect(), 0);
    let _writer = match inner.writer.lock() {
        Ok(guard) => guard,
        Err(_) => return fail_all("writer poisoned".into()),
    };
    if inner.failed.load(Ordering::Acquire) {
        return fail_all("reopen after uncertain persistence".into());
    }
    let current = match inner.head.read() {
        Ok(head) => head.clone(),
        Err(_) => return fail_all("head poisoned".into()),
    };
    let mut journal = match replication::current(&current) {
        Ok(value) => value,
        Err(error) => return fail_all(error.to_string()),
    };
    let mut event_cursor = journal.as_ref().map(|state| state.cursor);
    let mut events = Vec::new();
    let mut overlay = BTreeMap::new();
    let mut revision = current.revision;
    let mut auth_epoch = current.auth_epoch;
    let mut accepted = Vec::new();
    let mut results = Vec::new();
    let mut retained = requests.iter().map(|request| request.bytes).sum::<usize>();
    for (request_index, request) in requests.iter().enumerate() {
        // Pending transactions remain live while earlier requests are prepared.
        retained -= request.bytes;
        let result = (|| -> Result<u64> {
            let transaction = request
                .transaction
                .lock()
                .map_err(lock_err)?
                .take()
                .ok_or_else(|| err("consumed commit request"))?;
            transaction.snapshot.fence()?;
            let unchanged_view = Arc::ptr_eq(&transaction.snapshot.view, &current);
            for (key, expected) in &transaction.reads {
                let actual = match overlay.get(key) {
                    Some(value) => *value,
                    // Dependencies were captured from this exact immutable
                    // view. Only earlier accepted group members can change
                    // them; their overlay always takes precedence above.
                    None if unchanged_view => continue,
                    None => {
                        #[cfg(test)]
                        OCC_WORK.with(|work| {
                            let (reads, inserts) = work.get();
                            work.set((reads + 1, inserts));
                        });
                        current.dependency_version(key)?
                    }
                };
                if actual != *expected {
                    return Err(Error::Conflict);
                }
            }
            if transaction.rows.is_empty() && !transaction.catalog_changed {
                return Ok(revision);
            }
            let next = revision
                .checked_add(1)
                .ok_or_else(|| err("revision exhausted"))?;
            let next_auth = if transaction.auth_changed {
                auth_epoch
                    .checked_add(1)
                    .ok_or_else(|| err("authorization epoch exhausted"))?
            } else {
                auth_epoch
            };
            let prepared = Prepared::bounded(
                transaction,
                &current,
                next,
                next_auth,
                inner.options,
                inner
                    .commits
                    .options
                    .max_group_bytes
                    .saturating_sub(retained),
            )?;
            if let Some(base) = event_cursor {
                let event = replication::Event::encode(
                    base,
                    next,
                    next_auth,
                    prepared.catalog_bytes.as_deref(),
                    &prepared.transaction.identities,
                    &prepared.transaction.rows,
                    &prepared.encoded,
                )?;
                event_cursor = Some(replication::PagedReplicationCursor {
                    source: base.source,
                    revision: next,
                    digest: event[event.len() - 32..].try_into().map_err(err)?,
                });
                events.push(event);
            }
            // No request can observe the last member's tentative state. Keep
            // its durable versions, but avoid a redundant overlay allocation
            // for the common one-request group.
            if request_index + 1 < requests.len() {
                #[cfg(test)]
                OCC_WORK.with(|work| {
                    let (reads, inserts) = work.get();
                    work.set((
                        reads,
                        inserts + prepared.versions.keys.len() + prepared.transaction.rows.len(),
                    ));
                });
                for key in &prepared.versions.keys {
                    overlay.insert(key.clone(), next);
                }
                for (tid, id) in prepared.transaction.rows.keys() {
                    overlay.insert(tagged(b'r', &row_key(*tid, *id)), next);
                }
            }
            revision = next;
            auth_epoch = next_auth;
            retained += prepared.retained_bytes;
            accepted.push(prepared);
            Ok(next)
        })();
        results.push(result);
    }
    if accepted.is_empty() {
        return (results, 0);
    }
    let persisted = (|| -> Result<()> {
        let mut tx = inner.db.begin_write().map_err(err)?;
        tx.set_durability(redb::Durability::Immediate)
            .map_err(err)?;
        for prepared in &mut accepted {
            prepared.apply(&tx, current.format, inner.options)?;
            #[cfg(test)]
            if let Some(hook) = prepared.transaction.before_persist_test_hook.take() {
                hook();
            }
        }
        if let Some(state) = &mut journal {
            for event in &events {
                replication::append(&tx, state, event)?;
            }
        }
        // The writer excludes policy changes; wall-clock expiry still advances.
        for prepared in &accepted {
            prepared.transaction.snapshot.fence()?;
        }
        crate::durable::failpoint("paged_before_commit");
        if let Err(error) = tx.commit() {
            inner.failed.store(true, Ordering::Release);
            return Err(err(error));
        }
        crate::durable::failpoint("paged_after_persist");
        let catalog = accepted.last().unwrap().catalog.clone();
        let published = (|| {
            let view = View::new(
                inner.db.begin_read().map_err(err)?,
                catalog,
                revision,
                auth_epoch,
            )?;
            inner.publish(view)?;
            Ok(())
        })();
        if published.is_err() {
            inner.failed.store(true, Ordering::Release);
        }
        published
    })();
    if let Err(error) = persisted {
        let mut original = Some(error);
        for result in &mut results {
            if result.is_ok() {
                *result = Err(original.take().unwrap_or(Error::Aborted));
            }
        }
        return (results, 0);
    }
    let writes = accepted.len();
    (results, writes)
}

impl PagedTransaction {
    /// Prepare an exact transaction for an external consensus coordinator.
    /// This does not write, reserve a revision, or acknowledge a commit. A stale
    /// proposal must be rejected or retried after consensus orders earlier work.
    #[cfg(feature = "consensus")]
    pub(crate) fn replication_proposal(mut self) -> Result<Vec<u8>> {
        if self.failed {
            return Err(Error::Aborted);
        }
        self.finalize_constraints()?;
        self.snapshot.fence()?;
        let db = self.snapshot.db.clone();
        let inner = &db.inner;
        let _writer = inner.writer.lock().map_err(lock_err)?;
        let current = inner.head.read().map_err(lock_err)?.clone();
        for (key, expected) in &self.reads {
            if current.dependency_version(key)? != *expected {
                return Err(Error::Conflict);
            }
        }
        self.snapshot.fence()?;
        if self.rows.is_empty() && !self.catalog_changed {
            return Err(Error::Constraint(
                "consensus proposal must contain a change".into(),
            ));
        }
        let base = replication::current(&current)?
            .ok_or_else(|| err("consensus requires enabled replication"))?
            .cursor;
        let revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| err("revision exhausted"))?;
        let auth_epoch = if self.auth_changed {
            current
                .auth_epoch
                .checked_add(1)
                .ok_or_else(|| err("authorization epoch exhausted"))?
        } else {
            current.auth_epoch
        };
        let prepared = Prepared::new(self, &current, revision, auth_epoch, inner.options)?;
        replication::Event::encode(
            base,
            revision,
            auth_epoch,
            prepared.catalog_bytes.as_deref(),
            &prepared.transaction.identities,
            &prepared.transaction.rows,
            &prepared.encoded,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prepared_index_versions_cover_old_new_and_deleted_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "versions",
            1,
            vec![],
            vec![
                Relation::new("versions.Target", vec![Role::new("value", DataType::Int)]),
                Relation::new(
                    "versions.Link",
                    vec![
                        Role::new("target", DataType::Ref("versions.Target".into())),
                        Role::new("value", DataType::Int),
                    ],
                )
                .unique(&["target"])
                .unique(&["value"]),
            ],
        ))
        .unwrap();
        for n in 1..=3 {
            tx.insert_with_id(
                "versions.Target",
                Id::from_u128(n),
                vec![Value::Int(n as i64)],
            )
            .unwrap();
        }
        let link = Id::from_u128(10);
        tx.insert_with_id(
            "versions.Link",
            link,
            vec![Value::Ref(Id::from_u128(1)), Value::Int(7)],
        )
        .unwrap();
        tx.commit().unwrap();
        let mut stale_delete = db.admin().transaction().unwrap();
        stale_delete
            .delete("versions.Target", Id::from_u128(2))
            .unwrap();
        let mut stale_unique = db.admin().transaction().unwrap();
        stale_unique
            .insert_with_id(
                "versions.Link",
                Id::from_u128(11),
                vec![Value::Ref(Id::from_u128(3)), Value::Int(8)],
            )
            .unwrap();
        let mut update = db.admin().transaction().unwrap();
        update
            .update(
                "versions.Link",
                link,
                vec![Value::Ref(Id::from_u128(2)), Value::Int(8)],
            )
            .unwrap();
        let revision = update.commit().unwrap();
        assert!(matches!(stale_delete.commit(), Err(Error::Conflict)));
        assert!(matches!(stale_unique.commit(), Err(Error::Conflict)));
        let check = |revision, targets: &[u128], values: &[i64]| {
            let snapshot = db.admin().snapshot().unwrap();
            let target = snapshot.view.catalog.state.names["versions.Target"];
            let tid = snapshot.view.catalog.state.names["versions.Link"];
            for n in targets {
                let id = Id::from_u128(*n);
                assert_eq!(
                    snapshot
                        .view
                        .dependency_version(&tagged(b'n', &incoming_prefix(target, id)))
                        .unwrap(),
                    revision
                );
                assert_eq!(
                    snapshot
                        .view
                        .dependency_version(&tagged(b'u', &unique_key(tid, 0, &[Value::Ref(id)])))
                        .unwrap(),
                    revision
                );
            }
            for value in values {
                assert_eq!(
                    snapshot
                        .view
                        .dependency_version(&tagged(
                            b'u',
                            &unique_key(tid, 1, &[Value::Int(*value)])
                        ))
                        .unwrap(),
                    revision
                );
            }
            snapshot.verify().unwrap();
        };
        check(revision, &[1, 2], &[7, 8]);
        let mut delete = db.admin().transaction().unwrap();
        delete.delete("versions.Link", link).unwrap();
        let revision = delete.commit().unwrap();
        check(revision, &[2], &[8]);
    }

    fn fixture(path: &Path) -> PagedDatabase {
        let db = PagedDatabase::open_with_group_commit(
            path,
            Default::default(),
            GroupCommitOptions {
                collection_window: Duration::from_millis(40),
                ..Default::default()
            },
        )
        .unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "group",
                1,
                vec![],
                vec![
                    Relation::new("group.Item", vec![Role::new("value", DataType::Int)])
                        .unique(&["value"]),
                ],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        tx.insert_with_id("group.Item", Id::from_u128(1), vec![1.into()])
            .unwrap();
        tx.commit().unwrap();
        db
    }
    fn large_old_rows(path: &Path) {
        let db = PagedDatabase::open(path, PagedOptions::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "large",
                1,
                vec![],
                vec![Relation::new(
                    "large.Item",
                    vec![Role::new("payload", DataType::Bytes)],
                )],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        for id in 1..=3 {
            tx.insert_with_id(
                "large.Item",
                Id::from_u128(id),
                vec![Value::Bytes(vec![7; 8192])],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    #[test]
    fn preparation_rejects_large_old_rows_before_retaining_an_unbounded_update() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        large_old_rows(&path);
        let options = PagedOptions {
            max_transaction_bytes: 16 * 1024,
            decoded_cache_bytes: 0,
            ..Default::default()
        };
        let db = PagedDatabase::open(&path, options).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        for id in 1..=3 {
            tx.update("large.Item", Id::from_u128(id), vec![Value::Bytes(vec![])])
                .unwrap();
        }
        assert!(tx.bytes < options.max_transaction_bytes);
        assert!(
            matches!(tx.commit(), Err(Error::Constraint(message)) if message.contains("preparation byte budget"))
        );
        assert_eq!(db.stats().unwrap().revision, 1);
        for id in 1..=3 {
            assert_eq!(
                db.admin()
                    .get("large.Item", Id::from_u128(id))
                    .unwrap()
                    .unwrap()
                    .values[0],
                Value::Bytes(vec![7; 8192])
            );
        }
        db.admin().snapshot().unwrap().verify().unwrap();
        drop(db);
        assert_eq!(
            PagedDatabase::open(&path, options)
                .unwrap()
                .stats()
                .unwrap()
                .revision,
            1
        );
    }
    #[test]
    fn group_preparation_budget_rejects_individually_and_preserves_later_small_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        large_old_rows(&path);
        let db = PagedDatabase::open_with_group_commit(
            &path,
            PagedOptions {
                max_transaction_bytes: 64 * 1024,
                decoded_cache_bytes: 0,
                ..Default::default()
            },
            GroupCommitOptions {
                max_group_bytes: 16 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        let mut first = db.admin().transaction().unwrap();
        first
            .update("large.Item", Id::from_u128(1), vec![Value::Bytes(vec![])])
            .unwrap();
        let mut second = db.admin().transaction().unwrap();
        second
            .update("large.Item", Id::from_u128(2), vec![Value::Bytes(vec![])])
            .unwrap();
        let mut third = db.admin().transaction().unwrap();
        third
            .insert_with_id("large.Item", Id::from_u128(4), vec![Value::Bytes(vec![9])])
            .unwrap();
        let requests: Vec<_> = [first, second, third]
            .into_iter()
            .map(|tx| {
                Arc::new(Request {
                    bytes: tx.bytes.max(256),
                    transaction: Mutex::new(Some(tx)),
                    result: Mutex::new(None),
                })
            })
            .collect();
        assert!(requests.iter().map(|r| r.bytes).sum::<usize>() < 16 * 1024);
        let (results, writes) = process(&db.inner, &requests);
        assert_eq!(writes, 2);
        assert!(matches!(results[0], Ok(2)));
        assert!(
            matches!(&results[1], Err(Error::Constraint(message)) if message.contains("preparation byte budget"))
        );
        assert!(matches!(results[2], Ok(3)));
        assert_eq!(
            db.admin()
                .get("large.Item", Id::from_u128(2))
                .unwrap()
                .unwrap()
                .values[0],
            Value::Bytes(vec![7; 8192])
        );
        assert_eq!(db.admin().snapshot().unwrap().verify().unwrap().rows, 4);
        drop(db);
        assert_eq!(
            PagedDatabase::open(&path, Default::default())
                .unwrap()
                .stats()
                .unwrap()
                .revision,
            3
        );
    }
    #[test]
    fn concurrent_independent_writes_share_durability_keep_revisions_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = fixture(&path);
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let handles: Vec<_> = (2..=9u128)
            .map(|id| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut tx = db.session("owner").unwrap().transaction().unwrap();
                    tx.insert_with_id("group.Item", Id::from_u128(id), vec![(id as i64).into()])
                        .unwrap();
                    barrier.wait();
                    tx.commit().unwrap()
                })
            })
            .collect();
        barrier.wait();
        let revisions: BTreeSet<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(revisions, (2..=9).collect());
        let stats = db.group_commit_stats().unwrap();
        assert_eq!(stats.committed_writes, 9);
        assert!(stats.largest_group > 1);
        assert!(stats.durable_groups < stats.committed_writes);
        drop(db);
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        assert_eq!(db.stats().unwrap().revision, 9);
        assert_eq!(db.admin().snapshot().unwrap().verify().unwrap().rows, 9);
        assert!(matches!(
            db.anonymous().get("group.Item", Id::from_u128(1)),
            Err(Error::Denied)
        ));
    }
    #[test]
    fn unchanged_serial_commit_skips_occ_reads_and_overlay_but_stale_views_validate() {
        let dir = tempfile::tempdir().unwrap();
        let db = fixture(&dir.path().join("db"));
        let request = |tx: PagedTransaction| {
            Arc::new(Request {
                bytes: tx.bytes.max(256),
                transaction: Mutex::new(Some(tx)),
                result: Mutex::new(None),
            })
        };
        let mut serial = db.admin().transaction().unwrap();
        serial
            .insert_with_id("group.Item", Id::from_u128(2), vec![2.into()])
            .unwrap();
        assert!(Arc::ptr_eq(
            &serial.snapshot.view,
            &db.inner.head.read().unwrap()
        ));
        assert!(!serial.reads.is_empty());
        OCC_WORK.with(|work| work.set((0, 0)));
        let (results, writes) = process(&db.inner, &[request(serial)]);
        assert_eq!(writes, 1);
        assert!(matches!(results[0], Ok(2)));
        assert_eq!(OCC_WORK.with(std::cell::Cell::get), (0, 0));

        let mut stale_conflict = db.admin().transaction().unwrap();
        stale_conflict
            .update("group.Item", Id::from_u128(1), vec![5.into()])
            .unwrap();
        let mut stale_independent = db.admin().transaction().unwrap();
        stale_independent
            .insert_with_id("group.Item", Id::from_u128(3), vec![3.into()])
            .unwrap();
        let mut newer = db.admin().transaction().unwrap();
        newer
            .update("group.Item", Id::from_u128(1), vec![4.into()])
            .unwrap();
        newer.commit().unwrap();
        assert!(!Arc::ptr_eq(
            &stale_conflict.snapshot.view,
            &db.inner.head.read().unwrap()
        ));
        OCC_WORK.with(|work| work.set((0, 0)));
        let (results, writes) = process(
            &db.inner,
            &[request(stale_conflict), request(stale_independent)],
        );
        assert_eq!(writes, 1);
        assert!(matches!(results[0], Err(Error::Conflict)));
        assert!(matches!(results[1], Ok(4)));
        let (reads, overlay_inserts) = OCC_WORK.with(std::cell::Cell::get);
        assert!(
            reads > 0,
            "different immutable views must revalidate disk dependencies"
        );
        assert_eq!(
            overlay_inserts, 0,
            "the last accepted request has no later observer"
        );
        assert_eq!(db.admin().snapshot().unwrap().verify().unwrap().rows, 3);
    }

    #[test]
    fn conflicting_request_fails_individually_without_aborting_independent_request() {
        let dir = tempfile::tempdir().unwrap();
        let db = fixture(&dir.path().join("db"));
        let mut first = db.admin().transaction().unwrap();
        let mut conflict = db.admin().transaction().unwrap();
        let mut independent = db.admin().transaction().unwrap();
        first
            .update("group.Item", Id::from_u128(1), vec![10.into()])
            .unwrap();
        conflict
            .update("group.Item", Id::from_u128(1), vec![20.into()])
            .unwrap();
        independent
            .insert_with_id("group.Item", Id::from_u128(2), vec![2.into()])
            .unwrap();
        assert!(Arc::ptr_eq(&first.snapshot.view, &conflict.snapshot.view));
        assert!(Arc::ptr_eq(
            &first.snapshot.view,
            &independent.snapshot.view
        ));
        // Deterministic group order tests evolving-version validation without
        // relying on operating-system scheduling to pick the conflicting winner.
        let requests: Vec<_> = [first, conflict, independent]
            .into_iter()
            .map(|tx| {
                Arc::new(Request {
                    bytes: tx.bytes,
                    transaction: Mutex::new(Some(tx)),
                    result: Mutex::new(None),
                })
            })
            .collect();
        OCC_WORK.with(|work| work.set((0, 0)));
        let (results, writes) = process(&db.inner, &requests);
        let (reads, overlay_inserts) = OCC_WORK.with(std::cell::Cell::get);
        assert_eq!(
            reads, 0,
            "same-view validation must use only the group overlay"
        );
        assert!(
            overlay_inserts > 0,
            "earlier accepted writes must populate the overlay"
        );
        assert_eq!(writes, 2);
        assert!(matches!(results[0], Ok(2)));
        assert!(matches!(results[1], Err(Error::Conflict)));
        assert!(matches!(results[2], Ok(3)));
        assert_eq!(
            db.admin()
                .get("group.Item", Id::from_u128(1))
                .unwrap()
                .unwrap()
                .values,
            vec![10.into()].into_boxed_slice()
        );
        assert_eq!(db.admin().snapshot().unwrap().verify().unwrap().rows, 2);
    }
    #[test]
    fn prior_policy_change_fences_later_transaction_in_same_group() {
        let dir = tempfile::tempdir().unwrap();
        let db = fixture(&dir.path().join("db"));
        let mut grant = db.admin().transaction().unwrap();
        grant
            .grant("writer", "group.Item", Grant::read_write())
            .unwrap();
        grant.commit().unwrap();
        let mut revoke = db.admin().transaction().unwrap();
        let mut write = db.session("writer").unwrap().transaction().unwrap();
        write
            .update("group.Item", Id::from_u128(1), vec![99.into()])
            .unwrap();
        revoke.revoke("writer", "group.Item").unwrap();
        let requests: Vec<_> = [revoke, write]
            .into_iter()
            .map(|tx| {
                Arc::new(Request {
                    bytes: tx.bytes,
                    transaction: Mutex::new(Some(tx)),
                    result: Mutex::new(None),
                })
            })
            .collect();
        let (results, writes) = process(&db.inner, &requests);
        assert_eq!(writes, 1);
        assert!(results[0].is_ok());
        assert!(matches!(results[1], Err(Error::Conflict)));
        assert_eq!(
            db.admin()
                .get("group.Item", Id::from_u128(1))
                .unwrap()
                .unwrap()
                .values,
            vec![1.into()].into_boxed_slice()
        );
        assert!(matches!(
            db.session("writer")
                .unwrap()
                .get("group.Item", Id::from_u128(1)),
            Err(Error::Denied)
        ));
    }
}
