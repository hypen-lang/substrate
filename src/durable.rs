//! Durable checkpoints and bounded redo. Each checkpoint replacement, log prune,
//! and head update is one Immediate-durability redb transaction.
use crate::{
    codec,
    storage::{Op, State},
    *,
};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("liquid_log_v1");
const CONTROL: TableDefinition<&str, &[u8]> = TableDefinition::new("liquid_control_v1");
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("liquid_checkpoint_rows_v1");
const OVERLAY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("liquid_checkpoint_overlay_v3");
// A bounded materialized map, never a history of operations. Empty values are
// tombstones; nonempty values are canonical revision-independent row images.
const MAX_OVERLAY_KEYS: u64 = 4096;
const MAX_OVERLAY_BYTES: u64 = 8 * 1024 * 1024;
const MIN_COMPACTION_ROWS: u64 = 128;
const METADATA: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("liquid_checkpoint_metadata_v2");
const MANIFEST: &str = "checkpoint";
const CATALOG: &str = "catalog";
const HEAD: &str = "head";
const FORMAT: &str = "format";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointPolicy {
    pub max_log_commits: u64,
    pub max_log_bytes: u64,
}
impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            max_log_commits: 128,
            max_log_bytes: 8 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseOptions {
    /// None opts out of automatic bounds, useful for replay benchmarks.
    pub checkpoint_policy: Option<CheckpointPolicy>,
}
impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            checkpoint_policy: Some(CheckpointPolicy::default()),
        }
    }
}
impl DatabaseOptions {
    fn validate(self) -> Result<Self> {
        if self
            .checkpoint_policy
            .is_some_and(|p| p.max_log_commits == 0 || p.max_log_bytes == 0)
        {
            return Err(error("checkpoint bounds must be positive"));
        }
        Ok(self)
    }
}

/// Physical storage cache budget, independent of schema and redo semantics.
/// This is redb's cache target, not a bound on total engine/process memory.
#[derive(Clone, Copy, Debug)]
pub struct StorageOptions {
    pub cache_bytes: usize,
}
impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            cache_bytes: 64 * 1024 * 1024,
        }
    }
}
impl StorageOptions {
    fn validate(self) -> Result<Self> {
        if self.cache_bytes < 64 * 1024 {
            return Err(error("storage cache must be at least 64 KiB"));
        }
        Ok(self)
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryInfo {
    pub checkpoint_revision: u64,
    pub checkpoint_rows: u64,
    pub checkpoint_bytes: u64,
    pub replayed_commits: u64,
    pub replayed_bytes: u64,
    pub recovered_revision: u64,
}
/// Process-local diagnostics; byte counts are encoded application records, not
/// redb page writes or physical device traffic. Timings are cumulative nanoseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PersistenceStats {
    pub log_groups: u64,
    pub checkpoints: u64,
    pub redo_encoded_bytes: u64,
    pub redo_written_bytes: u64,
    pub checkpoint_written_bytes: u64,
    pub checkpoint_written_rows: u64,
    pub checkpoint_compactions: u64,
    pub checkpoint_compacted_rows: u64,
    pub checkpoint_compacted_bytes: u64,
    pub checkpoint_retained_overlay_bytes: u64,
    pub encoding_ns: u64,
    pub log_write_ns: u64,
    pub checkpoint_ns: u64,
    /// Time in redb's Immediate commit, including its internal page writes.
    /// This is not a count or isolated timing of filesystem sync syscalls.
    pub immediate_commit_ns: u64,
}

pub(crate) fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct StorageInfo {
    pub durable_revision: u64,
    pub checkpoint_revision: u64,
    pub checkpoint_rows: u64,
    pub checkpoint_bytes: u64,
    /// Materialized latest-row overrides, including tombstones. Not redo events.
    pub overlay_keys: u64,
    /// Encoded overlay values plus their 32-byte keys; bounded independently of redo.
    pub overlay_bytes: u64,
    pub log_commits: u64,
    pub log_bytes: u64,
    /// Since this database handle opened; not persisted or restored on recovery.
    pub persistence: PersistenceStats,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub revision: u64,
    pub rows: u64,
    pub bytes: u64,
    /// New checkpoint row images, not total live rows. Moving existing override
    /// images during compaction is reported separately in PersistenceStats.
    pub written_rows: u64,
    pub deleted_rows: u64,
    pub written_metadata: u64,
    pub deleted_metadata: u64,
    pub written_bytes: u64,
    pub pruned_commits: u64,
    pub pruned_bytes: u64,
}

// LCP1/2 contain seven u64 fields; LCP3 adds base/override physical accounting.
// Logical counts detect omitted visible sections, including override tombstones.
struct Manifest {
    format: u8,
    revision: u64,
    rows: u64,
    identities: u64,
    modules: u64,
    grants: u64,
    schema_grants: u64,
    bytes: u64,
    base_rows: u64,
    overlay_keys: u64,
    overlay_bytes: u64,
}
impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut out = match self.format {
            1 => b"LCP1".to_vec(),
            2 => b"LCP2".to_vec(),
            3 => b"LCP3".to_vec(),
            _ => unreachable!("validated manifest format"),
        };
        for n in [
            self.revision,
            self.rows,
            self.identities,
            self.modules,
            self.grants,
            self.schema_grants,
            self.bytes,
        ] {
            out.extend(n.to_le_bytes());
        }
        if self.format == 3 {
            for n in [self.base_rows, self.overlay_keys, self.overlay_bytes] {
                out.extend(n.to_le_bytes());
            }
        }
        out
    }
    fn decode(data: &[u8]) -> Result<Self> {
        if !((data.len() == 60 && matches!(&data[..4], b"LCP1" | b"LCP2"))
            || (data.len() == 84 && &data[..4] == b"LCP3"))
        {
            return Err(error("invalid checkpoint manifest"));
        }
        let n = data[4..]
            .chunks_exact(8)
            .map(|v| u64::from_le_bytes(v.try_into().expect("eight bytes")))
            .collect::<Vec<_>>();
        Ok(Self {
            format: data[3] - b'0',
            revision: n[0],
            rows: n[1],
            identities: n[2],
            modules: n[3],
            grants: n[4],
            schema_grants: n[5],
            bytes: n[6],
            base_rows: n.get(7).copied().unwrap_or(n[1]),
            overlay_keys: n.get(8).copied().unwrap_or(0),
            overlay_bytes: n.get(9).copied().unwrap_or(0),
        })
    }
    fn validate(&self, state: &State, bytes: u64) -> Result<()> {
        if self.rows != state.families.values().map(|f| f.len() as u64).sum::<u64>()
            || self.identities != state.identities.len() as u64
            || self.modules != state.modules.len() as u64
            || self.grants != state.grants.len() as u64
            || self.schema_grants != state.schema_grants.len() as u64
            || self.bytes != bytes
        {
            return Err(error(
                "checkpoint counts or byte length do not match manifest",
            ));
        }
        Ok(())
    }
}
fn error(s: impl std::fmt::Display) -> Error {
    Error::Storage(s.to_string())
}
fn row_key(tid: Id, id: Id) -> [u8; 32] {
    let mut k = [0; 32];
    k[..16].copy_from_slice(tid.0.as_bytes());
    k[16..].copy_from_slice(id.0.as_bytes());
    k
}
fn validate_row_key(key: &[u8], state: &State) -> Result<()> {
    if key.len() != 32 {
        return Err(error("invalid checkpoint row key"));
    }
    let tid = Id(uuid::Uuid::from_slice(&key[..16]).map_err(error)?);
    let id = Id(uuid::Uuid::from_slice(&key[16..]).map_err(error)?);
    if !state.relations.contains_key(&tid) || !state.identities.contains(&id) {
        return Err(error("unknown checkpoint row identity/type"));
    }
    Ok(())
}
#[derive(Default)]
struct Compactions {
    count: u64,
    rows: u64,
    bytes: u64,
    retained_bytes: u64,
}
struct PartialCompaction<'a> {
    pending: &'a BTreeSet<(Id, Id)>,
    projected_keys: u64,
    projected_bytes: u64,
}
type RowTable<'tx> = redb::Table<'tx, &'static [u8], &'static [u8]>;
fn compact_overlay<'tx>(
    tx: &'tx redb::WriteTransaction,
    rows: &mut redb::Table<&[u8], &[u8]>,
    mut overlay: RowTable<'tx>,
    stats: &mut Compactions,
    mut partial: Option<PartialCompaction<'_>>,
) -> Result<(RowTable<'tx>, u64, u64)> {
    failpoint("checkpoint_before_compaction");
    let partial_mode = partial.is_some();
    let mut removed_keys = Vec::new();
    let mut retained_keys = 0;
    let mut retained_bytes = 0;
    let mut flushed = 0;
    for entry in overlay.iter().map_err(error)? {
        let (key, value) = entry.map_err(error)?;
        let flush = if let Some(plan) = &partial {
            let key = key.value();
            if key.len() != 32 {
                return Err(error("invalid compaction row key"));
            }
            let tid = Id(uuid::Uuid::from_slice(&key[..16]).map_err(error)?);
            let id = Id(uuid::Uuid::from_slice(&key[16..]).map_err(error)?);
            !plan.pending.contains(&(tid, id))
                && (flushed < MIN_COMPACTION_ROWS
                    || plan.projected_keys > MAX_OVERLAY_KEYS
                    || plan.projected_bytes > MAX_OVERLAY_BYTES)
        } else {
            true
        };
        if flush {
            if value.value().is_empty() {
                rows.remove(key.value()).map_err(error)?;
            } else {
                rows.insert(key.value(), value.value()).map_err(error)?;
            }
            if let Some(plan) = &mut partial {
                plan.projected_keys = plan
                    .projected_keys
                    .checked_sub(1)
                    .ok_or_else(|| error("compaction count underflow"))?;
                plan.projected_bytes = plan
                    .projected_bytes
                    .checked_sub(32 + value.value().len() as u64)
                    .ok_or_else(|| error("compaction bytes underflow"))?;
            }
            stats.rows += 1;
            stats.bytes += value.value().len() as u64;
            if partial_mode {
                removed_keys.push(<[u8; 32]>::try_from(key.value()).map_err(error)?);
            }
            flushed += 1;
            failpoint("checkpoint_during_compaction");
        } else {
            retained_bytes += 32 + value.value().len() as u64;
            retained_keys += 1;
        }
    }
    if partial.is_some_and(|plan| {
        plan.projected_keys > MAX_OVERLAY_KEYS || plan.projected_bytes > MAX_OVERLAY_BYTES
    }) {
        return Err(error("compaction did not free sufficient capacity"));
    }
    if partial_mode {
        // The iterator and its page guards are gone. Unlike retain(false),
        // individual remove calls can reuse this transaction's changed pages.
        // Keep untouched remainder pages instead of copying/rebuilding them.
        for key in removed_keys {
            if overlay.remove(key.as_slice()).map_err(error)?.is_none() {
                return Err(error("compaction selected row disappeared"));
            }
            failpoint("checkpoint_during_overlay_removal");
        }
    } else {
        // Full/bulk merges do not modify the old overlay. Atomically dropping
        // its entire tree avoids per-key COW deletions and retains pinned readers.
        drop(overlay);
        if !tx.delete_table(OVERLAY).map_err(error)? {
            return Err(error("checkpoint overlay table missing"));
        }
        overlay = tx.open_table(OVERLAY).map_err(error)?;
    }
    stats.count += 1;
    failpoint("checkpoint_after_compaction");
    Ok((overlay, retained_keys, retained_bytes))
}

/// Coalesced changes since the last checkpoint. Reconstructed from the bounded
/// redo tail on open, so no independent dirty-state durability protocol is needed.
#[derive(Clone, Default)]
struct Pending {
    rows: BTreeSet<(Id, Id)>,
    metadata: BTreeMap<Vec<u8>, Op>,
}
impl Pending {
    fn record(&mut self, op: &Op) {
        match op {
            Op::Put(tid, row) => {
                self.rows.insert((*tid, row.id));
            }
            Op::Delete(tid, id) => {
                self.rows.insert((*tid, *id));
            }
            _ => {
                self.metadata.insert(
                    codec::metadata_key(op).expect("validated metadata"),
                    op.clone(),
                );
            }
        }
    }
    fn full(state: &State) -> Self {
        let mut out = Self::default();
        for module in state.modules.values() {
            out.record(&Op::Install((**module).clone()));
        }
        for id in &state.identities {
            out.record(&Op::Identity(*id));
        }
        for ((p, tid), g) in state.grants.iter() {
            out.record(&Op::Grant(p.clone(), *tid, Some(g.declaration.clone())));
        }
        for (p, name) in &state.schema_grants {
            out.record(&Op::SchemaGrant(p.clone(), name.clone()));
        }
        for (tid, family) in &state.families {
            for id in family.keys() {
                out.rows.insert((*tid, *id));
            }
        }
        out
    }
}

pub(crate) struct Durable {
    db: redb::Database,
    options: DatabaseOptions,
    checkpoint_format: u8,
    pending: Pending,
    pub info: StorageInfo,
    pub recovery: RecoveryInfo,
    #[cfg(test)]
    force_v2_checkpoint: bool,
    #[cfg(test)]
    force_full_compaction: bool,
}
impl Durable {
    pub fn open(path: &Path, options: DatabaseOptions) -> Result<(Self, State)> {
        Self::open_with_storage(path, options, StorageOptions::default())
    }
    pub fn open_with_storage(
        path: &Path,
        options: DatabaseOptions,
        storage: StorageOptions,
    ) -> Result<(Self, State)> {
        let options = options.validate()?;
        let storage = storage.validate()?;
        let db = redb::Database::builder()
            .set_cache_size(storage.cache_bytes)
            .create(path)
            .map_err(error)?;
        Self::recover(db, options)
    }
    pub(crate) fn recover(db: redb::Database, options: DatabaseOptions) -> Result<(Self, State)> {
        // A paged database is authoritative current storage, not a resident
        // checkpoint. Reject it before creating any resident-format tables.
        {
            let read = db.begin_read().map_err(error)?;
            if read
                .list_tables()
                .map_err(error)?
                .any(|table| table.name() == "liquid_paged_control_v1")
            {
                return Err(error("paged current storage requires PagedDatabase::open"));
            }
        }
        // Table creation is atomic too. Existing v1 log-only files remain readable.
        let tx = db.begin_write().map_err(error)?;
        tx.open_table(LOG).map_err(error)?;
        tx.open_table(CONTROL).map_err(error)?;
        tx.open_table(ROWS).map_err(error)?;
        tx.open_table(METADATA).map_err(error)?;
        tx.open_table(OVERLAY).map_err(error)?;
        tx.commit().map_err(error)?;
        let mut state = State::default();
        let mut info = StorageInfo::default();
        let legacy;
        let mut checkpoint_format = 2;
        let mut pending = Pending::default();
        {
            let tx = db.begin_read().map_err(error)?;
            let control = tx.open_table(CONTROL).map_err(error)?;
            let checkpoint_rows = tx.open_table(ROWS).map_err(error)?;
            let metadata = tx.open_table(METADATA).map_err(error)?;
            let overlay = tx.open_table(OVERLAY).map_err(error)?;
            let head = control
                .get(HEAD)
                .map_err(error)?
                .map(|v| v.value().to_vec());
            let format = control
                .get(FORMAT)
                .map_err(error)?
                .map(|v| v.value().to_vec());
            if format
                .as_deref()
                .is_some_and(|v| v != b"LDB1" && v != b"LDB2" && v != b"LDB3")
                || (format.is_some() != head.is_some())
            {
                return Err(error("missing or unsupported durable format/head"));
            }
            legacy = head.is_none();
            if let Some(raw) = control.get(MANIFEST).map_err(error)? {
                if legacy {
                    return Err(error("checkpoint has no durable head"));
                }
                let manifest = Manifest::decode(raw.value())?;
                let catalog = control
                    .get(CATALOG)
                    .map_err(error)?
                    .ok_or_else(|| error("missing checkpoint catalog"))?;
                checkpoint_format = manifest.format;
                if manifest.format != 3 && !overlay.is_empty().map_err(error)? {
                    return Err(error("orphan v3 overlay"));
                }
                let mut byte_count = catalog.value().len() as u64;
                if manifest.format == 1 {
                    if !metadata.is_empty().map_err(error)? {
                        return Err(error("orphan v2 metadata"));
                    }
                    codec::replay_catalog(catalog.value(), manifest.revision, &mut state)?;
                } else {
                    let (storage, catalog_magic): (&[u8], &[u8]) = if manifest.format == 3 {
                        (b"LDB3", b"LCC3")
                    } else {
                        (b"LDB2", b"LCC2")
                    };
                    if format.as_deref() != Some(storage) || catalog.value() != catalog_magic {
                        return Err(error("invalid checkpoint catalog version"));
                    }
                    for entry in metadata.iter().map_err(error)? {
                        let (key, value) = entry.map_err(error)?;
                        codec::replay_metadata_entry(value.value(), key.value(), &mut state)?;
                        byte_count = byte_count
                            .checked_add(value.value().len() as u64)
                            .ok_or_else(|| error("checkpoint size overflow"))?;
                    }
                }
                let mut overrides = BTreeMap::new();
                if manifest.format == 3 {
                    if manifest.overlay_keys > MAX_OVERLAY_KEYS
                        || manifest.overlay_bytes > MAX_OVERLAY_BYTES
                        || manifest.base_rows != checkpoint_rows.len().map_err(error)?
                        || manifest.overlay_keys != overlay.len().map_err(error)?
                    {
                        return Err(error("invalid checkpoint overlay bounds/counts"));
                    }
                    let mut overlay_bytes = 0u64;
                    for entry in overlay.iter().map_err(error)? {
                        let (key, value) = entry.map_err(error)?;
                        if key.value().len() != 32 {
                            return Err(error("invalid checkpoint overlay key"));
                        }
                        overlay_bytes = overlay_bytes
                            .checked_add(32 + value.value().len() as u64)
                            .filter(|bytes| *bytes <= MAX_OVERLAY_BYTES)
                            .ok_or_else(|| error("checkpoint overlay byte bound exceeded"))?;
                        overrides.insert(key.value().to_vec(), value.value().to_vec());
                    }
                    if overlay_bytes != manifest.overlay_bytes {
                        return Err(error("checkpoint overlay byte count mismatch"));
                    }
                    info.overlay_keys = manifest.overlay_keys;
                    info.overlay_bytes = manifest.overlay_bytes;
                }
                for entry in checkpoint_rows.iter().map_err(error)? {
                    let (key, value) = entry.map_err(error)?;
                    if value.value().is_empty() {
                        return Err(error("empty base checkpoint row"));
                    }
                    let replacement = overrides.remove(key.value());
                    let value = replacement.as_deref().unwrap_or(value.value());
                    if value.is_empty() {
                        // Tombstones are canonical only for an existing base key.
                        // Validate its identity/type even though no row is exposed.
                        validate_row_key(key.value(), &state)?;
                        continue;
                    }
                    codec::replay_checkpoint_row(
                        value,
                        if manifest.format == 1 {
                            manifest.revision
                        } else {
                            0
                        },
                        key.value(),
                        &mut state,
                    )?;
                    byte_count = byte_count
                        .checked_add(value.len() as u64)
                        .ok_or_else(|| error("checkpoint size overflow"))?;
                }
                for (key, value) in overrides {
                    if value.is_empty() {
                        return Err(error("checkpoint tombstone has no base row"));
                    }
                    codec::replay_checkpoint_row(&value, 0, &key, &mut state)?;
                    byte_count = byte_count
                        .checked_add(value.len() as u64)
                        .ok_or_else(|| error("checkpoint size overflow"))?;
                }
                manifest.validate(&state, byte_count)?;
                state.revision = manifest.revision;
                state.validate_recovered()?;
                info.checkpoint_revision = manifest.revision;
                info.checkpoint_rows = manifest.rows;
                info.checkpoint_bytes = manifest.bytes;
            } else if control.get(CATALOG).map_err(error)?.is_some()
                || !checkpoint_rows.is_empty().map_err(error)?
                || !metadata.is_empty().map_err(error)?
                || !overlay.is_empty().map_err(error)?
            {
                return Err(error("orphan checkpoint sections"));
            }
            let log = tx.open_table(LOG).map_err(error)?;
            for entry in log.iter().map_err(error)? {
                let (revision, bytes) = entry.map_err(error)?;
                if Some(revision.value()) != state.revision.checked_add(1) {
                    return Err(error("inconsistent redo sequence"));
                }
                codec::replay_observed(bytes.value(), revision.value(), &mut state, &mut |op| {
                    pending.record(op)
                })?;
                state.revision = revision.value();
                info.log_commits += 1;
                info.log_bytes += bytes.value().len() as u64;
            }
            if let Some(head) = head {
                let head = u64::from_le_bytes(
                    head.as_slice()
                        .try_into()
                        .map_err(|_| error("invalid durable head"))?,
                );
                if head != state.revision {
                    return Err(error(
                        "durable head disagrees with checkpoint and redo tail",
                    ));
                }
            }
        }
        state.validate_recovered()?;
        info.durable_revision = state.revision;
        let recovery = RecoveryInfo {
            checkpoint_revision: info.checkpoint_revision,
            checkpoint_rows: info.checkpoint_rows,
            checkpoint_bytes: info.checkpoint_bytes,
            replayed_commits: info.log_commits,
            replayed_bytes: info.log_bytes,
            recovered_revision: state.revision,
        };
        let mut durable = Self {
            db,
            options,
            checkpoint_format,
            pending,
            info,
            recovery,
            #[cfg(test)]
            force_v2_checkpoint: false,
            #[cfg(test)]
            force_full_compaction: false,
        };
        if legacy {
            // Upgrade after successfully validating the old log. No logical revision changes.
            let tx = durable.db.begin_write().map_err(error)?;
            tx.open_table(CONTROL)
                .map_err(error)?
                .insert(HEAD, state.revision.to_le_bytes().as_slice())
                .map_err(error)?;
            tx.open_table(CONTROL)
                .map_err(error)?
                .insert(FORMAT, b"LDB1".as_slice())
                .map_err(error)?;
            tx.commit().map_err(error)?;
        }
        // First open of a legacy/unbounded file may replay a long history once.
        if durable.exceeds_bounds(durable.info.log_commits, durable.info.log_bytes) {
            durable.checkpoint(&state)?;
        }
        Ok((durable, state))
    }
    fn exceeds_bounds(&self, commits: u64, bytes: u64) -> bool {
        self.options
            .checkpoint_policy
            .is_some_and(|p| commits >= p.max_log_commits || bytes >= p.max_log_bytes)
    }
    #[cfg(test)]
    pub fn append(&mut self, revision: u64, ops: &[Op], state: &State) -> Result<()> {
        self.append_group(&[(revision, ops)], state)
    }
    /// One Immediate redb transaction persists all independent accepted revisions.
    pub(crate) fn append_group(&mut self, records: &[(u64, &[Op])], state: &State) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut expected = self.info.durable_revision;
        for (revision, _) in records {
            expected = expected
                .checked_add(1)
                .ok_or_else(|| error("revision exhausted"))?;
            if *revision != expected {
                return Err(error("invalid append revision"));
            }
        }
        if state.revision != expected {
            return Err(error("invalid group head"));
        }
        let count = self
            .info
            .log_commits
            .checked_add(records.len() as u64)
            .ok_or_else(|| error("log count overflow"))?;
        // A count-bound group goes directly to its atomic checkpoint. Encoding
        // redo here would allocate/checksum records that are never written.
        if self.exceeds_bounds(count, self.info.log_bytes) {
            self.write_checkpoint(state, records.iter().flat_map(|(_, ops)| ops.iter()))?;
            return Ok(());
        }
        let encoding_start = Instant::now();
        let mut encoded = Vec::with_capacity(records.len());
        let mut size = self.info.log_bytes;
        for (revision, ops) in records {
            let bytes = codec::encode(*revision, ops, state)?;
            size = size
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| error("log size overflow"))?;
            encoded.push((*revision, bytes));
        }
        self.info.persistence.encoding_ns = self
            .info
            .persistence
            .encoding_ns
            .saturating_add(elapsed_ns(encoding_start));
        let encoded_bytes = size - self.info.log_bytes;
        self.info.persistence.redo_encoded_bytes = self
            .info
            .persistence
            .redo_encoded_bytes
            .saturating_add(encoded_bytes);
        if self.exceeds_bounds(count, size) {
            // Fold the entire group into the same atomic checkpoint when crossing a bound.
            self.write_checkpoint(state, records.iter().flat_map(|(_, ops)| ops.iter()))?;
        } else {
            let write_start = Instant::now();
            let mut tx = self.db.begin_write().map_err(error)?;
            tx.set_durability(redb::Durability::Immediate)
                .map_err(error)?;
            {
                let mut log = tx.open_table(LOG).map_err(error)?;
                for (revision, bytes) in &encoded {
                    log.insert(*revision, bytes.as_slice()).map_err(error)?;
                }
            }
            tx.open_table(CONTROL)
                .map_err(error)?
                .insert(HEAD, expected.to_le_bytes().as_slice())
                .map_err(error)?;
            failpoint("append_before_commit");
            let commit_start = Instant::now();
            tx.commit().map_err(error)?;
            self.info.persistence.immediate_commit_ns = self
                .info
                .persistence
                .immediate_commit_ns
                .saturating_add(elapsed_ns(commit_start));
            self.info.persistence.log_write_ns = self
                .info
                .persistence
                .log_write_ns
                .saturating_add(elapsed_ns(write_start));
            self.info.persistence.log_groups = self.info.persistence.log_groups.saturating_add(1);
            self.info.persistence.redo_written_bytes = self
                .info
                .persistence
                .redo_written_bytes
                .saturating_add(encoded_bytes);
            failpoint("append_after_commit");
            for (_, ops) in records {
                for op in *ops {
                    self.pending.record(op);
                }
            }
            self.info.durable_revision = expected;
            self.info.log_commits = count;
            self.info.log_bytes = size;
        }
        Ok(())
    }
    pub fn checkpoint(&mut self, state: &State) -> Result<CheckpointInfo> {
        if state.revision != self.info.durable_revision {
            return Err(error("checkpoint must match durable head"));
        }
        self.write_checkpoint(state, std::iter::empty())
    }
    fn write_checkpoint<'a>(
        &mut self,
        state: &State,
        extra: impl IntoIterator<Item = &'a Op>,
    ) -> Result<CheckpointInfo> {
        let checkpoint_start = Instant::now();
        failpoint("checkpoint_before_write");
        let use_overlay = true;
        #[cfg(test)]
        let use_overlay = use_overlay && !self.force_v2_checkpoint;
        let migrating = self.checkpoint_format == 1;
        let mut pending = if migrating {
            Pending::full(state)
        } else {
            self.pending.clone()
        };
        if !migrating {
            for op in extra {
                pending.record(op);
            }
        }
        // Large batches already amortize base-page changes. Do not route a bulk
        // load through repeated fill/merge cycles of the small override table.
        let mut direct_rows = migrating
            || self.info.checkpoint_bytes == 0
            || !use_overlay
            || pending.rows.len() > MAX_OVERLAY_KEYS as usize;
        let mut manifest = Manifest {
            format: if use_overlay { 3 } else { 2 },
            revision: state.revision,
            rows: 0,
            identities: state.identities.len() as u64,
            modules: state.modules.len() as u64,
            grants: state.grants.len() as u64,
            schema_grants: state.schema_grants.len() as u64,
            bytes: if migrating || self.info.checkpoint_bytes == 0 {
                4
            } else {
                self.info.checkpoint_bytes
            },
            base_rows: 0,
            overlay_keys: self.info.overlay_keys,
            overlay_bytes: self.info.overlay_bytes,
        };
        let mut compactions = Compactions::default();
        let mut result = CheckpointInfo {
            revision: state.revision,
            rows: 0,
            bytes: 0,
            written_rows: 0,
            deleted_rows: 0,
            written_metadata: 0,
            deleted_metadata: 0,
            written_bytes: 0,
            pruned_commits: self.info.log_commits,
            pruned_bytes: self.info.log_bytes,
        };
        let mut tx = self.db.begin_write().map_err(error)?;
        tx.set_durability(redb::Durability::Immediate)
            .map_err(error)?;
        {
            let mut rows = tx.open_table(ROWS).map_err(error)?;
            let mut metadata = tx.open_table(METADATA).map_err(error)?;
            let mut overlay = tx.open_table(OVERLAY).map_err(error)?;
            // Preflight without touching overlay pages. redb 3.x table deletion
            // cannot reclaim pages modified earlier in the same transaction.
            // Also ensures a large pending batch is written directly only once.
            let mut projected_keys = manifest.overlay_keys;
            let mut projected_bytes = manifest.overlay_bytes;
            let mut fresh_bytes = 0u64;
            if !direct_rows {
                for (tid, id) in &pending.rows {
                    let key = row_key(*tid, *id);
                    let row = state.row(*tid, *id);
                    let length = row
                        .map(|row| codec::encoded_checkpoint_row_len(*tid, row, state))
                        .transpose()?
                        .unwrap_or(0) as u64;
                    let retained =
                        row.is_some() || rows.get(key.as_slice()).map_err(error)?.is_some();
                    let previous = overlay.get(key.as_slice()).map_err(error)?;
                    if let Some(previous) = previous {
                        projected_keys = projected_keys
                            .checked_sub(1)
                            .ok_or_else(|| error("overlay count underflow"))?;
                        projected_bytes = projected_bytes
                            .checked_sub(32 + previous.value().len() as u64)
                            .ok_or_else(|| error("overlay size underflow"))?;
                    }
                    // A flush can move an override-only row into the base before
                    // its pending deletion; reserve a tombstone key conservatively.
                    fresh_bytes = fresh_bytes
                        .checked_add(32 + length)
                        .ok_or_else(|| error("overlay size overflow"))?;
                    if retained {
                        projected_keys += 1;
                        projected_bytes = projected_bytes
                            .checked_add(32 + length)
                            .ok_or_else(|| error("overlay size overflow"))?;
                    }
                }
                direct_rows = fresh_bytes > MAX_OVERLAY_BYTES;
            }
            if migrating {
                rows.retain(|_, _| false).map_err(error)?;
                metadata.retain(|_, _| false).map_err(error)?;
            }
            if (direct_rows
                || projected_keys > MAX_OVERLAY_KEYS
                || projected_bytes > MAX_OVERLAY_BYTES)
                && !overlay.is_empty().map_err(error)?
            {
                let partial = !direct_rows;
                #[cfg(test)]
                let partial = partial && !self.force_full_compaction;
                (overlay, manifest.overlay_keys, manifest.overlay_bytes) = compact_overlay(
                    &tx,
                    &mut rows,
                    overlay,
                    &mut compactions,
                    partial.then_some(PartialCompaction {
                        pending: &pending.rows,
                        projected_keys,
                        projected_bytes,
                    }),
                )?;
            }
            // Retain the old crash boundary even when an incremental checkpoint
            // does not clear tables. All changes still share one redb transaction.
            failpoint("checkpoint_after_clear");
            tx.open_table(CONTROL)
                .map_err(error)?
                .insert(
                    CATALOG,
                    if use_overlay {
                        b"LCC3".as_slice()
                    } else {
                        b"LCC2".as_slice()
                    },
                )
                .map_err(error)?;
            for (key, op) in &pending.metadata {
                let (old_bytes, new_bytes) = if matches!(op, Op::Grant(_, _, None)) {
                    let old = metadata.remove(key.as_slice()).map_err(error)?;
                    result.deleted_metadata += u64::from(old.is_some());
                    (old.map(|v| v.value().len() as u64).unwrap_or(0), 0)
                } else {
                    let bytes = codec::encode(0, std::slice::from_ref(op), state)?;
                    let old = metadata
                        .insert(key.as_slice(), bytes.as_slice())
                        .map_err(error)?;
                    result.written_metadata += 1;
                    result.written_bytes += bytes.len() as u64;
                    (
                        old.map(|v| v.value().len() as u64).unwrap_or(0),
                        bytes.len() as u64,
                    )
                };
                manifest.bytes = manifest
                    .bytes
                    .checked_sub(old_bytes)
                    .and_then(|n| n.checked_add(new_bytes))
                    .ok_or_else(|| error("checkpoint byte accounting"))?;
            }
            failpoint("checkpoint_after_catalog");
            for (tid, id) in &pending.rows {
                let key = row_key(*tid, *id);
                let bytes = state
                    .row(*tid, *id)
                    .map(|row| codec::encode_checkpoint_row(*tid, row, state))
                    .transpose()?;
                let new_bytes = bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
                let old_bytes = if direct_rows {
                    let old = if let Some(bytes) = &bytes {
                        rows.insert(key.as_slice(), bytes.as_slice())
                            .map_err(error)?
                    } else {
                        rows.remove(key.as_slice()).map_err(error)?
                    };
                    old.map_or(0, |value| value.value().len() as u64)
                } else {
                    let previous_override = overlay.get(key.as_slice()).map_err(error)?;
                    let base = if previous_override.is_none() || bytes.is_none() {
                        rows.get(key.as_slice()).map_err(error)?
                    } else {
                        None
                    };
                    let old_bytes = previous_override
                        .as_ref()
                        .or(base.as_ref())
                        .map_or(0, |value| value.value().len() as u64);
                    let base_exists = base.is_some();
                    drop(base);
                    drop(previous_override);
                    // A deleted row with no base image needs no tombstone. Each
                    // identity otherwise has exactly one newest materialized image.
                    let old = if bytes.is_some() || base_exists {
                        let old = overlay
                            .insert(key.as_slice(), bytes.as_deref().unwrap_or(&[]))
                            .map_err(error)?;
                        manifest.overlay_keys += 1;
                        manifest.overlay_bytes = manifest
                            .overlay_bytes
                            .checked_add(32 + new_bytes)
                            .ok_or_else(|| error("overlay size overflow"))?;
                        old.map(|value| value.value().len() as u64)
                    } else {
                        overlay
                            .remove(key.as_slice())
                            .map_err(error)?
                            .map(|value| value.value().len() as u64)
                    };
                    if let Some(old) = old {
                        manifest.overlay_keys = manifest
                            .overlay_keys
                            .checked_sub(1)
                            .ok_or_else(|| error("overlay count underflow"))?;
                        manifest.overlay_bytes = manifest
                            .overlay_bytes
                            .checked_sub(32 + old)
                            .ok_or_else(|| error("overlay size underflow"))?;
                    }
                    old_bytes
                };
                if bytes.is_some() {
                    result.written_rows += 1;
                    result.written_bytes += new_bytes;
                } else {
                    result.deleted_rows += u64::from(old_bytes != 0);
                }
                manifest.bytes = manifest
                    .bytes
                    .checked_sub(old_bytes)
                    .and_then(|n| n.checked_add(new_bytes))
                    .ok_or_else(|| error("checkpoint byte accounting"))?;
                failpoint("checkpoint_during_rows");
            }
            manifest.rows = state
                .families
                .values()
                .map(|family| family.len() as u64)
                .sum();
            manifest.base_rows = rows.len().map_err(error)?;
            if manifest.overlay_keys != overlay.len().map_err(error)? {
                return Err(error("checkpoint overlay accounting mismatch"));
            }
            if manifest.overlay_keys > MAX_OVERLAY_KEYS
                || manifest.overlay_bytes > MAX_OVERLAY_BYTES
            {
                return Err(error("checkpoint overlay bound exceeded"));
            }
        }
        {
            let mut control = tx.open_table(CONTROL).map_err(error)?;
            control
                .insert(MANIFEST, manifest.encode().as_slice())
                .map_err(error)?;
            control
                .insert(HEAD, state.revision.to_le_bytes().as_slice())
                .map_err(error)?;
            control
                .insert(
                    FORMAT,
                    if use_overlay {
                        b"LDB3".as_slice()
                    } else {
                        b"LDB2".as_slice()
                    },
                )
                .map_err(error)?;
        }
        failpoint("checkpoint_after_manifest");
        tx.open_table(LOG)
            .map_err(error)?
            .retain(|_, _| false)
            .map_err(error)?;
        failpoint("checkpoint_after_prune");
        failpoint("checkpoint_before_commit");
        let commit_start = Instant::now();
        tx.commit().map_err(error)?;
        self.info.persistence.immediate_commit_ns = self
            .info
            .persistence
            .immediate_commit_ns
            .saturating_add(elapsed_ns(commit_start));
        failpoint("checkpoint_after_commit");
        result.rows = manifest.rows;
        result.bytes = manifest.bytes;
        let mut persistence = self.info.persistence.clone();
        persistence.checkpoints = persistence.checkpoints.saturating_add(1);
        persistence.checkpoint_written_bytes = persistence
            .checkpoint_written_bytes
            .saturating_add(result.written_bytes);
        persistence.checkpoint_written_rows = persistence
            .checkpoint_written_rows
            .saturating_add(result.written_rows);
        persistence.checkpoint_compactions = persistence
            .checkpoint_compactions
            .saturating_add(compactions.count);
        persistence.checkpoint_compacted_rows = persistence
            .checkpoint_compacted_rows
            .saturating_add(compactions.rows);
        persistence.checkpoint_compacted_bytes = persistence
            .checkpoint_compacted_bytes
            .saturating_add(compactions.bytes);
        persistence.checkpoint_retained_overlay_bytes = persistence
            .checkpoint_retained_overlay_bytes
            .saturating_add(compactions.retained_bytes);
        persistence.checkpoint_ns = persistence
            .checkpoint_ns
            .saturating_add(elapsed_ns(checkpoint_start));
        self.info = StorageInfo {
            durable_revision: state.revision,
            checkpoint_revision: state.revision,
            checkpoint_rows: manifest.rows,
            checkpoint_bytes: manifest.bytes,
            overlay_keys: manifest.overlay_keys,
            overlay_bytes: manifest.overlay_bytes,
            log_commits: 0,
            log_bytes: 0,
            persistence,
        };
        self.checkpoint_format = manifest.format;
        self.pending = Pending::default();
        Ok(result)
    }
}

// Fault injection is absent from production binaries. Unit-test subprocesses exit
// without destructors at exact publication boundaries.
#[cfg(test)]
pub(crate) fn failpoint(name: &str) {
    if std::env::var("SUBSTRATE_TEST_CRASH_POINT").is_ok_and(|v| v == name) {
        std::process::exit(91);
    }
}
#[cfg(not(test))]
pub(crate) fn failpoint(_: &str) {}

#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "durable_io_probe.rs"]
mod io_probe;

impl Durable {
    pub(crate) fn replication_tail(
        &self,
        after: u64,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        if after < self.info.checkpoint_revision || after > self.info.durable_revision {
            return Err(error(
                "replication history unavailable; create a fresh seed",
            ));
        }
        if after == self.info.durable_revision {
            return Ok(Vec::new());
        }
        let read = self.db.begin_read().map_err(error)?;
        let log = read.open_table(LOG).map_err(error)?;
        let mut records = Vec::new();
        let mut size = 0usize;
        for entry in log.range((after + 1)..).map_err(error)? {
            let (revision, bytes) = entry.map_err(error)?;
            if records.len() == max_count {
                break;
            }
            let len = bytes.value().len();
            if len > max_bytes - size {
                if records.is_empty() {
                    return Err(error("replication record exceeds packet budget; reseed"));
                }
                break;
            }
            if revision.value() != after + records.len() as u64 + 1 {
                return Err(error("replication redo gap"));
            }
            records.push((revision.value(), bytes.value().to_vec()));
            size += len;
        }
        if records.is_empty() {
            return Err(error("replication redo unavailable; reseed"));
        }
        Ok(records)
    }
}
