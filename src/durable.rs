//! Durable checkpoints and bounded redo. Each checkpoint replacement, log prune,
//! and head update is one Immediate-durability redb transaction.
use crate::{
    codec,
    storage::{Op, State},
    *,
};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("liquid_log_v1");
const CONTROL: TableDefinition<&str, &[u8]> = TableDefinition::new("liquid_control_v1");
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("liquid_checkpoint_rows_v1");
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryInfo {
    pub checkpoint_revision: u64,
    pub checkpoint_rows: u64,
    pub checkpoint_bytes: u64,
    pub replayed_commits: u64,
    pub replayed_bytes: u64,
    pub recovered_revision: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageInfo {
    pub durable_revision: u64,
    pub checkpoint_revision: u64,
    pub checkpoint_rows: u64,
    pub checkpoint_bytes: u64,
    pub log_commits: u64,
    pub log_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub revision: u64,
    pub rows: u64,
    pub bytes: u64,
    /// Physical checkpoint entries changed by this operation, not total live rows.
    pub written_rows: u64,
    pub deleted_rows: u64,
    pub written_metadata: u64,
    pub deleted_metadata: u64,
    pub written_bytes: u64,
    pub pruned_commits: u64,
    pub pruned_bytes: u64,
}

// Manifest is LCP1 + seven little-endian u64 fields. Counts detect omitted sections.
struct Manifest {
    format: u8,
    revision: u64,
    rows: u64,
    identities: u64,
    modules: u64,
    grants: u64,
    schema_grants: u64,
    bytes: u64,
}
impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut out = if self.format == 1 {
            b"LCP1".to_vec()
        } else {
            b"LCP2".to_vec()
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
        out
    }
    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() != 60 || !matches!(&data[..4], b"LCP1" | b"LCP2") {
            return Err(error("invalid checkpoint manifest"));
        }
        let n = data[4..]
            .chunks_exact(8)
            .map(|v| u64::from_le_bytes(v.try_into().expect("eight bytes")))
            .collect::<Vec<_>>();
        Ok(Self {
            format: if &data[..4] == b"LCP1" { 1 } else { 2 },
            revision: n[0],
            rows: n[1],
            identities: n[2],
            modules: n[3],
            grants: n[4],
            schema_grants: n[5],
            bytes: n[6],
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
        for ((p, tid), g) in &state.grants {
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
}
impl Durable {
    pub fn open(path: &Path, options: DatabaseOptions) -> Result<(Self, State)> {
        let options = options.validate()?;
        let db = redb::Database::create(path).map_err(error)?;
        Self::recover(db, options)
    }
    pub(crate) fn recover(db: redb::Database, options: DatabaseOptions) -> Result<(Self, State)> {
        // Table creation is atomic too. Existing v1 log-only files remain readable.
        let tx = db.begin_write().map_err(error)?;
        tx.open_table(LOG).map_err(error)?;
        tx.open_table(CONTROL).map_err(error)?;
        tx.open_table(ROWS).map_err(error)?;
        tx.open_table(METADATA).map_err(error)?;
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
                .is_some_and(|v| v != b"LDB1" && v != b"LDB2")
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
                let mut byte_count = catalog.value().len() as u64;
                if manifest.format == 1 {
                    if !metadata.is_empty().map_err(error)? {
                        return Err(error("orphan v2 metadata"));
                    }
                    codec::replay_catalog(catalog.value(), manifest.revision, &mut state)?;
                } else {
                    if format.as_deref() != Some(b"LDB2") || catalog.value() != b"LCC2" {
                        return Err(error("invalid v2 checkpoint catalog"));
                    }
                    for entry in metadata.iter().map_err(error)? {
                        let (key, value) = entry.map_err(error)?;
                        codec::replay_metadata_entry(value.value(), key.value(), &mut state)?;
                        byte_count = byte_count
                            .checked_add(value.value().len() as u64)
                            .ok_or_else(|| error("checkpoint size overflow"))?;
                    }
                }
                for entry in checkpoint_rows.iter().map_err(error)? {
                    let (key, value) = entry.map_err(error)?;
                    codec::replay_checkpoint_row(
                        value.value(),
                        if manifest.format == 1 {
                            manifest.revision
                        } else {
                            0
                        },
                        key.value(),
                        &mut state,
                    )?;
                    byte_count = byte_count
                        .checked_add(value.value().len() as u64)
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
        let mut encoded = Vec::with_capacity(records.len());
        let mut size = self.info.log_bytes;
        for (revision, ops) in records {
            expected = expected
                .checked_add(1)
                .ok_or_else(|| error("revision exhausted"))?;
            if *revision != expected {
                return Err(error("invalid append revision"));
            }
            let bytes = codec::encode(*revision, ops, state)?;
            size = size
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| error("log size overflow"))?;
            encoded.push((*revision, bytes));
        }
        if state.revision != expected {
            return Err(error("invalid group head"));
        }
        let count = self
            .info
            .log_commits
            .checked_add(records.len() as u64)
            .ok_or_else(|| error("log count overflow"))?;
        if self.exceeds_bounds(count, size) {
            // Fold the entire group into the same atomic checkpoint when crossing a bound.
            let ops: Vec<Op> = records
                .iter()
                .flat_map(|(_, ops)| ops.iter().cloned())
                .collect();
            self.write_checkpoint(state, &ops)?;
        } else {
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
            tx.commit().map_err(error)?;
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
        self.write_checkpoint(state, &[])
    }
    fn write_checkpoint(&mut self, state: &State, extra: &[Op]) -> Result<CheckpointInfo> {
        failpoint("checkpoint_before_write");
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
        let mut manifest = Manifest {
            format: 2,
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
        };
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
            if migrating {
                rows.retain(|_, _| false).map_err(error)?;
                metadata.retain(|_, _| false).map_err(error)?;
            }
            // Retain the old crash boundary even when an incremental checkpoint
            // does not clear tables. All changes still share one redb transaction.
            failpoint("checkpoint_after_clear");
            tx.open_table(CONTROL)
                .map_err(error)?
                .insert(CATALOG, b"LCC2".as_slice())
                .map_err(error)?;
            for (key, op) in &pending.metadata {
                if let Some(old) = metadata.remove(key.as_slice()).map_err(error)? {
                    manifest.bytes = manifest
                        .bytes
                        .checked_sub(old.value().len() as u64)
                        .ok_or_else(|| error("checkpoint byte accounting"))?;
                    if matches!(op, Op::Grant(_, _, None)) {
                        result.deleted_metadata += 1;
                    }
                }
                if !matches!(op, Op::Grant(_, _, None)) {
                    let bytes = codec::encode(0, std::slice::from_ref(op), state)?;
                    metadata
                        .insert(key.as_slice(), bytes.as_slice())
                        .map_err(error)?;
                    manifest.bytes = manifest
                        .bytes
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| error("checkpoint byte overflow"))?;
                    result.written_metadata += 1;
                    result.written_bytes += bytes.len() as u64;
                }
            }
            failpoint("checkpoint_after_catalog");
            for (tid, id) in &pending.rows {
                let key = row_key(*tid, *id);
                if let Some(old) = rows.remove(key.as_slice()).map_err(error)? {
                    manifest.bytes = manifest
                        .bytes
                        .checked_sub(old.value().len() as u64)
                        .ok_or_else(|| error("checkpoint byte accounting"))?;
                    if state.row(*tid, *id).is_none() {
                        result.deleted_rows += 1;
                    }
                }
                if let Some(row) = state.row(*tid, *id) {
                    let bytes = codec::encode(0, &[Op::Put(*tid, (**row).clone())], state)?;
                    rows.insert(key.as_slice(), bytes.as_slice())
                        .map_err(error)?;
                    manifest.bytes = manifest
                        .bytes
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| error("checkpoint byte overflow"))?;
                    result.written_rows += 1;
                    result.written_bytes += bytes.len() as u64;
                }
                failpoint("checkpoint_during_rows");
            }
            manifest.rows = rows.len().map_err(error)?;
        }
        {
            let mut control = tx.open_table(CONTROL).map_err(error)?;
            control
                .insert(MANIFEST, manifest.encode().as_slice())
                .map_err(error)?;
            control
                .insert(HEAD, state.revision.to_le_bytes().as_slice())
                .map_err(error)?;
            control.insert(FORMAT, b"LDB2".as_slice()).map_err(error)?;
        }
        failpoint("checkpoint_after_manifest");
        tx.open_table(LOG)
            .map_err(error)?
            .retain(|_, _| false)
            .map_err(error)?;
        failpoint("checkpoint_after_prune");
        failpoint("checkpoint_before_commit");
        tx.commit().map_err(error)?;
        failpoint("checkpoint_after_commit");
        result.rows = manifest.rows;
        result.bytes = manifest.bytes;
        self.info = StorageInfo {
            durable_revision: state.revision,
            checkpoint_revision: state.revision,
            checkpoint_rows: manifest.rows,
            checkpoint_bytes: manifest.bytes,
            log_commits: 0,
            log_bytes: 0,
        };
        self.checkpoint_format = 2;
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
