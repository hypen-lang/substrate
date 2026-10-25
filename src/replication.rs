//! Bounded, trusted-host replication. Transfer authentication and leader election
//! are deliberately outside this module. Followers expose read-only snapshots.
use crate::{
    codec,
    storage::{Op, State},
    *,
};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};
use std::{fs::OpenOptions, path::Path, sync::Mutex};

pub const MAX_SEED_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_PACKET_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PACKET_RECORDS: usize = 128;
const CONTROL: TableDefinition<&str, &[u8]> = TableDefinition::new("replica_control_v1");
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("replica_packets_v1");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    /// Fresh stream identity for each Source instance, never an application entity ID.
    pub source: Id,
    pub revision: u64,
    pub digest: [u8; 32],
}
fn error(message: impl std::fmt::Display) -> Error {
    Error::Storage(format!("replication: {message}"))
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn cursor_bytes(cursor: Cursor) -> Vec<u8> {
    let mut out = cursor.source.0.as_bytes().to_vec();
    out.extend(cursor.revision.to_le_bytes());
    out.extend(cursor.digest);
    out
}
fn put(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    out.extend(u32::try_from(bytes.len()).map_err(error)?.to_le_bytes());
    out.extend(bytes);
    Ok(())
}
struct Input<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Input<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .ok_or_else(|| error("length overflow"))?;
        let bytes = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| error("truncated envelope"))?;
        self.at = end;
        Ok(bytes)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn variable(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn id(&mut self) -> Result<Id> {
        Ok(Id(uuid::Uuid::from_bytes(
            self.take(16)?.try_into().unwrap(),
        )))
    }
    fn cursor(&mut self) -> Result<Cursor> {
        Ok(Cursor {
            source: self.id()?,
            revision: self.u64()?,
            digest: self.take(32)?.try_into().unwrap(),
        })
    }
    fn finish(&self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(error("trailing bytes"))
        }
    }
}
fn decode_cursor(bytes: &[u8]) -> Result<Cursor> {
    let mut input = Input { bytes, at: 0 };
    let cursor = input.cursor()?;
    input.finish()?;
    Ok(cursor)
}
fn seed(state: &State, source: Id) -> Result<(Vec<u8>, Cursor)> {
    let mut out = b"LRS1".to_vec();
    out.extend(source.0.as_bytes());
    out.extend(state.revision.to_le_bytes());
    let mut metadata = Vec::new();
    for module in state.modules.values() {
        metadata.push(Op::Install((**module).clone()));
    }
    for id in &state.identities {
        metadata.push(Op::Identity(*id));
    }
    for ((principal, tid), grant) in &state.grants {
        metadata.push(Op::Grant(
            principal.clone(),
            *tid,
            Some(grant.declaration.clone()),
        ));
    }
    for (principal, name) in &state.schema_grants {
        metadata.push(Op::SchemaGrant(principal.clone(), name.clone()));
    }
    out.extend(u32::try_from(metadata.len()).map_err(error)?.to_le_bytes());
    for op in metadata {
        put(&mut out, &codec::metadata_key(&op)?)?;
        put(&mut out, &codec::encode(0, &[op], state)?)?;
        if out.len() > MAX_SEED_BYTES - 32 {
            return Err(error("seed size limit exceeded"));
        }
    }
    let count = state.families.values().map(|f| f.len()).sum::<usize>();
    out.extend(u32::try_from(count).map_err(error)?.to_le_bytes());
    for (tid, family) in &state.families {
        for row in family.values() {
            let mut key = tid.0.as_bytes().to_vec();
            key.extend(row.id.0.as_bytes());
            out.extend(&key);
            put(
                &mut out,
                &codec::encode(0, &[Op::Put(*tid, (**row).clone())], state)?,
            )?;
            if out.len() > MAX_SEED_BYTES - 32 {
                return Err(error("seed size limit exceeded"));
            }
        }
    }
    if out.len() > MAX_SEED_BYTES - 32 {
        return Err(error("seed size limit exceeded"));
    }
    let hash = digest(&out);
    out.extend(hash);
    Ok((
        out,
        Cursor {
            source,
            revision: state.revision,
            digest: hash,
        },
    ))
}
fn decode_seed(bytes: &[u8]) -> Result<(State, Cursor)> {
    if bytes.len() > MAX_SEED_BYTES || bytes.len() < 68 {
        return Err(error("invalid seed size"));
    }
    let (body, checksum) = bytes.split_at(bytes.len() - 32);
    if digest(body).as_slice() != checksum {
        return Err(error("seed checksum mismatch"));
    }
    let mut input = Input { bytes: body, at: 0 };
    if input.take(4)? != b"LRS1" {
        return Err(error("unsupported seed version"));
    }
    let source = input.id()?;
    let revision = input.u64()?;
    let mut state = State::default();
    let mut keys = std::collections::BTreeSet::new();
    for _ in 0..input.u32()? {
        let key = input.variable()?;
        let record = input.variable()?;
        if !keys.insert(key) {
            return Err(error("duplicate seed metadata"));
        }
        codec::replay_metadata_entry(record, key, &mut state)?;
    }
    for _ in 0..input.u32()? {
        let key = input.take(32)?;
        let record = input.variable()?;
        codec::replay_checkpoint_row(record, 0, key, &mut state)?;
    }
    input.finish()?;
    state.validate_recovered()?;
    state.revision = revision;
    Ok((
        state,
        Cursor {
            source,
            revision,
            digest: checksum.try_into().unwrap(),
        },
    ))
}
fn advance(cursor: Cursor, revision: u64, record: &[u8]) -> Cursor {
    let mut hash = Sha256::new();
    hash.update(b"Liquid replication record v1");
    hash.update(cursor_bytes(cursor));
    hash.update(revision.to_le_bytes());
    hash.update(record);
    Cursor {
        source: cursor.source,
        revision,
        digest: hash.finalize().into(),
    }
}
struct Packet<'a> {
    base: Cursor,
    head: Cursor,
    records: Vec<(u64, &'a [u8])>,
}
fn packet(base: Cursor, records: &[(u64, Vec<u8>)]) -> Result<(Vec<u8>, Cursor)> {
    let mut head = base;
    for (revision, record) in records {
        head = advance(head, *revision, record);
    }
    let mut out = b"LRP1".to_vec();
    out.extend(cursor_bytes(base));
    out.extend(cursor_bytes(head));
    out.extend(u32::try_from(records.len()).map_err(error)?.to_le_bytes());
    for (revision, record) in records {
        out.extend(revision.to_le_bytes());
        put(&mut out, record)?;
    }
    if out.len() > MAX_PACKET_BYTES {
        return Err(error("packet size limit exceeded"));
    }
    Ok((out, head))
}
fn decode_packet(bytes: &[u8]) -> Result<Packet<'_>> {
    if bytes.len() > MAX_PACKET_BYTES {
        return Err(error("packet size limit exceeded"));
    }
    let mut input = Input { bytes, at: 0 };
    if input.take(4)? != b"LRP1" {
        return Err(error("unsupported packet version"));
    }
    let base = input.cursor()?;
    let head = input.cursor()?;
    let count = input.u32()? as usize;
    if count == 0 || count > MAX_PACKET_RECORDS {
        return Err(error("invalid packet count"));
    }
    let mut expected = base;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let revision = input.u64()?;
        let record = input.variable()?;
        if Some(revision) != expected.revision.checked_add(1) {
            return Err(error("packet sequence gap"));
        }
        expected = advance(expected, revision, record);
        records.push((revision, record));
    }
    input.finish()?;
    if expected != head {
        return Err(error("packet checksum or lineage mismatch"));
    }
    Ok(Packet {
        base,
        head,
        records,
    })
}
fn replay(state: &mut State, packet: &Packet<'_>) -> Result<()> {
    if state.revision != packet.base.revision {
        return Err(error("state revision mismatch"));
    }
    for (revision, bytes) in &packet.records {
        codec::replay_observed(bytes, *revision, state, &mut |_| {})?;
        state.validate_recovered()?;
        state.revision = *revision;
    }
    Ok(())
}

/// A single serialized export stream. Keep returned packets until acknowledged.
/// Losing this handle or pruning its required redo requires a new seed/stream.
pub struct Source {
    db: Database,
    seed: Vec<u8>,
    cursor: Cursor,
}
impl Source {
    pub fn new(db: &Database) -> Result<Self> {
        let state = db.replication_capture()?;
        let (seed, cursor) = seed(&state, Id::new())?;
        Ok(Self {
            db: db.clone(),
            seed,
            cursor,
        })
    }
    pub fn seed(&self) -> &[u8] {
        &self.seed
    }
    pub fn cursor(&self) -> Cursor {
        self.cursor
    }
    /// Export the next bounded retained prefix; `None` means caught up at capture.
    pub fn next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        let overhead = 120 + MAX_PACKET_RECORDS * 12;
        let records = self.db.replication_tail(
            self.cursor.revision,
            MAX_PACKET_RECORDS,
            MAX_PACKET_BYTES - overhead,
        )?;
        if records.is_empty() {
            return Ok(None);
        }
        let (packet, cursor) = packet(self.cursor, &records)?;
        self.cursor = cursor;
        Ok(Some(packet))
    }
}
struct Inner {
    disk: redb::Database,
    state: State,
    cursor: Cursor,
    tail_records: usize,
    tail_bytes: usize,
    failed: bool,
}
/// Trusted-host follower. No writable Session or Database is exposed.
pub struct Replica {
    engine: Database,
    inner: Mutex<Inner>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Apply {
    Applied(Cursor),
    Duplicate(Cursor),
}
impl Replica {
    pub fn create(path: impl AsRef<Path>, bytes: &[u8]) -> Result<Self> {
        let (state, cursor) = decode_seed(bytes)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(error)?;
        let disk = redb::Database::builder().create_file(file).map_err(error)?;
        let mut tx = disk.begin_write().map_err(error)?;
        tx.set_durability(redb::Durability::Immediate)
            .map_err(error)?;
        checkpoint(&tx, bytes, cursor)?;
        tx.open_table(CONTROL)
            .map_err(error)?
            .insert("format", b"LRF1".as_slice())
            .map_err(error)?;
        tx.open_table(LOG).map_err(error)?;
        tx.commit().map_err(error)?;
        Ok(Self {
            engine: Database::from_state(state.clone(), None),
            inner: Mutex::new(Inner {
                disk,
                state,
                cursor,
                tail_records: 0,
                tail_bytes: 0,
                failed: false,
            }),
        })
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let disk = redb::Database::open(path).map_err(error)?;
        let (state, cursor, tail_records, tail_bytes) = {
            let read = disk.begin_read().map_err(error)?;
            let control = read.open_table(CONTROL).map_err(error)?;
            let get = |key| -> Result<Vec<u8>> {
                Ok(control
                    .get(key)
                    .map_err(error)?
                    .ok_or_else(|| error("missing follower metadata"))?
                    .value()
                    .to_vec())
            };
            if get("format")? != b"LRF1" {
                return Err(error("unsupported follower format"));
            }
            let seed_bytes = get("seed")?;
            let anchor_bytes = get("anchor")?;
            if checkpoint_digest(&seed_bytes, &anchor_bytes).as_slice() != get("checkpoint_digest")?
            {
                return Err(error("checkpoint binding mismatch"));
            }
            let (mut state, seed_cursor) = decode_seed(&seed_bytes)?;
            let mut cursor = decode_cursor(&anchor_bytes)?;
            if cursor.source != seed_cursor.source || cursor.revision != seed_cursor.revision {
                return Err(error("checkpoint cursor mismatch"));
            }
            let expected = decode_cursor(&get("cursor")?)?;
            let mut count = 0usize;
            let mut size = 0usize;
            for entry in read.open_table(LOG).map_err(error)?.iter().map_err(error)? {
                let (first, bytes) = entry.map_err(error)?;
                let packet = decode_packet(bytes.value())?;
                if packet.base != cursor || Some(first.value()) != cursor.revision.checked_add(1) {
                    return Err(error("persisted packet gap"));
                }
                count += packet.records.len();
                size += bytes.value().len();
                if count >= MAX_PACKET_RECORDS || size >= MAX_PACKET_BYTES {
                    return Err(error("unbounded follower redo"));
                }
                replay(&mut state, &packet)?;
                cursor = packet.head;
            }
            if cursor != expected {
                return Err(error("follower head mismatch"));
            }
            (state, cursor, count, size)
        };
        Ok(Self {
            engine: Database::from_state(state.clone(), None),
            inner: Mutex::new(Inner {
                disk,
                state,
                cursor,
                tail_records,
                tail_bytes,
                failed: false,
            }),
        })
    }
    pub fn cursor(&self) -> Result<Cursor> {
        let inner = self.inner.lock().map_err(error)?;
        if inner.failed {
            return Err(error("reopen after uncertain persistence"));
        }
        Ok(inner.cursor)
    }
    pub fn apply(&self, bytes: &[u8]) -> Result<Apply> {
        let packet = decode_packet(bytes)?;
        let mut inner = self.inner.lock().map_err(error)?;
        if inner.failed {
            return Err(error("reopen after uncertain persistence"));
        }
        if packet.head == inner.cursor {
            return Ok(Apply::Duplicate(inner.cursor));
        }
        if packet.base != inner.cursor {
            return Err(error(
                "wrong source, gap or divergent history; reseed if history was pruned",
            ));
        }
        let mut state = inner.state.clone();
        replay(&mut state, &packet)?;
        let compact = inner.tail_records + packet.records.len() >= MAX_PACKET_RECORDS
            || inner.tail_bytes + bytes.len() >= MAX_PACKET_BYTES;
        let new_seed = if compact {
            Some(seed(&state, packet.head.source)?.0)
        } else {
            None
        };
        let persist = (|| -> Result<()> {
            let mut tx = inner.disk.begin_write().map_err(error)?;
            tx.set_durability(redb::Durability::Immediate)
                .map_err(error)?;
            if let Some(seed) = &new_seed {
                checkpoint(&tx, seed, packet.head)?;
                tx.open_table(LOG)
                    .map_err(error)?
                    .retain(|_, _| false)
                    .map_err(error)?;
            } else {
                tx.open_table(LOG)
                    .map_err(error)?
                    .insert(packet.records[0].0, bytes)
                    .map_err(error)?;
                tx.open_table(CONTROL)
                    .map_err(error)?
                    .insert("cursor", cursor_bytes(packet.head).as_slice())
                    .map_err(error)?;
            }
            crate::durable::failpoint("replica_before_commit");
            tx.commit().map_err(error)?;
            crate::durable::failpoint("replica_after_commit");
            Ok(())
        })();
        if let Err(err) = persist {
            inner.failed = true;
            self.engine.replication_fail();
            return Err(err);
        }
        if let Err(err) = self.engine.replication_publish(state.clone()) {
            inner.failed = true;
            self.engine.replication_fail();
            return Err(err);
        }
        inner.state = state;
        inner.cursor = packet.head;
        if compact {
            inner.tail_records = 0;
            inner.tail_bytes = 0;
        } else {
            inner.tail_records += packet.records.len();
            inner.tail_bytes += bytes.len();
        }
        Ok(Apply::Applied(packet.head))
    }
    /// Eventual reads may use stale permissions until replication applies revocation.
    /// Principal selection is a trusted host action, not authentication.
    pub fn snapshot_eventual(&self, principal: &str) -> Snapshot {
        self.engine.session(principal).snapshot()
    }
    /// Caller must obtain this barrier from the trusted current leader. This proves
    /// only the supplied revision, not that the leader has made no later revocations.
    pub fn snapshot_at_cursor(&self, principal: &str, barrier: Cursor) -> Result<Snapshot> {
        let inner = self.inner.lock().map_err(error)?;
        if inner.failed || inner.cursor != barrier {
            return Err(error(
                "follower cursor differs; obtain and apply a fresh trusted barrier",
            ));
        }
        Ok(self.engine.session(principal).snapshot())
    }
    /// Authenticate against the follower's current replicated credential state.
    /// This has the same replication-lag limitation as eventual principal reads.
    pub fn authenticate_eventual(&self, token: &str) -> Result<Snapshot> {
        Ok(self.engine.authenticate(token)?.snapshot())
    }
}
fn checkpoint_digest(seed: &[u8], anchor: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"Liquid replica checkpoint v1");
    hash.update(anchor);
    hash.update(seed);
    hash.finalize().into()
}
fn checkpoint(tx: &redb::WriteTransaction, seed: &[u8], cursor: Cursor) -> Result<()> {
    let mut control = tx.open_table(CONTROL).map_err(error)?;
    let anchor = cursor_bytes(cursor);
    control.insert("seed", seed).map_err(error)?;
    control.insert("anchor", anchor.as_slice()).map_err(error)?;
    control.insert("cursor", anchor.as_slice()).map_err(error)?;
    control
        .insert(
            "checkpoint_digest",
            checkpoint_digest(seed, &anchor).as_slice(),
        )
        .map_err(error)?;
    Ok(())
}

#[cfg(test)]
#[path = "replication_tests.rs"]
mod tests;
