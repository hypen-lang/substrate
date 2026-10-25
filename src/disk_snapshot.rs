//! Immutable, disk-backed authorized read leases. The mutable engine remains in RAM.
use crate::{compiled::CompiledRelation, engine::LiveGuard, *};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{Cursor, Read},
    ops::Bound,
    path::Path,
    sync::Arc,
};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("spill_rows_v1");
const INDEX: TableDefinition<&[u8], u8> = TableDefinition::new("spill_equality_v1");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("spill_metadata_v1");
const BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct SpillOptions {
    /// redb's configured cache target, not an RSS or total allocator limit.
    pub cache_bytes: usize,
    pub max_row_bytes: usize,
    /// Encoded row bytes per returned page, in addition to the row-count bound.
    pub max_page_bytes: usize,
}
impl Default for SpillOptions {
    fn default() -> Self {
        Self {
            cache_bytes: 8 * 1024 * 1024,
            max_row_bytes: 1024 * 1024,
            max_page_bytes: 8 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SpillStats {
    pub revision: u64,
    pub families: usize,
    pub rows: u64,
    pub index_entries: u64,
    pub encoded_row_bytes: u64,
    pub cache_bytes: usize,
    /// The lease retains schemas and a weak authority guard, not source row Arcs.
    pub retained_source_row_payload_bytes: usize,
}
/// Read-only lease. There is intentionally no public file reopen constructor:
/// a file is data, not permission to bypass current source authority.
pub struct DiskSnapshot {
    disk: redb::Database,
    schemas: BTreeMap<String, Arc<CompiledRelation>>,
    live: LiveGuard,
    options: SpillOptions,
    stats: SpillStats,
}
fn error(message: impl std::fmt::Display) -> Error {
    Error::Storage(format!("disk snapshot: {message}"))
}
fn key(tid: Id, id: Id) -> Vec<u8> {
    [tid.0.as_bytes().as_slice(), id.0.as_bytes()].concat()
}
fn value_hash(value: &Value) -> [u8; 32] {
    let mut h = Sha256::new();
    match value {
        Value::Bool(v) => {
            h.update([0, u8::from(*v)]);
        }
        Value::Int(v) => {
            h.update([1]);
            h.update(v.to_le_bytes());
        }
        Value::Text(v) => {
            h.update([2]);
            h.update(v.as_bytes());
        }
        Value::Ref(v) => {
            h.update([3]);
            h.update(v.0.as_bytes());
        }
        Value::Decimal(v) => {
            h.update([4]);
            h.update(v.to_le_bytes());
        }
        Value::Timestamp(v) => {
            h.update([5]);
            h.update(v.to_le_bytes());
        }
        Value::Bytes(v) => {
            h.update([6]);
            h.update(v);
        }
    }
    h.finalize().into()
}
fn bucket(tid: Id, slot: usize, value: &Value) -> Vec<u8> {
    let mut out = tid.0.as_bytes().to_vec();
    out.extend((slot as u32).to_be_bytes());
    out.extend(value_hash(value));
    out
}
fn indexed_key(tid: Id, slot: usize, value: &Value, id: Id) -> Vec<u8> {
    let mut out = bucket(tid, slot, value);
    out.extend(id.0.as_bytes());
    out
}
fn row_size(row: &Row) -> Result<usize> {
    row.values.iter().try_fold(4usize + 32 + 32, |size, v| {
        let n = match v {
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Decimal(_) | Value::Timestamp(_) => 8,
            Value::Ref(_) => 16,
            Value::Text(v) => 4usize
                .checked_add(v.len())
                .ok_or_else(|| error("row size overflow"))?,
            Value::Bytes(v) => 4usize
                .checked_add(v.len())
                .ok_or_else(|| error("row size overflow"))?,
        };
        size.checked_add(n)
            .ok_or_else(|| error("row size overflow"))
    })
}
fn encode(tid: Id, row: &Row) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(row_size(row)?);
    out.extend(b"LDS1");
    out.extend(tid.0.as_bytes());
    out.extend(row.id.0.as_bytes());
    for v in &row.values {
        match v {
            Value::Bool(v) => out.push(u8::from(*v)),
            Value::Int(v) | Value::Decimal(v) | Value::Timestamp(v) => out.extend(v.to_le_bytes()),
            Value::Ref(v) => out.extend(v.0.as_bytes()),
            Value::Text(v) => {
                out.extend(u32::try_from(v.len()).map_err(error)?.to_le_bytes());
                out.extend(v.as_bytes());
            }
            Value::Bytes(v) => {
                out.extend(u32::try_from(v.len()).map_err(error)?.to_le_bytes());
                out.extend(v);
            }
        }
    }
    let digest = Sha256::digest(&out);
    out.extend(digest);
    Ok(out)
}
fn take<const N: usize>(c: &mut Cursor<&[u8]>) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    c.read_exact(&mut bytes).map_err(error)?;
    Ok(bytes)
}
fn variable(c: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let len = u32::from_le_bytes(take(c)?) as usize;
    let begin = c.position() as usize;
    let end = begin
        .checked_add(len)
        .ok_or_else(|| error("length overflow"))?;
    let bytes = c
        .get_ref()
        .get(begin..end)
        .ok_or_else(|| error("truncated row"))?
        .to_vec();
    c.set_position(end as u64);
    Ok(bytes)
}
fn decode(def: &CompiledRelation, id: Id, bytes: &[u8], max: usize) -> Result<Row> {
    if bytes.len() > max || bytes.len() < 68 {
        return Err(error("invalid row length"));
    }
    let (body, hash) = bytes.split_at(bytes.len() - 32);
    if Sha256::digest(body).as_slice() != hash {
        return Err(error("row checksum mismatch"));
    }
    let mut c = Cursor::new(body);
    if take::<4>(&mut c)? != *b"LDS1"
        || take::<16>(&mut c)? != *def.id.0.as_bytes()
        || take::<16>(&mut c)? != *id.0.as_bytes()
    {
        return Err(error("row format or identity mismatch"));
    }
    let mut values = Vec::with_capacity(def.roles.len());
    for role in &def.roles {
        values.push(match role.data_type {
            DataType::Bool => match take::<1>(&mut c)?[0] {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                _ => return Err(error("invalid bool")),
            },
            DataType::Int => Value::Int(i64::from_le_bytes(take(&mut c)?)),
            DataType::Decimal { .. } => Value::Decimal(i64::from_le_bytes(take(&mut c)?)),
            DataType::Timestamp => Value::Timestamp(i64::from_le_bytes(take(&mut c)?)),
            DataType::Ref(_) => Value::Ref(Id(uuid::Uuid::from_bytes(take(&mut c)?))),
            DataType::Text => Value::Text(String::from_utf8(variable(&mut c)?).map_err(error)?),
            DataType::Bytes => Value::Bytes(variable(&mut c)?),
        });
    }
    if c.position() != body.len() as u64 {
        return Err(error("trailing row bytes"));
    }
    def.check_values(&values).map_err(error)?;
    Ok(Row {
        id,
        values: values.into_boxed_slice(),
    })
}
impl Snapshot {
    /// Materialize exactly the selected families' visible rows. Source authority
    /// must remain live for the returned lease. Existing paths are never replaced.
    pub fn spill(
        &self,
        path: impl AsRef<Path>,
        families: &[&str],
        options: SpillOptions,
    ) -> Result<DiskSnapshot> {
        self.live.check_attached()?;
        if !(1..=64).contains(&families.len())
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&options.cache_bytes)
            || options.max_row_bytes == 0
            || options.max_row_bytes > options.max_page_bytes
            || options.max_page_bytes > 64 * 1024 * 1024
        {
            return Err(Error::Constraint("invalid spill bounds".into()));
        }
        let mut schemas = BTreeMap::new();
        for name in families {
            self.schema(name)?; // Also requires disclosure permission for reference domains.
            let def = self.resolve_read(name)?;
            if def.roles.len() > 256 || schemas.insert((*name).to_owned(), def).is_some() {
                return Err(Error::Constraint(
                    "duplicate family or too many roles".into(),
                ));
            }
        }
        let declarations: Vec<_> = schemas.values().map(|d| &d.declaration).collect();
        let metadata = serde_json::to_vec(&declarations).map_err(error)?;
        if metadata.len() > 4 * 1024 * 1024 {
            return Err(Error::Constraint("spill schema byte limit".into()));
        }
        let path = path.as_ref();
        let mut open = OpenOptions::new();
        open.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open.mode(0o600);
        }
        let file = open.open(path).map_err(error)?;
        let build = (|| {
            let mut builder = redb::Database::builder();
            builder.set_cache_size(options.cache_bytes);
            let disk = builder.create_file(file).map_err(error)?;
            let mut stats = SpillStats {
                revision: self.revision(),
                families: schemas.len(),
                rows: 0,
                index_entries: 0,
                encoded_row_bytes: 0,
                cache_bytes: options.cache_bytes,
                retained_source_row_payload_bytes: 0,
            };
            let mut tx = disk.begin_write().map_err(error)?;
            tx.set_durability(redb::Durability::Immediate)
                .map_err(error)?;
            tx.open_table(ROWS).map_err(error)?;
            tx.open_table(INDEX).map_err(error)?;
            let mut pending = 0usize;
            for (name, def) in &schemas {
                let mut after = None;
                loop {
                    self.live.check_attached()?;
                    let page = self.page_shared(name, None, after, 64)?;
                    for row in &page.rows {
                        if row_size(row)? > options.max_row_bytes {
                            return Err(Error::Constraint("row exceeds spill byte limit".into()));
                        }
                        let bytes = encode(def.id, row)?;
                        let row_key = key(def.id, row.id);
                        tx.open_table(ROWS)
                            .map_err(error)?
                            .insert(row_key.as_slice(), bytes.as_slice())
                            .map_err(error)?;
                        for (slot, value) in row.values.iter().enumerate() {
                            let key = indexed_key(def.id, slot, value, row.id);
                            tx.open_table(INDEX)
                                .map_err(error)?
                                .insert(key.as_slice(), 1)
                                .map_err(error)?;
                        }
                        stats.rows += 1;
                        stats.index_entries += row.values.len() as u64;
                        stats.encoded_row_bytes += bytes.len() as u64;
                        pending += bytes.len() + row.values.len() * 69;
                        if pending >= BATCH_BYTES {
                            tx.commit().map_err(error)?;
                            tx = disk.begin_write().map_err(error)?;
                            tx.set_durability(redb::Durability::Immediate)
                                .map_err(error)?;
                            pending = 0;
                        }
                    }
                    after = page.next;
                    if after.is_none() {
                        break;
                    }
                }
            }
            {
                let mut meta = tx.open_table(META).map_err(error)?;
                meta.insert("format", b"LDS1".as_slice()).map_err(error)?;
                meta.insert("schemas", metadata.as_slice()).map_err(error)?;
                let info = serde_json::to_vec(&stats).map_err(error)?;
                meta.insert("stats", info.as_slice()).map_err(error)?;
            }
            tx.commit().map_err(error)?;
            self.live.check_attached()?;
            Ok(DiskSnapshot {
                disk,
                schemas,
                live: self.live.clone(),
                options,
                stats,
            })
        })();
        if build.is_err() {
            let _ = std::fs::remove_file(path);
        }
        build
    }
}
impl DiskSnapshot {
    pub fn stats(&self) -> Result<SpillStats> {
        self.live.check_attached()?;
        Ok(self.stats.clone())
    }
    fn schema(&self, name: &str) -> Result<&CompiledRelation> {
        self.live.check_attached()?;
        self.schemas
            .get(name)
            .map(AsRef::as_ref)
            .ok_or(Error::Denied)
    }
    pub fn get(&self, name: &str, id: Id) -> Result<Option<Row>> {
        let def = self.schema(name)?;
        let read = self.disk.begin_read().map_err(error)?;
        let table = read.open_table(ROWS).map_err(error)?;
        let key = key(def.id, id);
        let row = table
            .get(key.as_slice())
            .map_err(error)?
            .map(|v| decode(def, id, v.value(), self.options.max_row_bytes))
            .transpose()?;
        self.live.check_attached()?;
        Ok(row)
    }
    /// ID-order keyset pagination. Equality uses a fixed-size digest index and
    /// verifies exact values, so digest collisions cannot add false matches.
    pub fn page(
        &self,
        name: &str,
        predicate: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<RowPage> {
        let def = self.schema(name)?;
        if !(1..=1000).contains(&limit) {
            return Err(Error::Query("page limit must be 1..=1000".into()));
        }
        let selected = predicate
            .map(|(role, value)| {
                let slot = def.role(role)?;
                if !def.roles[slot].data_type.accepts(value) {
                    return Err(Error::Query("predicate type mismatch".into()));
                }
                Ok((slot, value))
            })
            .transpose()?;
        let prefix = selected.map_or_else(
            || def.id.0.as_bytes().to_vec(),
            |(slot, value)| bucket(def.id, slot, value),
        );
        let lower = if let Some(id) = after {
            let mut b = prefix.clone();
            b.extend(id.0.as_bytes());
            Bound::Excluded(b)
        } else {
            Bound::Included(prefix.clone())
        };
        let read = self.disk.begin_read().map_err(error)?;
        let rows_table = read.open_table(ROWS).map_err(error)?;
        let indexes = read.open_table(INDEX).map_err(error)?;
        let lower_ref = match &lower {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => unreachable!(),
        };
        let mut output = Vec::new();
        let mut bytes = 0usize;
        let mut next = None;
        let mut accept = |k: &[u8], packed: &[u8]| -> Result<bool> {
            if k.len() != prefix.len() + 16 {
                return Err(error("invalid key length"));
            }
            let id = Id(uuid::Uuid::from_bytes(
                k[k.len() - 16..].try_into().map_err(error)?,
            ));
            if packed.len() > self.options.max_row_bytes {
                return Err(error("row exceeds lease bound"));
            }
            let row = decode(def, id, packed, self.options.max_row_bytes)?;
            if selected.is_some_and(|(slot, value)| &row.values[slot] != value) {
                return Ok(true);
            }
            if output.len() == limit || bytes + packed.len() > self.options.max_page_bytes {
                next = output.last().map(|r: &Row| r.id);
                return Ok(false);
            }
            bytes += packed.len();
            output.push(row);
            Ok(true)
        };
        if selected.is_some() {
            for entry in indexes
                .range::<&[u8]>((lower_ref, Bound::Unbounded))
                .map_err(error)?
            {
                let (k, _) = entry.map_err(error)?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if k.len() != 68 {
                    return Err(error("invalid index key"));
                }
                let primary = [def.id.0.as_bytes().as_slice(), &k[52..]].concat();
                let packed = rows_table
                    .get(primary.as_slice())
                    .map_err(error)?
                    .ok_or_else(|| error("dangling index"))?;
                if !accept(k, packed.value())? {
                    break;
                }
            }
        } else {
            for entry in rows_table
                .range::<&[u8]>((lower_ref, Bound::Unbounded))
                .map_err(error)?
            {
                let (k, v) = entry.map_err(error)?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if !accept(k, v.value())? {
                    break;
                }
            }
        }
        self.live.check_attached()?;
        Ok(RowPage {
            revision: self.stats.revision,
            rows: output,
            next,
        })
    }
    /// Streaming full integrity/index audit; ordinary point reads validate only
    /// touched records. This allocates at most one decoded row at a time.
    pub fn verify(&self) -> Result<()> {
        self.live.check_attached()?;
        let read = self.disk.begin_read().map_err(error)?;
        let rows = read.open_table(ROWS).map_err(error)?;
        let index = read.open_table(INDEX).map_err(error)?;
        let meta = read.open_table(META).map_err(error)?;
        let expected = [
            ("format", b"LDS1".to_vec()),
            (
                "schemas",
                serde_json::to_vec(
                    &self
                        .schemas
                        .values()
                        .map(|d| &d.declaration)
                        .collect::<Vec<_>>(),
                )
                .map_err(error)?,
            ),
            ("stats", serde_json::to_vec(&self.stats).map_err(error)?),
        ];
        if meta.len().map_err(error)? != expected.len() as u64 {
            return Err(error("metadata count mismatch"));
        }
        for (key, value) in expected {
            if meta
                .get(key)
                .map_err(error)?
                .is_none_or(|v| v.value() != value)
            {
                return Err(error("metadata mismatch"));
            }
        }
        if rows.len().map_err(error)? != self.stats.rows
            || index.len().map_err(error)? != self.stats.index_entries
        {
            return Err(error("entry count mismatch"));
        }
        let mut total = 0;
        for entry in rows.iter().map_err(error)? {
            let (k, v) = entry.map_err(error)?;
            let k = k.value();
            if k.len() != 32 {
                return Err(error("invalid row key"));
            }
            let tid = Id(uuid::Uuid::from_bytes(k[..16].try_into().map_err(error)?));
            let id = Id(uuid::Uuid::from_bytes(k[16..].try_into().map_err(error)?));
            let def = self
                .schemas
                .values()
                .find(|s| s.id == tid)
                .ok_or_else(|| error("unknown row family"))?;
            let row = decode(def, id, v.value(), self.options.max_row_bytes)?;
            total += v.value().len() as u64;
            for (slot, value) in row.values.iter().enumerate() {
                let key = indexed_key(tid, slot, value, id);
                if index.get(key.as_slice()).map_err(error)?.is_none() {
                    return Err(error("missing equality index"));
                }
            }
            self.live.check_attached()?;
        }
        if total != self.stats.encoded_row_bytes {
            return Err(error("row byte count mismatch"));
        }
        self.live.check_attached()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lease_does_not_retain_original_state_or_row_arcs() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let id = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let tid = snapshot.schema("core.Person").unwrap().id;
        let weak_state = Arc::downgrade(&snapshot.state);
        let weak_row = Arc::downgrade(snapshot.state.row(tid, id).unwrap());
        let directory = tempfile::tempdir().unwrap();
        let disk = snapshot
            .spill(
                directory.path().join("spill"),
                &["core.Person"],
                SpillOptions::default(),
            )
            .unwrap();
        drop(snapshot);
        let mut tx = db.admin().transaction();
        tx.delete("core.Person", id).unwrap();
        tx.commit().unwrap();
        assert!(weak_state.upgrade().is_none());
        assert!(weak_row.upgrade().is_none());
        assert_eq!(
            disk.get("core.Person", id).unwrap().unwrap().values[0],
            Value::Text("Ada".into())
        );
    }
    #[test]
    fn expiry_rotation_checksums_and_digest_collisions_fail_closed() {
        use crate::capability::{CapabilityScope, RelationCapability};
        let db = Database::memory();
        db.clock.set(100);
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let a = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        let b = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
        tx.grant("reader", "core.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let tid = db.admin().snapshot().schema("core.Person").unwrap().id;
        let cap = db
            .issue_capability(
                Id::new(),
                "reader",
                CapabilityScope {
                    relations: vec![RelationCapability {
                        relation: tid,
                        grant: Grant::read_only(),
                    }],
                    schema_namespace: None,
                    delegations: 0,
                },
            )
            .unwrap();
        let credential = db.issue_credential("reader", cap.id, 150).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let disk = db
            .authenticate(&credential.token)
            .unwrap()
            .snapshot()
            .spill(
                dir.path().join("spill"),
                &["core.Person"],
                SpillOptions::default(),
            )
            .unwrap();
        // Inject a synthetic digest collision: Bob is a candidate in Ada's bucket.
        let extra = indexed_key(tid, 0, &"Ada".into(), b);
        let tx = disk.disk.begin_write().unwrap();
        tx.open_table(INDEX)
            .unwrap()
            .insert(extra.as_slice(), 1)
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            disk.page("core.Person", Some(("name", &"Ada".into())), None, 10)
                .unwrap()
                .rows[0]
                .id,
            a
        );
        assert_eq!(
            disk.page("core.Person", Some(("name", &"Ada".into())), None, 10)
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(disk.verify().is_err());
        let tx = disk.disk.begin_write().unwrap();
        tx.open_table(INDEX)
            .unwrap()
            .remove(extra.as_slice())
            .unwrap();
        tx.commit().unwrap();
        disk.verify().unwrap();
        let key = key(tid, a);
        let read = disk.disk.begin_read().unwrap();
        let rows = read.open_table(ROWS).unwrap();
        let mut packed = rows.get(key.as_slice()).unwrap().unwrap().value().to_vec();
        drop(rows);
        drop(read);
        packed[40] ^= 1;
        let tx = disk.disk.begin_write().unwrap();
        tx.open_table(ROWS)
            .unwrap()
            .insert(key.as_slice(), packed.as_slice())
            .unwrap();
        tx.commit().unwrap();
        assert!(disk.get("core.Person", a).is_err());
        assert!(disk.verify().is_err());
        db.clock.set(150);
        assert!(matches!(disk.get("core.Person", b), Err(Error::Denied)));
        db.clock.set(160);
        let fresh = db.issue_credential("reader", cap.id, 200).unwrap();
        let lease = db
            .authenticate(&fresh.token)
            .unwrap()
            .snapshot()
            .spill(
                dir.path().join("fresh"),
                &["core.Person"],
                SpillOptions::default(),
            )
            .unwrap();
        db.rotate_credential(fresh.id, 210).unwrap();
        assert!(lease.stats().is_err());
    }
}
