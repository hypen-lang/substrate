//! Offline resident-format conversion. Application rows and allocations are
//! replayed into a disk scratch table, never a resident State.
use super::*;
use redb::ReadableTableMetadata;
use std::{
    fs,
    io::{Cursor, Read},
    path::PathBuf,
};
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("liquid_log_v1");
const CONTROL: TableDefinition<&str, &[u8]> = TableDefinition::new("liquid_control_v1");
const CHECKPOINT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("liquid_checkpoint_rows_v1");
const OVERLAY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("liquid_checkpoint_overlay_v3");
const METADATA: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("liquid_checkpoint_metadata_v2");
const SCRATCH: TableDefinition<&[u8], &[u8]> = TableDefinition::new("resident_import_rows_v1");
struct Staging(PathBuf);
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn fixed<const N: usize>(input: &mut Cursor<&[u8]>) -> Result<[u8; N]> {
    let mut out = [0; N];
    input.read_exact(&mut out).map_err(err)?;
    Ok(out)
}
fn id(input: &mut Cursor<&[u8]>) -> Result<Id> {
    Ok(Id(uuid::Uuid::from_bytes(fixed(input)?)))
}
fn variable<'a>(input: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let n = u32::from_le_bytes(fixed(input)?) as usize;
    let start = input.position() as usize;
    let end = start
        .checked_add(n)
        .ok_or_else(|| err("resident field overflow"))?;
    let bytes = input
        .get_ref()
        .get(start..end)
        .ok_or_else(|| err("truncated resident field"))?;
    input.set_position(end as u64);
    Ok(bytes)
}
struct Import<'a> {
    db: &'a redb::Database,
    state: State,
    options: PagedOptions,
    metadata: BTreeMap<Vec<u8>, usize>,
    metadata_bytes: usize,
    identities: u64,
    rows: u64,
}
impl Import<'_> {
    // Scratch work has no externally visible name. One final Immediate commit
    // flushes every earlier staging write before verification/publication.
    fn staging_write(&self) -> Result<redb::WriteTransaction> {
        let mut tx = self.db.begin_write().map_err(err)?;
        tx.set_durability(redb::Durability::None).map_err(err)?;
        Ok(tx)
    }
    fn metadata_charge(&mut self, key: Vec<u8>, bytes: usize) -> Result<()> {
        let bytes = bytes.saturating_add(key.len()).saturating_add(256);
        let total = self
            .metadata_bytes
            .saturating_sub(self.metadata.get(&key).copied().unwrap_or(0))
            .saturating_add(bytes);
        if total > self.options.max_catalog_bytes {
            return Err(err("resident metadata exceeds import catalog budget"));
        }
        self.metadata_bytes = total;
        self.metadata.insert(key, bytes);
        Ok(())
    }
    fn record(
        &mut self,
        bytes: &[u8],
        revision: u64,
        row_key: Option<&[u8]>,
        metadata_key: Option<&[u8]>,
        catalog: bool,
        checkpoint_revision: Option<u64>,
    ) -> Result<()> {
        let bound = if catalog {
            self.options.max_catalog_bytes
        } else {
            self.options.max_transaction_bytes
        };
        if bytes.len() > bound {
            return Err(err("resident record exceeds bounded import budget"));
        }
        let mut input = Cursor::new(bytes);
        let magic = fixed::<4>(&mut input)?;
        if !matches!(&magic, b"LQ01" | b"LQ02" | b"LQ03")
            || u64::from_le_bytes(fixed(&mut input)?) != revision
        {
            return Err(err("resident record version/revision mismatch"));
        }
        let count = u32::from_le_bytes(fixed(&mut input)?);
        if (row_key.is_some() || metadata_key.is_some()) && count != 1 {
            return Err(err("resident checkpoint entry count"));
        }
        let effective = checkpoint_revision.unwrap_or(revision);
        let mut changed = BTreeSet::new();
        for _ in 0..count {
            let tag = fixed::<1>(&mut input)?[0];
            if (catalog && !matches!(tag, 0 | 2)) || (row_key.is_some() && tag != 1) {
                return Err(err("invalid resident checkpoint operation"));
            }
            let op = match tag {
                0 => {
                    let json = variable(&mut input)?;
                    let op = match &magic {
                        b"LQ03" => serde_json::from_slice::<crate::wire::EventV3>(json)
                            .map_err(err)?
                            .into_op(),
                        b"LQ02" => serde_json::from_slice::<crate::wire::EventV2>(json)
                            .map_err(err)?
                            .into_op(),
                        _ => serde_json::from_slice::<crate::wire::EventV1>(json)
                            .map_err(err)?
                            .into_op(),
                    };
                    if (!matches!(&magic, b"LQ03") && crate::wire::event_v3(&op))
                        || !matches!(op, Op::Install(_) | Op::Grant(..) | Op::SchemaGrant(..))
                    {
                        return Err(err("invalid resident metadata event"));
                    }
                    op
                }
                1 => {
                    let tid = id(&mut input)?;
                    let identity = id(&mut input)?;
                    if let Some(key) = row_key
                        && key != encoding::row_key(tid, identity)
                    {
                        return Err(err("resident checkpoint row key mismatch"));
                    }
                    let def = self
                        .state
                        .relations
                        .get(&tid)
                        .ok_or_else(|| err("unknown resident row type"))?;
                    if magic != *b"LQ03" && crate::wire::relation_v3(def) {
                        return Err(err("old resident row version with extended types"));
                    }
                    let mut values = Vec::with_capacity(def.roles.len());
                    for role in &def.roles {
                        values.push(crate::codec::decode_value(&role.data_type, &mut input)?);
                    }
                    Op::Put(
                        tid,
                        Row {
                            id: identity,
                            values: values.into_boxed_slice(),
                        },
                    )
                }
                2 => Op::Identity(id(&mut input)?),
                3 => Op::Delete(id(&mut input)?, id(&mut input)?),
                _ => return Err(err("unknown resident operation")),
            };
            if let Some(key) = metadata_key
                && (matches!(op, Op::Grant(_, _, None))
                    || crate::codec::metadata_key(&op)?.as_slice() != key)
            {
                return Err(err("resident metadata key mismatch"));
            }
            let changed_key = match &op {
                Op::Put(tid, row) => Some((*tid, row.id)),
                Op::Delete(tid, id) => Some((*tid, *id)),
                _ => None,
            };
            if let Some(key) = changed_key
                && !changed.contains(&key)
            {
                let retained = bytes
                    .len()
                    .saturating_add(changed.len().saturating_add(1).saturating_mul(96));
                if changed.len() >= self.options.max_transaction_rows
                    || retained > self.options.max_transaction_bytes
                {
                    return Err(err(
                        "resident changed-key admission budget exceeded; increase import bounds or rebatch source",
                    ));
                }
                changed.insert(key);
            }
            if checkpoint_revision.is_none() && !catalog {
                self.validate_order(&op, magic == *b"LQ03")?;
            }
            self.apply(op, effective, row_key.is_some())?;
        }
        if input.position() != bytes.len() as u64 {
            return Err(err("trailing resident record bytes"));
        }
        if checkpoint_revision.is_none() && !catalog {
            self.check_references(changed)?;
        }
        Ok(())
    }
    fn validate_order(&self, op: &Op, deferred: bool) -> Result<()> {
        match op {
            Op::Install(_) => self.state.validate_replay_order_versioned(op, deferred),
            Op::Put(tid, row) => {
                let read = self.db.begin_read().map_err(err)?;
                let rows = read.open_table(SCRATCH).map_err(err)?;
                let def = self
                    .state
                    .relations
                    .get(tid)
                    .ok_or_else(|| err("unknown replay relation"))?;
                for (role, value) in def.roles.iter().zip(row.values.iter()) {
                    if let (Some(target), Value::Ref(id)) =
                        (role.data_type.reference_target(), value)
                    {
                        if deferred && role.deferrable {
                            continue;
                        }
                        let target = *self
                            .state
                            .names
                            .get(target)
                            .ok_or_else(|| err("unknown replay reference type"))?;
                        if self.state.row(target, *id).is_none()
                            && rows
                                .get(encoding::row_key(target, *id).as_slice())
                                .map_err(err)?
                                .is_none()
                        {
                            return Err(err("resident immediate reference order violation"));
                        }
                    }
                }
                Ok(())
            }
            Op::Delete(tid, id) if !deferred => {
                let read = self.db.begin_read().map_err(err)?;
                let incoming = read.open_table(INCOMING).map_err(err)?;
                let prefix = incoming_prefix(*tid, *id);
                let end = prefix_end(&prefix).ok_or_else(|| err("invalid reference prefix"))?;
                if incoming
                    .range(prefix.as_slice()..end.as_slice())
                    .map_err(err)?
                    .next()
                    .transpose()
                    .map_err(err)?
                    .is_some()
                {
                    return Err(err("resident immediate delete order violation"));
                }
                self.state.validate_replay_order_versioned(op, false)
            }
            _ => Ok(()),
        }
    }
    fn apply(&mut self, op: Op, revision: u64, checkpoint: bool) -> Result<()> {
        match &op {
            Op::Identity(identity) => {
                let tx = self.staging_write()?;
                let mut ids = tx.open_table(IDS).map_err(err)?;
                if ids
                    .get(identity.0.as_bytes().as_slice())
                    .map_err(err)?
                    .is_some()
                {
                    return Err(err("duplicate resident identity"));
                }
                ids.insert(identity.0.as_bytes().as_slice(), 0)
                    .map_err(err)?;
                drop(ids);
                tx.open_table(VERS)
                    .map_err(err)?
                    .insert(
                        tagged(b'a', identity.0.as_bytes()).as_slice(),
                        revision.max(1),
                    )
                    .map_err(err)?;
                tx.commit().map_err(err)?;
                self.identities += 1;
            }
            Op::Install(module) => {
                if crate::visibility::protected(&module.name)
                    && ![
                        crate::visibility::module(),
                        crate::capability::module(),
                        crate::auth::module(),
                        crate::schema_control::control_module(),
                    ]
                    .contains(&module)
                {
                    return Err(err(
                        "resident control module has no safe paged mapping (named views unsupported)",
                    ));
                }
                self.metadata_charge(
                    crate::codec::metadata_key(&op)?,
                    crate::codec::encoded_len(std::slice::from_ref(&op), &self.state)?,
                )?;
                let tx = self.staging_write()?;
                {
                    let mut ids = tx.open_table(IDS).map_err(err)?;
                    let mut versions = tx.open_table(VERS).map_err(err)?;
                    for relation in &module.relations {
                        if ids
                            .get(relation.id.0.as_bytes().as_slice())
                            .map_err(err)?
                            .is_some()
                        {
                            return Err(err("resident schema identity collision"));
                        }
                        ids.insert(relation.id.0.as_bytes().as_slice(), 0)
                            .map_err(err)?;
                        versions
                            .insert(
                                tagged(b'a', relation.id.0.as_bytes()).as_slice(),
                                revision.max(1),
                            )
                            .map_err(err)?;
                    }
                }
                self.state.apply_recovered(&op, revision)?;
                tx.commit().map_err(err)?;
            }
            Op::Grant(_, tid, _) => {
                if self
                    .state
                    .origins
                    .get(tid)
                    .is_some_and(|origin| crate::visibility::protected(&origin.0))
                {
                    return Err(err(
                        "explicit resident control-family grants cannot be mapped",
                    ));
                }
                self.metadata_charge(
                    crate::codec::metadata_key(&op)?,
                    crate::codec::encoded_len(std::slice::from_ref(&op), &self.state)?,
                )?;
                self.state.apply_recovered(&op, revision)?;
            }
            Op::SchemaGrant(..) => {
                self.metadata_charge(
                    crate::codec::metadata_key(&op)?,
                    crate::codec::encoded_len(std::slice::from_ref(&op), &self.state)?,
                )?;
                self.state.apply_recovered(&op, revision)?;
            }
            Op::Put(tid, row) => {
                let def = self
                    .state
                    .relations
                    .get(tid)
                    .ok_or_else(|| err("unknown resident relation"))?
                    .clone();
                let read = self.db.begin_read().map_err(err)?;
                if read
                    .open_table(IDS)
                    .map_err(err)?
                    .get(row.id.0.as_bytes().as_slice())
                    .map_err(err)?
                    .is_none()
                {
                    return Err(err("unallocated resident row"));
                }
                if crate::visibility::protected(&self.state.origins[tid].0) {
                    self.metadata_charge(
                        encoding::row_key(*tid, row.id),
                        crate::codec::encoded_len(std::slice::from_ref(&op), &self.state)?,
                    )?;
                    if !self.state.identities.contains(&row.id) {
                        self.state
                            .apply_recovered(&Op::Identity(row.id), revision)?;
                    }
                    let existed = self.state.row(*tid, row.id).is_some();
                    if checkpoint && existed {
                        return Err(err("duplicate resident checkpoint row"));
                    }
                    self.state.apply_recovered(&op, revision)?;
                    self.rows += u64::from(!existed);
                    return Ok(());
                }
                let length = row_encoded_len(&def, row)?;
                if length > self.options.max_row_bytes {
                    return Err(err("resident row exceeds paged row bound"));
                }
                def.check_values(&row.values)?;
                let key = encoding::row_key(*tid, row.id);
                let source = read.open_table(SCRATCH).map_err(err)?;
                let previous = source.get(key.as_slice()).map_err(err)?;
                if checkpoint && previous.is_some() {
                    return Err(err("duplicate resident checkpoint row"));
                }
                let old = previous
                    .as_ref()
                    .map(|bytes| decode_row(&def, row.id, bytes.value()).map(|(_, r)| r))
                    .transpose()?;
                let unique = read.open_table(UNIQUE).map_err(err)?;
                for i in 0..def.unique.len() {
                    if !unique_applies(&def, row, i) {
                        continue;
                    }
                    let k = unique_key(*tid, i, &def.unique_values(row, i));
                    if unique
                        .get(k.as_slice())
                        .map_err(err)?
                        .is_some_and(|v| v.value() != row.id.0.as_bytes())
                    {
                        return Err(err("resident unique constraint violation"));
                    }
                }
                let encoded = encode_row(&def, row, revision.max(1))?;
                let tx = self.staging_write()?;
                if let Some(old) = &old {
                    self.indexes(&tx, *tid, old, false)?;
                }
                tx.open_table(SCRATCH)
                    .map_err(err)?
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(err)?;
                self.indexes(&tx, *tid, row, true)?;
                tx.commit().map_err(err)?;
                self.rows += u64::from(old.is_none());
            }
            Op::Delete(tid, identity) => {
                let def = self
                    .state
                    .relations
                    .get(tid)
                    .ok_or_else(|| err("unknown deleted resident relation"))?
                    .clone();
                if crate::visibility::protected(&self.state.origins[tid].0) {
                    self.state.apply_recovered(&op, revision)?;
                    self.rows -= 1;
                    return Ok(());
                }
                let read = self.db.begin_read().map_err(err)?;
                let rows = read.open_table(SCRATCH).map_err(err)?;
                let key = encoding::row_key(*tid, *identity);
                let previous = rows
                    .get(key.as_slice())
                    .map_err(err)?
                    .ok_or_else(|| err("resident delete of missing row"))?;
                let (_, old) = decode_row(&def, *identity, previous.value())?;
                let tx = self.staging_write()?;
                self.indexes(&tx, *tid, &old, false)?;
                tx.open_table(SCRATCH)
                    .map_err(err)?
                    .remove(key.as_slice())
                    .map_err(err)?;
                tx.commit().map_err(err)?;
                self.rows -= 1;
            }
        }
        Ok(())
    }
    fn indexes(&self, tx: &redb::WriteTransaction, tid: Id, row: &Row, insert: bool) -> Result<()> {
        let def = &self.state.relations[&tid];
        let mut index = tx.open_table(INDEX).map_err(err)?;
        for (slot, value) in row.values.iter().enumerate() {
            if !PagedFormat::V3.role_index(def, slot) {
                continue;
            }
            let key = index_key(tid, slot, value, row.id);
            if insert {
                index.insert(key.as_slice(), 0).map_err(err)?;
            } else {
                index.remove(key.as_slice()).map_err(err)?;
            }
        }
        let mut unique = tx.open_table(UNIQUE).map_err(err)?;
        for i in 0..def.unique.len() {
            if !unique_applies(def, row, i) {
                continue;
            }
            let key = unique_key(tid, i, &def.unique_values(row, i));
            if insert {
                unique
                    .insert(key.as_slice(), row.id.0.as_bytes().as_slice())
                    .map_err(err)?;
            } else {
                unique.remove(key.as_slice()).map_err(err)?;
            }
        }
        let mut incoming = tx.open_table(INCOMING).map_err(err)?;
        for (slot, target) in &def.reference_slots {
            if let Value::Ref(id) = row.values[*slot] {
                let key = incoming_key(self.state.names[target], id, tid, row.id, *slot);
                if insert {
                    incoming.insert(key.as_slice(), 0).map_err(err)?;
                } else {
                    incoming.remove(key.as_slice()).map_err(err)?;
                }
            }
        }
        Ok(())
    }
    fn check_references(&self, keys: impl IntoIterator<Item = (Id, Id)>) -> Result<()> {
        let read = self.db.begin_read().map_err(err)?;
        let rows = read.open_table(SCRATCH).map_err(err)?;
        let incoming = read.open_table(INCOMING).map_err(err)?;
        for (tid, id) in keys {
            if crate::visibility::protected(&self.state.origins[&tid].0) {
                continue;
            }
            let key = encoding::row_key(tid, id);
            if let Some(bytes) = rows.get(key.as_slice()).map_err(err)? {
                let def = &self.state.relations[&tid];
                let (_, row) = decode_row(def, id, bytes.value())?;
                for (slot, target) in &def.reference_slots {
                    if let Value::Ref(id) = row.values[*slot]
                        && rows
                            .get(encoding::row_key(self.state.names[target], id).as_slice())
                            .map_err(err)?
                            .is_none()
                    {
                        return Err(err("resident dangling reference"));
                    }
                }
            } else {
                let prefix = incoming_prefix(tid, id);
                let end = prefix_end(&prefix).ok_or_else(|| err("invalid incoming prefix"))?;
                if incoming
                    .range(prefix.as_slice()..end.as_slice())
                    .map_err(err)?
                    .next()
                    .transpose()
                    .map_err(err)?
                    .is_some()
                {
                    return Err(err("resident delete leaves references"));
                }
            }
        }
        self.state.validate_recovered()?;
        Ok(())
    }
    fn audit_checkpoint_refs(&self) -> Result<()> {
        let read = self.db.begin_read().map_err(err)?;
        let rows = read.open_table(SCRATCH).map_err(err)?;
        for entry in rows.iter().map_err(err)? {
            let (key, _) = entry.map_err(err)?;
            let key = key.value();
            self.check_references([(
                Id(uuid::Uuid::from_bytes(key[..16].try_into().map_err(err)?)),
                Id(uuid::Uuid::from_bytes(key[16..].try_into().map_err(err)?)),
            )])?;
        }
        Ok(())
    }
}
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
    fn parse(bytes: &[u8]) -> Result<Self> {
        if !((bytes.len() == 60 && matches!(&bytes[..4], b"LCP1" | b"LCP2"))
            || (bytes.len() == 84 && &bytes[..4] == b"LCP3"))
        {
            return Err(err("unsupported resident checkpoint"));
        }
        let values: Vec<_> = bytes[4..]
            .chunks_exact(8)
            .map(|v| u64::from_le_bytes(v.try_into().expect("eight bytes")))
            .collect();
        Ok(Self {
            format: bytes[3] - b'0',
            revision: values[0],
            rows: values[1],
            identities: values[2],
            modules: values[3],
            grants: values[4],
            schema_grants: values[5],
            bytes: values[6],
            base_rows: *values.get(7).unwrap_or(&values[1]),
            overlay_keys: *values.get(8).unwrap_or(&0),
            overlay_bytes: *values.get(9).unwrap_or(&0),
        })
    }
}
fn empty_data() -> CatalogData {
    CatalogData {
        reserved: BTreeSet::new(),
        control: Default::default(),
        modules: vec![],
        grants: vec![],
        visibility: BTreeMap::new(),
        visibility_ids: BTreeMap::new(),
        owners: BTreeMap::new(),
    }
}
impl PagedDatabase {
    /// Offline bounded conversion of resident LDB1/LDB2/LDB3 files. Ordinary
    /// schemas, rows, grants, visibility and canonical auth/capability/namespace
    /// metadata are mapped. Unsupported control modules fail closed. Large legacy
    /// monolithic catalogs or individual redo records above configured bounds
    /// must be checkpointed/rebatched with the resident engine before conversion.
    pub fn import_resident_file(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        options: PagedOptions,
    ) -> Result<PagedImportStats> {
        let destination = destination.as_ref();
        if destination.try_exists().map_err(err)? {
            return Err(err("import destination already exists"));
        }
        let source = redb::Database::builder()
            .set_cache_size(options.cache_bytes)
            .open(source.as_ref())
            .map_err(err)?;
        let read = source.begin_read().map_err(err)?;
        let names: BTreeSet<_> = read
            .list_tables()
            .map_err(err)?
            .map(|t| t.name().to_owned())
            .collect();
        let allowed: BTreeSet<_> = [
            LOG.name(),
            CONTROL.name(),
            CHECKPOINT.name(),
            OVERLAY.name(),
            METADATA.name(),
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        if !names.contains(LOG.name())
            || !names.is_subset(&allowed)
            || read.list_multimap_tables().map_err(err)?.next().is_some()
        {
            return Err(err("source is not a supported resident file"));
        }
        let control = if names.contains(CONTROL.name()) {
            Some(read.open_table(CONTROL).map_err(err)?)
        } else {
            None
        };
        let get = |name: &str| -> Result<Option<Vec<u8>>> {
            let Some(table) = &control else {
                return Ok(None);
            };
            let Some(value) = table.get(name).map_err(err)? else {
                return Ok(None);
            };
            if value.value().len() > options.max_catalog_bytes {
                return Err(err("resident catalog/control exceeds import budget"));
            }
            Ok(Some(value.value().to_vec()))
        };
        let format = get("format")?;
        let head = get("head")?;
        if format.is_some() != head.is_some()
            || format
                .as_deref()
                .is_some_and(|v| !matches!(v, b"LDB1" | b"LDB2" | b"LDB3"))
        {
            return Err(err("resident format/head mismatch"));
        }
        let head = head
            .map(|v| {
                v.try_into()
                    .map(u64::from_le_bytes)
                    .map_err(|_| err("resident head length"))
            })
            .transpose()?;
        let manifest = get("checkpoint")?
            .map(|v| Manifest::parse(&v))
            .transpose()?;
        let catalog = get("catalog")?;
        let metadata = names
            .contains(METADATA.name())
            .then(|| read.open_table(METADATA).map_err(err))
            .transpose()?;
        let base = names
            .contains(CHECKPOINT.name())
            .then(|| read.open_table(CHECKPOINT).map_err(err))
            .transpose()?;
        let overlay = names
            .contains(OVERLAY.name())
            .then(|| read.open_table(OVERLAY).map_err(err))
            .transpose()?;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let staging = Staging(parent.join(format!(".substrate-import-{}", Id::new())));
        let mut file = fs::OpenOptions::new();
        file.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file.mode(0o600);
        }
        let file = file.open(&staging.0).map_err(err)?;
        let target =
            Self::open_with_factory(options, |builder| builder.create_file(file).map_err(err))?;
        {
            let tx = target.inner.db.begin_write().map_err(err)?;
            tx.open_table(SCRATCH).map_err(err)?;
            tx.commit().map_err(err)?;
        }
        let mut import = Import {
            db: &target.inner.db,
            state: State::default(),
            options,
            metadata: BTreeMap::new(),
            metadata_bytes: 0,
            identities: 0,
            rows: 0,
        };
        let mut revision = 0;
        if let Some(manifest) = manifest {
            if head.is_none() {
                return Err(err("resident checkpoint without head"));
            }
            let catalog = catalog
                .as_deref()
                .ok_or_else(|| err("resident checkpoint missing catalog"))?;
            let mut bytes = catalog.len() as u64;
            if manifest.format == 1 {
                if metadata.as_ref().is_some_and(|t| t.len().unwrap_or(1) != 0) {
                    return Err(err("orphan resident metadata"));
                }
                import.record(
                    catalog,
                    manifest.revision,
                    None,
                    None,
                    true,
                    Some(manifest.revision),
                )?;
            } else {
                let (expected_format, expected_catalog) = if manifest.format == 3 {
                    (b"LDB3", b"LCC3")
                } else {
                    (b"LDB2", b"LCC2")
                };
                if format.as_deref() != Some(expected_format.as_slice())
                    || catalog != expected_catalog
                {
                    return Err(err("resident checkpoint format mismatch"));
                }
                let metadata = metadata
                    .as_ref()
                    .ok_or_else(|| err("resident checkpoint missing metadata table"))?;
                for item in metadata.iter().map_err(err)? {
                    let (key, value) = item.map_err(err)?;
                    import.record(
                        value.value(),
                        0,
                        None,
                        Some(key.value()),
                        true,
                        Some(manifest.revision),
                    )?;
                    bytes = bytes
                        .checked_add(value.value().len() as u64)
                        .ok_or_else(|| err("checkpoint byte overflow"))?;
                }
            }
            let base = base
                .as_ref()
                .ok_or_else(|| err("resident checkpoint missing rows table"))?;
            if manifest.format != 3 && overlay.as_ref().is_some_and(|t| t.len().unwrap_or(1) != 0) {
                return Err(err("orphan resident overlay"));
            }
            if manifest.format == 3 {
                let overlay = overlay
                    .as_ref()
                    .ok_or_else(|| err("missing resident overlay"))?;
                if manifest.overlay_keys > 4096
                    || manifest.overlay_bytes > 8 * 1024 * 1024
                    || manifest.base_rows != base.len().map_err(err)?
                    || manifest.overlay_keys != overlay.len().map_err(err)?
                {
                    return Err(err("resident overlay bounds/count mismatch"));
                }
                let mut size = 0u64;
                for item in overlay.iter().map_err(err)? {
                    let (k, v) = item.map_err(err)?;
                    if k.value().len() != 32 {
                        return Err(err("resident overlay key length"));
                    }
                    size = size
                        .checked_add(32 + v.value().len() as u64)
                        .filter(|n| *n <= 8 * 1024 * 1024)
                        .ok_or_else(|| err("resident overlay exceeds bound"))?;
                }
                if size != manifest.overlay_bytes {
                    return Err(err("resident overlay byte mismatch"));
                }
            }
            for item in base.iter().map_err(err)? {
                let (key, value) = item.map_err(err)?;
                if value.value().is_empty() {
                    return Err(err("empty resident base row"));
                }
                let replacement = overlay
                    .as_ref()
                    .map(|t| t.get(key.value()).map_err(err))
                    .transpose()?
                    .flatten();
                let value = replacement.as_ref().map_or(value.value(), |v| v.value());
                if value.is_empty() {
                    let k = key.value();
                    if k.len() != 32 {
                        return Err(err("resident tombstone key"));
                    }
                    let tid = Id(uuid::Uuid::from_bytes(k[..16].try_into().map_err(err)?));
                    let ids = target.inner.db.begin_read().map_err(err)?;
                    if !import.state.relations.contains_key(&tid)
                        || ids
                            .open_table(IDS)
                            .map_err(err)?
                            .get(&k[16..])
                            .map_err(err)?
                            .is_none()
                    {
                        return Err(err("resident tombstone identity/type"));
                    }
                    continue;
                }
                import.record(
                    value,
                    if manifest.format == 1 {
                        manifest.revision
                    } else {
                        0
                    },
                    Some(key.value()),
                    None,
                    false,
                    Some(manifest.revision),
                )?;
                bytes = bytes
                    .checked_add(value.len() as u64)
                    .ok_or_else(|| err("checkpoint byte overflow"))?;
            }
            if manifest.format == 3 {
                for item in overlay.as_ref().unwrap().iter().map_err(err)? {
                    let (key, value) = item.map_err(err)?;
                    if base.get(key.value()).map_err(err)?.is_some() {
                        continue;
                    }
                    if value.value().is_empty() {
                        return Err(err("resident tombstone without base"));
                    }
                    import.record(
                        value.value(),
                        0,
                        Some(key.value()),
                        None,
                        false,
                        Some(manifest.revision),
                    )?;
                    bytes = bytes
                        .checked_add(value.value().len() as u64)
                        .ok_or_else(|| err("checkpoint byte overflow"))?;
                }
            }
            if manifest.rows != import.rows
                || manifest.identities != import.identities
                || manifest.modules != import.state.modules.len() as u64
                || manifest.grants != import.state.grants.iter().count() as u64
                || manifest.schema_grants != import.state.schema_grants.len() as u64
                || manifest.bytes != bytes
            {
                return Err(err("resident checkpoint manifest mismatch"));
            }
            import.audit_checkpoint_refs()?;
            revision = manifest.revision;
        } else if catalog.is_some()
            || metadata.as_ref().is_some_and(|t| t.len().unwrap_or(1) != 0)
            || base.as_ref().is_some_and(|t| t.len().unwrap_or(1) != 0)
            || overlay.as_ref().is_some_and(|t| t.len().unwrap_or(1) != 0)
        {
            return Err(err("orphan resident checkpoint sections"));
        }
        for item in read.open_table(LOG).map_err(err)?.iter().map_err(err)? {
            let (key, value) = item.map_err(err)?;
            if revision.checked_add(1) != Some(key.value()) {
                return Err(err("resident redo sequence mismatch"));
            }
            import.record(value.value(), key.value(), None, None, false, None)?;
            revision = key.value();
            crate::durable::failpoint("resident_import_after_record");
        }
        if head.is_some_and(|head| head != revision) {
            return Err(err("resident durable head mismatch"));
        }
        import.finish(revision)?;
        drop(import);
        drop(target);
        let verified = Self::open(&staging.0, options)?;
        let result = verified.admin().snapshot()?.verify()?;
        drop(verified);
        crate::durable::failpoint("resident_import_before_publish");
        fs::hard_link(&staging.0, destination).map_err(err)?;
        #[cfg(unix)]
        fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(err)?;
        crate::durable::failpoint("resident_import_after_publish");
        fs::remove_file(&staging.0).map_err(err)?;
        #[cfg(unix)]
        fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(err)?;
        Ok(PagedImportStats {
            source_format: String::from_utf8_lossy(format.as_deref().unwrap_or(b"LDB1"))
                .into_owned(),
            revision,
            rows: result.rows,
            allocated_ids: result.allocated_ids,
        })
    }
}
impl Import<'_> {
    fn finish(&self, revision: u64) -> Result<()> {
        self.state.validate_recovered()?;
        let mut data = authority::extract(&empty_data(), &self.state)?;
        // Preserve legacy grant-only reads. Do not grant ownership write/reference
        // authority that the resident policy never possessed.
        for (tid, policy) in &self.state.schema_visibility {
            if policy.owner != HOST
                && self
                    .state
                    .grant(&policy.owner, *tid)
                    .is_none_or(|g| g.declaration != Grant::read_write())
            {
                return Err(err(
                    "resident owner lacks unrestricted write/reference; paged ownership would broaden authority",
                ));
            }
        }
        for (tid, origin) in &self.state.origins {
            if crate::visibility::protected(&origin.0) {
                continue;
            }
            data.visibility
                .entry(*tid)
                .or_insert_with(|| (data.owners[&origin.0].clone(), Visibility::Private));
            // A namespace owner with no resident visibility policy could gain
            // ambient read/write. Require host ownership for this legacy case.
            if !self.state.schema_visibility.contains_key(tid) && data.owners[&origin.0] != HOST {
                return Err(err("legacy resident namespace ownership cannot be widened"));
            }
        }
        if !data.visibility_ids.is_empty() {
            for tid in data.visibility.keys() {
                data.visibility_ids
                    .entry(*tid)
                    .or_insert_with(|| authority::visibility_id(*tid));
            }
        }
        let mut compiled = Catalog::compile(data)?;
        compiled.data.reserved = authority::catalog_ids(&compiled.state);
        let catalog = serde_json::to_vec(&compiled.data).map_err(err)?;
        if catalog.len() > self.options.max_catalog_bytes {
            return Err(err("mapped resident catalog exceeds budget"));
        }
        let read = self.db.begin_read().map_err(err)?;
        let source = read.open_table(SCRATCH).map_err(err)?;
        let mut iter = source.iter().map_err(err)?.peekable();
        while iter.peek().is_some() {
            let tx = self.staging_write()?;
            {
                let mut writer = RowWriter::new(
                    tx.open_table(ROWS).map_err(err)?,
                    PagedFormat::V3,
                    self.options.max_row_bytes,
                );
                let mut bytes = 0usize;
                let mut count = 0;
                while let Some(next) = iter.peek() {
                    let next = next.as_ref().map_err(err)?;
                    let cost = next.1.value().len().saturating_add(128);
                    if cost > self.options.max_transaction_bytes {
                        return Err(err("import row exceeds batch bound"));
                    }
                    if count > 0
                        && (count >= self.options.max_transaction_rows
                            || cost > self.options.max_transaction_bytes - bytes)
                    {
                        break;
                    }
                    let (key, value) = iter.next().unwrap().map_err(err)?;
                    let id = Id(uuid::Uuid::from_bytes(
                        key.value()[16..].try_into().map_err(err)?,
                    ));
                    if compiled.data.reserved.contains(&id) {
                        return Err(err(
                            "mapped control identity has resident application membership",
                        ));
                    }
                    writer.insert(key.value(), value.value().to_vec())?;
                    bytes += cost;
                    count += 1;
                }
                writer.finish()?;
            }
            tx.commit().map_err(err)?;
        }
        drop(iter);
        drop(source);
        drop(read);
        let mut tx = self.db.begin_write().map_err(err)?;
        tx.set_durability(redb::Durability::Immediate)
            .map_err(err)?;
        {
            let mut ids = tx.open_table(IDS).map_err(err)?;
            let mut versions = tx.open_table(VERS).map_err(err)?;
            for id in &compiled.data.reserved {
                if ids.get(id.0.as_bytes().as_slice()).map_err(err)?.is_some()
                    && !self.state.identities.contains(id)
                    && !self.state.relations.contains_key(id)
                {
                    return Err(err(
                        "mapped control identity collides with resident allocation",
                    ));
                }
                ids.insert(id.0.as_bytes().as_slice(), 0).map_err(err)?;
                versions
                    .insert(tagged(b'a', id.0.as_bytes()).as_slice(), revision.max(1))
                    .map_err(err)?;
            }
            let mut meta = tx.open_table(META).map_err(err)?;
            meta.insert("catalog", catalog.as_slice()).map_err(err)?;
            meta.insert("revision", revision.to_le_bytes().as_slice())
                .map_err(err)?;
            meta.insert("auth_epoch", revision.to_le_bytes().as_slice())
                .map_err(err)?;
        }
        tx.delete_table(SCRATCH).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn memory_fixture() -> (Database, Id, Id) {
        let db = Database::memory();
        let target = Id::from_u128(11);
        let link = Id::from_u128(12);
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "old",
                1,
                vec![],
                vec![
                    Relation::new(
                        "old.Target",
                        vec![Role::new("value", DataType::Float.nullable())],
                    ),
                    Relation::new(
                        "old.Link",
                        vec![Role::new("target", DataType::Ref("old.Target".into())).deferrable()],
                    ),
                ],
            ),
            Visibility::Private,
        )
        .unwrap();
        tx.defer_constraints().unwrap();
        tx.insert_with_id("old.Link", link, vec![Value::Ref(target)])
            .unwrap();
        tx.insert_with_id("old.Target", target, vec![Value::Null])
            .unwrap();
        tx.grant("reader", "old.Target", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        (db, target, link)
    }
    fn checkpoint_fixture(path: &Path, format: u8) -> (Id, Id) {
        let (db, target, link) = memory_fixture();
        let snapshot = db.admin().snapshot();
        let state = &snapshot.state;
        let disk = redb::Database::create(path).unwrap();
        let tx = disk.begin_write().unwrap();
        let mut ops = Vec::new();
        ops.extend(state.modules.values().map(|m| Op::Install((**m).clone())));
        ops.extend(state.identities.iter().copied().map(Op::Identity));
        ops.extend(state.grants.iter().map(|((principal, tid), grant)| {
            Op::Grant(principal.clone(), *tid, Some(grant.declaration.clone()))
        }));
        ops.extend(
            state
                .schema_grants
                .iter()
                .map(|(p, n)| Op::SchemaGrant(p.clone(), n.clone())),
        );
        let mut length = 0u64;
        let mut rows = 0u64;
        {
            let mut control = tx.open_table(CONTROL).unwrap();
            if format == 1 {
                let catalog = crate::codec::encode(state.revision, &ops, state).unwrap();
                length += catalog.len() as u64;
                control.insert("catalog", catalog.as_slice()).unwrap();
            } else {
                let magic = if format == 3 { b"LCC3" } else { b"LCC2" };
                control.insert("catalog", magic.as_slice()).unwrap();
                length += 4;
                let mut metadata = tx.open_table(METADATA).unwrap();
                for op in &ops {
                    let key = crate::codec::metadata_key(op).unwrap();
                    let value = crate::codec::encode(0, std::slice::from_ref(op), state).unwrap();
                    length += value.len() as u64;
                    metadata.insert(key.as_slice(), value.as_slice()).unwrap();
                }
            }
            let mut table = tx.open_table(CHECKPOINT).unwrap();
            for (tid, family) in &state.families {
                for row in family.values() {
                    let value = crate::codec::encode(
                        if format == 1 { state.revision } else { 0 },
                        &[Op::Put(*tid, (**row).clone())],
                        state,
                    )
                    .unwrap();
                    table
                        .insert(encoding::row_key(*tid, row.id).as_slice(), value.as_slice())
                        .unwrap();
                    length += value.len() as u64;
                    rows += 1;
                }
            }
            let mut manifest = format!("LCP{format}").into_bytes();
            for value in [
                state.revision,
                rows,
                state.identities.len() as u64,
                state.modules.len() as u64,
                state.grants.iter().count() as u64,
                state.schema_grants.len() as u64,
                length,
            ] {
                manifest.extend(value.to_le_bytes());
            }
            if format == 3 {
                for value in [rows, 0, 0] {
                    manifest.extend(value.to_le_bytes());
                }
                tx.open_table(OVERLAY).unwrap();
            }
            control.insert("checkpoint", manifest.as_slice()).unwrap();
            control
                .insert("format", format!("LDB{format}").as_bytes())
                .unwrap();
            control
                .insert("head", 2u64.to_le_bytes().as_slice())
                .unwrap();
        }
        let event = crate::codec::encode(
            2,
            &[Op::Put(
                state.names["old.Target"],
                Row {
                    id: target,
                    values: vec![Value::try_from(1.5).unwrap()].into_boxed_slice(),
                },
            )],
            state,
        )
        .unwrap();
        tx.open_table(LOG)
            .unwrap()
            .insert(2, event.as_slice())
            .unwrap();
        tx.commit().unwrap();
        (target, link)
    }
    #[test]
    fn streams_all_resident_checkpoint_versions_and_redo_with_visibility_and_types() {
        for format in 1..=3 {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let destination = dir.path().join("destination");
            let (target, link) = checkpoint_fixture(&source, format);
            let options = PagedOptions {
                max_transaction_rows: 1,
                max_transaction_bytes: 32 * 1024,
                decoded_cache_bytes: 0,
                ..Default::default()
            };
            let report =
                PagedDatabase::import_resident_file(&source, &destination, options).unwrap();
            assert_eq!(report.source_format, format!("LDB{format}"));
            assert_eq!(report.revision, 2);
            assert_eq!(report.rows, 2);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
            let imported = PagedDatabase::open(&destination, options).unwrap();
            assert_eq!(
                imported
                    .session("reader")
                    .unwrap()
                    .get("old.Target", target)
                    .unwrap()
                    .unwrap()
                    .values[0],
                Value::try_from(1.5).unwrap()
            );
            assert!(matches!(
                imported.anonymous().get("old.Target", target),
                Err(Error::Denied)
            ));
            assert!(matches!(
                imported.session("reader").unwrap().get("old.Link", link),
                Err(Error::Denied)
            ));
            imported.admin().snapshot().unwrap().verify().unwrap();
            let original = Database::open(&source).unwrap();
            assert_eq!(original.admin().snapshot().revision(), 2);
            assert_eq!(
                original
                    .admin()
                    .snapshot()
                    .get("old.Target", target)
                    .unwrap()
                    .unwrap()
                    .values[0],
                Value::try_from(1.5).unwrap()
            );
        }
    }
    #[test]
    fn log_only_import_preserves_deferred_refs_deleted_ids_and_authenticated_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        let db = Database::open_with_options(
            &source,
            DatabaseOptions {
                checkpoint_policy: None,
            },
        )
        .unwrap();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let live = tx.insert("core.Person", vec!["live".into()]).unwrap();
        let deleted = tx.insert("core.Person", vec!["deleted".into()]).unwrap();
        tx.commit().unwrap();
        let mut tx = db.admin().transaction();
        tx.delete("core.Person", deleted).unwrap();
        tx.commit().unwrap();
        drop(db);
        PagedDatabase::import_resident_file(&source, &destination, Default::default()).unwrap();
        let db = PagedDatabase::open(&destination, Default::default()).unwrap();
        assert!(
            db.session("unrelated")
                .unwrap()
                .get("core.Person", live)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            db.anonymous().get("core.Person", live),
            Err(Error::Denied)
        ));
        let mut tx = db.admin().transaction().unwrap();
        assert!(
            tx.insert_with_id("core.Person", deleted, vec!["reuse".into()])
                .is_err()
        );
    }
    #[test]
    fn imported_policy_occurrence_ids_survive_reopen_visibility_edits_and_extensions() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        let db = Database::open(&source).unwrap();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), Visibility::Private)
            .unwrap();
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let tid = snapshot.state.names["core.Person"];
        let policy_id = snapshot.state.schema_visibility[&tid].row_id;
        drop(snapshot);
        db.checkpoint().unwrap();
        drop(db);
        PagedDatabase::import_resident_file(&source, &destination, Default::default()).unwrap();
        let db = PagedDatabase::open(&destination, Default::default()).unwrap();
        assert_eq!(
            db.admin()
                .snapshot()
                .unwrap()
                .view
                .catalog
                .state
                .schema_visibility[&tid]
                .row_id,
            policy_id
        );
        let mut tx = db.admin().transaction().unwrap();
        tx.set_visibility("core.Person", Visibility::Public)
            .unwrap();
        tx.install(Module::new(
            "extension",
            1,
            vec![],
            vec![Relation::new(
                "extension.Item",
                vec![Role::new("n", DataType::Int)],
            )],
        ))
        .unwrap();
        tx.commit().unwrap();
        drop(db);
        let db = PagedDatabase::open(&destination, Default::default()).unwrap();
        let snapshot = db.admin().snapshot().unwrap();
        assert_eq!(
            snapshot.view.catalog.state.schema_visibility[&tid].row_id,
            policy_id
        );
        snapshot.verify().unwrap();
        let mut data = snapshot.view.catalog.data.clone();
        data.visibility_ids.remove(&tid);
        assert!(Catalog::compile(data).is_err());
        let mut data = snapshot.view.catalog.data.clone();
        for id in data.visibility_ids.values_mut() {
            *id = policy_id;
        }
        assert!(Catalog::compile(data).is_err());
        let mut data = snapshot.view.catalog.data.clone();
        data.visibility_ids.insert(tid, tid);
        assert!(Catalog::compile(data).is_err());
    }
    #[test]
    fn imports_credentials_capabilities_and_preserves_live_revocation() {
        use crate::capability::{CapabilityScope, RelationCapability};
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        let db = Database::open(&source).unwrap();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), Visibility::Private)
            .unwrap();
        let person = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.grant("owner", "core.Person", Grant::read_write())
            .unwrap();
        tx.commit().unwrap();
        let relation = db.admin().snapshot().schema("core.Person").unwrap().id;
        let capability = db
            .issue_capability(
                Id::new(),
                "owner",
                CapabilityScope {
                    relations: vec![RelationCapability {
                        relation,
                        grant: Grant::read_only(),
                    }],
                    schema_namespace: None,
                    delegations: 0,
                },
            )
            .unwrap();
        let expiry = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let credential = db.issue_credential("owner", capability.id, expiry).unwrap();
        db.checkpoint().unwrap();
        drop(db);
        PagedDatabase::import_resident_file(&source, &destination, Default::default()).unwrap();
        let imported = PagedDatabase::open(&destination, Default::default()).unwrap();
        let session = imported.authenticate(&credential.token).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert!(snapshot.get("core.Person", person).unwrap().is_some());
        let mut write = session.transaction().unwrap();
        assert!(matches!(
            write.update("core.Person", person, vec!["no".into()]),
            Err(Error::Denied)
        ));
        imported.revoke_capability(capability.id).unwrap();
        assert!(matches!(
            snapshot.get("core.Person", person),
            Err(Error::Denied)
        ));
        assert!(imported.authenticate(&credential.token).is_err());
    }
    #[test]
    fn rejects_nondeferrable_forward_refs_and_out_of_order_imports_in_v3_redo() {
        for wrong_module_order in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let destination = dir.path().join("destination");
            let mut state = State::default();
            let module = Module::new(
                "strict",
                1,
                vec![],
                vec![
                    Relation::new("strict.Target", vec![Role::new("n", DataType::Float)]),
                    Relation::new(
                        "strict.Link",
                        vec![Role::new("target", DataType::Ref("strict.Target".into()))],
                    ),
                ],
            );
            state
                .apply_recovered(&Op::Install(module.clone()), 1)
                .unwrap();
            let target = Id::from_u128(100);
            let link = Id::from_u128(101);
            let ops = if wrong_module_order {
                vec![
                    Op::Install(Module::new(
                        "later",
                        1,
                        vec![("strict".into(), 1)],
                        vec![Relation::new(
                            "later.Value",
                            vec![Role::new("n", DataType::Float)],
                        )],
                    )),
                    Op::Install(module),
                ]
            } else {
                vec![
                    Op::Install(module),
                    Op::Identity(target),
                    Op::Identity(link),
                    Op::Put(
                        state.names["strict.Link"],
                        Row {
                            id: link,
                            values: vec![Value::Ref(target)].into_boxed_slice(),
                        },
                    ),
                    Op::Put(
                        state.names["strict.Target"],
                        Row {
                            id: target,
                            values: vec![Value::try_from(1.0).unwrap()].into_boxed_slice(),
                        },
                    ),
                ]
            };
            let bytes = crate::codec::encode(1, &ops, &state).unwrap();
            assert_eq!(&bytes[..4], b"LQ03");
            {
                let db = redb::Database::create(&source).unwrap();
                let tx = db.begin_write().unwrap();
                tx.open_table(LOG)
                    .unwrap()
                    .insert(1, bytes.as_slice())
                    .unwrap();
                tx.commit().unwrap();
            }
            assert!(
                PagedDatabase::import_resident_file(&source, &destination, Default::default())
                    .is_err()
            );
            assert!(!destination.exists());
            assert!(Database::open(&source).is_err());
        }
    }
    #[test]
    fn corrupt_redo_and_budget_errors_never_publish_or_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        checkpoint_fixture(&source, 3);
        assert!(
            PagedDatabase::import_resident_file(
                &source,
                &destination,
                PagedOptions {
                    max_transaction_bytes: 1,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(!destination.exists());
        {
            let disk = redb::Database::open(&source).unwrap();
            let tx = disk.begin_write().unwrap();
            tx.open_table(LOG)
                .unwrap()
                .insert(2, b"broken".as_slice())
                .unwrap();
            tx.commit().unwrap();
        }
        assert!(
            PagedDatabase::import_resident_file(&source, &destination, Default::default()).is_err()
        );
        assert!(!destination.exists());
        fs::write(&destination, b"existing").unwrap();
        assert!(
            PagedDatabase::import_resident_file(&source, &destination, Default::default()).is_err()
        );
        assert_eq!(fs::read(&destination).unwrap(), b"existing");
    }
    #[test]
    fn crashes_publish_only_a_complete_verified_resident_import() {
        for point in [
            "resident_import_after_record",
            "resident_import_before_publish",
            "resident_import_after_publish",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let destination = dir.path().join("destination");
            checkpoint_fixture(&source, 3);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "paged::resident_import::tests::import_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("SUBSTRATE_RESIDENT_IMPORT_SOURCE", &source)
                .env("SUBSTRATE_RESIDENT_IMPORT_DESTINATION", &destination)
                .env("SUBSTRATE_TEST_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(91));
            assert_eq!(
                destination.exists(),
                point == "resident_import_after_publish"
            );
            if destination.exists() {
                let db = PagedDatabase::open(&destination, Default::default()).unwrap();
                assert_eq!(db.admin().snapshot().unwrap().verify().unwrap().rows, 2);
                assert_eq!(db.stats().unwrap().revision, 2);
            }
            assert_eq!(
                Database::open(&source)
                    .unwrap()
                    .admin()
                    .snapshot()
                    .revision(),
                2
            );
        }
    }
    #[test]
    #[ignore]
    fn import_child() {
        PagedDatabase::import_resident_file(
            std::env::var_os("SUBSTRATE_RESIDENT_IMPORT_SOURCE").unwrap(),
            std::env::var_os("SUBSTRATE_RESIDENT_IMPORT_DESTINATION").unwrap(),
            Default::default(),
        )
        .unwrap();
    }
}
