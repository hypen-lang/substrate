//! Explicit streaming integrity audit. No whole-database maps or row vectors;
//! each check retains at most a decoded source row and bounded encoded keys.
use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PagedVerifyStats {
    pub revision: u64,
    pub rows: u64,
    pub role_index_entries: u64,
    pub unique_index_entries: u64,
    pub incoming_index_entries: u64,
    /// Includes catalog/control IDs and historical identities retained after deletion.
    pub allocated_ids: u64,
    /// Includes internal control-plane relation types.
    pub relation_types: u64,
}

fn invalid(message: &str) -> Error {
    err(format!("integrity audit: {message}"))
}
fn key_id(bytes: &[u8]) -> Result<Id> {
    Ok(Id(uuid::Uuid::from_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("malformed identity key"))?,
    )))
}
fn key_slot(bytes: &[u8]) -> Result<usize> {
    let value = u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("malformed index slot"))?,
    );
    usize::try_from(value).map_err(|_| invalid("index slot exceeds platform bound"))
}
fn increment(count: &mut u64) -> Result<()> {
    *count = count
        .checked_add(1)
        .ok_or_else(|| invalid("entry count overflow"))?;
    Ok(())
}

impl PagedSnapshot {
    /// Audit this pinned revision's rows, allocations and all secondary indexes.
    /// This administrator-only scan is explicit, may take time proportional to
    /// the database, and does not run automatically when a database is opened.
    /// Writers may continue; the audit observes one consistent redb snapshot.
    pub fn verify(&self) -> Result<PagedVerifyStats> {
        if self.principal.is_some() {
            return Err(Error::Denied);
        }
        self.fence()?;
        let meta = self.view.read.open_table(META).map_err(err)?;
        if meta
            .get("format")
            .map_err(err)?
            .is_none_or(|v| v.value() != self.view.format.bytes())
            || read_u64(&meta, "revision")? != self.view.revision
            || read_u64(&meta, "auth_epoch")? != self.view.auth_epoch
            || self.view.auth_epoch > self.view.revision
        {
            return Err(invalid("metadata head mismatch"));
        }
        let catalog = meta
            .get("catalog")
            .map_err(err)?
            .ok_or_else(|| invalid("missing catalog"))?;
        if catalog.value().len() > self.db.inner.options.max_catalog_bytes {
            return Err(invalid("catalog exceeds configured limit"));
        }
        // Catalog structure/ownership is compiled and validated when this view
        // is created. Confirm the pinned durable declaration is the same one.
        let declared: CatalogData = serde_json::from_slice(catalog.value()).map_err(err)?;
        let compiled = &self.view.catalog.data;
        if declared.modules != compiled.modules
            || declared.grants != compiled.grants
            || declared.visibility != compiled.visibility
            || declared.visibility_ids != compiled.visibility_ids
            || declared.owners != compiled.owners
            || declared.control != compiled.control
            || declared.reserved != compiled.reserved
        {
            return Err(invalid("compiled catalog differs from durable declaration"));
        }
        drop(declared);
        authority::validate_disk_reservations(&self.view)?;
        for entry in meta.iter().map_err(err)? {
            let (key, _) = entry.map_err(err)?;
            if !matches!(
                key.value(),
                "format" | "catalog" | "revision" | "auth_epoch"
            ) {
                return Err(invalid("unknown metadata key"));
            }
        }
        drop(catalog);
        drop(meta);

        let rows = &self.view.rows;
        let index = self.view.read.open_table(INDEX).map_err(err)?;
        let unique = self.view.read.open_table(UNIQUE).map_err(err)?;
        let incoming = self.view.read.open_table(INCOMING).map_err(err)?;
        let ids = self.view.read.open_table(IDS).map_err(err)?;
        let versions = self.view.read.open_table(VERS).map_err(err)?;
        let mut stats = PagedVerifyStats {
            revision: self.view.revision,
            ..PagedVerifyStats::default()
        };

        // Historical conflict entries survive deletion. Check their timestamps
        // and the reverse allocation mapping without collecting deleted rows.
        for entry in versions.iter().map_err(err)? {
            self.fence()?;
            let (key, version) = entry.map_err(err)?;
            if version.value() == 0 || version.value() > self.view.revision {
                return Err(invalid("invalid conflict version"));
            }
            let key = key.value();
            if key.first() == Some(&b'a') && key != b"auth" {
                let id = key_id(&key[1..])?;
                if ids
                    .get(id.0.as_bytes().as_slice())
                    .map_err(err)?
                    .is_none_or(|v| v.value() != 0)
                {
                    return Err(invalid("historical identity is not allocated"));
                }
            }
        }

        for entry in ids.iter().map_err(err)? {
            self.fence()?;
            let (key, value) = entry.map_err(err)?;
            let id = key_id(key.value())?;
            if value.value() != 0 {
                return Err(invalid("invalid allocation marker"));
            }
            let version = versions
                .get(tagged(b'a', id.0.as_bytes()).as_slice())
                .map_err(err)?;
            if version.is_none_or(|v| v.value() == 0 || v.value() > self.view.revision) {
                return Err(invalid("missing or invalid allocation version"));
            }
            increment(&mut stats.allocated_ids)?;
        }
        for tid in self.view.catalog.state.relations.keys() {
            if ids.get(tid.0.as_bytes().as_slice()).map_err(err)?.is_none()
                && !(self.view.catalog.data.reserved.is_empty()
                    && crate::visibility::protected(&self.view.catalog.state.origins[tid].0))
            {
                return Err(invalid("schema identity is not allocated"));
            }
            increment(&mut stats.relation_types)?;
        }

        // Forward checks catch missing entries even if unrelated extra entries
        // happen to keep a secondary table's total length unchanged.
        for entry in rows.iter().map_err(err)? {
            self.fence()?;
            let (key, bytes) = entry.map_err(err)?;
            let key = key.value();
            if key.len() != 32 {
                return Err(invalid("malformed row key"));
            }
            let tid = key_id(&key[..16])?;
            let id = key_id(&key[16..])?;
            if self.view.catalog.state.relations.contains_key(&id)
                || self.view.catalog.state.identities.contains(&id)
                || ids.get(id.0.as_bytes().as_slice()).map_err(err)?.is_none()
            {
                return Err(invalid(
                    "row identity is unallocated or collides with schema",
                ));
            }
            let (revision, row) = self.audit_decode(tid, id, bytes.value())?;
            if versions
                .get(tagged(b'r', key).as_slice())
                .map_err(err)?
                .is_some_and(|v| v.value() != revision)
            {
                return Err(invalid("legacy row revision differs from conflict version"));
            }
            let def = &self.view.catalog.state.relations[&tid];
            for (slot, value) in row.values.iter().enumerate() {
                if !self.view.format.role_index(def, slot) {
                    continue;
                }
                if index
                    .get(index_key(tid, slot, value, id).as_slice())
                    .map_err(err)?
                    .is_none_or(|v| v.value() != 0)
                {
                    return Err(invalid("missing or invalid role index entry"));
                }
            }
            for constraint in 0..def.unique.len() {
                if !unique_applies(def, &row, constraint) {
                    continue;
                }
                let key = unique_key(tid, constraint, &def.unique_values(&row, constraint));
                if unique
                    .get(key.as_slice())
                    .map_err(err)?
                    .is_none_or(|v| v.value() != id.0.as_bytes())
                {
                    return Err(invalid("missing or incorrect unique index entry"));
                }
            }
            for (slot, target) in &def.reference_slots {
                if matches!(row.values[*slot], Value::Null) {
                    continue;
                }
                let Value::Ref(target_id) = row.values[*slot] else {
                    return Err(invalid("invalid reference value"));
                };
                let target_tid = *self
                    .view
                    .catalog
                    .state
                    .names
                    .get(target)
                    .ok_or_else(|| invalid("unknown reference type"))?;
                if rows
                    .get(row_key(target_tid, target_id).as_slice())
                    .map_err(err)?
                    .is_none()
                {
                    return Err(invalid("dangling typed reference"));
                }
                if incoming
                    .get(incoming_key(target_tid, target_id, tid, id, *slot).as_slice())
                    .map_err(err)?
                    .is_none_or(|v| v.value() != 0)
                {
                    return Err(invalid("missing or invalid incoming index entry"));
                }
            }
            increment(&mut stats.rows)?;
        }

        // Reverse checks reject orphan/stale/malformed entries and wrong unique
        // owners. Decode directly rather than allowing a warm row cache to hide
        // corruption, or populating that cache with a full audit's working set.
        for entry in index.iter().map_err(err)? {
            self.fence()?;
            let (key, marker) = entry.map_err(err)?;
            let key = key.value();
            if key.len() < 41 || marker.value() != 0 {
                return Err(invalid("malformed role index entry"));
            }
            let tid = key_id(&key[..16])?;
            let slot = key_slot(&key[16..24])?;
            let id = key_id(&key[key.len() - 16..])?;
            let row = self.audit_row(rows, tid, id)?;
            let value = row
                .values
                .get(slot)
                .ok_or_else(|| invalid("role index slot out of range"))?;
            if !self
                .view
                .format
                .role_index(&self.view.catalog.state.relations[&tid], slot)
                || index_key(tid, slot, value, id) != key
            {
                return Err(invalid("stale or noncanonical role index entry"));
            }
            increment(&mut stats.role_index_entries)?;
        }
        for entry in unique.iter().map_err(err)? {
            self.fence()?;
            let (key, owner) = entry.map_err(err)?;
            let key = key.value();
            if key.len() < 25 {
                return Err(invalid("malformed unique index key"));
            }
            let tid = key_id(&key[..16])?;
            let constraint = key_slot(&key[16..24])?;
            let id = key_id(owner.value())?;
            let row = self.audit_row(rows, tid, id)?;
            let def = &self.view.catalog.state.relations[&tid];
            if constraint >= def.unique.len()
                || !unique_applies(def, &row, constraint)
                || unique_key(tid, constraint, &def.unique_values(&row, constraint)) != key
            {
                return Err(invalid("stale or noncanonical unique index entry"));
            }
            increment(&mut stats.unique_index_entries)?;
        }
        for entry in incoming.iter().map_err(err)? {
            self.fence()?;
            let (key, marker) = entry.map_err(err)?;
            let key = key.value();
            if key.len() != 72 || marker.value() != 0 {
                return Err(invalid("malformed incoming index entry"));
            }
            let target_tid = key_id(&key[..16])?;
            let target_id = key_id(&key[16..32])?;
            let tid = key_id(&key[32..48])?;
            let id = key_id(&key[48..64])?;
            let slot = key_slot(&key[64..72])?;
            let row = self.audit_row(rows, tid, id)?;
            let def = &self.view.catalog.state.relations[&tid];
            let target = def
                .reference_slots
                .iter()
                .find(|(s, _)| *s == slot)
                .and_then(|(_, name)| self.view.catalog.state.names.get(name));
            if target != Some(&target_tid) || row.values.get(slot) != Some(&Value::Ref(target_id)) {
                return Err(invalid("stale incoming index entry"));
            }
            if rows
                .get(row_key(target_tid, target_id).as_slice())
                .map_err(err)?
                .is_none()
            {
                return Err(invalid("incoming index references missing target"));
            }
            increment(&mut stats.incoming_index_entries)?;
        }
        self.fence()?;
        Ok(stats)
    }

    fn audit_decode(&self, tid: Id, id: Id, bytes: &[u8]) -> Result<(u64, Row)> {
        if bytes.len() > self.db.inner.options.max_row_bytes {
            return Err(invalid("row exceeds configured bound"));
        }
        let def = self
            .view
            .catalog
            .state
            .relations
            .get(&tid)
            .ok_or_else(|| invalid("row has unknown relation type"))?;
        let (revision, row) = decode_row(def, id, bytes)?;
        if revision == 0 || revision > self.view.revision {
            return Err(invalid("row revision exceeds pinned head"));
        }
        def.check_values(&row.values)
            .map_err(|_| invalid("row violates schema constraints"))?;
        Ok((revision, row))
    }

    fn audit_row(&self, rows: &dense::RowStore, tid: Id, id: Id) -> Result<Row> {
        let bytes = rows
            .get(row_key(tid, id).as_slice())
            .map_err(err)?
            .ok_or_else(|| invalid("secondary index references missing row"))?;
        self.audit_decode(tid, id, bytes.value())
            .map(|(_, row)| row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, PagedDatabase, Id, Id, Id) {
        let directory = tempfile::tempdir().unwrap();
        let db =
            PagedDatabase::open(directory.path().join("data"), PagedOptions::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        let person = Relation::new(
            "audit.Person",
            vec![
                Role::new("name", DataType::Text),
                Role::new("age", DataType::Int),
            ],
        )
        .unique(&["name"]);
        let edge = Relation::new(
            "audit.Edge",
            vec![
                Role::new("person", DataType::Ref("audit.Person".into())),
                Role::new("label", DataType::Text),
            ],
        );
        tx.install_owned(
            Module::new("audit", 1, vec![], vec![person, edge]),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        let first = tx
            .insert("audit.Person", vec!["Alice".into(), 1.into()])
            .unwrap();
        let second = tx
            .insert("audit.Person", vec!["Bob".into(), 2.into()])
            .unwrap();
        let edge = tx
            .insert("audit.Edge", vec![first.into(), "link".into()])
            .unwrap();
        tx.commit().unwrap();
        (directory, db, first, second, edge)
    }
    fn reopen(directory: &tempfile::TempDir, db: PagedDatabase) -> PagedDatabase {
        drop(db);
        PagedDatabase::open(directory.path().join("data"), PagedOptions::default()).unwrap()
    }

    #[test]
    fn audit_checks_mixed_writes_historical_ids_and_pinned_snapshots() {
        let (directory, db, first, second, edge) = fixture();
        let pinned = db.admin().snapshot().unwrap();
        let before = pinned.verify().unwrap();
        assert_eq!(
            (
                before.rows,
                before.role_index_entries,
                before.unique_index_entries,
                before.incoming_index_entries
            ),
            (3, 4, 2, 1)
        );
        assert_eq!((before.allocated_ids, before.relation_types), (8, 3));
        assert!(matches!(
            db.session("owner").unwrap().snapshot().unwrap().verify(),
            Err(Error::Denied)
        ));
        assert!(matches!(
            db.anonymous().snapshot().unwrap().verify(),
            Err(Error::Denied)
        ));
        let mut tx = db.admin().transaction().unwrap();
        tx.update("audit.Person", second, vec!["Bobby".into(), 3.into()])
            .unwrap();
        tx.update("audit.Edge", edge, vec![second.into(), "moved".into()])
            .unwrap();
        tx.delete("audit.Person", first).unwrap();
        tx.commit().unwrap();
        let after = db.admin().snapshot().unwrap().verify().unwrap();
        assert_eq!(
            (
                after.rows,
                after.role_index_entries,
                after.unique_index_entries,
                after.incoming_index_entries
            ),
            (2, 3, 1, 1)
        );
        assert_eq!(after.allocated_ids, 8);
        assert_eq!(pinned.verify().unwrap(), before);
        drop(pinned);
        assert_eq!(
            reopen(&directory, db)
                .admin()
                .snapshot()
                .unwrap()
                .verify()
                .unwrap(),
            after
        );
    }

    #[test]
    fn audit_detects_missing_and_extra_entries_in_every_secondary_index() {
        for kind in 0..3 {
            for missing in [true, false] {
                let (directory, db, first, second, edge) = fixture();
                let snapshot = db.admin().snapshot().unwrap();
                let person_tid = snapshot.view.catalog.state.names["audit.Person"];
                let edge_tid = snapshot.view.catalog.state.names["audit.Edge"];
                drop(snapshot);
                let tx = db.inner.db.begin_write().unwrap();
                match kind {
                    0 => {
                        let mut table = tx.open_table(INDEX).unwrap();
                        let key = index_key(
                            person_tid,
                            1,
                            &Value::Int(if missing { 1 } else { 99 }),
                            first,
                        );
                        if missing {
                            table.remove(key.as_slice()).unwrap();
                        } else {
                            table.insert(key.as_slice(), 0).unwrap();
                        }
                    }
                    1 => {
                        let mut table = tx.open_table(UNIQUE).unwrap();
                        let key = unique_key(
                            person_tid,
                            0,
                            &[Value::Text(if missing { "Alice" } else { "stale" }.into())],
                        );
                        if missing {
                            table.remove(key.as_slice()).unwrap();
                        } else {
                            table
                                .insert(key.as_slice(), first.0.as_bytes().as_slice())
                                .unwrap();
                        }
                    }
                    _ => {
                        let mut table = tx.open_table(INCOMING).unwrap();
                        let key = incoming_key(
                            person_tid,
                            if missing { first } else { second },
                            edge_tid,
                            edge,
                            0,
                        );
                        if missing {
                            table.remove(key.as_slice()).unwrap();
                        } else {
                            table.insert(key.as_slice(), 0).unwrap();
                        }
                    }
                }
                tx.commit().unwrap();
                let db = reopen(&directory, db);
                assert!(
                    db.admin().snapshot().unwrap().verify().is_err(),
                    "kind={kind}, missing={missing}"
                );
            }
        }
    }

    #[test]
    fn audit_detects_wrong_unique_owner_malformed_keys_and_missing_schema_id() {
        for kind in 0..4 {
            let (directory, db, _, second, _) = fixture();
            let snapshot = db.admin().snapshot().unwrap();
            let tid = snapshot.view.catalog.state.names["audit.Person"];
            drop(snapshot);
            let tx = db.inner.db.begin_write().unwrap();
            match kind {
                0 => {
                    let mut table = tx.open_table(UNIQUE).unwrap();
                    table
                        .insert(
                            unique_key(tid, 0, &["Alice".into()]).as_slice(),
                            second.0.as_bytes().as_slice(),
                        )
                        .unwrap();
                }
                1 => {
                    tx.open_table(INDEX)
                        .unwrap()
                        .insert(b"bad".as_slice(), 0)
                        .unwrap();
                }
                2 => {
                    tx.open_table(INCOMING)
                        .unwrap()
                        .insert(b"bad".as_slice(), 0)
                        .unwrap();
                }
                _ => {
                    tx.open_table(IDS)
                        .unwrap()
                        .remove(tid.0.as_bytes().as_slice())
                        .unwrap();
                }
            }
            tx.commit().unwrap();
            if kind == 3 {
                drop(db);
                assert!(
                    PagedDatabase::open(directory.path().join("data"), PagedOptions::default())
                        .is_err()
                );
            } else {
                assert!(
                    reopen(&directory, db)
                        .admin()
                        .snapshot()
                        .unwrap()
                        .verify()
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn audit_detects_lost_historical_allocations_and_invalid_versions() {
        for kind in 0..3 {
            let (directory, db, _, second, _) = fixture();
            let mut tx = db.admin().transaction().unwrap();
            tx.delete("audit.Person", second).unwrap();
            tx.commit().unwrap();
            let tx = db.inner.db.begin_write().unwrap();
            if kind == 0 {
                tx.open_table(IDS)
                    .unwrap()
                    .remove(second.0.as_bytes().as_slice())
                    .unwrap();
            } else {
                tx.open_table(VERS)
                    .unwrap()
                    .insert(b"catalog".as_slice(), if kind == 1 { 0 } else { u64::MAX })
                    .unwrap();
            }
            tx.commit().unwrap();
            assert!(
                reopen(&directory, db)
                    .admin()
                    .snapshot()
                    .unwrap()
                    .verify()
                    .is_err()
            );
        }
    }
}
