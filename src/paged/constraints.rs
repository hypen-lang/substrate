//! Transaction-local foreign-key deferral and bounded, authorized cascades.
use super::*;

impl PagedTransaction {
    /// Defer only explicitly deferrable foreign keys until commit. Unique and
    /// check constraints remain immediate. A failed operation cannot be revived.
    pub fn defer_constraints(&mut self) -> Result<()> {
        self.mutate(|tx| {
            tx.defer_foreign_keys = true;
            Ok(())
        })
    }

    fn incoming_refs(&mut self, tid: Id, id: Id) -> Result<BTreeSet<(Id, Id, usize)>> {
        let prefix = incoming_prefix(tid, id);
        self.track(tagged(b'n', &prefix))?;
        let end = prefix_end(&prefix);
        let view = self.snapshot.view.clone();
        let mut incoming = BTreeSet::new();
        let mut examined = self.constraint_work;
        for entry in view
            .incoming
            .range::<&[u8]>((
                std::ops::Bound::Included(prefix.as_slice()),
                end.as_deref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            ))
            .map_err(err)?
        {
            examined = examined
                .checked_add(1)
                .ok_or_else(|| Error::Constraint("cascade work overflow".into()))?;
            if examined > self.snapshot.db.inner.options.max_dependencies {
                return Err(Error::Constraint("cascade edge budget exceeded".into()));
            }
            let (key, marker) = entry.map_err(err)?;
            let key = key.value();
            if key.len() != 72 || marker.value() != 0 {
                return Err(err("malformed incoming key"));
            }
            let source = Id(uuid::Uuid::from_slice(&key[32..48]).map_err(err)?);
            let occurrence = Id(uuid::Uuid::from_slice(&key[48..64]).map_err(err)?);
            let slot = usize::try_from(u64::from_be_bytes(key[64..72].try_into().map_err(err)?))
                .map_err(err)?;
            let def = self
                .catalog
                .state
                .relations
                .get(&source)
                .cloned()
                .ok_or_else(|| err("unknown incoming source"))?;
            if def
                .roles
                .get(slot)
                .and_then(|role| role.data_type.reference_target())
                .and_then(|name| self.catalog.state.names.get(name))
                != Some(&tid)
            {
                return Err(err("invalid incoming reference role"));
            }
            let staged = self.rows.contains_key(&(source, occurrence));
            let row = self.raw(source, occurrence)?;
            match row {
                Some(row) if row.values.get(slot) == Some(&Value::Ref(id)) => {
                    if !incoming.contains(&(source, occurrence, slot)) {
                        self.charge(80)?;
                        incoming.insert((source, occurrence, slot));
                    }
                }
                _ if staged => {}
                _ => return Err(err("dangling incoming reference index")),
            }
        }
        // Add transaction-local edges that are not yet present on disk.
        let mut staged_bytes = 0usize;
        for (&(source, occurrence), row) in &self.rows {
            let Some(row) = row else { continue };
            let def = &self.catalog.state.relations[&source];
            for (slot, target) in &def.reference_slots {
                examined = examined
                    .checked_add(1)
                    .ok_or_else(|| Error::Constraint("cascade work overflow".into()))?;
                if examined > self.snapshot.db.inner.options.max_dependencies {
                    return Err(Error::Constraint(
                        "cascade traversal work budget exceeded".into(),
                    ));
                }
                if self.catalog.state.names[target] == tid && row.values[*slot] == Value::Ref(id) {
                    let key = (source, occurrence, *slot);
                    if !incoming.contains(&key) {
                        if incoming.len() >= self.snapshot.db.inner.options.max_dependencies {
                            return Err(Error::Constraint("cascade edge budget exceeded".into()));
                        }
                        staged_bytes = staged_bytes
                            .checked_add(80)
                            .ok_or_else(|| Error::Constraint("cascade byte overflow".into()))?;
                        if self.bytes.checked_add(staged_bytes).is_none_or(|bytes| {
                            bytes > self.snapshot.db.inner.options.max_transaction_bytes
                        }) {
                            return Err(Error::Constraint("cascade byte budget exceeded".into()));
                        }
                        incoming.insert(key);
                    }
                }
            }
        }
        self.charge(staged_bytes)?;
        self.constraint_work = examined;
        Ok(incoming)
    }

    pub(super) fn delete_closure(&mut self, name: &str, ids: &[Id]) -> Result<()> {
        let def = self.def(name)?;
        if ids.len() > self.snapshot.db.inner.options.max_transaction_rows {
            return Err(Error::Constraint("delete root budget exceeded".into()));
        }
        self.charge(ids.len().saturating_mul(32))?;
        let mut closure = BTreeSet::new();
        let mut todo: Vec<_> = ids.iter().map(|&id| (def.id, id)).collect();
        let mut restrictions = Vec::new();
        let mut edges = 0usize;
        while let Some((tid, id)) = todo.pop() {
            if closure.contains(&(tid, id)) {
                continue;
            }
            if closure.len() >= self.snapshot.db.inner.options.max_transaction_rows {
                return Err(Error::Constraint("cascade row budget exceeded".into()));
            }
            self.charge(64)?;
            closure.insert((tid, id));
            let row = self
                .raw(tid, id)?
                .ok_or(if self.snapshot.principal.is_some() {
                    Error::Denied
                } else {
                    Error::NotFound
                })?;
            self.permission(tid, &row, Permission::Write)?;
            for (source, occurrence, slot) in self.incoming_refs(tid, id)? {
                edges = edges
                    .checked_add(1)
                    .ok_or_else(|| Error::Constraint("cascade work overflow".into()))?;
                if edges > self.snapshot.db.inner.options.max_dependencies {
                    return Err(Error::Constraint("cascade edge budget exceeded".into()));
                }
                self.charge(80)?;
                let role = &self.catalog.state.relations[&source].roles[slot];
                if role.on_delete == OnDelete::Cascade {
                    todo.push((source, occurrence));
                } else {
                    restrictions.push((source, occurrence, role.deferrable));
                }
            }
        }
        for (source, occurrence, deferrable) in restrictions {
            if !(closure.contains(&(source, occurrence)) || deferrable && self.defer_foreign_keys) {
                return Err(Error::Constraint("occurrence is referenced".into()));
            }
        }
        // Authorization and closure discovery finish before staging any delete.
        for (tid, id) in closure {
            self.stage(tid, id, None)?;
        }
        Ok(())
    }

    pub(super) fn finalize_constraints(&mut self) -> Result<()> {
        if !self.defer_foreign_keys {
            return Ok(());
        }
        self.charge(self.rows.len().saturating_mul(32))?;
        let keys: Vec<_> = self.rows.keys().copied().collect();
        for (tid, id) in keys {
            let def = self.catalog.state.relations[&tid].clone();
            if let Some(Some(row)) = self.rows.get(&(tid, id)) {
                let refs: Vec<_> = def
                    .reference_slots
                    .iter()
                    .filter_map(|(slot, target)| match row.values[*slot] {
                        Value::Ref(id) if def.roles[*slot].deferrable => {
                            Some((self.catalog.state.names[target], id))
                        }
                        _ => None,
                    })
                    .collect();
                for (target, target_id) in refs {
                    self.reference(target, target_id)?;
                }
            } else if !self.incoming_refs(tid, id)?.is_empty() {
                return Err(Error::Constraint(
                    "deferred reference target is still referenced".into(),
                ));
            }
        }
        self.snapshot.fence()
    }
}
