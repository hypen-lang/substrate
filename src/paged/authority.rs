//! Bounded durable control-plane metadata. Reuse the existing policy compiler and
//! mutation logic without ever putting application records into a resident State.
use super::*;
use crate::capability::{CapabilityScope, CompiledCapability, DelegatedCapability};
use sha2::{Digest, Sha256};

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlData {
    modules: Vec<Module>,
    rows: Vec<(Id, Row)>,
    identities: BTreeSet<Id>,
    schema_grants: BTreeSet<(String, String)>,
}
#[derive(Clone, Copy, Default)]
pub(super) struct Selection {
    pub capability: Option<Id>,
    pub credential: Option<Id>,
}
fn canonical(name: &str) -> Option<&'static Module> {
    match name {
        "liquid_security" => Some(crate::capability::module()),
        "liquid_auth" => Some(crate::auth::module()),
        "liquid_control" => Some(crate::schema_control::control_module()),
        _ => None,
    }
}
pub(super) fn reserved_type(id: Id) -> bool {
    [
        crate::capability::TYPE_ID,
        crate::auth::TYPE_ID,
        crate::visibility::TYPE_ID,
        crate::views::TYPE_ID,
    ]
    .contains(&id)
        || crate::schema_control::control_module()
            .relations
            .iter()
            .any(|r| r.id == id)
}
pub(super) fn invalidate_roots(
    data: &mut CatalogData,
    catalog: &Catalog,
    principal: &str,
    tid: Id,
) -> Result<()> {
    if let Some(roots) = catalog
        .state
        .capability_roots
        .get(&(principal.to_owned(), tid))
    {
        for root in roots {
            let (_, row) = data
                .control
                .rows
                .iter_mut()
                .find(|(family, row)| *family == crate::capability::TYPE_ID && row.id == *root)
                .ok_or_else(|| err("missing capability root record"))?;
            let Some(Value::Bool(revoked)) = row.values.get_mut(1) else {
                return Err(err("malformed capability root record"));
            };
            *revoked = true;
        }
    }
    Ok(())
}
pub(super) fn catalog_ids(state: &State) -> BTreeSet<Id> {
    state
        .identities
        .iter()
        .chain(state.relations.keys())
        .copied()
        .collect()
}
/// Catalog-size work only: old native files have no reservation ledger and did
/// not persist synthesized policy IDs. They may be sealed by the next write.
pub(super) fn validate_disk_reservations(view: &View) -> Result<()> {
    let expected = catalog_ids(&view.catalog.state);
    let recorded = &view.catalog.data.reserved;
    if !recorded.is_empty() && recorded != &expected {
        return Err(err("incomplete catalog identity reservation ledger"));
    }
    let application_types: BTreeSet<_> = view
        .catalog
        .data
        .modules
        .iter()
        .flat_map(|m| m.relations.iter().map(|r| r.id))
        .collect();
    for id in expected {
        let present = view
            .ids
            .get(id.0.as_bytes().as_slice())
            .map_err(err)?
            .is_some();
        if present && !recorded.contains(&id) && !application_types.contains(&id) {
            return Err(err(
                "catalog identity collides with allocated application identity",
            ));
        }
        if !present && (recorded.contains(&id) || application_types.contains(&id)) {
            return Err(err("missing catalog identity allocation"));
        }
    }
    Ok(())
}
pub(super) fn visibility_id(tid: Id) -> Id {
    let mut hash = Sha256::new();
    hash.update(b"substrate-paged-visibility-v1:");
    hash.update(tid.0.as_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Id(uuid::Uuid::from_bytes(bytes))
}
pub(super) fn compile_control(data: &CatalogData, state: &mut State) -> Result<()> {
    if !data.visibility_ids.is_empty()
        && (data.visibility_ids.len() != data.visibility.len()
            || data
                .visibility
                .keys()
                .any(|tid| !data.visibility_ids.contains_key(tid))
            || data
                .visibility_ids
                .values()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != data.visibility_ids.len())
    {
        return Err(err("invalid visibility occurrence identity map"));
    }
    for module in &data.control.modules {
        if canonical(&module.name) != Some(module) {
            return Err(err("noncanonical control module"));
        }
        state.apply_recovered(&Op::Install(module.clone()), 0)?;
    }
    for id in &data.control.identities {
        state.apply_recovered(&Op::Identity(*id), 0)?;
    }
    let mut seen = BTreeSet::new();
    for (tid, row) in &data.control.rows {
        let def = state
            .relations
            .get(tid)
            .ok_or_else(|| err("unknown control row"))?;
        if canonical(&state.origins[tid].0).is_none() || !seen.insert((*tid, row.id)) {
            return Err(err("invalid control row family or duplicate"));
        }
        if !def.reference_slots.is_empty() {
            return Err(err("control row cannot reference application records"));
        }
        state.apply_recovered(&Op::Put(*tid, row.clone()), 0)?;
    }
    for (principal, namespace) in &data.control.schema_grants {
        if principal.is_empty() || principal.starts_with('\0') {
            return Err(err("invalid schema authority"));
        }
        state.apply_recovered(&Op::SchemaGrant(principal.clone(), namespace.clone()), 0)?;
    }
    // Synthesize the existing immutable policy representation from the paged
    // visibility map. Its stable IDs are reserved, never application identities.
    if !data.visibility.is_empty() {
        state.apply_recovered(&Op::Install(crate::visibility::module().clone()), 0)?;
    }
    for (tid, (owner, visibility)) in &data.visibility {
        let id = data
            .visibility_ids
            .get(tid)
            .copied()
            .unwrap_or_else(|| visibility_id(*tid));
        state.apply_recovered(&Op::Identity(id), 0)?;
        state.apply_recovered(
            &Op::Put(
                crate::visibility::TYPE_ID,
                Row {
                    id,
                    values: vec![
                        Value::Text(tid.to_string()),
                        Value::Text(owner.clone()),
                        Value::Text(visibility.name().into()),
                    ]
                    .into_boxed_slice(),
                },
            ),
            0,
        )?;
        // Paged namespace owners already have full native authority. Make the
        // same rights explicit to the shared credential/schema policy compiler.
        if owner != HOST {
            state.apply(
                &Op::Grant(owner.clone(), *tid, Some(Grant::read_write())),
                0,
            );
        }
    }
    state.validate_recovered()?;
    Ok(())
}
pub(super) fn extract(previous: &CatalogData, state: &State) -> Result<CatalogData> {
    let mut data = previous.clone();
    let mut pending: Vec<Module> = state
        .modules
        .values()
        .filter(|m| {
            !crate::visibility::protected(&m.name)
                && !data
                    .modules
                    .iter()
                    .any(|old| old.name == m.name && old.version == m.version)
        })
        .map(|m| (**m).clone())
        .collect();
    while !pending.is_empty() {
        let i = pending
            .iter()
            .position(|m| {
                m.imports.iter().all(|(n, v)| {
                    data.modules
                        .iter()
                        .any(|old| &old.name == n && &old.version == v)
                }) && (m.version == 1
                    || data
                        .modules
                        .iter()
                        .any(|old| old.name == m.name && old.version + 1 == m.version))
            })
            .ok_or_else(|| err("control mutation produced unordered schema"))?;
        data.modules.push(pending.remove(i));
    }
    // Previously installed definitions are immutable, even in host metadata work.
    for module in &previous.modules {
        if state
            .modules
            .get(&(module.name.clone(), module.version))
            .map(AsRef::as_ref)
            != Some(module)
        {
            return Err(err("control mutation rewrote an existing schema"));
        }
    }
    data.grants = state
        .grants
        .iter()
        .filter(|((p, tid), g)| {
            if crate::visibility::protected(&state.origins[*tid].0) {
                return false;
            }
            let implicit_owner = previous
                .visibility
                .get(tid)
                .is_some_and(|(owner, _)| owner == *p)
                || state
                    .schema_visibility
                    .get(*tid)
                    .is_some_and(|policy| policy.owner == **p);
            !implicit_owner
                || g.declaration != Grant::read_write()
                || previous.grants.iter().any(|(old_p, old_tid, old_g)| {
                    old_p == *p && old_tid == *tid && old_g == &g.declaration
                })
        })
        .map(|((p, tid), g)| (p.clone(), *tid, g.declaration.clone()))
        .collect();
    data.visibility = state
        .schema_visibility
        .iter()
        .map(|(tid, p)| (*tid, (p.owner.clone(), p.visibility)))
        .collect();
    data.visibility_ids = if state
        .schema_visibility
        .iter()
        .any(|(tid, policy)| policy.row_id != visibility_id(*tid))
    {
        state
            .schema_visibility
            .iter()
            .map(|(tid, policy)| (*tid, policy.row_id))
            .collect()
    } else {
        BTreeMap::new()
    };
    // Empty module revisions still own a namespace even without a visibility row.
    for module in &data.modules {
        if !data.owners.contains_key(&module.name) {
            let owner = state
                .schema_visibility
                .iter()
                .find_map(|(tid, policy)| {
                    (state.origins[tid].0 == module.name).then(|| policy.owner.clone())
                })
                .or_else(|| {
                    state
                        .names
                        .get("liquid_control.Namespace")
                        .and_then(|tid| state.families.get(tid))
                        .and_then(|rows| {
                            rows.values().find_map(|row| match row.values.as_ref() {
                                [Value::Text(namespace), Value::Text(owner), ..]
                                    if namespace == &module.name =>
                                {
                                    Some(owner.clone())
                                }
                                _ => None,
                            })
                        })
                })
                .unwrap_or_else(|| HOST.into());
            data.owners.insert(module.name.clone(), owner);
        }
    }
    for (tid, (owner, _)) in &data.visibility {
        let namespace = &state.origins[tid].0;
        if data
            .owners
            .insert(namespace.clone(), owner.clone())
            .is_some_and(|old| old != *owner && old != HOST)
        {
            return Err(err("inconsistent namespace owner"));
        }
    }
    let visibility_ids: BTreeSet<_> = state.schema_visibility.values().map(|p| p.row_id).collect();
    data.control = ControlData {
        modules: state
            .modules
            .values()
            .filter(|m| canonical(&m.name).is_some())
            .map(|m| (**m).clone())
            .collect(),
        rows: state
            .families
            .iter()
            .filter(|(tid, _)| canonical(&state.origins[*tid].0).is_some())
            .flat_map(|(tid, rows)| rows.values().map(move |r| (*tid, (**r).clone())))
            .collect(),
        identities: state
            .identities
            .iter()
            .filter(|id| !visibility_ids.contains(id))
            .copied()
            .collect(),
        schema_grants: state.schema_grants.iter().cloned().collect(),
    };
    Ok(data)
}
pub(super) fn validate_visibility_identity_extension(old: &Catalog, new: &Catalog) -> Result<()> {
    if old.state.schema_visibility.iter().any(|(tid, policy)| {
        new.state
            .schema_visibility
            .get(tid)
            .is_none_or(|next| next.row_id != policy.row_id)
    }) {
        return Err(err("visibility occurrence identity changed"));
    }
    Ok(())
}
pub(super) fn authority_changed(old: &CatalogData, new: &CatalogData) -> bool {
    old.visibility
        .iter()
        .any(|(id, p)| new.visibility.get(id) != Some(p))
        || old.grants.iter().any(|g| !new.grants.contains(g))
        || new.grants.iter().any(|g| {
            old.visibility.contains_key(&g.1)
                && !old.grants.contains(g)
                && old
                    .visibility
                    .get(&g.1)
                    .is_none_or(|(owner, _)| owner != &g.0 || g.2 != Grant::read_write())
        })
        || old.control.rows.iter().any(|(tid, row)| {
            // Proposal/lifecycle/alias bookkeeping is not live data authority.
            (*tid == crate::auth::TYPE_ID
                || *tid == crate::capability::TYPE_ID
                || *tid == crate::schema_control::control_module().relations[0].id)
                && !new.control.rows.contains(&(*tid, row.clone()))
        })
}
pub(super) fn resolve_selection(
    state: &State,
    principal: Option<&str>,
    selection: Selection,
    now: u64,
) -> Result<(Option<Arc<CompiledCapability>>, Option<u64>)> {
    let mut deadline = None;
    if let Some(id) = selection.credential {
        let c = state.credentials.get(&id).ok_or(Error::Denied)?;
        if c.revoked
            || Some(c.principal.as_str()) != principal
            || Some(c.capability) != selection.capability
        {
            return Err(Error::Denied);
        }
        crate::auth::check_deadline(Some(c.expires_at), now)?;
        deadline = Some(c.expires_at);
    }
    let scope = selection
        .capability
        .map(|id| {
            let (cap, _) =
                crate::capability::resolve(state, principal.ok_or(Error::Denied)?, id, now)?;
            deadline = crate::auth::earliest(deadline, cap.record.expires_at);
            Ok::<_, Error>(cap)
        })
        .transpose()?;
    Ok((scope, deadline))
}
pub(crate) struct MetadataSnapshot {
    _database: Database,
    snapshot: Snapshot,
}
impl std::ops::Deref for MetadataSnapshot {
    type Target = Snapshot;
    fn deref(&self) -> &Snapshot {
        &self.snapshot
    }
}
impl PagedSnapshot {
    fn metadata_database(&self) -> Result<Database> {
        self.fence()?;
        let mut state = self.view.catalog.state.clone();
        state.revision = self.view.revision;
        let mut db = Database::from_state(state, None);
        db.clock = self.db.inner.clock.clone();
        Ok(db)
    }
    fn bind_metadata(&self, db: &Database) -> Result<Session> {
        let mut session = match self.principal.as_deref() {
            None => db.admin(),
            Some(ANON) => db.anonymous(),
            Some(p) => db.session(p),
        };
        if let Some(id) = self.selection.capability {
            session = session.with_capability(id)?;
        }
        session.credential = self.selection.credential;
        session.snapshot().check_live()?;
        Ok(session)
    }
    pub(crate) fn metadata_snapshot(&self) -> Result<MetadataSnapshot> {
        let db = self.metadata_database()?;
        let result = self.bind_metadata(&db)?.snapshot();
        self.fence()?;
        Ok(MetadataSnapshot {
            _database: db,
            snapshot: result,
        })
    }
    pub(super) fn permission_available(&self, tid: Id, permission: Permission) -> Result<bool> {
        self.fence()?;
        if self.principal.is_none() {
            return Ok(true);
        }
        if let Some(scope) = &self.scope {
            return Ok(scope.grants.get(&tid).is_some_and(|g| match permission {
                Permission::Read => g.read,
                Permission::Write => g.write,
                Permission::Reference => g.reference,
            }));
        }
        if matches!(permission, Permission::Read) {
            return Ok(discover(
                &self.view.catalog,
                self.principal.as_deref(),
                None,
                tid,
            ));
        }
        Ok(self
            .view
            .catalog
            .state
            .grant(self.principal.as_deref().unwrap_or(ANON), tid)
            .is_some_and(|g| match permission {
                Permission::Read => g.read,
                Permission::Write => g.write,
                Permission::Reference => g.reference,
            }))
    }
}
impl PagedSession {
    pub(super) fn control<T>(&self, mut operation: impl FnMut(&Session) -> Result<T>) -> Result<T> {
        self.control_database(|snapshot, db| operation(&snapshot.bind_metadata(db)?))
    }
    fn control_database<T>(
        &self,
        mut operation: impl FnMut(&PagedSnapshot, &Database) -> Result<T>,
    ) -> Result<T> {
        for attempt in 0..8 {
            let snapshot = self.snapshot()?;
            let db = snapshot.metadata_database()?;
            let result = operation(&snapshot, &db)?;
            snapshot.fence()?;
            let final_state = db.admin().snapshot().state.clone();
            if final_state.revision == snapshot.view.revision {
                return Ok(result);
            }
            let data = extract(&snapshot.view.catalog.data, &final_state)?;
            let mut tx = self.db.admin().transaction()?;
            // Bind the admin-only metadata persistence to the exact initiating
            // authority and catalog revision. No check/commit race on revocation.
            if !Arc::ptr_eq(&tx.snapshot.view, &snapshot.view) {
                if attempt < 7 {
                    continue;
                }
                return Err(Error::Conflict);
            }
            tx.snapshot = snapshot;
            for module in &data.modules {
                for relation in &module.relations {
                    if !tx.catalog.state.relations.contains_key(&relation.id)
                        && tx.allocated(relation.id)?
                    {
                        return Err(Error::Constraint(
                            "schema identity already allocated".into(),
                        ));
                    }
                }
            }
            let new_ids: Vec<_> = data
                .control
                .identities
                .difference(&tx.catalog.data.control.identities)
                .copied()
                .collect();
            for id in new_ids {
                if tx
                    .snapshot
                    .view
                    .ids
                    .get(id.0.as_bytes().as_slice())
                    .map_err(err)?
                    .is_some()
                {
                    return Err(Error::Constraint(
                        "control identity already allocated".into(),
                    ));
                }
                tx.track(tagged(b'a', id.0.as_bytes()))?;
            }
            tx.auth_changed = authority_changed(&tx.catalog.data, &data);
            tx.set_catalog(data)?;
            match tx.commit() {
                Ok(_) => return Ok(result),
                Err(Error::Conflict) if attempt < 7 => continue,
                Err(e) => return Err(e),
            }
        }
        Err(Error::Conflict)
    }
    pub fn with_capability(&self, id: Id) -> Result<Self> {
        if self.selection.capability.is_some_and(|old| old != id) {
            return Err(Error::Denied);
        }
        self.snapshot()?.fence()?;
        let mut selected = self.clone();
        selected.selection.capability = Some(id);
        selected.snapshot()?;
        Ok(selected)
    }
    pub fn delegate_capability(
        &self,
        parent: Id,
        id: Id,
        subject: &str,
        scope: CapabilityScope,
    ) -> Result<DelegatedCapability> {
        self.delegate_capability_until(parent, id, subject, scope, None)
    }
    pub fn delegate_capability_until(
        &self,
        parent: Id,
        id: Id,
        subject: &str,
        scope: CapabilityScope,
        expires_at: Option<u64>,
    ) -> Result<DelegatedCapability> {
        self.control(|s| {
            s.delegate_capability_until(parent, id, subject, scope.clone(), expires_at)
        })
    }
    pub fn revoke_capability(&self, id: Id) -> Result<()> {
        self.control(|s| s.revoke_capability(id))
    }
}
impl PagedDatabase {
    pub(super) fn host_control<T>(
        &self,
        mut operation: impl FnMut(&Database) -> Result<T>,
    ) -> Result<T> {
        self.admin().control_database(|_, db| operation(db))
    }
    pub fn authenticate(&self, token: &str) -> Result<PagedSession> {
        let snapshot = self.admin().snapshot()?;
        let db = snapshot.metadata_database()?;
        let authenticated = db.authenticate(token)?;
        let session = PagedSession {
            db: self.clone(),
            principal: authenticated.principal.clone(),
            selection: Selection {
                capability: authenticated.capability,
                credential: authenticated.credential,
            },
        };
        session.snapshot()?;
        Ok(session)
    }
    pub fn issue_capability(
        &self,
        id: Id,
        owner: &str,
        scope: CapabilityScope,
    ) -> Result<DelegatedCapability> {
        self.issue_capability_until(id, owner, scope, None)
    }
    pub fn issue_capability_until(
        &self,
        id: Id,
        owner: &str,
        scope: CapabilityScope,
        expires_at: Option<u64>,
    ) -> Result<DelegatedCapability> {
        self.host_control(|db| db.issue_capability_until(id, owner, scope.clone(), expires_at))
    }
    pub fn revoke_capability(&self, id: Id) -> Result<()> {
        self.host_control(|db| db.revoke_capability(id))
    }
    pub fn issue_credential(
        &self,
        principal: &str,
        capability: Id,
        expires_at: u64,
    ) -> Result<crate::auth::IssuedCredential> {
        self.host_control(|db| db.issue_credential(principal, capability, expires_at))
    }
    pub fn rotate_credential(
        &self,
        id: Id,
        expires_at: u64,
    ) -> Result<crate::auth::IssuedCredential> {
        self.host_control(|db| db.rotate_credential(id, expires_at))
    }
    pub fn revoke_credential(&self, id: Id) -> Result<()> {
        self.host_control(|db| db.revoke_credential(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::RelationCapability;
    fn setup() -> (tempfile::TempDir, PagedDatabase, Id) {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
        db.inner.clock.set(100);
        let relation = Relation::new("app.Item", vec![Role::new("n", DataType::Int)]);
        let tid = relation.id;
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new("app", 1, vec![], vec![relation]),
            "owner",
            Visibility::Public,
        )
        .unwrap();
        tx.insert("app.Item", vec![1.into()]).unwrap();
        tx.insert("app.Item", vec![2.into()]).unwrap();
        tx.grant("reader", "app.Item", Grant::read_write()).unwrap();
        tx.commit().unwrap();
        (dir, db, tid)
    }
    fn scope(tid: Id, delegations: u8) -> CapabilityScope {
        CapabilityScope {
            relations: vec![RelationCapability {
                relation: tid,
                grant: Grant {
                    filter: RowFilter::Equals {
                        role: "n".into(),
                        value: 1.into(),
                    },
                    ..Grant::read_write()
                },
            }],
            schema_namespace: None,
            delegations,
        }
    }
    #[test]
    fn credential_expiry_fences_native_sql_and_pending_commit() {
        let (_dir, db, tid) = setup();
        let cap = Id::new();
        db.issue_capability_until(cap, "reader", scope(tid, 1), Some(110))
            .unwrap();
        let credential = db.issue_credential("reader", cap, 110).unwrap();
        let session = db.authenticate(&credential.token).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(
            snapshot.scan_page("app.Item", None, 10).unwrap().rows.len(),
            1
        );
        let sql = snapshot.sql().unwrap();
        let mut prepared = sql.prepare("SELECT n FROM app.Item WHERE n = ?1").unwrap();
        assert_eq!(
            prepared
                .query(&[rusqlite::types::Value::Integer(1)])
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(
            prepared
                .query(&[rusqlite::types::Value::Integer(2)])
                .unwrap()
                .rows
                .is_empty()
        );
        let mut tx = session.transaction().unwrap();
        tx.insert("app.Item", vec![1.into()]).unwrap();
        db.inner.clock.set(110);
        assert!(matches!(tx.commit(), Err(Error::Denied)));
        assert!(snapshot.scan_page("app.Item", None, 10).is_err());
        assert!(
            prepared
                .query(&[rusqlite::types::Value::Integer(1)])
                .is_err()
        );
        assert!(db.authenticate(&credential.token).is_err());
        assert_eq!(
            db.admin()
                .snapshot()
                .unwrap()
                .scan_page("app.Item", None, 10)
                .unwrap()
                .rows
                .len(),
            2
        );
    }
    #[test]
    fn delegation_attenuation_rotation_and_reopen_preserve_scope() {
        let (dir, db, tid) = setup();
        let root = Id::new();
        db.issue_capability_until(root, "reader", scope(tid, 1), Some(200))
            .unwrap();
        let parent = db.session("reader").unwrap().with_capability(root).unwrap();
        let child = Id::new();
        assert!(
            parent
                .delegate_capability_until(root, child, "child", scope(tid, 0), Some(201))
                .is_err()
        );
        parent
            .delegate_capability_until(root, child, "child", scope(tid, 0), Some(190))
            .unwrap();
        let old = db.issue_credential("child", child, 180).unwrap();
        let held = db
            .authenticate(&old.token)
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        let fresh = db.rotate_credential(old.id, 185).unwrap();
        assert!(db.authenticate(&old.token).is_err());
        assert!(held.query("SELECT n FROM app.Item", &[]).is_err());
        let s = db.authenticate(&fresh.token).unwrap();
        assert!(s.with_capability(root).is_err());
        assert_eq!(
            s.snapshot()
                .unwrap()
                .sql()
                .unwrap()
                .query("SELECT n FROM app.Item", &[])
                .unwrap()
                .rows
                .len(),
            1
        );
        drop(held);
        drop(s);
        drop(parent);
        drop(db);
        let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
        db.inner.clock.set(120);
        assert!(db.authenticate(&old.token).is_err());
        assert_eq!(
            db.authenticate(&fresh.token)
                .unwrap()
                .snapshot()
                .unwrap()
                .scan_page("app.Item", None, 10)
                .unwrap()
                .rows
                .len(),
            1
        );
        db.revoke_capability(root).unwrap();
        assert!(db.authenticate(&fresh.token).is_err());
    }
    #[test]
    fn read_only_control_does_not_commit_and_malformed_control_rolls_back() {
        let (_dir, db, _) = setup();
        let policy = crate::schema_control::NamespacePolicy {
            namespace: "agent".into(),
            owner: "owner".into(),
            auto_approve: true,
            enabled: true,
            quotas: Default::default(),
        };
        db.configure_namespace(policy).unwrap();
        let before = db.stats().unwrap().revision;
        db.session("owner").unwrap().schema_namespaces().unwrap();
        assert_eq!(db.stats().unwrap().revision, before);
        let current = db.inner.head.read().unwrap().catalog.clone();
        let mut data = current.data.clone();
        data.control.identities.insert(Id::new());
        data.control.rows.push((
            Id::new(),
            Row {
                id: Id::new(),
                values: vec![].into_boxed_slice(),
            },
        ));
        let mut tx = db.admin().transaction().unwrap();
        assert!(tx.mutate(|tx| tx.set_catalog(data)).is_err());
        assert!(matches!(tx.commit(), Err(Error::Aborted)));
        assert_eq!(db.stats().unwrap().revision, before);
    }
    #[test]
    fn control_budget_failure_does_not_partially_issue_authority() {
        let (dir, db, tid) = setup();
        let bytes = serde_json::to_vec(&db.inner.head.read().unwrap().catalog.data)
            .unwrap()
            .len();
        let revision = db.stats().unwrap().revision;
        drop(db);
        let db = PagedDatabase::open(
            dir.path().join("db"),
            PagedOptions {
                max_catalog_bytes: bytes + 100,
                ..Default::default()
            },
        )
        .unwrap();
        db.inner.clock.set(100);
        let cap = Id::new();
        assert!(matches!(
            db.issue_capability(cap, "reader", scope(tid, 0)),
            Err(Error::Constraint(_))
        ));
        assert_eq!(db.stats().unwrap().revision, revision);
        assert!(db.session("reader").unwrap().with_capability(cap).is_err());
        assert_eq!(
            db.admin()
                .snapshot()
                .unwrap()
                .scan_page("app.Item", None, 10)
                .unwrap()
                .rows
                .len(),
            2
        );
    }
    #[test]
    fn empty_module_ownership_survives_control_changes() {
        let (_dir, db, tid) = setup();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new("empty", 1, vec![], vec![]),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        tx.commit().unwrap();
        db.issue_capability(Id::new(), "reader", scope(tid, 0))
            .unwrap();
        let mut tx = db.session("owner").unwrap().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "empty",
                2,
                vec![],
                vec![Relation::new(
                    "empty.New",
                    vec![Role::new("n", DataType::Int)],
                )],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        tx.commit().unwrap();
    }
    #[test]
    #[ignore]
    fn control_crash_child() {
        let Ok(path) = std::env::var("PAGED_AUTH_TEST_PATH") else {
            return;
        };
        let db = PagedDatabase::open(path, PagedOptions::default()).unwrap();
        db.inner.clock.set(150);
        db.revoke_capability(Id::from_u128(9900)).unwrap();
    }
    #[test]
    fn control_revocation_crash_is_atomic_with_durable_authority() {
        for point in ["paged_before_commit", "paged_after_persist"] {
            let (dir, db, tid) = setup();
            let cap = Id::from_u128(9900);
            db.issue_capability(cap, "reader", scope(tid, 0)).unwrap();
            let credential = db.issue_credential("reader", cap, 200).unwrap();
            drop(db);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "paged::authority::tests::control_crash_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("PAGED_AUTH_TEST_PATH", dir.path().join("db"))
                .env("SUBSTRATE_TEST_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(91));
            let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
            db.inner.clock.set(150);
            assert_eq!(
                db.authenticate(&credential.token).is_ok(),
                point == "paged_before_commit"
            );
            db.admin().snapshot().unwrap().verify().unwrap();
        }
    }

    #[test]
    fn control_does_not_persist_implicit_owner_grants_or_transfer_ownership() {
        let (_dir, db, tid) = setup();
        db.issue_capability(Id::new(), "owner", scope(tid, 0))
            .unwrap();
        assert!(
            !db.inner
                .head
                .read()
                .unwrap()
                .catalog
                .data
                .grants
                .iter()
                .any(|(p, _, _)| p == "owner")
        );
        let policy = crate::schema_control::NamespacePolicy {
            namespace: "app".into(),
            owner: "other".into(),
            auto_approve: true,
            enabled: true,
            quotas: Default::default(),
        };
        assert!(db.configure_namespace(policy).is_err());
        assert_eq!(
            db.inner.head.read().unwrap().catalog.data.owners["app"],
            "owner"
        );
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "hosted",
            1,
            vec![],
            vec![Relation::new(
                "hosted.Row",
                vec![Role::new("n", DataType::Int)],
            )],
        ))
        .unwrap();
        tx.commit().unwrap();
        db.configure_namespace(crate::schema_control::NamespacePolicy {
            namespace: "hosted".into(),
            owner: "new".into(),
            auto_approve: true,
            enabled: true,
            quotas: Default::default(),
        })
        .unwrap();
        assert_eq!(
            db.inner.head.read().unwrap().catalog.data.owners["hosted"],
            "new"
        );
    }

    #[test]
    fn typed_write_denial_hides_shape_and_global_allocations() {
        let (_dir, db, _) = setup();
        let allocated = db
            .admin()
            .snapshot()
            .unwrap()
            .scan_page("app.Item", None, 1)
            .unwrap()
            .rows[0]
            .id;
        for id in [allocated, Id::new()] {
            for values in [vec![], vec![Value::Text("bad".into())], vec![1.into()]] {
                let mut tx = db.session("outsider").unwrap().transaction().unwrap();
                assert!(matches!(
                    tx.insert_with_id("app.Item", id, values),
                    Err(Error::Denied)
                ));
                assert!(matches!(tx.commit(), Err(Error::Aborted)));
            }
            let mut tx = db.session("outsider").unwrap().transaction().unwrap();
            assert!(matches!(
                tx.extend("app.Item", "app.Item", id, vec![]),
                Err(Error::Denied)
            ));
        }
    }
}
