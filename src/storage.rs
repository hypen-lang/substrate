//! Physical state and versioned redo records. No public raw-key write interface.
use crate::{
    compiled::{CompiledGrant, CompiledRelation},
    *,
};
use im::{OrdMap, OrdSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

pub(crate) type Bucket = (Id, usize, Value);
pub(crate) type UniqueKey = (Id, usize, Vec<Value>);
pub(crate) type RowKey = (Id, Id);
enum ReadPolicy {
    All,
    Filter(CompiledGrant),
    Denied,
}
impl ReadPolicy {
    fn allows(&self, row: &Row) -> bool {
        match self {
            Self::All => true,
            Self::Filter(grant) => grant.allows(row),
            Self::Denied => false,
        }
    }
}
/// Completed reference-closure results for one immutable state and principal.
/// Snapshot clones share it; policy/state rebinding resets it before use.
#[derive(Default)]
pub(crate) struct VisibilityMemo {
    state: Weak<State>,
    principal: Option<String>,
    completed: BTreeMap<RowKey, bool>,
    policies: BTreeMap<Id, ReadPolicy>,
    #[cfg(test)]
    pub expanded: usize,
}
impl VisibilityMemo {
    fn bind(&mut self, state: &Arc<State>, principal: &str) {
        if self.state.as_ptr() != Arc::as_ptr(state) || self.principal.as_deref() != Some(principal)
        {
            self.state = Arc::downgrade(state);
            self.principal = Some(principal.into());
            self.completed.clear();
            self.policies.clear();
            #[cfg(test)]
            {
                self.expanded = 0;
            }
        }
    }
    fn policy(&mut self, state: &State, principal: &str, tid: Id) -> &ReadPolicy {
        self.policies.entry(tid).or_insert_with(|| {
            let grant = (principal != crate::visibility::ANONYMOUS)
                .then(|| state.grant(principal, tid))
                .flatten()
                .filter(|grant| grant.read);
            if grant.is_some_and(|grant| matches!(grant.filter, RowFilter::All))
                || state.ambient_read(principal, tid)
            {
                ReadPolicy::All
            } else if let Some(grant) = grant {
                ReadPolicy::Filter(grant.clone())
            } else {
                ReadPolicy::Denied
            }
        })
    }
    /// Borrowed scan rows need no second lookup. Scalar families have no
    /// reference closure, so their compiled family policy is the whole decision.
    pub fn all_rows_visible(
        &mut self,
        state: &Arc<State>,
        principal: &str,
        relation: &CompiledRelation,
    ) -> bool {
        self.bind(state, principal);
        relation.reference_slots.is_empty()
            && matches!(self.policy(state, principal, relation.id), ReadPolicy::All)
    }
    pub fn visible_row(
        &mut self,
        state: &Arc<State>,
        principal: &str,
        relation: &CompiledRelation,
        row: &Row,
    ) -> bool {
        if relation.reference_slots.is_empty() {
            self.bind(state, principal);
            return self.policy(state, principal, relation.id).allows(row);
        }
        self.visible(state, principal, (relation.id, row.id))
    }
    pub fn visible(&mut self, state: &Arc<State>, principal: &str, root: RowKey) -> bool {
        self.bind(state, principal);
        if state.relations[&root.0].reference_slots.is_empty() {
            return state
                .row(root.0, root.1)
                .is_some_and(|row| self.policy(state, principal, root.0).allows(row));
        }
        if let Some(result) = self.completed.get(&root) {
            return *result;
        }
        // Walk each new reachable node once. A node is visible iff its own grant
        // permits it AND every reachable participant is visible. Cycles with no
        // denied exit are valid (greatest fixed point), so ordinary DFS "visiting"
        // markers must not be cached as successful results.
        let mut pending = vec![root];
        let mut visited = BTreeSet::new();
        let mut parents = BTreeMap::<RowKey, Vec<RowKey>>::new();
        let mut denied = BTreeSet::new();
        while let Some(key @ (tid, id)) = pending.pop() {
            if let Some(visible) = self.completed.get(&key) {
                if !visible {
                    denied.insert(key);
                }
                continue;
            }
            if !visited.insert(key) {
                continue;
            }
            #[cfg(test)]
            {
                self.expanded += 1;
            }
            let Some(row) = state.row(tid, id) else {
                denied.insert(key);
                continue;
            };
            if !self.policy(state, principal, tid).allows(row) {
                denied.insert(key);
                continue;
            }
            for (slot, target) in &state.relations[&tid].reference_slots {
                if let Value::Ref(target_id) = &row.values[*slot] {
                    let child = (state.names[target], *target_id);
                    parents.entry(child).or_default().push(key);
                    pending.push(child);
                }
            }
        }
        // Propagate a denied exit backwards, including through whole cycles.
        // Unrelated valid branches stay true, even if the requested root is false.
        let mut pending: Vec<_> = denied.iter().copied().collect();
        while let Some(child) = pending.pop() {
            if let Some(ancestors) = parents.get(&child) {
                for parent in ancestors {
                    if denied.insert(*parent) {
                        pending.push(*parent);
                    }
                }
            }
        }
        for key in visited {
            self.completed.insert(key, !denied.contains(&key));
        }
        self.completed[&root]
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Key {
    Catalog,
    Visibility,
    Allocated(Id),
    Policy(String),
    Revocation(String),
    Row(Id, Id),
    Family(Id),
    Bucket(Bucket),
    Unique(UniqueKey),
    Incoming(RowKey),
}
#[derive(Clone, Default)]
pub(crate) struct State {
    pub revision: u64,
    pub identities: OrdSet<Id>,
    pub names: OrdMap<String, Id>,
    pub relations: OrdMap<Id, Arc<CompiledRelation>>,
    pub modules: OrdMap<(String, u64), Arc<Module>>,
    pub origins: OrdMap<Id, (String, u64)>,
    pub families: OrdMap<Id, OrdMap<Id, Arc<Row>>>,
    pub index: OrdMap<Bucket, OrdSet<Id>>,
    pub unique: OrdMap<UniqueKey, Id>,
    pub incoming: OrdMap<RowKey, OrdSet<(Id, Id, usize)>>,
    pub grants: OrdMap<(String, Id), CompiledGrant>,
    pub schema_visibility: OrdMap<Id, Arc<crate::visibility::Policy>>,
    /// Capability overlays must never gain ambient authority. Not persisted.
    pub visibility_disabled: bool,
    pub credentials: OrdMap<Id, Arc<crate::auth::Credential>>,
    pub views: OrdMap<Id, Arc<crate::views::ViewDefinition>>,
    pub capabilities: OrdMap<Id, Arc<crate::capability::CompiledCapability>>,
    pub capability_roots: OrdMap<(String, Id), OrdSet<Id>>,
    pub schema_grants: OrdSet<(String, String)>,
    pub versions: OrdMap<Key, u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Op {
    Install(Module),
    Identity(Id),
    Put(Id, Row),
    Delete(Id, Id),
    Grant(String, Id, Option<Grant>),
    SchemaGrant(String, String),
}
impl State {
    pub fn version(&self, key: &Key) -> u64 {
        self.versions.get(key).copied().unwrap_or(0)
    }
    pub fn relation(&self, name: &str) -> Result<Arc<CompiledRelation>> {
        self.names
            .get(name)
            .and_then(|id| self.relations.get(id))
            .cloned()
            .ok_or_else(|| Error::Schema(format!("unknown relation {name}")))
    }
    pub fn row(&self, tid: Id, id: Id) -> Option<&Arc<Row>> {
        self.families.get(&tid)?.get(&id)
    }
    pub fn grant(&self, principal: &str, tid: Id) -> Option<&CompiledGrant> {
        self.grants.get(&(principal.into(), tid))
    }
    pub fn ambient_read(&self, principal: &str, tid: Id) -> bool {
        if self.visibility_disabled {
            return false;
        }
        self.schema_visibility
            .get(&tid)
            .is_some_and(|policy| match policy.visibility {
                Visibility::Public => true,
                Visibility::Authenticated => {
                    principal != crate::visibility::ANONYMOUS && !principal.is_empty()
                }
                Visibility::Private => {
                    principal != crate::visibility::ANONYMOUS
                        && policy.owner != crate::visibility::HOST
                        && policy.owner == principal
                }
            })
    }
    pub fn can_read(&self, principal: Option<&str>, tid: Id) -> bool {
        principal.is_none_or(|p| {
            (p != crate::visibility::ANONYMOUS && self.grant(p, tid).is_some_and(|g| g.read))
                || self.ambient_read(p, tid)
        })
    }
    pub fn can_discover(&self, principal: Option<&str>, tid: Id) -> bool {
        self.can_read(principal, tid)
            || principal.is_some_and(|p| {
                p != crate::visibility::ANONYMOUS
                    && self.grant(p, tid).is_some_and(|g| g.write || g.reference)
            })
    }
    pub fn row_read_allowed(&self, principal: &str, tid: Id, row: &Row) -> bool {
        // Explicit grants are the common path for scoped and private readers.
        // A missing/filtered grant still falls through to ambient visibility:
        // narrow grants never reduce a public or authenticated baseline.
        (principal != crate::visibility::ANONYMOUS
            && self
                .grant(principal, tid)
                .is_some_and(|g| g.read && g.allows(row)))
            || self.ambient_read(principal, tid)
    }
    /// Greatest reference-closed subset of rows satisfying the principal's policy.
    /// Every reachable participant is checked; cycles terminate via the visited set.
    pub fn visible(
        &self,
        principal: Option<&str>,
        tid: Id,
        id: Id,
        seen: &mut BTreeSet<RowKey>,
        reads: &mut BTreeSet<Key>,
    ) -> bool {
        if principal.is_none() {
            reads.insert(Key::Row(tid, id));
            return self.row(tid, id).is_some();
        }
        let mut pending = vec![(tid, id)];
        while let Some((tid, id)) = pending.pop() {
            if !seen.insert((tid, id)) {
                continue;
            }
            reads.insert(Key::Row(tid, id));
            let Some(row) = self.row(tid, id) else {
                return false;
            };
            let Some(p) = principal else {
                return true;
            };
            let def = &self.relations[&tid];
            if !self.row_read_allowed(p, tid, row) {
                return false;
            }
            for (role, value) in def.roles.iter().zip(row.values.iter()) {
                if let (DataType::Ref(target), Value::Ref(target_id)) = (&role.data_type, value) {
                    pending.push((self.names[target], *target_id));
                }
            }
        }
        true
    }

    fn touch(&mut self, key: Key, rev: u64) {
        self.versions.insert(key, rev);
    }
    fn remove_compiled_row(&mut self, tid: Id, id: Id) {
        if tid == crate::visibility::TYPE_ID
            && let Some(row) = self.row(tid, id)
            && let Ok(policy) = crate::visibility::compile(self, row)
        {
            self.schema_visibility.remove(&policy.relation);
        }
        if tid == crate::views::TYPE_ID {
            self.views.remove(&id);
        }
        if tid == crate::auth::TYPE_ID {
            self.credentials.remove(&id);
        }
        if tid == crate::capability::TYPE_ID
            && let Some(cap) = self.capabilities.remove(&id)
            && cap.record.parent.is_none()
        {
            for r in &cap.record.scope.relations {
                let key = (cap.record.authority.clone(), r.relation);
                if let Some(ids) = self.capability_roots.get_mut(&key) {
                    ids.remove(&id);
                }
            }
        }
    }
    fn remove_row(&mut self, tid: Id, id: Id, rev: u64) {
        self.remove_compiled_row(tid, id);
        let Some(row) = self.families.get_mut(&tid).and_then(|f| f.remove(&id)) else {
            return;
        };
        let def = self.relations[&tid].clone();
        for (i, value) in row.values.iter().enumerate() {
            let key = (tid, i, value.clone());
            if let Some(set) = self.index.get_mut(&key) {
                set.remove(&id);
                if set.is_empty() {
                    self.index.remove(&key);
                }
            }
            self.touch(Key::Bucket(key), rev);
            if let (DataType::Ref(target), Value::Ref(target_id)) = (&def.roles[i].data_type, value)
            {
                let target_key = (self.names[target], *target_id);
                if let Some(set) = self.incoming.get_mut(&target_key) {
                    set.remove(&(tid, id, i));
                    if set.is_empty() {
                        self.incoming.remove(&target_key);
                    }
                }
                self.touch(Key::Incoming(target_key), rev);
            }
        }
        for i in 0..def.unique.len() {
            let key = (tid, i, def.unique_values(&row, i));
            self.unique.remove(&key);
            self.touch(Key::Unique(key), rev);
        }
        self.touch(Key::Row(tid, id), rev);
        self.touch(Key::Family(tid), rev);
    }
    /// Only validated transactions or trusted, version-checked recovery call apply.
    pub fn apply(&mut self, op: &Op, rev: u64) {
        match op {
            Op::Install(module) => {
                for relation in &module.relations {
                    self.touch(Key::Allocated(relation.id), rev);
                    self.names.insert(relation.name.clone(), relation.id);
                    self.relations.insert(
                        relation.id,
                        Arc::new(CompiledRelation::new(relation.clone())),
                    );
                    self.origins
                        .insert(relation.id, (module.name.clone(), module.version));
                    self.families.insert(relation.id, OrdMap::new());
                }
                self.modules.insert(
                    (module.name.clone(), module.version),
                    Arc::new(module.clone()),
                );
                self.touch(Key::Catalog, rev);
            }
            Op::Identity(id) => {
                self.identities.insert(*id);
                self.touch(Key::Allocated(*id), rev);
            }
            Op::Put(tid, row) => {
                let previous = self.row(*tid, row.id).cloned();
                self.remove_compiled_row(*tid, row.id);
                let def = self.relations[tid].clone();
                self.families
                    .get_mut(tid)
                    .expect("installed relation")
                    .insert(row.id, Arc::new(row.clone()));
                for (i, value) in row.values.iter().enumerate() {
                    let old_value = previous.as_ref().map(|old| &old.values[i]);
                    let changed = old_value != Some(value);
                    if changed && let Some(old_value) = old_value {
                        let old_key = (*tid, i, old_value.clone());
                        if let Some(set) = self.index.get_mut(&old_key) {
                            set.remove(&row.id);
                            if set.is_empty() {
                                self.index.remove(&old_key);
                            }
                        }
                        self.touch(Key::Bucket(old_key), rev);
                        if let (DataType::Ref(target), Value::Ref(target_id)) =
                            (&def.roles[i].data_type, old_value)
                        {
                            let key = (self.names[target], *target_id);
                            if let Some(set) = self.incoming.get_mut(&key) {
                                set.remove(&(*tid, row.id, i));
                                if set.is_empty() {
                                    self.incoming.remove(&key);
                                }
                            }
                            self.touch(Key::Incoming(key), rev);
                        }
                    }
                    let key = (*tid, i, value.clone());
                    if changed {
                        let mut set = self.index.get(&key).cloned().unwrap_or_default();
                        set.insert(row.id);
                        self.index.insert(key.clone(), set);
                    }
                    // Keep the original OCC invalidation, even for unchanged
                    // buckets. Only physical index mutations are skipped.
                    self.touch(Key::Bucket(key), rev);
                    if let (DataType::Ref(target), Value::Ref(target_id)) =
                        (&def.roles[i].data_type, value)
                    {
                        let key = (self.names[target], *target_id);
                        if changed {
                            let mut set = self.incoming.get(&key).cloned().unwrap_or_default();
                            set.insert((*tid, row.id, i));
                            self.incoming.insert(key, set);
                        }
                        self.touch(Key::Incoming(key), rev);
                    }
                }
                for i in 0..def.unique.len() {
                    let key = (*tid, i, def.unique_values(row, i));
                    let old_key = previous
                        .as_ref()
                        .map(|old| (*tid, i, def.unique_values(old, i)));
                    if old_key.as_ref() != Some(&key) {
                        if let Some(old_key) = old_key {
                            self.unique.remove(&old_key);
                            self.touch(Key::Unique(old_key), rev);
                        }
                        self.unique.insert(key.clone(), row.id);
                    }
                    self.touch(Key::Unique(key), rev);
                }
                if *tid == crate::visibility::TYPE_ID
                    && let Ok(policy) = crate::visibility::compile(self, row)
                {
                    // Initial metadata for an additive schema cannot affect old readers.
                    // Changing existing schema visibility invalidates retained ambient handles.
                    if self.version(&Key::Allocated(policy.relation)) != rev {
                        self.touch(Key::Visibility, rev);
                    }
                    self.schema_visibility
                        .insert(policy.relation, Arc::new(policy));
                }
                if *tid == crate::auth::TYPE_ID
                    && let Ok(c) = crate::auth::compile(self, row)
                {
                    self.credentials.insert(row.id, Arc::new(c));
                }
                if *tid == crate::views::TYPE_ID
                    && let Ok(view) = crate::views::compile(self, row)
                {
                    self.views.insert(row.id, Arc::new(view));
                }
                if *tid == crate::capability::TYPE_ID
                    && let Ok(cap) = crate::capability::compile(self, row)
                {
                    if cap.record.parent.is_none() {
                        for r in &cap.record.scope.relations {
                            let key = (cap.record.authority.clone(), r.relation);
                            let mut ids =
                                self.capability_roots.get(&key).cloned().unwrap_or_default();
                            ids.insert(row.id);
                            self.capability_roots.insert(key, ids);
                        }
                    }
                    self.capabilities.insert(row.id, Arc::new(cap));
                }
                self.touch(Key::Row(*tid, row.id), rev);
                self.touch(Key::Family(*tid), rev);
            }
            Op::Delete(tid, id) => {
                if *tid == crate::visibility::TYPE_ID {
                    self.touch(Key::Visibility, rev);
                }
                self.remove_row(*tid, *id, rev)
            }
            Op::Grant(p, tid, grant) => {
                let key = (p.clone(), *tid);
                if grant.is_none() || self.grants.contains_key(&key) {
                    self.touch(Key::Revocation(p.clone()), rev);
                }
                if let Some(g) = grant {
                    self.grants
                        .insert(key, CompiledGrant::new(g.clone(), &self.relations[tid]));
                } else {
                    self.grants.remove(&key);
                }
                self.touch(Key::Policy(p.clone()), rev);
            }
            Op::SchemaGrant(p, module) => {
                self.schema_grants.insert((p.clone(), module.clone()));
                self.touch(Key::Policy(p.clone()), rev);
            }
        }
    }
}

impl State {
    /// A redo stream preserves immediate constraint order. Checkpoint rows may
    /// appear in key order instead, and are validated as a complete state.
    pub(crate) fn validate_replay_order(&self, op: &Op) -> Result<()> {
        let invalid = || Error::Storage("invalid redo constraint order".into());
        match op {
            Op::Install(module) => {
                let mut seen = BTreeSet::new();
                for (name, version) in &module.imports {
                    if name == &module.name
                        || !seen.insert(name)
                        || !self.modules.contains_key(&(name.clone(), *version))
                    {
                        return Err(invalid());
                    }
                }
                for def in &module.relations {
                    for role in &def.roles {
                        if let DataType::Ref(target) = &role.data_type {
                            if module.relations.iter().any(|r| &r.name == target) {
                                continue;
                            }
                            let tid = self.names.get(target).ok_or_else(invalid)?;
                            let (owner, version) = &self.origins[tid];
                            if owner != &module.name
                                && !module
                                    .imports
                                    .iter()
                                    .any(|(n, v)| n == owner && v >= version)
                            {
                                return Err(invalid());
                            }
                        }
                    }
                }
            }
            Op::Put(tid, row) => {
                let def = self.relations.get(tid).ok_or_else(invalid)?;
                for (role, value) in def.roles.iter().zip(row.values.iter()) {
                    if let (DataType::Ref(target), Value::Ref(id)) = (&role.data_type, value) {
                        let tid = self.names.get(target).ok_or_else(invalid)?;
                        if self.row(*tid, *id).is_none() {
                            return Err(invalid());
                        }
                    }
                }
            }
            Op::Delete(tid, id)
                if self
                    .incoming
                    .get(&(*tid, *id))
                    .is_some_and(|s| !s.is_empty()) =>
            {
                return Err(invalid());
            }
            _ => {}
        }
        Ok(())
    }
    /// Recovery validates structure before any unchecked indexing in apply. These
    /// checks also make logical corruption fail closed instead of panicking.
    pub(crate) fn apply_recovered(&mut self, op: &Op, rev: u64) -> Result<()> {
        let invalid = || Error::Storage("invalid recovered operation".into());
        match op {
            Op::Install(module) => {
                if !crate::model::identifier(&module.name)
                    || ["main", "temp"]
                        .iter()
                        .any(|n| module.name.eq_ignore_ascii_case(n))
                    || module.name.to_ascii_lowercase().starts_with("sqlite_")
                {
                    return Err(invalid());
                }
                let latest = self
                    .modules
                    .keys()
                    .filter(|(name, _)| name == &module.name)
                    .map(|(_, v)| *v)
                    .max()
                    .unwrap_or(0);
                if Some(module.version) != latest.checked_add(1)
                    || self
                        .modules
                        .keys()
                        .any(|(n, _)| n != &module.name && n.eq_ignore_ascii_case(&module.name))
                {
                    return Err(invalid());
                }
                let mut names = self
                    .names
                    .keys()
                    .map(|n| n.to_ascii_lowercase())
                    .collect::<BTreeSet<_>>();
                let mut ids = BTreeSet::new();
                for def in &module.relations {
                    def.validate(&module.name).map_err(|_| invalid())?;
                    if !names.insert(def.name.to_ascii_lowercase())
                        || !ids.insert(def.id)
                        || self.relations.contains_key(&def.id)
                        || self.identities.contains(&def.id)
                    {
                        return Err(invalid());
                    }
                }
            }
            Op::Identity(id) => {
                if self.identities.contains(id) || self.relations.contains_key(id) {
                    return Err(invalid());
                }
            }
            Op::Put(tid, row) => {
                let def = self.relations.get(tid).ok_or_else(invalid)?;
                if !self.identities.contains(&row.id) || row.values.len() != def.roles.len() {
                    return Err(invalid());
                }
                for (role, value) in def.roles.iter().zip(row.values.iter()) {
                    if !role.data_type.accepts(value) {
                        return Err(invalid());
                    }
                    if let DataType::Ref(target) = &role.data_type
                        && !self.names.contains_key(target)
                    {
                        return Err(invalid());
                    }
                }
                def.check_values(&row.values).map_err(|_| invalid())?;
                for i in 0..def.unique.len() {
                    if self
                        .unique
                        .get(&(*tid, i, def.unique_values(row, i)))
                        .is_some_and(|id| *id != row.id)
                    {
                        return Err(invalid());
                    }
                }
            }
            Op::Delete(tid, id) => {
                if self.row(*tid, *id).is_none() {
                    return Err(invalid());
                }
            }
            Op::Grant(_, tid, grant) => {
                let def = self.relations.get(tid).ok_or_else(invalid)?;
                if let Some(Grant {
                    filter: RowFilter::Equals { role, value },
                    ..
                }) = grant
                {
                    let i = def.role(role).map_err(|_| invalid())?;
                    if !def.roles[i].data_type.accepts(value) {
                        return Err(invalid());
                    }
                }
            }
            Op::SchemaGrant(_, name) => {
                if !crate::model::identifier(name) {
                    return Err(invalid());
                }
            }
        }
        if let Op::Put(tid, row) = op
            && *tid == crate::capability::TYPE_ID
        {
            crate::capability::compile(self, row).map_err(|_| invalid())?;
        }
        if let Op::Put(tid, row) = op
            && *tid == crate::auth::TYPE_ID
        {
            crate::auth::compile(self, row).map_err(|_| invalid())?;
        }
        if let Op::Put(tid, row) = op
            && *tid == crate::views::TYPE_ID
        {
            crate::views::compile(self, row).map_err(|_| invalid())?;
        }
        if let Op::Put(tid, row) = op
            && *tid == crate::visibility::TYPE_ID
        {
            crate::visibility::compile(self, row).map_err(|_| invalid())?;
        }
        self.apply(op, rev);
        Ok(())
    }
    /// Validate constraints whose targets can appear later in a checkpoint's row
    /// order. All indexes have already been built through apply_recovered.
    pub(crate) fn validate_recovered(&self) -> Result<()> {
        let invalid = || Error::Storage("invalid recovered catalog or reference".into());
        for module in self.modules.values() {
            let mut seen = BTreeSet::new();
            for (name, version) in &module.imports {
                if name == &module.name
                    || !seen.insert(name)
                    || !self.modules.contains_key(&(name.clone(), *version))
                {
                    return Err(invalid());
                }
            }
            for def in &module.relations {
                for role in &def.roles {
                    if let DataType::Ref(target) = &role.data_type {
                        let tid = self.names.get(target).ok_or_else(invalid)?;
                        let (owner, version) = &self.origins[tid];
                        let valid = if owner == &module.name {
                            *version <= module.version
                        } else {
                            module
                                .imports
                                .iter()
                                .any(|(n, v)| n == owner && v >= version)
                        };
                        if !valid {
                            return Err(invalid());
                        }
                    }
                }
            }
        }
        for (tid, family) in &self.families {
            let def = &self.relations[tid];
            for row in family.values() {
                for (role, value) in def.roles.iter().zip(row.values.iter()) {
                    if let (DataType::Ref(target), Value::Ref(id)) = (&role.data_type, value)
                        && self.row(self.names[target], *id).is_none()
                    {
                        return Err(invalid());
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod ambient_read_tests {
    use super::*;

    #[test]
    fn grant_fast_path_preserves_ambient_union_and_scope_limits() {
        for mode in [
            Visibility::Public,
            Visibility::Authenticated,
            Visibility::Private,
        ] {
            for read in [false, true] {
                let db = Database::memory();
                let mut tx = db.admin().transaction();
                tx.install_with_visibility(
                    Module::new(
                        "access",
                        1,
                        vec![],
                        vec![Relation::new(
                            "access.Row",
                            vec![Role::new("value", DataType::Int)],
                        )],
                    ),
                    mode,
                )
                .unwrap();
                let matched = tx.insert("access.Row", vec![1.into()]).unwrap();
                let outside = tx.insert("access.Row", vec![2.into()]).unwrap();
                for principal in ["reader", crate::visibility::ANONYMOUS] {
                    tx.grant(
                        principal,
                        "access.Row",
                        Grant {
                            read,
                            write: false,
                            reference: false,
                            filter: RowFilter::Equals {
                                role: "value".into(),
                                value: 1.into(),
                            },
                        },
                    )
                    .unwrap();
                }
                tx.commit().unwrap();
                let snapshot = db.admin().snapshot();
                let state = &snapshot.state;
                let tid = state.names["access.Row"];
                let ambient = mode != Visibility::Private;
                assert_eq!(state.can_read(Some("reader"), tid), read || ambient);
                assert_eq!(
                    state.row_read_allowed("reader", tid, state.row(tid, matched).unwrap()),
                    read || ambient
                );
                assert_eq!(
                    state.row_read_allowed("reader", tid, state.row(tid, outside).unwrap()),
                    ambient
                );
                // Anonymous callers cannot activate an explicit grant, even one
                // mistakenly installed for the reserved identity by the host.
                for id in [matched, outside] {
                    assert_eq!(
                        state.row_read_allowed(
                            crate::visibility::ANONYMOUS,
                            tid,
                            state.row(tid, id).unwrap()
                        ),
                        mode == Visibility::Public
                    );
                }
                let mut scoped = (**state).clone();
                scoped.visibility_disabled = true;
                assert_eq!(
                    scoped.row_read_allowed("reader", tid, state.row(tid, matched).unwrap()),
                    read
                );
                assert!(!scoped.row_read_allowed("reader", tid, state.row(tid, outside).unwrap()));
                assert!(!scoped.row_read_allowed(
                    crate::visibility::ANONYMOUS,
                    tid,
                    state.row(tid, matched).unwrap()
                ));
            }
        }
    }
}

#[cfg(test)]
mod update_index_tests {
    use super::*;
    use proptest::prelude::*;

    fn fixture() -> (State, Id, Id, Id, Id) {
        let target = Relation::new("mutation.Target", vec![Role::new("n", DataType::Int)]);
        let tid = target.id;
        let item = Relation::new(
            "mutation.Item",
            vec![
                Role::new("key", DataType::Int),
                Role::new("value", DataType::Int),
                Role::new("target", DataType::Ref("mutation.Target".into())),
            ],
        )
        .unique(&["key"]);
        let item_tid = item.id;
        let mut state = State::default();
        state.apply(
            &Op::Install(Module::new("mutation", 1, vec![], vec![target, item])),
            1,
        );
        let (a, b, id) = (Id::new(), Id::new(), Id::new());
        for target in [a, b] {
            state.apply(&Op::Identity(target), 1);
            state.apply(
                &Op::Put(
                    tid,
                    Row {
                        id: target,
                        values: vec![1.into()].into_boxed_slice(),
                    },
                ),
                1,
            );
        }
        state.apply(&Op::Identity(id), 1);
        state.apply(
            &Op::Put(
                item_tid,
                Row {
                    id,
                    values: vec![1.into(), 2.into(), a.into()].into_boxed_slice(),
                },
            ),
            1,
        );
        (state, item_tid, id, a, b)
    }
    #[test]
    fn unchanged_indexes_share_storage_but_keep_occ_invalidation() {
        let (mut state, tid, id, a, _) = fixture();
        let before = state.clone();
        let target_tid = state.names["mutation.Target"];
        state.apply(
            &Op::Put(
                tid,
                Row {
                    id,
                    values: vec![1.into(), 3.into(), a.into()].into_boxed_slice(),
                },
            ),
            2,
        );
        assert!(before.index[&(tid, 0, 1.into())].ptr_eq(&state.index[&(tid, 0, 1.into())]));
        assert!(before.index[&(tid, 2, a.into())].ptr_eq(&state.index[&(tid, 2, a.into())]));
        assert!(before.incoming.ptr_eq(&state.incoming));
        assert!(before.unique.ptr_eq(&state.unique));
        for key in [
            Key::Bucket((tid, 0, 1.into())),
            Key::Bucket((tid, 2, a.into())),
            Key::Incoming((target_tid, a)),
            Key::Unique((tid, 0, vec![1.into()])),
            Key::Row(tid, id),
            Key::Family(tid),
        ] {
            assert_eq!(state.version(&key), 2);
        }
        assert_eq!(before.row(tid, id).unwrap().values[1], Value::Int(2));
        let unchanged = state.clone();
        state.apply(&Op::Put(tid, (**state.row(tid, id).unwrap()).clone()), 3);
        assert!(unchanged.index.ptr_eq(&state.index));
        assert!(unchanged.incoming.ptr_eq(&state.incoming));
        assert!(unchanged.unique.ptr_eq(&state.unique));
        assert_eq!(state.version(&Key::Family(tid)), 3);
    }
    proptest! {
        #[test]
        fn differential_updates_match_remove_and_reinsert_including_versions(
            changes in prop::collection::vec((-20i64..20,-20i64..20,any::<bool>()),1..80)
        ) {
            let (mut actual,tid,id,a,b)=fixture(); let mut reference=actual.clone();
            for (index,(key,value,right)) in changes.into_iter().enumerate() {
                let revision=index as u64+2;
                let op=Op::Put(tid,Row{id,values:vec![key.into(),value.into(),if right {b.into()} else {a.into()}].into_boxed_slice()});
                actual.apply(&op,revision);
                // Original implementation always removed every index entry first.
                reference.remove_row(tid,id,revision); reference.apply(&op,revision);
                prop_assert_eq!(&actual.index,&reference.index);
                prop_assert_eq!(&actual.incoming,&reference.incoming);
                prop_assert_eq!(&actual.unique,&reference.unique);
                prop_assert_eq!(&actual.versions,&reference.versions);
                prop_assert_eq!(&actual.families,&reference.families);
            }
            actual.apply(&Op::Delete(tid,id),100);reference.apply(&Op::Delete(tid,id),100);
            prop_assert_eq!(&actual.index,&reference.index);
            prop_assert_eq!(&actual.incoming,&reference.incoming);
            prop_assert_eq!(&actual.unique,&reference.unique);
            prop_assert_eq!(&actual.versions,&reference.versions);
        }
    }
}
