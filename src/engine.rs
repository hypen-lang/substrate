use crate::{
    compiled::CompiledRelation,
    durable::Durable,
    model::identifier,
    storage::{Key, Op, State, VisibilityMemo},
    *,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex, Weak},
};

struct Head {
    state: Arc<State>,
    failed: bool,
}
/// Privileged embedding handle. Only trusted host code should hold this value.
/// Authenticate externally, then hand an application its restricted Session.
#[derive(Clone)]
pub struct Database {
    head: Arc<Mutex<Head>>,
    coordinator: Arc<commit::Coordinator>,
    pub(crate) clock: Arc<crate::auth::Clock>,
}
#[path = "commit.rs"]
mod commit;
pub use commit::{GroupCommitOptions, GroupCommitStats};
#[path = "maintenance.rs"]
mod maintenance;
pub use maintenance::{CheckpointCompletion, Checkpointer, CheckpointerOptions, CheckpointerStats};

impl Default for Database {
    fn default() -> Self {
        Self::memory()
    }
}
impl Database {
    pub fn memory() -> Self {
        Self::from_state(State::default(), None)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, DatabaseOptions::default())
    }
    pub fn open_with_options(path: impl AsRef<Path>, options: DatabaseOptions) -> Result<Self> {
        let (durable, state) = Durable::open(path.as_ref(), options)?;
        Ok(Self::from_state(state, Some(durable)))
    }
    pub fn open_with_commit_options(
        path: impl AsRef<Path>,
        options: DatabaseOptions,
        commits: GroupCommitOptions,
    ) -> Result<Self> {
        commits.validate()?;
        let (durable, state) = Durable::open(path.as_ref(), options)?;
        Ok(Self::from_state_with_commits(state, Some(durable), commits))
    }
    pub fn memory_with_commit_options(commits: GroupCommitOptions) -> Result<Self> {
        commits.validate()?;
        Ok(Self::from_state_with_commits(
            State::default(),
            None,
            commits,
        ))
    }
    pub fn group_commit_stats(&self) -> GroupCommitStats {
        self.coordinator.stats()
    }
    pub(crate) fn from_state(state: State, durable: Option<Durable>) -> Self {
        Self::from_state_with_commits(state, durable, GroupCommitOptions::default())
    }
    fn from_state_with_commits(
        state: State,
        durable: Option<Durable>,
        options: GroupCommitOptions,
    ) -> Self {
        Self {
            coordinator: Arc::new(commit::Coordinator::new(durable, options)),
            clock: Arc::new(crate::auth::Clock::default()),
            head: Arc::new(Mutex::new(Head {
                state: Arc::new(state),
                failed: false,
            })),
        }
    }
    /// Atomically persist current state and prune its covered log, without
    /// changing the logical revision or invalidating retained snapshots/transactions.
    pub fn checkpoint(&self) -> Result<CheckpointInfo> {
        // Lock order is writer -> published head. Never wait for disk with head held.
        let mut writer = self
            .coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?;
        let state = {
            let head = self
                .head
                .lock()
                .map_err(|_| Error::Storage("database lock poisoned".into()))?;
            if head.failed {
                return Err(Error::Storage(
                    "reopen after uncertain storage failure".into(),
                ));
            }
            head.state.clone()
        };
        let durable = writer
            .as_mut()
            .ok_or_else(|| Error::Storage("memory database has no checkpoint storage".into()))?;
        let result = durable.checkpoint(&state);
        if result.is_err() {
            self.head
                .lock()
                .map_err(|_| Error::Storage("database lock poisoned".into()))?
                .failed = true;
        }
        result
    }
    pub fn storage_info(&self) -> Result<StorageInfo> {
        let writer = self
            .coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?;
        if self
            .head
            .lock()
            .map_err(|_| Error::Storage("database lock poisoned".into()))?
            .failed
        {
            return Err(Error::Storage(
                "reopen after uncertain storage failure".into(),
            ));
        }
        writer
            .as_ref()
            .map(|d| d.info.clone())
            .ok_or_else(|| Error::Storage("not a durable database".into()))
    }
    /// Work performed by this handle's open, before optional post-open maintenance.
    pub fn recovery_info(&self) -> Result<RecoveryInfo> {
        self.coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?
            .as_ref()
            .map(|d| d.recovery.clone())
            .ok_or_else(|| Error::Storage("not a durable database".into()))
    }
    pub fn admin(&self) -> Session {
        Session {
            db: self.clone(),
            principal: None,
            capability: None,
            credential: None,
        }
    }
    /// An unauthenticated, non-admin session. It can only read public families.
    pub fn anonymous(&self) -> Session {
        self.session(crate::visibility::ANONYMOUS)
    }
    pub fn session(&self, principal: &str) -> Session {
        Session {
            db: self.clone(),
            principal: Some(principal.into()),
            capability: None,
            credential: None,
        }
    }
}
/// A result-release fence. Revocation linearizes at head publication; already
/// returned values cannot be recalled. No lock is held during query execution.
#[derive(Clone)]
pub(crate) struct LiveGuard {
    head: Weak<Mutex<Head>>,
    dependencies: Vec<(Key, u64)>,
    denied: bool,
    privileged: bool,
    clock: Arc<crate::auth::Clock>,
    expires_at: Option<u64>,
}
impl LiveGuard {
    /// A spilled read lease must remain attached even for host/admin snapshots.
    pub(crate) fn check_attached(&self) -> Result<()> {
        let authority = self.head.upgrade().ok_or(Error::Denied)?;
        let head = authority.lock().map_err(|_| Error::Denied)?;
        if head.failed {
            return Err(Error::Denied);
        }
        self.check_state(&head.state, head.failed)
    }
    fn check_state(&self, state: &State, failed: bool) -> Result<()> {
        if self.privileged {
            return Ok(());
        }
        if self.expires_at.is_some_and(|end| self.clock.now() >= end)
            || failed
            || self.denied
            || self
                .dependencies
                .iter()
                .any(|(key, v)| state.version(key) != *v)
        {
            Err(Error::Denied)
        } else {
            Ok(())
        }
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self.privileged {
            return Ok(());
        }
        let authority = self.head.upgrade().ok_or(Error::Denied)?;
        let head = authority.lock().map_err(|_| Error::Denied)?;
        self.check_state(&head.state, head.failed)
    }
}
impl Database {
    pub(crate) fn check_capability(&self, subject: &str, id: Id) -> Result<()> {
        let head = self.head.lock().map_err(|_| Error::Denied)?;
        if head.failed {
            return Err(Error::Denied);
        }
        crate::capability::resolve(&head.state, subject, id, self.clock.now()).map(|_| ())
    }
}
#[derive(Clone)]
pub struct Session {
    pub(crate) db: Database,
    pub(crate) principal: Option<String>,
    pub(crate) capability: Option<Id>,
    pub(crate) credential: Option<Id>,
}
impl Session {
    pub fn snapshot(&self) -> Snapshot {
        let (state, failed) = {
            let head = self.db.head.lock().expect("database lock poisoned");
            (head.state.clone(), head.failed)
        };
        // Resolve capability ancestry and construct scoped policy state against
        // the captured immutable revision, outside the publication lock. The
        // resulting guard still checks live dependencies before every operation.
        let (state, guard) = self.authorize_state(state, failed);
        Snapshot {
            state,
            principal: self.principal.clone(),
            live: guard,
            visibility: Arc::new(Mutex::new(VisibilityMemo::default())),
        }
    }
    fn authorize_state(&self, state: Arc<State>, failed: bool) -> (Arc<State>, LiveGuard) {
        let mut guard = LiveGuard {
            head: Arc::downgrade(&self.db.head),
            dependencies: vec![],
            denied: failed,
            privileged: self.principal.is_none(),
            clock: self.db.clock.clone(),
            expires_at: None,
        };
        let Some(principal) = &self.principal else {
            return (state, guard);
        };
        if let Some(id) = self.credential {
            match state.credentials.get(&id) {
                Some(c)
                    if !c.revoked
                        && c.principal == *principal
                        && Some(c.capability) == self.capability =>
                {
                    guard.expires_at = Some(c.expires_at);
                    let key = Key::Row(crate::auth::TYPE_ID, id);
                    guard.dependencies.push((key.clone(), state.version(&key)));
                }
                _ => guard.denied = true,
            }
        }
        if let Some(id) = self.capability {
            match crate::capability::resolve(&state, principal, id, self.db.clock.now()) {
                Ok((cap, keys)) => {
                    guard.expires_at =
                        crate::auth::earliest(guard.expires_at, cap.record.expires_at);
                    guard.dependencies.extend(keys.into_iter().map(|key| {
                        let v = state.version(&key);
                        (key, v)
                    }));
                    let mut effective = (*state).clone();
                    effective.grants.clear();
                    effective.schema_grants.clear();
                    effective.visibility_disabled = true;
                    for (id, grant) in &cap.grants {
                        effective
                            .grants
                            .insert((principal.clone(), *id), grant.clone());
                    }
                    (Arc::new(effective), guard)
                }
                Err(_) => {
                    guard.denied = true;
                    (state, guard)
                }
            }
        } else {
            let key = Key::Revocation(principal.clone());
            guard.dependencies.push((key.clone(), state.version(&key)));
            guard
                .dependencies
                .push((Key::Visibility, state.version(&Key::Visibility)));
            (state, guard)
        }
    }
    pub fn transaction(&self) -> Transaction {
        let snapshot = self.snapshot();
        let mut reads = BTreeSet::new();
        if let Some(p) = &self.principal {
            reads.insert(Key::Policy(p.clone()));
            reads.insert(Key::Visibility);
        }
        Transaction {
            db: self.db.clone(),
            base: snapshot.state.clone(),
            work: (*snapshot.state).clone(),
            principal: self.principal.clone(),
            reads,
            ops: vec![],
            failed: false,
            live: snapshot.live.clone(),
            control_capability: None,
        }
    }
}

/// Data stays pinned; changed authority invalidates retained restricted snapshots.
#[derive(Clone)]
pub struct Snapshot {
    pub(crate) state: Arc<State>,
    pub(crate) principal: Option<String>,
    pub(crate) live: LiveGuard,
    visibility: Arc<Mutex<VisibilityMemo>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub revision: u64,
    pub identities: usize,
    pub relations: usize,
    pub rows: usize,
    pub role_index_entries: usize,
    pub reference_index_entries: usize,
}
impl Snapshot {
    pub(crate) fn check_live(&self) -> Result<()> {
        self.live.check()
    }
    pub fn revision(&self) -> u64 {
        self.state.revision
    }
    pub(crate) fn resolve_read(&self, name: &str) -> Result<Arc<CompiledRelation>> {
        self.check_live()?;
        // Hide unknown versus unauthorized names from restricted callers.
        let def = self.state.relation(name).map_err(|e| {
            if self.principal.is_some() {
                Error::Denied
            } else {
                e
            }
        })?;
        if !self.state.can_read(self.principal.as_deref(), def.id) {
            return Err(Error::Denied);
        }
        Ok(def)
    }
    fn complete_schema_visible(&self, def: &Relation) -> bool {
        def.roles.iter().all(|role| match &role.data_type {
            DataType::Ref(name) => self
                .state
                .names
                .get(name)
                .is_some_and(|id| self.can_discover(*id)),
            _ => true,
        })
    }
    pub fn relations(&self) -> Vec<Relation> {
        if self.check_live().is_err() {
            return vec![];
        }
        let result = self
            .state
            .relations
            .values()
            .filter(|d| {
                self.state.can_read(self.principal.as_deref(), d.id)
                    && self.complete_schema_visible(d)
            })
            .map(|d| d.declaration.clone())
            .collect();
        if self.check_live().is_err() {
            vec![]
        } else {
            result
        }
    }
    pub fn schema(&self, name: &str) -> Result<Relation> {
        let result = self.resolve_read(name)?.declaration.clone();
        if !self.complete_schema_visible(&result) {
            return Err(Error::Denied);
        }
        self.check_live()?;
        Ok(result)
    }
    pub fn scan(&self, name: &str) -> Result<Vec<Row>> {
        self.read_rows(name, None, None)
    }
    pub fn lookup(&self, name: &str, role: &str, value: &Value) -> Result<Vec<Row>> {
        self.read_rows(name, Some((role, value)), None)
    }
    pub(crate) fn read_rows(
        &self,
        name: &str,
        predicate: Option<(&str, &Value)>,
        reads: Option<&mut BTreeSet<Key>>,
    ) -> Result<Vec<Row>> {
        self.map_rows(name, predicate, reads, Row::clone)
    }
    /// Internal projection boundary: authorization traverses the full row and
    /// reference closure before the mapper sees it, regardless of selected cells.
    pub(crate) fn map_rows<T>(
        &self,
        name: &str,
        predicate: Option<(&str, &Value)>,
        mut reads: Option<&mut BTreeSet<Key>>,
        mut map: impl FnMut(&Row) -> T,
    ) -> Result<Vec<T>> {
        let def = self.resolve_read(name)?;
        let (bucket, family) = if let Some((role, value)) = predicate {
            let i = def.role(role)?;
            if !def.roles[i].data_type.accepts(value) {
                return Err(Error::Query("predicate type mismatch".into()));
            }
            let key = (def.id, i, value.clone());
            if let Some(reads) = &mut reads {
                reads.insert(Key::Bucket(key.clone()));
            }
            (self.state.index.get(&key), None)
        } else {
            if let Some(reads) = &mut reads {
                reads.insert(Key::Family(def.id));
            }
            (None, Some(&self.state.families[&def.id]))
        };
        // Borrow candidates directly: no intermediate vector of all identities.
        let candidates = bucket
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .chain(family.into_iter().flat_map(|rows| rows.keys().copied()));
        let rows = candidates
            .filter(|id| {
                if let Some(reads) = &mut reads {
                    // Transactional reads deliberately retain full dependency
                    // traversal. A snapshot memo hit must never omit OCC keys.
                    self.state.visible(
                        self.principal.as_deref(),
                        def.id,
                        *id,
                        &mut BTreeSet::new(),
                        reads,
                    )
                } else {
                    self.visible_for_cursor(def.id, *id)
                }
            })
            .map(|id| map(self.state.row(def.id, id).expect("visible row")))
            .collect();
        self.check_live()?;
        Ok(rows)
    }
    pub fn get(&self, name: &str, id: Id) -> Result<Option<Row>> {
        self.read_one(name, id, None)
    }
    fn read_one(
        &self,
        name: &str,
        id: Id,
        reads: Option<&mut BTreeSet<Key>>,
    ) -> Result<Option<Row>> {
        let def = self.resolve_read(name)?;
        let visible = if let Some(reads) = reads {
            self.state.visible(
                self.principal.as_deref(),
                def.id,
                id,
                &mut BTreeSet::new(),
                reads,
            )
        } else {
            self.visible_for_cursor(def.id, id)
        };
        let row = visible.then(|| (**self.state.row(def.id, id).expect("visible row")).clone());
        self.check_live()?;
        Ok(row)
    }
    /// Immutable snapshot visibility: cache only completed closure decisions.
    /// Callers must keep entry/result live gates; cached data never caches authority.
    pub(crate) fn visible_for_cursor(&self, relation: Id, id: Id) -> bool {
        let Some(principal) = self.principal.as_deref() else {
            return self.state.row(relation, id).is_some();
        };
        self.visibility
            .lock()
            .is_ok_and(|mut memo| memo.visible(&self.state, principal, (relation, id)))
    }
    /// Global physical statistics are privileged; app aggregates use authorized scans.
    pub fn stats(&self) -> Result<Stats> {
        if self.principal.is_some() {
            return Err(Error::Denied);
        }
        Ok(Stats {
            revision: self.revision(),
            identities: self.state.identities.len(),
            relations: self.state.relations.len(),
            rows: self.state.families.values().map(|f| f.len()).sum(),
            role_index_entries: self.state.index.values().map(|s| s.len()).sum(),
            reference_index_entries: self.state.incoming.values().map(|s| s.len()).sum(),
        })
    }
    /// Rebuild all correctness-critical indexes independently from canonical rows.
    pub fn verify_indexes(&self) -> Result<()> {
        if self.principal.is_some() {
            return Err(Error::Denied);
        }
        let mut rebuilt = (*self.state).clone();
        rebuilt.index.clear();
        rebuilt.unique.clear();
        rebuilt.incoming.clear();
        for tid in self.state.families.keys() {
            rebuilt.families.insert(*tid, im::OrdMap::new());
        }
        for (tid, family) in &self.state.families {
            for row in family.values() {
                let def = &self.state.relations[tid];
                if !self.state.identities.contains(&row.id) || row.values.len() != def.roles.len() {
                    return Err(Error::Storage("invalid canonical row".into()));
                }
                for (role, value) in def.roles.iter().zip(row.values.iter()) {
                    if !role.data_type.accepts(value) {
                        return Err(Error::Storage("invalid scalar".into()));
                    }
                    if let (DataType::Ref(target), Value::Ref(id)) = (&role.data_type, value)
                        && self.state.row(self.state.names[target], *id).is_none()
                    {
                        return Err(Error::Storage("dangling reference".into()));
                    }
                }
                for i in 0..def.unique.len() {
                    if rebuilt
                        .unique
                        .contains_key(&(*tid, i, def.unique_values(row, i)))
                    {
                        return Err(Error::Storage("duplicate unique key".into()));
                    }
                }
                rebuilt.apply(&Op::Put(*tid, (**row).clone()), self.revision());
            }
        }
        if rebuilt.index != self.state.index
            || rebuilt.unique != self.state.unique
            || rebuilt.incoming != self.state.incoming
        {
            return Err(Error::Storage("index inconsistency".into()));
        }
        Ok(())
    }
}

/// Optimistic serializable transaction. Any failed mutation poisons the transaction.
/// Drop rolls back. Read dependencies include empty index buckets and incoming edges.
pub struct Transaction {
    db: Database,
    base: Arc<State>,
    work: State,
    principal: Option<String>,
    reads: BTreeSet<Key>,
    ops: Vec<Op>,
    failed: bool,
    live: LiveGuard,
    control_capability: Option<Arc<crate::capability::CompiledCapability>>,
}
impl Transaction {
    pub(crate) fn db_now(&self) -> u64 {
        self.db.clock.now()
    }
    /// Privileged quota metadata, retaining the same family phantom dependency
    /// as scan without cloning rows or traversing their reference graphs.
    pub(crate) fn count_internal_family(&mut self, name: &str) -> Result<usize> {
        self.admin()?;
        self.live.check()?;
        let def = self.def(name)?;
        self.reads.insert(Key::Family(def.id));
        Ok(self.work.families[&def.id].len())
    }
    pub(crate) fn track_authority_key(&mut self, key: Key) {
        self.reads.insert(key);
    }
    pub(crate) fn bind_session(&mut self, session: &Session) -> Result<()> {
        let (_, guard) = session.authorize_state(self.base.clone(), false);
        guard.check()?;
        self.live = guard;
        Ok(())
    }
    pub(crate) fn expire_at(&mut self, expires_at: Option<u64>) {
        self.live.privileged = false;
        self.live.expires_at = crate::auth::earliest(self.live.expires_at, expires_at);
    }
    pub(crate) fn bind_schema_session(
        &mut self,
        session: &Session,
        namespace: Option<&str>,
    ) -> Result<String> {
        self.admin()?;
        let actor = session
            .principal
            .as_deref()
            .filter(|p| *p != crate::visibility::ANONYMOUS)
            .ok_or(Error::Denied)?;
        let (_, guard) = session.authorize_state(self.base.clone(), false);
        guard.check()?;
        self.live = guard;
        if let Some(id) = session.capability {
            let (cap, keys) =
                crate::capability::resolve(&self.base, actor, id, self.db.clock.now())?;
            if namespace.is_some() && cap.record.scope.schema_namespace.as_deref() != namespace {
                return Err(Error::Denied);
            }
            if cap.record.scope.schema_namespace.is_none() {
                return Err(Error::Denied);
            }
            for key in keys {
                self.reads.insert(key);
            }
            let owner = cap.record.authority.clone();
            self.control_capability = Some(cap);
            Ok(owner)
        } else {
            Ok(actor.into())
        }
    }
    pub(crate) fn has_schema_capability(&self) -> bool {
        self.control_capability.is_some()
    }
    pub(crate) fn schema_namespace_scope(&self) -> Option<&str> {
        self.control_capability
            .as_ref()
            .and_then(|c| c.record.scope.schema_namespace.as_deref())
    }
    pub(crate) fn check_dynamic_descriptor(
        &mut self,
        descriptor: &RuntimeDescriptor,
    ) -> Result<()> {
        self.mutation(|tx| tx.validate_dynamic_descriptor(descriptor))
    }
    /// Control-plane validation participates in the normal catalog/policy OCC
    /// dependencies, but never changes the caller's privilege or working state.
    pub(crate) fn schema_control_snapshot(&mut self, principal: &str) -> Result<Snapshot> {
        self.admin()?;
        self.reads.insert(Key::Catalog);
        self.reads.insert(Key::Policy(principal.into()));
        let mut snapshot = self.snapshot();
        if let Some(cap) = &self.control_capability {
            let mut effective = (*snapshot.state).clone();
            effective.grants.clear();
            effective.visibility_disabled = true;
            for (id, grant) in &cap.grants {
                effective
                    .grants
                    .insert((principal.into(), *id), grant.clone());
            }
            snapshot.state = Arc::new(effective);
        }
        Ok(snapshot)
    }
    pub(crate) fn check_additive_module(&self, module: &Module) -> Result<()> {
        self.admin()?;
        let mut staged = Transaction {
            db: self.db.clone(),
            base: self.base.clone(),
            work: self.work.clone(),
            principal: None,
            reads: BTreeSet::new(),
            ops: vec![],
            failed: false,
            live: self.live.clone(),
            control_capability: None,
        };
        staged.install(module.clone())
    }
    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            state: Arc::new(self.work.clone()),
            principal: self.principal.clone(),
            live: self.live.clone(),
            visibility: Arc::new(Mutex::new(VisibilityMemo::default())),
        }
    }
    fn stage(&mut self, op: Op) {
        self.work.apply(&op, self.base.revision + 1);
        self.ops.push(op);
    }
    fn mutation<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let result = self.live.check().and_then(|_| f(self));
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn admin(&self) -> Result<()> {
        if self.principal.is_none() {
            Ok(())
        } else {
            Err(Error::Denied)
        }
    }
    fn def(&self, name: &str) -> Result<Arc<CompiledRelation>> {
        self.work.relation(name).map_err(|e| {
            if self.principal.is_some() {
                Error::Denied
            } else {
                e
            }
        })
    }
    fn may_write(&self, def: &Relation, row: &Row) -> Result<()> {
        if self.principal.as_deref() == Some(crate::visibility::ANONYMOUS) {
            return Err(Error::Denied);
        }
        if let Some(p) = &self.principal
            && !self
                .work
                .grant(p, def.id)
                .is_some_and(|g| g.write && g.allows(row))
        {
            return Err(Error::Denied);
        }
        Ok(())
    }
    fn reference(&mut self, tid: Id, id: Id) -> Result<()> {
        if self.principal.as_deref() == Some(crate::visibility::ANONYMOUS) {
            return Err(Error::Denied);
        }
        self.reads.insert(Key::Row(tid, id));
        let row = self.work.row(tid, id);
        if let Some(p) = &self.principal
            && !row.is_some_and(|r| {
                self.work
                    .grant(p, tid)
                    .is_some_and(|g| g.reference && g.allows(r))
            })
        {
            return Err(Error::Denied);
        }
        row.ok_or_else(|| Error::Constraint("invalid typed reference".into()))?;
        Ok(())
    }
    fn validate_row(&mut self, def: &CompiledRelation, row: &Row) -> Result<()> {
        if row.values.len() != def.roles.len() {
            return Err(Error::Constraint("wrong arity".into()));
        }
        // Check types before policy evaluation indexes the tuple.
        for (role, value) in def.roles.iter().zip(row.values.iter()) {
            if !role.data_type.accepts(value) {
                return Err(Error::Constraint("role type mismatch".into()));
            }
        }
        self.may_write(def, row)?;
        def.check_values(&row.values)?;
        if def.id == crate::visibility::TYPE_ID {
            crate::visibility::compile(&self.work, row)?;
        }
        for (role, value) in def.roles.iter().zip(row.values.iter()) {
            if let (DataType::Ref(target), Value::Ref(id)) = (&role.data_type, value) {
                self.reference(self.work.names[target], *id)?;
            }
        }
        for i in 0..def.unique.len() {
            let key = (def.id, i, def.unique_values(row, i));
            self.reads.insert(Key::Unique(key.clone()));
            if self.work.unique.get(&key).is_some_and(|id| *id != row.id) {
                return Err(Error::Constraint("unique key unavailable".into()));
            }
        }
        Ok(())
    }
    pub fn grant_schema(&mut self, principal: &str, namespace: &str) -> Result<()> {
        self.mutation(|tx| {
            tx.admin()?;
            if !identifier(namespace) {
                return Err(Error::Schema("invalid namespace".into()));
            }
            tx.reads.insert(Key::Policy(principal.into()));
            tx.stage(Op::SchemaGrant(principal.into(), namespace.into()));
            Ok(())
        })
    }
    pub fn grant(&mut self, principal: &str, relation: &str, grant: Grant) -> Result<()> {
        self.mutation(|tx| {
            tx.admin()?;
            let def = tx.def(relation)?;
            if let RowFilter::Equals { role, value } = &grant.filter {
                let i = def.role(role)?;
                if !def.roles[i].data_type.accepts(value) {
                    return Err(Error::Schema("policy type mismatch".into()));
                }
            }
            tx.reads.insert(Key::Policy(principal.into()));
            tx.invalidate_capability_roots(principal, def.id)?;
            tx.stage(Op::Grant(principal.into(), def.id, Some(grant)));
            Ok(())
        })
    }
    pub fn revoke(&mut self, principal: &str, relation: &str) -> Result<()> {
        self.mutation(|tx| {
            tx.admin()?;
            let def = tx.def(relation)?;
            tx.reads.insert(Key::Policy(principal.into()));
            tx.invalidate_capability_roots(principal, def.id)?;
            tx.stage(Op::Grant(principal.into(), def.id, None));
            Ok(())
        })
    }
    /// Idempotent host bootstrap. Existing definitions and policies are preserved.
    pub fn install_manifest(&mut self, manifest: &SchemaManifest) -> Result<SchemaPlan> {
        self.install_manifest_with_visibility(manifest, Visibility::Authenticated)
    }
    /// Initial visibility applies only to newly installed modules, never existing families.
    pub fn install_manifest_with_visibility(
        &mut self,
        manifest: &SchemaManifest,
        visibility: Visibility,
    ) -> Result<SchemaPlan> {
        self.mutation(|tx| {
            tx.admin()?;
            tx.reads.insert(Key::Catalog);
            let plan = tx.snapshot().plan_manifest(manifest)?;
            for module in manifest.canonical()?.modules {
                if !tx
                    .work
                    .modules
                    .contains_key(&(module.name.clone(), module.version))
                {
                    tx.install_with_visibility(module, visibility)?;
                }
            }
            Ok(plan)
        })
    }
    /// Install an additive module. New app families default to authenticated visibility.
    pub fn install(&mut self, module: Module) -> Result<()> {
        self.install_with_visibilities(module, BTreeMap::new())
    }
    pub fn install_with_visibility(
        &mut self,
        module: Module,
        visibility: Visibility,
    ) -> Result<()> {
        let modes = module
            .relations
            .iter()
            .map(|r| (r.name.clone(), visibility))
            .collect();
        self.install_with_visibilities(module, modes)
    }
    /// Overrides name only newly installed relations; unspecified families use Authenticated.
    pub fn install_with_visibilities(
        &mut self,
        module: Module,
        visibility: BTreeMap<String, Visibility>,
    ) -> Result<()> {
        self.mutation(|tx| {
            let modes = crate::visibility::normalize(&module, visibility)?;
            tx.install_inner(module.clone())?;
            if !crate::visibility::protected(&module.name) {
                let owner = match &tx.principal {
                    Some(principal) => principal.clone(),
                    None => tx
                        .visibility_namespace_owner(&module.name)?
                        .unwrap_or_else(|| crate::visibility::HOST.into()),
                };
                for relation in &module.relations {
                    tx.store_visibility(&relation.name, &owner, modes[&relation.name])?;
                }
            }
            Ok(())
        })
    }
    /// Host-only mutation. Namespace owners use Session::set_schema_visibility.
    pub fn set_schema_visibility(&mut self, relation: &str, visibility: Visibility) -> Result<()> {
        self.mutation(|tx| {
            tx.admin()?;
            let def = tx.def(relation)?;
            let owner = match tx.work.schema_visibility.get(&def.id) {
                Some(policy) => policy.owner.clone(),
                None => tx
                    .visibility_namespace_owner(def.name.split_once('.').ok_or(Error::Denied)?.0)?
                    .unwrap_or_else(|| crate::visibility::HOST.into()),
            };
            tx.store_visibility(relation, &owner, visibility)
        })
    }
    /// Called after namespace ownership has been checked, or during a validated install.
    pub(crate) fn store_visibility(
        &mut self,
        relation: &str,
        owner: &str,
        visibility: Visibility,
    ) -> Result<()> {
        let def = self.def(relation)?;
        let namespace = relation.split_once('.').ok_or(Error::Denied)?.0;
        if crate::visibility::protected(namespace)
            || owner.is_empty()
            || owner.len() > 128
            || owner == crate::visibility::ANONYMOUS
        {
            return Err(Error::Denied);
        }
        self.reads.insert(Key::Visibility);
        self.reads.insert(Key::Catalog);
        if self
            .work
            .schema_visibility
            .get(&def.id)
            .is_some_and(|p| p.owner == owner && p.visibility == visibility)
        {
            return Ok(());
        }
        let module = crate::visibility::module();
        match self
            .work
            .modules
            .get(&(module.name.clone(), module.version))
        {
            Some(existing) if existing.as_ref() != module => return Err(Error::Denied),
            Some(_) => {}
            None => self.stage(Op::Install(module.clone())),
        }
        let id = self
            .work
            .schema_visibility
            .get(&def.id)
            .map(|p| p.row_id)
            .unwrap_or_default();
        self.reads.insert(Key::Row(crate::visibility::TYPE_ID, id));
        if !self.work.identities.contains(&id) {
            self.reads.insert(Key::Allocated(id));
            self.stage(Op::Identity(id));
        }
        let row = Row {
            id,
            values: vec![
                Value::Text(def.id.to_string()),
                Value::Text(owner.into()),
                Value::Text(visibility.name().into()),
            ]
            .into_boxed_slice(),
        };
        crate::visibility::compile(&self.work, &row)?;
        self.stage(Op::Put(crate::visibility::TYPE_ID, row));
        Ok(())
    }
    pub(crate) fn bind_namespace_visibility_owner(
        &mut self,
        namespace: &str,
        owner: &str,
    ) -> Result<()> {
        self.admin()?;
        let policies: Vec<_> = self
            .work
            .schema_visibility
            .values()
            .filter(|p| self.work.origins[&p.relation].0 == namespace)
            .map(|p| (self.work.relations[&p.relation].name.clone(), p.visibility))
            .collect();
        for (relation, mode) in policies {
            self.store_visibility(&relation, owner, mode)?;
        }
        Ok(())
    }
    fn install_inner(&mut self, module: Module) -> Result<()> {
        if self.principal.as_deref() == Some(crate::visibility::ANONYMOUS) {
            return Err(Error::Denied);
        }
        if self.principal.is_some() {
            self.reads.insert(Key::Catalog);
            if let Some(key) = self.snapshot().schema_namespace_dependency(&module.name)? {
                self.reads.insert(Key::Bucket(key));
            }
        }
        if self.principal.is_some()
            && ((module.name.eq_ignore_ascii_case("liquid_control")
                || module.name.eq_ignore_ascii_case("liquid_security")
                || module.name.eq_ignore_ascii_case("liquid_auth")
                || module.name.eq_ignore_ascii_case("liquid_views")
                || module.name.eq_ignore_ascii_case("liquid_view_catalog")
                || module.name.eq_ignore_ascii_case("liquid_visibility"))
                || self.snapshot().schema_namespace_managed(&module.name)?)
        {
            // A legacy broad namespace grant must not bypass managed approvals,
            // quota accounting or alias reservations. The controller stages its
            // validated installation through the trusted host transaction.
            return Err(Error::Denied);
        }
        if let Some(p) = &self.principal
            && !self
                .work
                .schema_grants
                .contains(&(p.clone(), module.name.clone()))
        {
            return Err(Error::Denied);
        }
        self.reads.insert(Key::Catalog);
        if !identifier(&module.name)
            || ["main", "temp"]
                .iter()
                .any(|n| module.name.eq_ignore_ascii_case(n))
            || module.name.to_ascii_lowercase().starts_with("sqlite_")
        {
            return Err(Error::Schema("invalid or reserved module name".into()));
        }
        let latest = self
            .work
            .modules
            .keys()
            .filter(|(n, _)| n == &module.name)
            .map(|(_, v)| *v)
            .max()
            .unwrap_or(0);
        if module.version != latest + 1 {
            return Err(Error::Schema(
                "module versions must be successive starting at 1".into(),
            ));
        }
        if self
            .work
            .modules
            .keys()
            .any(|(n, _)| n != &module.name && n.eq_ignore_ascii_case(&module.name))
        {
            return Err(Error::Schema("case-insensitive namespace collision".into()));
        }
        let mut imports = BTreeSet::new();
        for (name, version) in &module.imports {
            if name == &module.name
                || !imports.insert(name)
                || !self.work.modules.contains_key(&(name.clone(), *version))
            {
                return Err(Error::Schema("invalid or missing import".into()));
            }
        }
        let mut names = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for def in &module.relations {
            self.reads.insert(Key::Allocated(def.id));
            if self.principal.is_some() && crate::schema_control::reserved_schema_id(def.id) {
                return Err(Error::Denied);
            }
            def.validate(&module.name)?;
            if !names.insert(def.name.to_ascii_lowercase())
                || self
                    .work
                    .names
                    .keys()
                    .any(|n| n.eq_ignore_ascii_case(&def.name))
                || !ids.insert(def.id)
                || self.work.relations.contains_key(&def.id)
                || self.work.identities.contains(&def.id)
            {
                return Err(Error::Schema(
                    "existing name or identity cannot be redefined".into(),
                ));
            }
        }
        let local_names: BTreeSet<_> = module
            .relations
            .iter()
            .map(|def| def.name.as_str())
            .collect();
        for def in &module.relations {
            for role in &def.roles {
                if let DataType::Ref(target) = &role.data_type {
                    if local_names.contains(target.as_str()) {
                        continue;
                    }
                    let target_def = self.def(target)?;
                    let (owner, version) = &self.work.origins[&target_def.id];
                    if owner != &module.name
                        && !module
                            .imports
                            .iter()
                            .any(|(n, v)| n == owner && v >= version)
                    {
                        return Err(Error::Schema(
                            "reference is outside explicit import contract".into(),
                        ));
                    }
                    if let Some(p) = &self.principal
                        && !self
                            .work
                            .grant(p, target_def.id)
                            .is_some_and(|g| g.reference)
                    {
                        return Err(Error::Denied);
                    }
                }
            }
        }
        self.stage(Op::Install(module.clone()));
        if let Some(p) = self.principal.clone() {
            for def in module.relations {
                self.stage(Op::Grant(p.clone(), def.id, Some(Grant::read_write())));
            }
        }
        Ok(())
    }
    pub fn check_write_schema(&mut self, relation: &str, schema_id: Id) -> Result<()> {
        self.mutation(|tx| {
            let def = tx.def(relation)?;
            if let Some(p) = &tx.principal
                && !tx.work.grant(p, def.id).is_some_and(|g| g.write)
            {
                return Err(Error::Denied);
            }
            if def.id != schema_id {
                return Err(Error::Schema("generated schema identity mismatch".into()));
            }
            Ok(())
        })
    }
    fn named_values(
        &self,
        def: &CompiledRelation,
        fields: impl IntoIterator<Item = (impl Into<String>, Value)>,
        base: Option<&Row>,
    ) -> Result<Vec<Value>> {
        let mut supplied = std::collections::BTreeMap::new();
        for (name, value) in fields {
            let name = name.into();
            if def.role(&name).is_err() {
                return Err(Error::Constraint(format!(
                    "unknown field {}.{name}",
                    def.name
                )));
            }
            if supplied.insert(name.clone(), value).is_some() {
                return Err(Error::Constraint(format!("duplicate field {name}")));
            }
        }
        def.roles
            .iter()
            .enumerate()
            .map(|(i, role)| {
                supplied
                    .remove(&role.name)
                    .or_else(|| base.map(|r| r.values[i].clone()))
                    .ok_or_else(|| {
                        Error::Constraint(format!(
                            "missing required field {}.{}",
                            def.name, role.name
                        ))
                    })
            })
            .collect()
    }
    /// Insert using field names. Unknown, duplicate, and missing fields abort the transaction.
    pub fn insert_named(
        &mut self,
        relation: &str,
        fields: impl IntoIterator<Item = (impl Into<String>, Value)>,
    ) -> Result<Id> {
        self.mutation(|tx| {
            let def = tx.def(relation)?;
            let values = tx.named_values(&def, fields, None)?;
            tx.insert(relation, values)
        })
    }
    /// Patch supplied fields; omitted fields retain their values. Both old/new policy checks apply.
    pub fn update_named(
        &mut self,
        relation: &str,
        id: Id,
        fields: impl IntoIterator<Item = (impl Into<String>, Value)>,
    ) -> Result<()> {
        self.mutation(|tx| {
            let def = tx.def(relation)?;
            let old = tx.work.row(def.id, id).ok_or(if tx.principal.is_some() {
                Error::Denied
            } else {
                Error::NotFound
            })?;
            tx.may_write(&def, old)?;
            let values = tx.named_values(&def, fields, Some(old))?;
            tx.update(relation, id, values)
        })
    }
    pub fn insert_record<R: Record>(&mut self, record: R) -> Result<TypedId<R>> {
        self.mutation(|tx| {
            if tx.def(R::RELATION)?.id != R::TYPE_ID
                || R::FINGERPRINT.is_some_and(|f| tx.work.relations[&R::TYPE_ID].fingerprint != f)
            {
                return Err(Error::Schema("generated schema identity mismatch".into()));
            }
            tx.insert_named(R::RELATION, record.fields())
                .map(TypedId::from_raw)
        })
    }
    pub fn replace_record<R: Record>(&mut self, id: TypedId<R>, record: R) -> Result<()> {
        self.mutation(|tx| {
            if tx.def(R::RELATION)?.id != R::TYPE_ID
                || R::FINGERPRINT.is_some_and(|f| tx.work.relations[&R::TYPE_ID].fingerprint != f)
            {
                return Err(Error::Schema("generated schema identity mismatch".into()));
            }
            let def = tx.def(R::RELATION)?;
            let values = tx.named_values(&def, record.fields(), None)?;
            tx.update(R::RELATION, id.raw(), values)
        })
    }
    pub fn insert(&mut self, relation: &str, values: Vec<Value>) -> Result<Id> {
        self.insert_with_id(relation, Id::new(), values)
    }
    /// Explicit identity allocation for SQL/clients. Retired IDs cannot be reused.
    pub fn insert_with_id(&mut self, relation: &str, id: Id, values: Vec<Value>) -> Result<Id> {
        self.mutation(|tx| {
            let def = tx.def(relation)?;
            if tx.work.identities.contains(&id) || tx.work.relations.contains_key(&id) {
                return Err(Error::Constraint("identity collision".into()));
            }
            tx.reads.insert(Key::Allocated(id));
            let row = Row {
                id,
                values: values.into_boxed_slice(),
            };
            tx.reads.insert(Key::Row(def.id, id));
            tx.validate_row(&def, &row)?;
            tx.stage(Op::Identity(id));
            tx.stage(Op::Put(def.id, row));
            Ok(id)
        })
    }
    /// Explicitly add a membership to the SAME identity, authorized through source.
    pub fn add_membership(
        &mut self,
        source: &str,
        id: Id,
        target: &str,
        values: Vec<Value>,
    ) -> Result<()> {
        self.mutation(|tx| {
            let source = tx.def(source)?;
            tx.reference(source.id, id)?;
            let def = tx.def(target)?;
            tx.reads.insert(Key::Row(def.id, id));
            if tx.work.row(def.id, id).is_some() {
                return Err(Error::Constraint("membership already exists".into()));
            }
            let row = Row {
                id,
                values: values.into_boxed_slice(),
            };
            tx.validate_row(&def, &row)?;
            tx.stage(Op::Put(def.id, row));
            Ok(())
        })
    }
    pub fn update(&mut self, relation: &str, id: Id, values: Vec<Value>) -> Result<()> {
        self.mutation(|tx| {
            let def = tx.def(relation)?;
            tx.reads.insert(Key::Row(def.id, id));
            let old = tx.work.row(def.id, id).ok_or(if tx.principal.is_some() {
                Error::Denied
            } else {
                Error::NotFound
            })?;
            tx.may_write(&def, old)?;
            let row = Row {
                id,
                values: values.into_boxed_slice(),
            };
            tx.validate_row(&def, &row)?;
            tx.stage(Op::Put(def.id, row));
            Ok(())
        })
    }
    /// RESTRICT deletion of a membership/occurrence; identities are never recycled.
    pub fn delete(&mut self, relation: &str, id: Id) -> Result<()> {
        self.mutation(|tx| {
            let def = tx.def(relation)?;
            tx.reads.insert(Key::Row(def.id, id));
            let old = tx.work.row(def.id, id).ok_or(if tx.principal.is_some() {
                Error::Denied
            } else {
                Error::NotFound
            })?;
            tx.may_write(&def, old)?;
            tx.reads.insert(Key::Incoming((def.id, id)));
            if tx
                .work
                .incoming
                .get(&(def.id, id))
                .is_some_and(|s| !s.is_empty())
            {
                return Err(Error::Constraint("occurrence is referenced".into()));
            }
            tx.stage(Op::Delete(def.id, id));
            Ok(())
        })
    }
    pub fn scan(&mut self, relation: &str) -> Result<Vec<Row>> {
        self.track_name(relation);
        self.snapshot()
            .read_rows(relation, None, Some(&mut self.reads))
    }
    pub fn lookup(&mut self, relation: &str, role: &str, value: &Value) -> Result<Vec<Row>> {
        self.track_name(relation);
        self.snapshot()
            .read_rows(relation, Some((role, value)), Some(&mut self.reads))
    }
    pub fn get(&mut self, relation: &str, id: Id) -> Result<Option<Row>> {
        self.track_name(relation);
        self.snapshot()
            .read_one(relation, id, Some(&mut self.reads))
    }
    fn track_name(&mut self, relation: &str) {
        if !self.work.names.contains_key(relation) {
            self.reads.insert(Key::Catalog);
        }
    }
    pub fn commit(self) -> Result<u64> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let bytes = crate::codec::encoded_len(&self.ops, &self.work)?;
        let request = commit::Request::new(self.base, self.reads, self.ops, self.live, bytes);
        self.db.coordinator.submit(&self.db.head, request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn installing_a_module_shares_old_physical_rows() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let id = tx.insert("core.Person", vec!["Alice".into()]).unwrap();
        tx.commit().unwrap();
        let before = db.admin().snapshot();
        let tid = before.schema("core.Person").unwrap().id;
        let old = before.state.row(tid, id).unwrap();
        let mut tx = db.admin().transaction();
        tx.install(demo::fitness()).unwrap();
        tx.commit().unwrap();
        let after = db.admin().snapshot();
        assert!(Arc::ptr_eq(old, after.state.row(tid, id).unwrap()));
        assert!(Arc::ptr_eq(
            &before.state.relations[&tid],
            &after.state.relations[&tid]
        ));
    }
}

#[cfg(test)]
#[path = "visibility_tests.rs"]
mod visibility_tests;

// Trusted-host replication hooks. Followers keep this memory-only handle private.
impl Database {
    pub(crate) fn replication_capture(&self) -> Result<Arc<State>> {
        let writer = self
            .coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?;
        if writer.is_none() {
            return Err(Error::Storage("replication source must be durable".into()));
        }
        let head = self
            .head
            .lock()
            .map_err(|_| Error::Storage("head lock poisoned".into()))?;
        if head.failed {
            return Err(Error::Storage("reopen failed source".into()));
        }
        Ok(head.state.clone())
    }
    pub(crate) fn replication_tail(
        &self,
        after: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let writer = self
            .coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?;
        if self
            .head
            .lock()
            .map_err(|_| Error::Storage("head lock poisoned".into()))?
            .failed
        {
            return Err(Error::Storage("reopen failed source".into()));
        }
        writer
            .as_ref()
            .ok_or_else(|| Error::Storage("replication source must be durable".into()))?
            .replication_tail(after, count, bytes)
    }
    pub(crate) fn replication_publish(&self, state: State) -> Result<()> {
        let writer = self
            .coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?;
        if writer.is_some() {
            return Err(Error::Storage(
                "follower publication requires private memory engine".into(),
            ));
        }
        let mut head = self
            .head
            .lock()
            .map_err(|_| Error::Storage("head lock poisoned".into()))?;
        if head.failed {
            return Err(Error::Storage("reopen failed follower".into()));
        }
        head.state = Arc::new(state);
        Ok(())
    }
    pub(crate) fn replication_fail(&self) {
        if let Ok(mut head) = self.head.lock() {
            head.failed = true;
        }
    }
}

#[cfg(test)]
mod legacy_visibility_tests {
    use super::*;
    #[test]
    fn explicit_visibility_change_adopts_legacy_namespace_owner_only_when_requested() {
        let db = Database::memory();
        db.configure_namespace(crate::schema_control::NamespacePolicy {
            namespace: "legacy".into(),
            owner: "owner".into(),
            auto_approve: false,
            enabled: true,
            quotas: crate::schema_control::SchemaQuotas::default(),
        })
        .unwrap();
        let mut tx = db.admin().transaction();
        tx.install_inner(Module::new(
            "legacy",
            1,
            vec![],
            vec![Relation::new("legacy.Row", vec![])],
        ))
        .unwrap();
        tx.insert("legacy.Row", vec![]).unwrap();
        tx.commit().unwrap();
        assert!(db.session("owner").snapshot().scan("legacy.Row").is_err());
        let mut tx = db.admin().transaction();
        tx.set_schema_visibility("legacy.Row", Visibility::Private)
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            db.session("owner")
                .snapshot()
                .scan("legacy.Row")
                .unwrap()
                .len(),
            1
        );
        assert!(db.session("other").snapshot().scan("legacy.Row").is_err());
        assert!(
            db.session("owner")
                .transaction()
                .insert("legacy.Row", vec![])
                .is_err()
        );
    }
    #[test]
    fn pre_visibility_redo_and_checkpoint_preserve_grant_only_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.redb");
        let schema = Module::new(
            "legacy",
            1,
            vec![],
            vec![Relation::new(
                "legacy.Row",
                vec![Role::new("value", DataType::Int)],
            )],
        );
        {
            let db = Database::open(&path).unwrap();
            let mut tx = db.admin().transaction();
            // Emit the exact legacy Install operation, without the new separate policy rows.
            tx.install_inner(schema.clone()).unwrap();
            tx.insert("legacy.Row", vec![1.into()]).unwrap();
            tx.insert("legacy.Row", vec![2.into()]).unwrap();
            tx.grant(
                "reader",
                "legacy.Row",
                Grant {
                    filter: RowFilter::Equals {
                        role: "value".into(),
                        value: 1.into(),
                    },
                    ..Grant::read_only()
                },
            )
            .unwrap();
            tx.commit().unwrap();
        }
        for checkpoint in [false, true] {
            let db = Database::open(&path).unwrap();
            assert!(
                db.session("stranger")
                    .snapshot()
                    .scan("legacy.Row")
                    .is_err()
            );
            assert!(db.anonymous().snapshot().scan("legacy.Row").is_err());
            assert_eq!(
                db.session("reader")
                    .snapshot()
                    .scan("legacy.Row")
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                db.admin().snapshot().schema("legacy.Row").unwrap(),
                schema.relations[0]
            );
            let mut tx = db.admin().transaction();
            let manifest = SchemaManifest {
                format: 1,
                modules: vec![schema.clone()],
            };
            tx.install_manifest(&manifest).unwrap();
            tx.commit().unwrap();
            assert!(db.admin().snapshot().state.schema_visibility.is_empty());
            if !checkpoint {
                db.checkpoint().unwrap();
            }
        }
    }
}
