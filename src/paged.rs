//! Explicit disk-backed current-state engine. Resident `Database` remains unchanged.
//! Local principal sessions are a trusted embedding API, not remote authentication.
//!
//! Current rows, role indexes, uniqueness, incoming references, allocated IDs and
//! conflict timestamps live in one redb file. Opening compiles only the bounded
//! catalog. Each snapshot pins a database revision; retaining it can retain old
//! disk pages. Cache budgets do not bound caller-owned results or total RSS.
//!
//! DataFusion SQL, generated SDK records, local/TLS services, scoped credentials
//! and delegated capabilities share these paged snapshots. Bounded control-plane
//! metadata reuses the resident engine's policy compiler; application rows never
//! enter a resident State. Live authority fences protect reads and commits.
//! Bounded group commit, explicit offline paged-format import and trusted-host
//! replication share the durable transaction boundary. An explicit offline importer
//! also maps bounded resident checkpoints/redo; authenticated replication transport
//! remains a separate host integration.
use crate::{
    compiled::CompiledRelation,
    storage::{Op, State},
    *,
};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
mod authority;
mod commit;
mod constraints;
mod dense;
#[cfg(test)]
mod dense_tests;
mod encoding;
mod metrics;
pub use metrics::{
    PagedIoCounts, PagedIoMetrics, PagedReadCounts, PagedReadMetrics, PagedStorageBreakdown,
    PagedTableStorage,
};
mod packed_range;
mod packed_scan;
#[cfg(test)]
mod packed_scan_tests;
mod probe;
mod range;
mod service;
mod sql;
pub use sql::{PagedSqlQuery, PagedSqlSnapshot};
mod replication;
mod resident_import;
mod transfer;
mod verify;
use encoding::*;
pub use replication::{PagedReplica, PagedReplicationApply, PagedReplicationCursor};
pub use transfer::PagedImportStats;
pub use verify::PagedVerifyStats;
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("paged_rows_v1");
const INDEX: TableDefinition<&[u8], u8> = TableDefinition::new("paged_index_v1");
const UNIQUE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("paged_unique_v1");
const INCOMING: TableDefinition<&[u8], u8> = TableDefinition::new("paged_incoming_v1");
const IDS: TableDefinition<&[u8], u8> = TableDefinition::new("paged_ids_v1");
const VERS: TableDefinition<&[u8], u64> = TableDefinition::new("paged_versions_v1");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("liquid_paged_control_v1");
const ANON: &str = "\0anonymous";
const HOST: &str = "\0host";

/// Physical index layout is pinned per file. Old files retain their exact
/// layout; a new layout never requires rewriting existing application rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PagedFormat {
    V1,
    V2,
    V3,
}
impl PagedFormat {
    fn parse(bytes: &[u8]) -> Result<Self> {
        match bytes {
            b"LPG1" => Ok(Self::V1),
            b"LPG2" => Ok(Self::V2),
            b"LPG3" => Ok(Self::V3),
            _ => Err(err("unsupported paged format")),
        }
    }
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::V1 => b"LPG1",
            Self::V2 => b"LPG2",
            Self::V3 => b"LPG3",
        }
    }
    fn role_index(self, def: &CompiledRelation, slot: usize) -> bool {
        self == Self::V1
            || def.roles[slot].data_type.is_nullable()
            || !def
                .unique
                .iter()
                .any(|key| key.len() == 1 && key[0] == def.roles[slot].name)
    }
}
fn err(e: impl std::fmt::Display) -> Error {
    Error::Storage(format!("paged: {e}"))
}
fn lock_err<T>(_: std::sync::PoisonError<T>) -> Error {
    err("lock poisoned")
}
#[derive(Clone, Copy, Debug)]
pub struct PagedOptions {
    /// redb page-cache target in bytes; not a process RSS limit.
    pub cache_bytes: usize,
    /// Decoded-row cache accounting budget; keys include the pinned snapshot revision.
    /// Zero disables decoded caching.
    pub decoded_cache_bytes: usize,
    /// Admission budget for retained rows, dependencies and estimated index work.
    pub max_transaction_bytes: usize,
    /// Maximum distinct changed memberships in one transaction.
    pub max_transaction_rows: usize,
    /// Maximum returned rows in a cursor page.
    pub max_page_rows: usize,
    /// Maximum packed row bytes returned in a page, excluding caller overhead.
    pub max_page_bytes: usize,
    /// Maximum serialized catalog bytes; bounds resident schema/policy metadata.
    pub max_catalog_bytes: usize,
    /// Maximum packed row size on write and read.
    pub max_row_bytes: usize,
    /// Maximum OCC read dependencies and staged index-effect count.
    pub max_dependencies: usize,
    /// Maximum visited rows and reference edges per visibility traversal.
    pub max_visibility_rows: usize,
}
impl Default for PagedOptions {
    fn default() -> Self {
        Self {
            cache_bytes: 64 * 1024 * 1024,
            decoded_cache_bytes: 8 * 1024 * 1024,
            max_transaction_bytes: 8 * 1024 * 1024,
            max_transaction_rows: 4096,
            max_page_rows: 1024,
            max_page_bytes: 8 * 1024 * 1024,
            max_catalog_bytes: 8 * 1024 * 1024,
            max_row_bytes: 1024 * 1024,
            max_dependencies: 16384,
            max_visibility_rows: 4096,
        }
    }
}
#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CatalogData {
    /// Explicit metadata allocations; empty only for legacy native files.
    #[serde(default)]
    reserved: BTreeSet<Id>,
    #[serde(default)]
    control: authority::ControlData,
    modules: Vec<Module>,
    grants: Vec<(String, Id, Grant)>,
    visibility: BTreeMap<Id, (String, Visibility)>,
    /// Imported policy occurrence IDs; absent uses the original deterministic IDs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    visibility_ids: BTreeMap<Id, Id>,
    owners: BTreeMap<String, String>,
}
struct Catalog {
    data: CatalogData,
    state: State,
}
impl Catalog {
    fn compile(data: CatalogData) -> Result<Self> {
        let mut state = State::default();
        for module in &data.modules {
            if crate::visibility::protected(&module.name)
                || module
                    .relations
                    .iter()
                    .any(|r| authority::reserved_type(r.id))
            {
                return Err(err("unsupported system namespace"));
            }
            let owner = data
                .owners
                .get(&module.name)
                .ok_or_else(|| err("missing namespace owner"))?;
            if owner.is_empty() || (owner.starts_with('\0') && owner != HOST) {
                return Err(err("invalid namespace owner"));
            }
            state.validate_replay_order(&Op::Install(module.clone()))?;
            state.apply_recovered(&Op::Install(module.clone()), 0)?;
        }
        let mut seen = BTreeSet::new();
        for (principal, tid, grant) in &data.grants {
            if principal.is_empty() || principal.starts_with('\0') || !seen.insert((principal, tid))
            {
                return Err(err("invalid catalog grant"));
            }
            let def = state
                .relations
                .get(tid)
                .ok_or_else(|| err("unknown grant relation"))?;
            validate_grant(def, grant)?;
            state.apply(&Op::Grant(principal.clone(), *tid, Some(grant.clone())), 0);
        }
        for (tid, (owner, _)) in &data.visibility {
            if !state.relations.contains_key(tid)
                || owner.is_empty()
                || (owner.starts_with('\0') && owner != HOST)
            {
                return Err(err("invalid visibility owner"));
            }
        }
        if data.owners.len()
            != data
                .modules
                .iter()
                .map(|m| &m.name)
                .collect::<BTreeSet<_>>()
                .len()
        {
            return Err(err("orphan namespace owner"));
        }
        for (tid, (owner, _)) in &data.visibility {
            if data.owners.get(&state.origins[tid].0) != Some(owner) {
                return Err(err("visibility owner mismatch"));
            }
        }
        if data.visibility.len() != state.relations.len() {
            return Err(err("missing relation visibility"));
        }
        authority::compile_control(&data, &mut state)?;
        Ok(Self { data, state })
    }
}
fn validate_grant(def: &CompiledRelation, grant: &Grant) -> Result<()> {
    if let RowFilter::Equals { role, value } = &grant.filter {
        let slot = def.role(role)?;
        if !def.roles[slot].data_type.accepts(value) {
            return Err(Error::Schema("grant type mismatch".into()));
        }
    }
    Ok(())
}
struct View {
    format: PagedFormat,
    rows: dense::RowStore,
    index: redb::ReadOnlyTable<&'static [u8], u8>,
    unique: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    incoming: redb::ReadOnlyTable<&'static [u8], u8>,
    ids: redb::ReadOnlyTable<&'static [u8], u8>,
    versions: redb::ReadOnlyTable<&'static [u8], u64>,
    read: redb::ReadTransaction,
    catalog: Arc<Catalog>,
    revision: u64,
    auth_epoch: u64,
}
impl View {
    fn new(
        read: redb::ReadTransaction,
        catalog: Arc<Catalog>,
        revision: u64,
        auth_epoch: u64,
    ) -> Result<Self> {
        let format = {
            let meta = read.open_table(META).map_err(err)?;
            let format = meta
                .get("format")
                .map_err(err)?
                .ok_or_else(|| err("missing paged format"))?;
            PagedFormat::parse(format.value())?
        };
        Ok(Self {
            format,
            rows: dense::RowStore::open(&read, format == PagedFormat::V3)?,
            index: read.open_table(INDEX).map_err(err)?,
            unique: read.open_table(UNIQUE).map_err(err)?,
            incoming: read.open_table(INCOMING).map_err(err)?,
            ids: read.open_table(IDS).map_err(err)?,
            versions: read.open_table(VERS).map_err(err)?,
            read,
            catalog,
            revision,
            auth_epoch,
        })
    }
    /// Packed row revisions are authoritative while a membership exists. Only
    /// absent/deleted memberships need a separate OCC tombstone. Old files may
    /// retain redundant present-row entries; they never override the packed row.
    fn dependency_version(&self, key: &[u8]) -> Result<u64> {
        if key.first() == Some(&b'r') {
            if key.len() != 33 {
                return Err(err("malformed row dependency"));
            }
            if let Some(row) = self.rows.get(&key[1..]).map_err(err)? {
                let version = row_revision(row.value())?;
                if version == 0 || version > self.revision {
                    return Err(err("invalid row dependency revision"));
                }
                return Ok(version);
            }
        }
        let value = self.versions.get(key).map_err(err)?.map(|v| v.value());
        if value.is_some_and(|v| v == 0 || v > self.revision) {
            return Err(err("invalid dependency revision"));
        }
        Ok(value.unwrap_or(0))
    }
}
#[derive(Default)]
struct Cache {
    rows: BTreeMap<(Id, Id, u64), CachedRow>,
    queue: VecDeque<(Id, Id, u64)>,
    bytes: usize,
}
struct CachedRow {
    row: Arc<Row>,
    packed_len: usize,
    cost: usize,
}
struct Inner {
    db: redb::Database,
    head: RwLock<Arc<View>>,
    auth_epoch: AtomicU64,
    writer: Mutex<()>,
    commits: commit::Coordinator,
    failed: AtomicBool,
    read_only: AtomicBool,
    cache: Mutex<Cache>,
    options: PagedOptions,
    clock: Arc<crate::auth::Clock>,
}
impl Inner {
    /// Called only after persistence succeeds, under the serialized writer.
    /// Publish revocation before releasing the head lock: old readers can fail
    /// early, while new snapshots cannot observe a new view with an old fence.
    fn publish(&self, view: View) -> Result<()> {
        let next = Arc::new(view);
        let mut head = self.head.write().map_err(lock_err)?;
        self.auth_epoch.store(next.auth_epoch, Ordering::Release);
        let previous = std::mem::replace(&mut *head, next);
        drop(head);
        drop(previous);
        Ok(())
    }
}
/// Mutable current-state database whose row and index capacity is disk-backed.
#[derive(Clone)]
pub struct PagedDatabase {
    inner: Arc<Inner>,
}
/// Trusted local principal or host-administrator handle.
#[derive(Clone)]
pub struct PagedSession {
    db: PagedDatabase,
    principal: Option<String>,
    selection: authority::Selection,
}
/// Consistent pinned data revision, subject to current authorization fences.
#[derive(Clone)]
pub struct PagedSnapshot {
    db: PagedDatabase,
    view: Arc<View>,
    principal: Option<String>,
    selection: authority::Selection,
    scope: Option<Arc<crate::capability::CompiledCapability>>,
    deadline: Option<u64>,
    metrics: Option<Arc<metrics::ReadCounters>>,
}
#[derive(Debug)]
/// A bounded page in occurrence-ID order. Use its cursor with the same snapshot
/// and query. A final full page can have a cursor followed by an empty page.
pub struct PagedPage {
    pub rows: Vec<Row>,
    /// Resume exclusively after this ID; `None` marks the end.
    pub next: Option<Id>,
}
#[derive(Clone, Copy, Debug)]
pub struct PagedStats {
    pub revision: u64,
    pub decoded_cache_bytes: usize,
    pub decoded_cache_entries: usize,
    pub configured_cache_bytes: usize,
}
/// Optimistic transaction with bounded private changes and on-disk dependencies.
/// Any failed operation poisons the transaction; commit then returns `Aborted`.
pub struct PagedTransaction {
    #[cfg(test)]
    before_persist_test_hook: Option<Box<dyn FnOnce() + Send>>,
    snapshot: PagedSnapshot,
    catalog: Arc<Catalog>,
    rows: BTreeMap<(Id, Id), Option<Row>>,
    staged_unique: BTreeMap<Vec<u8>, Id>,
    identities: BTreeSet<Id>,
    reads: BTreeMap<Vec<u8>, u64>,
    bytes: usize,
    effects: usize,
    failed: bool,
    catalog_changed: bool,
    auth_changed: bool,
    defer_foreign_keys: bool,
    constraint_work: usize,
}
impl PagedDatabase {
    pub fn open(path: impl AsRef<Path>, options: PagedOptions) -> Result<Self> {
        Self::open_with_factory(options, |builder| builder.create(path).map_err(err))
    }
    fn open_with_factory(
        options: PagedOptions,
        create: impl FnOnce(&redb::Builder) -> Result<redb::Database>,
    ) -> Result<Self> {
        if options.cache_bytes < 1024 * 1024
            || options.max_transaction_bytes == 0
            || options.max_transaction_rows == 0
            || options.max_page_rows == 0
            || options.max_page_bytes == 0
            || options.max_catalog_bytes == 0
            || options.max_row_bytes == 0
            || options.max_dependencies == 0
            || options.max_visibility_rows == 0
        {
            return Err(Error::Constraint("invalid paged limits".into()));
        }
        let mut builder = redb::Database::builder();
        builder.set_cache_size(options.cache_bytes);
        let db = create(&builder)?;
        let existing = db
            .begin_read()
            .map_err(err)?
            .list_tables()
            .map_err(err)?
            .map(|t| t.name().to_owned())
            .collect::<BTreeSet<_>>();
        let expected = [
            "paged_rows_v1",
            "paged_index_v1",
            "paged_unique_v1",
            "paged_incoming_v1",
            "paged_ids_v1",
            "paged_versions_v1",
            "liquid_paged_control_v1",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
        let mut replicated = expected.clone();
        replicated.insert("paged_replication_log_v1".into());
        replicated.insert("paged_replication_control_v1".into());
        if !existing.is_empty() && existing != expected && existing != replicated {
            return Err(err(
                "not a complete paged database; explicit import required",
            ));
        }
        if db
            .begin_read()
            .map_err(err)?
            .list_multimap_tables()
            .map_err(err)?
            .next()
            .is_some()
        {
            return Err(err("unexpected multimap tables; explicit import required"));
        }
        let fresh = existing.is_empty();
        if fresh {
            let tx = db.begin_write().map_err(err)?;
            tx.open_table(ROWS).map_err(err)?;
            tx.open_table(INDEX).map_err(err)?;
            tx.open_table(UNIQUE).map_err(err)?;
            tx.open_table(INCOMING).map_err(err)?;
            tx.open_table(IDS).map_err(err)?;
            tx.open_table(VERS).map_err(err)?;
            {
                let mut meta = tx.open_table(META).map_err(err)?;
                if fresh {
                    meta.insert("format", PagedFormat::V3.bytes())
                        .map_err(err)?;
                    meta.insert(
                        "catalog",
                        serde_json::to_vec(&CatalogData::default())
                            .map_err(err)?
                            .as_slice(),
                    )
                    .map_err(err)?;
                    meta.insert("revision", 0u64.to_le_bytes().as_slice())
                        .map_err(err)?;
                    meta.insert("auth_epoch", 0u64.to_le_bytes().as_slice())
                        .map_err(err)?;
                }
            }
            tx.commit().map_err(err)?;
        }
        let read = db.begin_read().map_err(err)?;
        let (catalog, revision, auth_epoch) = {
            let meta = read.open_table(META).map_err(err)?;
            PagedFormat::parse(
                meta.get("format")
                    .map_err(err)?
                    .ok_or_else(|| err("missing paged format"))?
                    .value(),
            )?;
            let bytes = meta
                .get("catalog")
                .map_err(err)?
                .ok_or_else(|| err("missing catalog"))?;
            if bytes.value().len() > options.max_catalog_bytes {
                return Err(err("catalog exceeds configured limit"));
            }
            let catalog = Catalog::compile(serde_json::from_slice(bytes.value()).map_err(err)?)?;
            (
                Arc::new(catalog),
                read_u64(&meta, "revision")?,
                read_u64(&meta, "auth_epoch")?,
            )
        };
        if auth_epoch > revision {
            return Err(err("authorization epoch exceeds durable head"));
        }
        let view = Arc::new(View::new(read, catalog, revision, auth_epoch)?);
        authority::validate_disk_reservations(&view)?;
        let read_only = replication::validate(&view)?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                head: RwLock::new(view),
                auth_epoch: AtomicU64::new(auth_epoch),
                writer: Mutex::new(()),
                commits: commit::Coordinator::new(GroupCommitOptions {
                    collection_window: std::time::Duration::ZERO,
                    ..Default::default()
                }),
                failed: AtomicBool::new(false),
                read_only: AtomicBool::new(read_only),
                cache: Mutex::new(Cache::default()),
                options,
                clock: Arc::new(crate::auth::Clock::default()),
            }),
        })
    }
    pub fn admin(&self) -> PagedSession {
        PagedSession {
            db: self.clone(),
            principal: None,
            selection: authority::Selection::default(),
        }
    }
    /// Trusted local embedding API. This does not authenticate network credentials.
    pub fn session(&self, principal: &str) -> Result<PagedSession> {
        if principal.is_empty() || principal.starts_with('\0') {
            return Err(Error::Denied);
        }
        Ok(PagedSession {
            db: self.clone(),
            principal: Some(principal.into()),
            selection: authority::Selection::default(),
        })
    }
    pub fn anonymous(&self) -> PagedSession {
        PagedSession {
            db: self.clone(),
            principal: Some(ANON.into()),
            selection: authority::Selection::default(),
        }
    }
    pub fn stats(&self) -> Result<PagedStats> {
        let cache = self.inner.cache.lock().map_err(lock_err)?;
        Ok(PagedStats {
            revision: self.inner.head.read().map_err(lock_err)?.revision,
            decoded_cache_bytes: cache.bytes,
            decoded_cache_entries: cache.rows.len(),
            configured_cache_bytes: self.inner.options.cache_bytes,
        })
    }
}
fn read_u64(meta: &impl ReadableTable<&'static str, &'static [u8]>, key: &str) -> Result<u64> {
    Ok(u64::from_le_bytes(
        meta.get(key)
            .map_err(err)?
            .ok_or_else(|| err("missing head"))?
            .value()
            .try_into()
            .map_err(err)?,
    ))
}
impl PagedSession {
    pub fn snapshot(&self) -> Result<PagedSnapshot> {
        if self.db.inner.failed.load(Ordering::Acquire) || self.db.inner.writer.is_poisoned() {
            return Err(err("reopen after uncertain persistence"));
        }
        let view = self.db.inner.head.read().map_err(lock_err)?.clone();
        let (scope, deadline) = authority::resolve_selection(
            &view.catalog.state,
            self.principal.as_deref(),
            self.selection,
            self.db.inner.clock.now(),
        )?;
        let snapshot = PagedSnapshot {
            db: self.db.clone(),
            view,
            principal: self.principal.clone(),
            selection: self.selection,
            scope,
            deadline,
            metrics: None,
        };
        snapshot.fence()?;
        Ok(snapshot)
    }
    pub fn transaction(&self) -> Result<PagedTransaction> {
        if self.db.inner.read_only.load(Ordering::Acquire) {
            return Err(Error::Denied);
        }
        self.build_transaction()
    }
    fn build_transaction(&self) -> Result<PagedTransaction> {
        let snapshot = self.snapshot()?;
        let mut tx = PagedTransaction {
            #[cfg(test)]
            before_persist_test_hook: None,
            catalog: snapshot.view.catalog.clone(),
            snapshot,
            rows: BTreeMap::new(),
            staged_unique: BTreeMap::new(),
            identities: BTreeSet::new(),
            reads: BTreeMap::new(),
            bytes: 0,
            effects: 0,
            failed: false,
            catalog_changed: false,
            auth_changed: false,
            defer_foreign_keys: false,
            constraint_work: 0,
        };
        tx.track(b"catalog".to_vec())?;
        tx.track(b"auth".to_vec())?;
        Ok(tx)
    }
    pub fn get(&self, name: &str, id: Id) -> Result<Option<Row>> {
        self.snapshot()?.get(name, id)
    }
}
impl PagedSnapshot {
    /// Stream validated, authorized rows without constructing result pages.
    /// Initial root decoding bypasses the cache; complex reference closures retain
    /// the existing bounded visibility/cache path. The visitor must not retain borrowed rows;
    /// memory is fixed row scratch plus the bounded visibility traversal. Replacing
    /// variable fields can temporarily retain old and new row payloads.
    /// Authority remains snapshot-bound and is fenced before and after the scan.
    fn visit_authorized_rows(
        &self,
        name: &str,
        check: &mut impl FnMut() -> Result<()>,
        visitor: &mut impl FnMut(&Row) -> Result<()>,
    ) -> Result<()> {
        check()?;
        self.fence()?;
        let def = self.def(name)?;
        if !discover(
            &self.view.catalog,
            self.principal.as_deref(),
            self.scope.as_deref(),
            def.id,
        ) {
            return Err(Error::Denied);
        }
        let policy = self.scan_read_policy(def.id);
        // One edge into a scalar family needs no graph allocation or root
        // reread. Still read and validate the target: unrestricted visibility is
        // not a reason to hide a dangling or corrupt typed reference.
        let terminal = if let [(slot, name)] = def.reference_slots.as_slice()
            && !def.roles[*slot].data_type.is_nullable()
        {
            let tid = self.view.catalog.state.names[name];
            let target = &self.view.catalog.state.relations[&tid];
            target
                .reference_slots
                .is_empty()
                .then(|| (*slot, target.clone(), self.scan_read_policy(tid)))
        } else {
            None
        };
        let options = self.db.inner.options;
        let mut scanned = 0usize;
        let mut examined = 0usize;
        let mut last_accepted_examined = 0usize;
        let mut page_rows = 0usize;
        let mut page_bytes = 0usize;
        let mut terminal_cursor = dense::ScanCursor::default();
        let mut root_scratch = None;
        let mut target_scratch = None;
        self.view
            .rows
            .visit_family(def.id.0.as_bytes(), &mut |item| {
                check()?;
                if scanned.is_multiple_of(256) {
                    self.fence()?;
                }
                scanned = scanned.wrapping_add(1);
                examined += 1;
                if examined > options.max_page_rows.saturating_mul(64) {
                    return Err(Error::Query("page visibility work budget exceeded".into()));
                }
                let (key, packed) = item?;
                if key.len() != 32 {
                    return Err(err("malformed row key"));
                }
                let id = Id(uuid::Uuid::from_slice(&key[16..]).map_err(err)?);
                self.count(metrics::ReadCounter::RootScanRow);
                let row = self.decode_scan_row(&def, id, packed, &mut root_scratch)?;
                let visible = if def.reference_slots.is_empty() {
                    policy.allows(row)
                } else if let Some((slot, target, target_policy)) = &terminal {
                    let bound = self.db.inner.options.max_visibility_rows;
                    if bound == 0 {
                        return Err(Error::Query("reference visibility budget exceeded".into()));
                    }
                    if !policy.allows(row) {
                        false
                    } else {
                        check()?;
                        if bound < 2 {
                            return Err(Error::Query(
                                "reference visibility budget exceeded".into(),
                            ));
                        }
                        let Value::Ref(target_id) = row.values[*slot] else {
                            return Err(err("invalid typed reference"));
                        };
                        self.count(metrics::ReadCounter::ReferenceCheck);
                        let packed = terminal_cursor.get(&self.view.rows, target.id, target_id)?;
                        match target_policy {
                            ScanReadPolicy::Filter(_) => {
                                let target_row = self.decode_scan_row(
                                    target,
                                    target_id,
                                    packed,
                                    &mut target_scratch,
                                )?;
                                target_policy.allows(target_row)
                            }
                            ScanReadPolicy::All | ScanReadPolicy::Denied => {
                                // Full validation still detects malformed referenced
                                // rows, but an unconditional policy needs no owned
                                // copy of their strings/blobs/values.
                                self.validate_uncached(target, packed)?;
                                matches!(target_policy, ScanReadPolicy::All)
                            }
                        }
                    }
                } else {
                    self.visible_checked(def.id, row.id, check)?
                };
                if visible {
                    // Retain the ordinary SQL cursor's row/page work limits even
                    // though this visitor does not allocate a page or clone rows.
                    let size = packed.len();
                    if size > options.max_page_bytes {
                        return Err(Error::Query("row exceeds page byte bound".into()));
                    }
                    if page_bytes > options.max_page_bytes - size {
                        page_rows = 0;
                        page_bytes = 0;
                        // A real page restarts after its last returned row, so
                        // denied candidates since that row are examined again.
                        examined -= last_accepted_examined;
                    }
                    visitor(row)?;
                    last_accepted_examined = examined;
                    page_rows += 1;
                    page_bytes += size;
                    if page_rows == 256.min(options.max_page_rows) {
                        page_rows = 0;
                        page_bytes = 0;
                        examined = 0;
                        last_accepted_examined = 0;
                    }
                }
                Ok(())
            })?;
        check()?;
        self.fence()
    }

    fn decode_uncached(&self, def: &CompiledRelation, id: Id, bytes: &[u8]) -> Result<Row> {
        self.count(metrics::ReadCounter::DecodedRow);
        if bytes.len() > self.db.inner.options.max_row_bytes {
            return Err(err("row exceeds configured bound"));
        }
        let (revision, row) = decode_row(def, id, bytes)?;
        if revision == 0 || revision > self.view.revision {
            return Err(err("row revision is outside snapshot history"));
        }
        Ok(row)
    }

    fn decode_scan_row<'a>(
        &self,
        def: &CompiledRelation,
        id: Id,
        bytes: &[u8],
        scratch: &'a mut Option<Row>,
    ) -> Result<&'a Row> {
        if bytes.len() > self.db.inner.options.max_row_bytes {
            return Err(err("row exceeds configured bound"));
        }
        if let Some(row) = scratch {
            self.count(metrics::ReadCounter::DecodedRow);
            let revision = decode_row_into(def, id, bytes, row)?;
            if revision == 0 || revision > self.view.revision {
                return Err(err("row revision is outside snapshot history"));
            }
        } else {
            *scratch = Some(self.decode_uncached(def, id, bytes)?);
        }
        Ok(scratch.as_ref().expect("decoded scan scratch row"))
    }

    fn validate_uncached(&self, def: &CompiledRelation, bytes: &[u8]) -> Result<()> {
        self.count(metrics::ReadCounter::ValidatedPackedRow);
        if bytes.len() > self.db.inner.options.max_row_bytes {
            return Err(err("row exceeds configured bound"));
        }
        let revision = validate_row(def, bytes)?;
        if revision == 0 || revision > self.view.revision {
            return Err(err("row revision is outside snapshot history"));
        }
        Ok(())
    }

    fn scan_read_policy(&self, tid: Id) -> ScanReadPolicy {
        // Scoped credentials attenuate even public/owned schemas; do not let
        // schema visibility override the credential's compiled row predicate.
        if let Some(scope) = &self.scope {
            return ScanReadPolicy::grant(scope.grants.get(&tid));
        }
        let Some(principal) = self.principal.as_deref() else {
            return ScanReadPolicy::All;
        };
        let Some((owner, visibility)) = self.view.catalog.data.visibility.get(&tid) else {
            return ScanReadPolicy::Denied;
        };
        if principal == owner
            || *visibility == Visibility::Public
            || (*visibility == Visibility::Authenticated && principal != ANON)
        {
            ScanReadPolicy::All
        } else if principal == ANON {
            ScanReadPolicy::Denied
        } else {
            ScanReadPolicy::grant(self.view.catalog.state.grant(principal, tid))
        }
    }

    pub fn revision(&self) -> u64 {
        self.view.revision
    }
    pub(crate) fn fence(&self) -> Result<()> {
        if self.deadline.is_some() {
            crate::auth::check_deadline(self.deadline, self.db.inner.clock.now())?;
        }
        if self.db.inner.failed.load(Ordering::Acquire)
            || self.db.inner.writer.is_poisoned()
            || self.db.inner.head.is_poisoned()
        {
            return Err(err("reopen after uncertain persistence"));
        }
        if self.principal.is_some()
            && self.db.inner.auth_epoch.load(Ordering::Acquire) != self.view.auth_epoch
        {
            return Err(Error::Denied);
        }
        Ok(())
    }
    fn def(&self, name: &str) -> Result<Arc<CompiledRelation>> {
        self.view.catalog.state.relation(name).map_err(|e| {
            if self.principal.is_some() {
                Error::Denied
            } else {
                e
            }
        })
    }
    fn raw(&self, tid: Id, id: Id) -> Result<Option<Arc<Row>>> {
        self.raw_sized(tid, id, None)
            .map(|result| result.map(|(row, _)| row))
    }
    fn raw_sized(
        &self,
        tid: Id,
        id: Id,
        def: Option<&CompiledRelation>,
    ) -> Result<Option<(Arc<Row>, usize)>> {
        // A revision identifies the entire immutable redb read transaction.
        // Cache raw data only; every caller still applies its own live authority.
        let cache_key = (tid, id, self.view.revision);
        if let Some(cached) = self
            .db
            .inner
            .cache
            .lock()
            .map_err(lock_err)?
            .rows
            .get(&cache_key)
        {
            self.count(metrics::ReadCounter::DecodedCacheHit);
            return Ok(Some((cached.row.clone(), cached.packed_len)));
        }
        self.count(metrics::ReadCounter::DecodedCacheMiss);
        let table = &self.view.rows;
        let key = row_key_array(tid, id);
        self.count(metrics::ReadCounter::RowTableGet);
        let Some(bytes) = table.get(key.as_slice()).map_err(err)? else {
            return Ok(None);
        };
        self.decode_cached_sized(tid, id, bytes.value(), def)
    }
    fn decode_cached(&self, tid: Id, id: Id, bytes: &[u8]) -> Result<Option<Arc<Row>>> {
        self.decode_cached_sized(tid, id, bytes, None)
            .map(|result| result.map(|(row, _)| row))
    }
    fn decode_cached_sized(
        &self,
        tid: Id,
        id: Id,
        bytes: &[u8],
        compiled: Option<&CompiledRelation>,
    ) -> Result<Option<(Arc<Row>, usize)>> {
        self.count(metrics::ReadCounter::DecodedRow);
        if bytes.len() > self.db.inner.options.max_row_bytes {
            return Err(err("row exceeds configured bound"));
        }
        let revision = row_revision(bytes)?;
        if revision == 0 || revision > self.view.revision {
            return Err(err("row revision is outside snapshot history"));
        }
        let def = match compiled {
            Some(def) => {
                debug_assert_eq!(def.id, tid);
                def
            }
            None => self
                .view
                .catalog
                .state
                .relations
                .get(&tid)
                .ok_or_else(|| err("unknown row type"))?,
        };
        let (_, row) = decode_row(def, id, bytes)?;
        let cost = 128
            + std::mem::size_of::<usize>()
            + row.values.len() * std::mem::size_of::<Value>()
            + row
                .values
                .iter()
                .map(|v| match v {
                    Value::Text(v) => v.capacity(),
                    Value::Bytes(v) => v.capacity(),
                    _ => 0,
                })
                .sum::<usize>();
        let row = Arc::new(row);
        let budget = self.db.inner.options.decoded_cache_bytes;
        if cost <= budget {
            let mut cache = self.db.inner.cache.lock().map_err(lock_err)?;
            if !cache.rows.contains_key(&(tid, id, self.view.revision)) {
                while cache.bytes > budget - cost {
                    if let Some(key) = cache.queue.pop_front() {
                        if let Some(cached) = cache.rows.remove(&key) {
                            self.count(metrics::ReadCounter::DecodedCacheEviction);
                            cache.bytes -= cached.cost;
                        }
                    } else {
                        break;
                    }
                }
                cache.bytes += cost;
                cache.queue.push_back((tid, id, self.view.revision));
                cache.rows.insert(
                    (tid, id, self.view.revision),
                    CachedRow {
                        row: row.clone(),
                        packed_len: bytes.len(),
                        cost,
                    },
                );
            }
        }
        Ok(Some((row, bytes.len())))
    }
    fn visible_checked(
        &self,
        tid: Id,
        id: Id,
        check: &mut impl FnMut() -> Result<()>,
    ) -> Result<bool> {
        let root = (tid, id);
        let mut traversed = 0usize;
        let mut stack = vec![(tid, id)];
        let mut seen = BTreeSet::new();
        while let Some((tid, id)) = stack.pop() {
            check()?;
            if !seen.insert((tid, id)) {
                continue;
            }
            self.count(metrics::ReadCounter::VisibilityNode);
            if seen.len() > self.db.inner.options.max_visibility_rows {
                return Err(Error::Query("reference visibility budget exceeded".into()));
            }
            let Some(row) = self.raw(tid, id)? else {
                return if (tid, id) == root {
                    Ok(false)
                } else {
                    Err(err("dangling typed reference"))
                };
            };
            if !allowed(
                &self.view.catalog,
                self.principal.as_deref(),
                self.scope.as_deref(),
                tid,
                &row,
                Permission::Read,
            ) {
                return Ok(false);
            }
            for (slot, target) in &self.view.catalog.state.relations[&tid].reference_slots {
                if let Value::Ref(id) = row.values[*slot] {
                    self.count(metrics::ReadCounter::ReferenceCheck);
                    traversed += 1;
                    if traversed > self.db.inner.options.max_visibility_rows {
                        return Err(Error::Query(
                            "reference visibility edge budget exceeded".into(),
                        ));
                    }
                    stack.push((self.view.catalog.state.names[target], id));
                }
            }
        }
        Ok(true)
    }
    pub fn get(&self, name: &str, id: Id) -> Result<Option<Row>> {
        self.get_checked(name, id, &mut || Ok(()))
    }
    fn get_checked(
        &self,
        name: &str,
        id: Id,
        check: &mut impl FnMut() -> Result<()>,
    ) -> Result<Option<Row>> {
        check()?;
        self.fence()?;
        let def = self.def(name)?;
        if !discover(
            &self.view.catalog,
            self.principal.as_deref(),
            self.scope.as_deref(),
            def.id,
        ) {
            return Err(Error::Denied);
        }
        let row = if def.reference_slots.is_empty() {
            self.raw(def.id, id)?
                .filter(|row| {
                    allowed(
                        &self.view.catalog,
                        self.principal.as_deref(),
                        self.scope.as_deref(),
                        def.id,
                        row,
                        Permission::Read,
                    )
                })
                .map(|row| (*row).clone())
        } else if self.visible_checked(def.id, id, check)? {
            self.raw(def.id, id)?.map(|r| (*r).clone())
        } else {
            None
        };
        check()?;
        self.fence()?;
        Ok(row)
    }
    pub fn schemas(&self) -> Result<Vec<Relation>> {
        self.fence()?;
        let rows = self
            .view
            .catalog
            .state
            .relations
            .values()
            .filter(|r| {
                !crate::visibility::protected(r.name.split('.').next().unwrap_or(""))
                    && discover(
                        &self.view.catalog,
                        self.principal.as_deref(),
                        self.scope.as_deref(),
                        r.id,
                    )
                    && r.reference_slots.iter().all(|(_, target)| {
                        discover(
                            &self.view.catalog,
                            self.principal.as_deref(),
                            self.scope.as_deref(),
                            self.view.catalog.state.names[target],
                        )
                    })
            })
            .map(|r| r.declaration.clone())
            .collect();
        self.fence()?;
        Ok(rows)
    }
    pub fn scan_page(&self, name: &str, after: Option<Id>, limit: usize) -> Result<PagedPage> {
        self.page(name, None, after, limit)
    }
    pub fn lookup_page(
        &self,
        name: &str,
        role: &str,
        value: &Value,
        after: Option<Id>,
        limit: usize,
    ) -> Result<PagedPage> {
        self.page(name, Some((role, value)), after, limit)
    }
    fn page(
        &self,
        name: &str,
        filter: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<PagedPage> {
        self.page_checked(name, filter, after, limit, &mut || Ok(()))
    }
    fn page_checked(
        &self,
        name: &str,
        filter: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
    ) -> Result<PagedPage> {
        check()?;
        self.fence()?;
        let options = self.db.inner.options;
        if limit == 0 || limit > options.max_page_rows {
            return Err(Error::Query("page row bound exceeded".into()));
        }
        let def = self.def(name)?;
        if !discover(
            &self.view.catalog,
            self.principal.as_deref(),
            self.scope.as_deref(),
            def.id,
        ) {
            return Err(Error::Denied);
        }
        let slot = filter.map(|(role, _)| def.role(role)).transpose()?;
        let prefix = if let Some((_, value)) = filter {
            let slot = slot.expect("filter has compiled slot");
            if !def.roles[slot].data_type.accepts(value) {
                return Err(Error::Query("index value type mismatch".into()));
            }
            index_prefix(def.id, slot, value)
        } else {
            def.id.0.as_bytes().to_vec()
        };
        let mut start = prefix.clone();
        if let Some(id) = after {
            start.extend(id.0.as_bytes());
        }
        let end = prefix_end(&prefix);
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut examined = 0usize;
        let mut next = None;
        let mut consume = |key: &[u8], packed: Option<&[u8]>| -> Result<bool> {
            check()?;
            if key.len() != prefix.len() + 16 {
                return Err(err("malformed index key"));
            }
            let id = Id(uuid::Uuid::from_slice(
                key.get(
                    key.len()
                        .checked_sub(16)
                        .ok_or_else(|| err("short index key"))?..,
                )
                .ok_or_else(|| err("short index key"))?,
            )
            .map_err(err)?);
            if after.is_some_and(|a| id <= a) {
                return Ok(true);
            }
            examined += 1;
            if examined > options.max_page_rows.saturating_mul(64) {
                return Err(Error::Query("page visibility work budget exceeded".into()));
            }
            let row = match packed {
                Some(packed) => self.decode_cached(def.id, id, packed)?,
                None => self.raw(def.id, id)?,
            }
            .ok_or_else(|| err("dangling index"))?;
            if let Some((_, value)) = filter
                && &row.values[slot.expect("filter has compiled slot")] != value
            {
                return Err(err("index value mismatch"));
            }
            let visible = if def.reference_slots.is_empty() {
                allowed(
                    &self.view.catalog,
                    self.principal.as_deref(),
                    self.scope.as_deref(),
                    def.id,
                    &row,
                    Permission::Read,
                )
            } else {
                self.visible_checked(def.id, id, check)?
            };
            if !visible {
                return Ok(true);
            }
            let size = row_encoded_len(&def, &row)?;
            if size > options.max_page_bytes {
                return Err(Error::Query("row exceeds page byte bound".into()));
            }
            if bytes > options.max_page_bytes - size {
                next = rows.last().map(|r: &Row| r.id);
                return Ok(false);
            }
            bytes += size;
            rows.push((*row).clone());
            if rows.len() == limit {
                next = Some(id);
                return Ok(false);
            }
            Ok(true)
        };
        let unique_filter = filter
            .filter(|(_, value)| !matches!(value, Value::Null))
            .and_then(|(role, value)| {
                def.unique
                    .iter()
                    .position(|roles| roles.len() == 1 && roles[0] == role)
                    .map(|constraint| (constraint, value))
            });
        if let Some((constraint, value)) = unique_filter {
            let key = unique_key(def.id, constraint, std::slice::from_ref(value));
            if let Some(id) = self.view.unique.get(key.as_slice()).map_err(err)? {
                let id = Id(uuid::Uuid::from_slice(id.value()).map_err(err)?);
                let mut key = prefix.clone();
                key.extend_from_slice(id.0.as_bytes());
                consume(&key, None)?;
            }
        } else if filter.is_some() {
            let table = &self.view.index;
            for item in table
                .range::<&[u8]>((
                    std::ops::Bound::Included(start.as_slice()),
                    end.as_deref()
                        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                ))
                .map_err(err)?
            {
                let (k, _) = item.map_err(err)?;
                if !consume(k.value(), None)? {
                    break;
                }
            }
        } else {
            let table = &self.view.rows;
            for item in table
                .range::<&[u8]>((
                    std::ops::Bound::Included(start.as_slice()),
                    end.as_deref()
                        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                ))
                .map_err(err)?
            {
                let (k, packed) = item.map_err(err)?;
                if !consume(k.value(), Some(packed.value()))? {
                    break;
                }
            }
        }
        check()?;
        self.fence()?;
        Ok(PagedPage { rows, next })
    }
}
enum ScanReadPolicy {
    All,
    Filter(crate::compiled::CompiledGrant),
    Denied,
}

type PackedCursorEntry = (dense::RowKeyGuard, dense::RowValueGuard);

/// One retained target page/cursor for one family, never a target-sized map.
/// Nearby monotonic references advance sequentially in either direction;
/// direction changes or large gaps seek through the index after at most eight
/// entries. Unrelated skipped rows are not decoded.
#[derive(Default)]
struct TerminalCursor {
    range: Option<dense::RowRange>,
    current: Option<PackedCursorEntry>,
    reverse: bool,
    #[cfg(test)]
    seek_count: usize,
}
impl TerminalCursor {
    fn seek(&mut self, table: &dense::RowStore, tid: Id, key: &[u8], reverse: bool) -> Result<()> {
        let end = prefix_end(tid.0.as_bytes());
        let bounds = if reverse {
            (
                std::ops::Bound::Included(tid.0.as_bytes().as_slice()),
                std::ops::Bound::Included(key),
            )
        } else {
            (
                std::ops::Bound::Included(key),
                end.as_deref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            )
        };
        let mut range = table.range::<&[u8]>(bounds).map_err(err)?;
        self.current = if reverse {
            range.next_back()
        } else {
            range.next()
        }
        .transpose()
        .map_err(err)?;
        self.range = Some(range);
        self.reverse = reverse;
        #[cfg(test)]
        {
            self.seek_count += 1;
        }
        Ok(())
    }
    fn before_target(&self, key: &[u8]) -> bool {
        self.current.as_ref().is_some_and(|(current, _)| {
            if self.reverse {
                current.value() > key
            } else {
                current.value() < key
            }
        })
    }
    fn get(&mut self, table: &dense::RowStore, tid: Id, id: Id) -> Result<&[u8]> {
        self.get_with_missing(table, tid, id, "dangling typed reference")
    }
    fn get_with_missing(
        &mut self,
        table: &dense::RowStore,
        tid: Id,
        id: Id,
        missing: &str,
    ) -> Result<&[u8]> {
        let mut key = [0; 32];
        key[..16].copy_from_slice(tid.0.as_bytes());
        key[16..].copy_from_slice(id.0.as_bytes());
        let reverse = self.current.as_ref().map_or(self.reverse, |(current, _)| {
            match current.value().cmp(key.as_slice()) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => self.reverse,
            }
        });
        if self.current.is_none() || self.reverse != reverse {
            self.seek(table, tid, &key, reverse)?;
        } else {
            for _ in 0..8 {
                if !self.before_target(&key) {
                    break;
                }
                let range = self
                    .range
                    .as_mut()
                    .ok_or_else(|| err("missing reference cursor"))?;
                self.current = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                }
                .transpose()
                .map_err(err)?;
            }
            if self.before_target(&key) {
                self.seek(table, tid, &key, reverse)?;
            }
        }
        let (current, value) = self
            .current
            .as_ref()
            .filter(|(current, _)| current.value() == key.as_slice())
            .ok_or_else(|| err(missing))?;
        debug_assert_eq!(current.value().len(), 32);
        Ok(value.value())
    }
}
impl ScanReadPolicy {
    fn grant(grant: Option<&crate::compiled::CompiledGrant>) -> Self {
        match grant {
            Some(grant) if grant.read => match grant.filter {
                RowFilter::All => Self::All,
                RowFilter::Equals { .. } => Self::Filter(grant.clone()),
            },
            _ => Self::Denied,
        }
    }
    fn allows(&self, row: &Row) -> bool {
        match self {
            Self::All => true,
            Self::Filter(grant) => grant.allows(row),
            Self::Denied => false,
        }
    }
}
#[derive(Clone, Copy)]
enum Permission {
    Read,
    Write,
    Reference,
}
fn discover(
    catalog: &Catalog,
    principal: Option<&str>,
    scope: Option<&crate::capability::CompiledCapability>,
    tid: Id,
) -> bool {
    if let Some(scope) = scope {
        return scope.grants.get(&tid).is_some_and(|g| g.read);
    }
    let Some(principal) = principal else {
        return true;
    };
    let Some((owner, visibility)) = catalog.data.visibility.get(&tid) else {
        return false;
    };
    owner == principal
        || *visibility == Visibility::Public
        || (*visibility == Visibility::Authenticated && principal != ANON)
        || catalog.state.grant(principal, tid).is_some_and(|g| g.read)
}
fn allowed(
    catalog: &Catalog,
    principal: Option<&str>,
    scope: Option<&crate::capability::CompiledCapability>,
    tid: Id,
    row: &Row,
    permission: Permission,
) -> bool {
    if let Some(scope) = scope {
        return scope.grants.get(&tid).is_some_and(|g| {
            (match permission {
                Permission::Read => g.read,
                Permission::Write => g.write,
                Permission::Reference => g.reference,
            }) && g.allows(row)
        });
    }
    let Some(principal) = principal else {
        return true;
    };
    let Some((owner, visibility)) = catalog.data.visibility.get(&tid) else {
        return false;
    };
    if principal == owner {
        return true;
    }
    if matches!(permission, Permission::Read)
        && (*visibility == Visibility::Public
            || (*visibility == Visibility::Authenticated && principal != ANON))
    {
        return true;
    }
    if principal == ANON {
        return false;
    }
    catalog.state.grant(principal,tid).is_some_and(|g|match permission {Permission::Read=>g.read,Permission::Write=>g.write,Permission::Reference=>g.reference} && g.allows(row))
}
fn tagged(tag: u8, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len().saturating_add(1));
    out.push(tag);
    out.extend_from_slice(key);
    out
}
impl PagedTransaction {
    fn mutate<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let out = self
            .snapshot
            .fence()
            .and_then(|_| f(self))
            .and_then(|value| {
                self.snapshot.fence()?;
                Ok(value)
            });
        if out.is_err() {
            self.failed = true
        }
        out
    }
    fn charge(&mut self, n: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(n)
            .ok_or_else(|| Error::Constraint("transaction byte overflow".into()))?;
        if self.bytes > self.snapshot.db.inner.options.max_transaction_bytes {
            return Err(Error::Constraint("transaction byte budget exceeded".into()));
        }
        Ok(())
    }
    fn track(&mut self, key: Vec<u8>) -> Result<()> {
        if !self.reads.contains_key(&key) {
            if self.reads.len() >= self.snapshot.db.inner.options.max_dependencies {
                return Err(Error::Constraint(
                    "transaction dependency budget exceeded".into(),
                ));
            }
            self.charge(key.len() + 32)?;
            let version = self.snapshot.view.dependency_version(&key)?;
            self.reads.insert(key, version);
        }
        Ok(())
    }
    fn raw(&mut self, tid: Id, id: Id) -> Result<Option<Arc<Row>>> {
        self.track(tagged(b'r', &row_key(tid, id)))?;
        if let Some(row) = self.rows.get(&(tid, id)) {
            return Ok(row.clone().map(Arc::new));
        }
        self.snapshot.raw(tid, id)
    }
    fn def(&self, name: &str) -> Result<Arc<CompiledRelation>> {
        self.catalog.state.relation(name).map_err(|e| {
            if self.snapshot.principal.is_some() {
                Error::Denied
            } else {
                e
            }
        })
    }
    fn permission(&self, tid: Id, row: &Row, permission: Permission) -> Result<()> {
        if allowed(
            &self.catalog,
            self.snapshot.principal.as_deref(),
            self.snapshot.scope.as_deref(),
            tid,
            row,
            permission,
        ) {
            Ok(())
        } else {
            Err(Error::Denied)
        }
    }
    fn reference(&mut self, tid: Id, id: Id) -> Result<()> {
        self.track(tagged(b'r', &row_key(tid, id)))?;
        let missing = || {
            if self.snapshot.principal.is_some() {
                Error::Denied
            } else {
                Error::Constraint("invalid typed reference".into())
            }
        };
        // A same-transaction target is already owned by the staging map.
        // Reference validation needs only a borrow, not a cloned Row, its
        // variable payloads, and a new Arc. Keep tracking and permissions in
        // the same order as the persisted-row path.
        if let Some(row) = self.rows.get(&(tid, id)) {
            return self.permission(
                tid,
                row.as_ref().ok_or_else(missing)?,
                Permission::Reference,
            );
        }
        let row = self.snapshot.raw(tid, id)?.ok_or_else(missing)?;
        self.permission(tid, &row, Permission::Reference)
    }
    fn allocated(&mut self, id: Id) -> Result<bool> {
        self.track(tagged(b'a', id.0.as_bytes()))?;
        Ok(authority::reserved_type(id)
            || self.identities.contains(&id)
            || self.catalog.state.identities.contains(&id)
            || self
                .snapshot
                .view
                .ids
                .get(id.0.as_bytes().as_slice())
                .map_err(err)?
                .is_some())
    }
    fn stage(&mut self, tid: Id, id: Id, row: Option<Row>) -> Result<()> {
        if !self.rows.contains_key(&(tid, id))
            && self.rows.len() >= self.snapshot.db.inner.options.max_transaction_rows
        {
            return Err(Error::Constraint("transaction row budget exceeded".into()));
        }
        let bytes = if let Some(row) = &row {
            row_encoded_len(&self.catalog.state.relations[&tid], row)?
        } else {
            32
        };
        if bytes > self.snapshot.db.inner.options.max_row_bytes {
            return Err(Error::Constraint("row exceeds configured bound".into()));
        }
        let def = &self.catalog.state.relations[&tid];
        let effects = 2usize
            .saturating_add(2 * def.roles.len())
            .saturating_add(2 * def.unique.len())
            .saturating_add(2 * def.reference_slots.len());
        self.effects = self.effects.saturating_add(effects);
        if self.effects > self.snapshot.db.inner.options.max_dependencies {
            return Err(Error::Constraint(
                "transaction index-effect budget exceeded".into(),
            ));
        }
        let resident = row.as_ref().map_or(0, |row| {
            64usize
                .saturating_add(
                    row.values
                        .len()
                        .saturating_mul(std::mem::size_of::<Value>()),
                )
                .saturating_add(
                    row.values
                        .iter()
                        .map(|v| match v {
                            Value::Text(v) => v.capacity(),
                            Value::Bytes(v) => v.capacity(),
                            _ => 0,
                        })
                        .sum::<usize>(),
                )
        });
        self.charge(
            bytes
                .max(resident)
                .saturating_add(effects.saturating_mul(64)),
        )?;
        let def = self.catalog.state.relations[&tid].clone();
        let new_unique = row
            .as_ref()
            .map(|row| {
                (0..def.unique.len())
                    .filter(|&i| unique_applies(&def, row, i))
                    .map(|i| unique_key(tid, i, &def.unique_values(row, i)))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.charge(
            new_unique
                .iter()
                .map(|key| key.len().saturating_add(64))
                .sum(),
        )?;
        if let Some(Some(old)) = self.rows.get(&(tid, id)) {
            for i in 0..def.unique.len() {
                self.staged_unique
                    .remove(&unique_key(tid, i, &def.unique_values(old, i)));
            }
        }
        for key in new_unique {
            self.staged_unique.insert(key, id);
        }
        self.rows.insert((tid, id), row);
        Ok(())
    }
    fn validate(&mut self, def: &CompiledRelation, row: &Row) -> Result<()> {
        self.permission(def.id, row, Permission::Write)?;
        if row.values.len() != def.roles.len()
            || !def
                .roles
                .iter()
                .zip(row.values.iter())
                .all(|(r, v)| r.data_type.accepts(v))
        {
            return Err(Error::Constraint("role arity/type mismatch".into()));
        }
        let size = row_encoded_len(def, row)?;
        if size > self.snapshot.db.inner.options.max_row_bytes {
            return Err(Error::Constraint("row exceeds configured bound".into()));
        }
        def.check_values(&row.values)?;
        for (slot, target) in &def.reference_slots {
            if let Value::Ref(id) = row.values[*slot] {
                if self.defer_foreign_keys && def.roles[*slot].deferrable {
                    continue;
                }
                self.reference(self.catalog.state.names[target], id)?;
            }
        }
        for i in 0..def.unique.len() {
            if !unique_applies(def, row, i) {
                continue;
            }
            let key = unique_key(def.id, i, &def.unique_values(row, i));
            self.track(tagged(b'u', &key))?;
            let existing = {
                let table = &self.snapshot.view.unique;
                table
                    .get(key.as_slice())
                    .map_err(err)?
                    .map(|v| uuid::Uuid::from_slice(v.value()).map(Id).map_err(err))
                    .transpose()?
            };
            if let Some(id) = existing
                && id != row.id
            {
                let still_conflicts = match self.rows.get(&(def.id, id)) {
                    Some(None) => false,
                    Some(Some(r)) => def.unique_values(r, i) == def.unique_values(row, i),
                    None => true,
                };
                if still_conflicts {
                    return Err(Error::Constraint("unique key unavailable".into()));
                }
            }
            if self.staged_unique.get(&key).is_some_and(|id| *id != row.id) {
                return Err(Error::Constraint("unique key unavailable".into()));
            }
        }
        Ok(())
    }
    pub fn insert(&mut self, name: &str, values: Vec<Value>) -> Result<Id> {
        let id = Id::new();
        self.insert_with_id(name, id, values)?;
        Ok(id)
    }
    pub fn insert_with_id(&mut self, name: &str, id: Id, values: Vec<Value>) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            let row = Row {
                id,
                values: values.into_boxed_slice(),
            };
            // Reject inaccessible writes before revealing shape or allocation state.
            tx.permission(def.id, &row, Permission::Write)?;
            if tx.allocated(id)? || tx.catalog.state.relations.contains_key(&id) {
                return Err(Error::Constraint("identity already allocated".into()));
            }
            // The global allocation dependency already excludes every competing
            // membership for a fresh identity, including another relation family.
            // Ordinary absent/present membership reads still track row revisions.
            tx.validate(&def, &row)?;
            tx.stage(def.id, id, Some(row))?;
            tx.identities.insert(id);
            Ok(())
        })
    }
    /// Attach a new typed membership while requiring reference authority on its source.
    pub fn extend(&mut self, source: &str, target: &str, id: Id, values: Vec<Value>) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(target)?;
            let row = Row {
                id,
                values: values.into_boxed_slice(),
            };
            tx.permission(def.id, &row, Permission::Write)?;
            let source = tx.def(source)?;
            tx.reference(source.id, id)?;
            if tx.raw(def.id, id)?.is_some() {
                return Err(Error::Constraint("membership already exists".into()));
            }
            tx.validate(&def, &row)?;
            tx.stage(def.id, id, Some(row))
        })
    }
    pub fn update(&mut self, name: &str, id: Id, values: Vec<Value>) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            let old = tx
                .raw(def.id, id)?
                .ok_or(if tx.snapshot.principal.is_some() {
                    Error::Denied
                } else {
                    Error::NotFound
                })?;
            tx.permission(def.id, &old, Permission::Write)?;
            let row = Row {
                id,
                values: values.into_boxed_slice(),
            };
            tx.validate(&def, &row)?;
            tx.stage(def.id, id, Some(row))
        })
    }
    pub fn delete(&mut self, name: &str, id: Id) -> Result<()> {
        self.delete_many(name, &[id])
    }
    /// Delete a set of explicit roots as one operation. Restrict constraints
    /// consider the entire set and its authorized cascade closure.
    pub fn delete_many(&mut self, name: &str, ids: &[Id]) -> Result<()> {
        self.mutate(|tx| tx.delete_closure(name, ids))
    }
    pub fn get(&mut self, name: &str, id: Id) -> Result<Option<Row>> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            if !discover(
                &tx.catalog,
                tx.snapshot.principal.as_deref(),
                tx.snapshot.scope.as_deref(),
                def.id,
            ) {
                return Err(Error::Denied);
            }
            let root = tx.raw(def.id, id)?;
            let Some(root) = root else { return Ok(None) };
            let mut traversed = 0usize;
            let mut todo = vec![(def.id, id)];
            let mut seen = BTreeSet::new();
            while let Some((tid, id)) = todo.pop() {
                if !seen.insert((tid, id)) {
                    continue;
                }
                if seen.len() > tx.snapshot.db.inner.options.max_visibility_rows {
                    return Err(Error::Query("reference visibility budget exceeded".into()));
                }
                let Some(row) = tx.raw(tid, id)? else {
                    return Ok(None);
                };
                if !allowed(
                    &tx.catalog,
                    tx.snapshot.principal.as_deref(),
                    tx.snapshot.scope.as_deref(),
                    tid,
                    &row,
                    Permission::Read,
                ) {
                    return Ok(None);
                }
                for (slot, target) in &tx.catalog.state.relations[&tid].reference_slots {
                    if let Value::Ref(id) = row.values[*slot] {
                        traversed += 1;
                        if traversed > tx.snapshot.db.inner.options.max_visibility_rows {
                            return Err(Error::Query(
                                "reference visibility edge budget exceeded".into(),
                            ));
                        }
                        todo.push((tx.catalog.state.names[target], id));
                    }
                }
            }
            Ok(Some((*root).clone()))
        })
    }
    /// Host-only installation with default authenticated visibility and a reserved host owner.
    pub fn install(&mut self, module: Module) -> Result<()> {
        self.install_owned(module, HOST, Visibility::Authenticated)
    }
    /// Atomically install additive definitions and initial visibility. The host
    /// assigns a namespace initially; its owner may install subsequent versions.
    pub fn install_owned(
        &mut self,
        module: Module,
        owner: &str,
        visibility: Visibility,
    ) -> Result<()> {
        self.mutate(|tx| {
            if tx.snapshot.selection.capability.is_some() {
                return Err(Error::Denied);
            }
            if owner.is_empty()
                || (owner.starts_with('\0') && owner != HOST)
                || crate::visibility::protected(&module.name)
            {
                return Err(Error::Denied);
            }
            if tx.snapshot.principal.is_some()
                && tx
                    .snapshot
                    .metadata_snapshot()?
                    .schema_namespace_managed(&module.name)?
            {
                return Err(Error::Denied);
            }
            if let Some(principal) = &tx.snapshot.principal
                && (principal != owner
                    || tx.catalog.data.owners.get(&module.name) != Some(principal))
            {
                return Err(Error::Denied);
            }
            if tx
                .catalog
                .data
                .owners
                .get(&module.name)
                .is_some_and(|existing| existing != owner)
            {
                return Err(Error::Denied);
            }
            for def in &module.relations {
                if tx.allocated(def.id)? {
                    return Err(Error::Constraint(
                        "schema identity already allocated".into(),
                    ));
                }
            }
            let mut data = tx.catalog.data.clone();
            data.owners.insert(module.name.clone(), owner.into());
            for def in &module.relations {
                data.visibility.insert(def.id, (owner.into(), visibility));
                if !data.visibility_ids.is_empty() {
                    data.visibility_ids
                        .insert(def.id, authority::visibility_id(def.id));
                }
            }
            data.modules.push(module);
            tx.set_catalog(data)?;
            Ok(())
        })
    }
    fn set_catalog(&mut self, data: CatalogData) -> Result<()> {
        let mut catalog = Catalog::compile(data)?;
        authority::validate_visibility_identity_extension(&self.catalog, &catalog)?;
        let expected = authority::catalog_ids(&catalog.state);
        let previous = authority::catalog_ids(&self.catalog.state);
        for id in expected.difference(&previous) {
            self.track(tagged(b'a', id.0.as_bytes()))?;
            if self.identities.contains(id)
                || self
                    .snapshot
                    .view
                    .ids
                    .get(id.0.as_bytes().as_slice())
                    .map_err(err)?
                    .is_some()
            {
                return Err(Error::Constraint(
                    "catalog identity already allocated".into(),
                ));
            }
        }
        catalog.data.reserved = expected;
        let bytes = serde_json::to_vec(&catalog.data).map_err(err)?.len();
        if bytes > self.snapshot.db.inner.options.max_catalog_bytes {
            return Err(Error::Constraint("catalog byte budget exceeded".into()));
        }
        self.charge(bytes)?;
        self.catalog = Arc::new(catalog);
        self.catalog_changed = true;
        Ok(())
    }
    pub fn grant(&mut self, principal: &str, name: &str, grant: Grant) -> Result<()> {
        self.mutate(|tx| {
            if principal.is_empty() || principal.starts_with('\0') {
                return Err(Error::Denied);
            }
            let def = tx.def(name)?;
            tx.owner(def.id)?;
            validate_grant(&def, &grant)?;
            let mut data = tx.catalog.data.clone();
            authority::invalidate_roots(&mut data, &tx.catalog, principal, def.id)?;
            data.grants
                .retain(|(p, tid, _)| p != principal || *tid != def.id);
            data.grants.push((principal.into(), def.id, grant));
            tx.set_catalog(data)?;
            tx.auth_changed = true;
            Ok(())
        })
    }
    pub fn revoke(&mut self, principal: &str, name: &str) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            tx.owner(def.id)?;
            let mut data = tx.catalog.data.clone();
            authority::invalidate_roots(&mut data, &tx.catalog, principal, def.id)?;
            data.grants
                .retain(|(p, tid, _)| p != principal || *tid != def.id);
            tx.set_catalog(data)?;
            tx.auth_changed = true;
            Ok(())
        })
    }
    fn owner(&self, tid: Id) -> Result<()> {
        if self.snapshot.selection.capability.is_some() {
            return Err(Error::Denied);
        }
        if let Some(p) = &self.snapshot.principal
            && self
                .catalog
                .data
                .visibility
                .get(&tid)
                .is_none_or(|(owner, _)| owner != p)
        {
            return Err(Error::Denied);
        }
        Ok(())
    }
    pub fn set_visibility(&mut self, name: &str, visibility: Visibility) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            tx.owner(def.id)?;
            let mut data = tx.catalog.data.clone();
            data.visibility
                .get_mut(&def.id)
                .ok_or_else(|| err("missing visibility"))?
                .1 = visibility;
            tx.set_catalog(data)?;
            tx.auth_changed = true;
            Ok(())
        })
    }
}
impl PagedTransaction {
    /// Validate in queue order and acknowledge only after Immediate durability.
    /// Independent accepted writes can share one durable group, each retaining
    /// its own revision. Conflicts are rejected individually before persistence.
    pub fn commit(mut self) -> Result<u64> {
        if self.failed {
            return Err(Error::Aborted);
        }
        if self.snapshot.db.inner.read_only.load(Ordering::Acquire) {
            return Err(Error::Denied);
        }
        self.snapshot.fence()?;
        self.finalize_constraints()?;
        let db = self.snapshot.db.clone();
        db.inner.commits.submit(self, &db.inner)
    }
}
/// Bounded per-commit coalescing, never a data-sized secondary in-memory index.
struct VersionChanges {
    keys: BTreeSet<Vec<u8>>,
    bytes: usize,
    limit: usize,
}
impl VersionChanges {
    fn new(limit: usize) -> Self {
        Self {
            keys: BTreeSet::new(),
            bytes: 0,
            limit,
        }
    }
    fn insert(&mut self, key: Vec<u8>) -> Result<()> {
        if !self.keys.contains(&key) {
            let bytes = self.bytes.saturating_add(key.len()).saturating_add(64);
            if bytes > self.limit {
                return Err(Error::Constraint(
                    "commit version-key budget exceeded".into(),
                ));
            }
            self.bytes = bytes;
            self.keys.insert(key);
        }
        Ok(())
    }
}
// SQL-compatible uniqueness: NULL does not claim a unique tuple. Ordinary
// role indexes still retain NULL for native equality and SQL IS NULL scans.
fn unique_applies(def: &CompiledRelation, row: &Row, constraint: usize) -> bool {
    def.unique_applies(row, constraint)
}
#[allow(clippy::too_many_arguments)]
fn change_indexes(
    format: PagedFormat,
    catalog: &Catalog,
    tid: Id,
    row: &Row,
    insert: bool,
    index: &mut redb::Table<&[u8], u8>,
    unique: &mut redb::Table<&[u8], &[u8]>,
    incoming: &mut redb::Table<&[u8], u8>,
) -> Result<()> {
    let def = &catalog.state.relations[&tid];
    for (slot, value) in row.values.iter().enumerate() {
        if !format.role_index(def, slot) {
            continue;
        }
        let key = index_key(tid, slot, value, row.id);
        if insert {
            index.insert(key.as_slice(), 0).map_err(err)?;
        } else {
            index.remove(key.as_slice()).map_err(err)?;
        }
    }
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
    for (slot, target) in &def.reference_slots {
        if let Value::Ref(id) = row.values[*slot] {
            let target = catalog.state.names[target];
            let key = incoming_key(target, id, tid, row.id, *slot);
            if insert {
                incoming.insert(key.as_slice(), 0).map_err(err)?;
            } else {
                incoming.remove(key.as_slice()).map_err(err)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod scan_profile;
#[cfg(test)]
mod tests;

/// Physical rewrites remain inside the same redb transaction as logical indexes.
enum RowWriter<'a> {
    Legacy(redb::Table<'a, &'static [u8], &'static [u8]>),
    Dense(dense::DenseWriter<'a>),
}
impl<'a> RowWriter<'a> {
    fn new(
        table: redb::Table<'a, &'static [u8], &'static [u8]>,
        format: PagedFormat,
        max_row_bytes: usize,
    ) -> Self {
        if format == PagedFormat::V3 {
            Self::Dense(dense::DenseWriter::new(table, max_row_bytes))
        } else {
            Self::Legacy(table)
        }
    }
    fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        match self {
            Self::Legacy(table) => {
                table.insert(key, value.as_slice()).map_err(err)?;
                Ok(())
            }
            Self::Dense(table) => table.insert_owned(key, value),
        }
    }
    fn remove(&mut self, key: &[u8]) -> Result<()> {
        match self {
            Self::Legacy(table) => {
                table.remove(key).map_err(err)?;
                Ok(())
            }
            Self::Dense(table) => table.remove(key),
        }
    }
    fn finish(self) -> Result<()> {
        match self {
            Self::Legacy(_) => Ok(()),
            Self::Dense(table) => table.finish(),
        }
    }
}
