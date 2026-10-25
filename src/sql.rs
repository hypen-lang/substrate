//! SQLite virtual-table interface; all canonical reads/writes use the Liquid kernel.
use crate::*;
use rusqlite::{
    Connection, ffi,
    hooks::{AuthAction, AuthContext, Authorization},
    types::{Value as SqlValue, ValueRef},
    vtab::{
        Context, CreateVTab, IndexConstraintOp, IndexInfo, UpdateVTab, VTab, VTabConnection,
        VTabCursor, VTabKind, Values, read_only_module, update_module,
    },
};
use std::{
    collections::BTreeSet,
    os::raw::c_int,
    sync::{Arc, Mutex},
};

pub struct SqliteSnapshot {
    connection: Connection,
    live: crate::engine::LiveGuard,
}
#[derive(Debug, Clone, PartialEq)]
pub struct SqlTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}
#[derive(Clone)]
struct Source {
    snapshot: Snapshot,
    relation: Arc<crate::compiled::CompiledRelation>,
    transaction: Option<Arc<Mutex<Transaction>>>,
}
impl Snapshot {
    pub fn sqlite_compat(&self) -> Result<SqliteSnapshot> {
        SqliteSnapshot::new(self.clone())
    }
}
impl SqliteSnapshot {
    fn new(snapshot: Snapshot) -> Result<Self> {
        snapshot.check_live()?;
        Ok(Self {
            live: snapshot.live.clone(),
            connection: connection(snapshot, None, None)?,
        })
    }

    /// Define a local, read-only invoker view bound to this snapshot. Not persisted.
    pub fn view(&mut self, name: &str, select: &str) -> Result<()> {
        self.live.check()?;
        if !crate::model::identifier(name) {
            return Err(Error::Query("invalid view name".into()));
        }
        {
            let statement = prepare_one(&self.connection, select)?;
            if !statement.readonly() || statement.column_count() == 0 {
                return Err(Error::Query("view must be a read query".into()));
            }
        }
        self.connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        let result = (|| {
            self.connection.execute_batch("PRAGMA query_only=OFF;")?;
            self.connection
                .execute(&format!("CREATE TEMP VIEW {} AS {select}", quote(name)), [])?;
            Ok::<_, rusqlite::Error>(())
        })();
        let reset = self.connection.execute_batch("PRAGMA query_only=ON;");
        authorize_reads(&self.connection);
        result?;
        reset?;
        Ok(())
    }
    /// Execute a read query against one pinned, authorized snapshot.
    /// Parameters use rusqlite SQL values. Decimal roles expose exact minor units.
    pub fn query(&self, sql: &str, params: &[SqlValue]) -> Result<SqlTable> {
        self.prepare(sql)?.query(params)
    }
    /// Prepare once and rebind parameters against the same immutable snapshot.
    pub fn prepare(&self, sql: &str) -> Result<SqliteQuery<'_>> {
        self.live.check()?;
        let statement = prepare_one(&self.connection, sql)?;
        if !statement.readonly() || statement.column_count() == 0 {
            return Err(Error::Query(
                "read-only result-producing SQL required".into(),
            ));
        }
        Ok(SqliteQuery {
            statement,
            live: self.live.clone(),
        })
    }
}
pub struct SqliteQuery<'a> {
    statement: rusqlite::Statement<'a>,
    live: crate::engine::LiveGuard,
}
impl SqliteQuery<'_> {
    pub fn query(&mut self, params: &[SqlValue]) -> Result<SqlTable> {
        self.live.check()?;
        let columns = self
            .statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let count = columns.len();
        let rows = self
            .statement
            .query_map(rusqlite::params_from_iter(params), |row| {
                (0..count).map(|i| row.get(i)).collect()
            })?
            .collect::<rusqlite::Result<Vec<Vec<SqlValue>>>>()?;
        self.live.check()?;
        Ok(SqlTable { columns, rows })
    }
}

fn prepare_one<'a>(connection: &'a Connection, sql: &str) -> Result<rusqlite::Statement<'a>> {
    let mut batch = rusqlite::Batch::new(connection, sql);
    let statement = batch
        .next()?
        .ok_or_else(|| Error::Query("empty SQL".into()))?;
    if batch.next()?.is_some() {
        return Err(Error::Query("exactly one SQL statement required".into()));
    }
    Ok(statement)
}
fn authorize_reads(connection: &Connection) {
    connection.authorizer(Some(|context: AuthContext<'_>| match context.action {
        AuthAction::Select | AuthAction::Read { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        AuthAction::Function { function_name }
            if !function_name.eq_ignore_ascii_case("load_extension") =>
        {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }));
}
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn sql_error(error: Error) -> rusqlite::Error {
    rusqlite::Error::ModuleError(error.to_string())
}
fn sql_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Text | DataType::Ref(_) => "TEXT",
        DataType::Bytes => "BLOB",
        _ => "INTEGER",
    }
}
/// Only lossless conversions are pushed. Otherwise scan and let SQLite apply its
/// affinity/coercion rules. SQLite always rechecks pushed predicates (omit=false).
fn liquid_value(value: SqlValue, data_type: &DataType) -> Option<Value> {
    match (value, data_type) {
        (SqlValue::Integer(v), DataType::Int) => Some(Value::Int(v)),
        (SqlValue::Integer(v), DataType::Timestamp) => Some(Value::Timestamp(v)),
        (SqlValue::Integer(v), DataType::Decimal { .. }) => Some(Value::Decimal(v)),
        (SqlValue::Integer(v @ 0..=1), DataType::Bool) => Some(Value::Bool(v == 1)),
        (SqlValue::Text(v), DataType::Text) => Some(Value::Text(v)),
        (SqlValue::Text(v), DataType::Ref(_)) => {
            uuid::Uuid::parse_str(&v).ok().map(|id| Value::Ref(Id(id)))
        }
        (SqlValue::Blob(v), DataType::Bytes) => Some(Value::Bytes(v)),
        _ => None,
    }
}

#[repr(C)]
struct LiquidTab {
    base: ffi::sqlite3_vtab,
    source: Source,
}
// SAFETY: repr(C), sqlite3_vtab is the first field, and no borrowed data escapes.
unsafe impl<'vtab> VTab<'vtab> for LiquidTab {
    type Aux = Source;
    type Cursor = LiquidCursor;
    fn connect(
        _db: &mut VTabConnection,
        aux: Option<&Source>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let source = aux
            .ok_or_else(|| rusqlite::Error::ModuleError("missing snapshot".into()))?
            .clone();
        let columns = std::iter::once("id TEXT PRIMARY KEY NOT NULL".to_string())
            .chain(
                source
                    .relation
                    .roles
                    .iter()
                    .map(|r| format!("{} {} NOT NULL", quote(&r.name), sql_type(&r.data_type))),
            )
            .collect::<Vec<_>>()
            .join(",");
        Ok((
            format!("CREATE TABLE x({columns}) WITHOUT ROWID"),
            Self {
                base: ffi::sqlite3_vtab::default(),
                source,
            },
        ))
    }
    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        let choice = info
            .constraints()
            .enumerate()
            .filter(|(i, c)| {
                c.is_usable()
                    && c.column() >= 0
                    && c.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
                    && info.collation(*i).is_ok_and(|c| c == "BINARY")
            })
            // Predicate order must never choose a broad bucket over a UUID lookup.
            .min_by_key(|(_, c)| if c.column() == 0 { 0 } else { 1 })
            .map(|(i, c)| (i, c.column()));
        if let Some((i, column)) = choice {
            info.constraint_usage(i).set_argv_index(1);
            info.set_idx_num(column + 1);
            info.set_idx_str(if column == 0 {
                "identity_lookup"
            } else {
                "role_index_eq"
            });
            info.set_estimated_cost(if column == 0 { 1.0 } else { 10.0 });
            info.set_estimated_rows(if column == 0 { 1 } else { 10 });
        } else if self.source.transaction.is_none() && self.numeric_range_index(info)? {
            // Immutable cursors can preserve role order. Mutable cursors retain
            // tracked family scans so transaction phantom dependencies are exact.
        } else {
            info.set_idx_num(0);
            info.set_idx_str("family_scan");
            info.set_estimated_cost(1_000_000.0);
        }
        Ok(())
    }
    fn open(&mut self) -> rusqlite::Result<LiquidCursor> {
        Ok(LiquidCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            source: self.source.clone(),
            rows: vec![],
            position: 0,
            paging: None,
            range_paging: None,
        })
    }
}
impl LiquidTab {
    fn numeric_range_index(&self, info: &mut IndexInfo) -> rusqlite::Result<bool> {
        let numeric = |column: i32| {
            column > 0
                && self
                    .source
                    .relation
                    .roles
                    .get((column - 1) as usize)
                    .is_some_and(|r| {
                        matches!(
                            r.data_type,
                            DataType::Bool
                                | DataType::Int
                                | DataType::Decimal { .. }
                                | DataType::Timestamp
                        )
                    })
        };
        let order = if info.num_of_order_by() == 1 {
            info.order_bys()
                .next()
                .map(|o| (o.column(), o.is_order_by_desc()))
                .filter(|(c, _)| numeric(*c))
        } else {
            None
        };
        let ranges = info
            .constraints()
            .enumerate()
            .filter(|(i, c)| {
                c.is_usable()
                    && numeric(c.column())
                    && info.collation(*i).is_ok_and(|c| c == "BINARY")
            })
            .filter_map(|(i, c)| {
                let kind = match c.operator() {
                    IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GE => 1,
                    IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GT => 2,
                    IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LE => 3,
                    IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LT => 4,
                    _ => return None,
                };
                Some((i, c.column(), kind))
            })
            .collect::<Vec<_>>();
        let Some(column) = order
            .map(|(c, _)| c)
            .or_else(|| ranges.first().map(|(_, c, _)| *c))
        else {
            return Ok(false);
        };
        let lower = ranges.iter().find(|(_, c, k)| *c == column && *k <= 2);
        let upper = ranges.iter().find(|(_, c, k)| *c == column && *k >= 3);
        let mut arg = 1;
        for (i, _, _) in [lower, upper].into_iter().flatten() {
            info.constraint_usage(*i).set_argv_index(arg);
            arg += 1;
        }
        let descending = order.is_some_and(|(_, desc)| desc);
        info.set_idx_num(-column);
        info.set_idx_str(&format!(
            "role_index_range/{}/{}/{}",
            lower.map_or(0, |(_, _, k)| *k),
            upper.map_or(0, |(_, _, k)| *k),
            u8::from(descending)
        ));
        // Report ordering only for a single integer-affinity term. Text/ref
        // collations are intentionally not claimed, and LIMIT is never consumed.
        info.set_order_by_consumed(order.is_some());
        info.set_estimated_cost(100.0);
        info.set_estimated_rows(100);
        Ok(true)
    }
}
impl CreateVTab<'_> for LiquidTab {
    const KIND: VTabKind = VTabKind::Default;
}
#[repr(C)]
struct LiquidCursor {
    base: ffi::sqlite3_vtab_cursor,
    source: Source,
    rows: Vec<Arc<Row>>,
    position: usize,
    paging: Option<(Option<(String, Value)>, Id)>,
    range_paging: Option<(crate::dx::NumericRange, (Value, Id))>,
}
// SAFETY: repr(C), sqlite3_vtab_cursor first; owned snapshot outlives each cursor.
unsafe impl VTabCursor for LiquidCursor {
    fn filter(
        &mut self,
        index: c_int,
        description: Option<&str>,
        args: &Values<'_>,
    ) -> rusqlite::Result<()> {
        self.position = 0;
        self.paging = None;
        self.range_paging = None;
        let Source {
            snapshot,
            relation,
            transaction,
        } = &self.source;
        if index < 0 {
            use std::ops::Bound::{Excluded, Included, Unbounded};
            let slot = (-index - 1) as usize;
            let parts = description
                .unwrap_or_default()
                .split('/')
                .collect::<Vec<_>>();
            if parts.len() != 4 || parts[0] != "role_index_range" {
                return Err(sql_error(Error::Query("invalid range plan".into())));
            }
            let mut arg = 0;
            let mut bound = |kind: &str| -> rusqlite::Result<std::ops::Bound<Value>> {
                if kind == "0" {
                    return Ok(Unbounded);
                }
                let value =
                    liquid_value(args.get::<SqlValue>(arg)?, &relation.roles[slot].data_type);
                arg += 1;
                // Unsupported SQLite coercions still scan in index order and are
                // rechecked by SQLite; claiming ordering must survive fallback.
                Ok(match (kind, value) {
                    ("1" | "3", Some(v)) => Included(v),
                    ("2" | "4", Some(v)) => Excluded(v),
                    _ => Unbounded,
                })
            };
            let range = crate::dx::NumericRange {
                slot,
                lower: bound(parts[1])?,
                upper: bound(parts[2])?,
                descending: parts[3] == "1",
            };
            let page = snapshot
                .range_page_shared(&relation.name, &range, None, 64)
                .map_err(sql_error)?;
            self.rows = page.rows;
            self.range_paging = page.next.map(|after| (range, after));
            return Ok(());
        }
        let pushed = if index > 0 {
            let data_type = if index == 1 {
                DataType::Ref(relation.name.clone())
            } else {
                relation.roles[(index - 2) as usize].data_type.clone()
            };
            liquid_value(args.get::<SqlValue>(0)?, &data_type)
        } else {
            None
        };
        self.rows = if let Some(transaction) = transaction {
            let mut tx = transaction.lock().map_err(|_| sql_error(Error::Aborted))?;
            match (index, pushed) {
                (1, Some(Value::Ref(id))) => {
                    tx.get(&relation.name, id).map(|r| r.into_iter().collect())
                }
                (n, Some(value)) if n > 1 => tx.lookup(
                    &relation.name,
                    &relation.roles[(n - 2) as usize].name,
                    &value,
                ),
                _ => tx.scan(&relation.name),
            }
            .map(|rows| rows.into_iter().map(Arc::new).collect())
        } else {
            match (index, pushed) {
                (1, Some(Value::Ref(id))) => snapshot
                    .get_shared(&relation.name, id)
                    .map(|r| r.into_iter().collect()),
                (n, value) => {
                    let predicate = value
                        .filter(|_| n > 1)
                        .map(|v| (relation.roles[(n - 2) as usize].name.clone(), v));
                    snapshot
                        .page_shared(
                            &relation.name,
                            predicate.as_ref().map(|(r, v)| (r.as_str(), v)),
                            None,
                            64,
                        )
                        .map(|page| {
                            self.paging = page.next.map(|next| (predicate, next));
                            page.rows
                        })
                }
            }
        }
        .map_err(sql_error)?;
        Ok(())
    }
    fn next(&mut self) -> rusqlite::Result<()> {
        self.position += 1;
        if self.position >= self.rows.len()
            && let Some((range, after)) = self.range_paging.take()
        {
            let page = self
                .source
                .snapshot
                .range_page_shared(&self.source.relation.name, &range, Some(&after), 64)
                .map_err(sql_error)?;
            self.rows = page.rows;
            self.position = 0;
            self.range_paging = page.next.map(|after| (range, after));
        }
        if self.position >= self.rows.len()
            && let Some((predicate, after)) = self.paging.take()
        {
            let page = self
                .source
                .snapshot
                .page_shared(
                    &self.source.relation.name,
                    predicate.as_ref().map(|(r, v)| (r.as_str(), v)),
                    Some(after),
                    64,
                )
                .map_err(sql_error)?;
            self.rows = page.rows;
            self.position = 0;
            self.paging = page.next.map(|next| (predicate, next));
        }
        Ok(())
    }
    fn eof(&self) -> bool {
        self.position >= self.rows.len()
    }
    fn column(&self, context: &mut Context, column: c_int) -> rusqlite::Result<()> {
        let row = &self.rows[self.position];
        if column == 0 {
            return context.set_result(&row.id.to_string());
        }
        // SQLite copies the requested result. Borrow strings/blobs from the
        // retained row instead of allocating a second temporary payload first.
        match &row.values[(column - 1) as usize] {
            Value::Bool(v) => context.set_result(v),
            Value::Int(v) | Value::Decimal(v) | Value::Timestamp(v) => context.set_result(v),
            Value::Text(v) => context.set_result(v),
            Value::Ref(v) => context.set_result(&v.to_string()),
            Value::Bytes(v) => context.set_result(v),
        }
    }
    fn rowid(&self) -> rusqlite::Result<i64> {
        Err(rusqlite::Error::ModuleError(
            "Liquid tables use UUID primary keys, not rowids".into(),
        ))
    }
}

fn connection(
    snapshot: Snapshot,
    transaction: Option<Arc<Mutex<Transaction>>>,
    selected: Option<&BTreeSet<Id>>,
) -> Result<Connection> {
    snapshot.check_live()?;
    let connection = Connection::open_in_memory()?;
    let mut namespaces = BTreeSet::new();
    for relation in snapshot
        .state
        .relations
        .values()
        .filter(|d| selected.is_none_or(|ids| ids.contains(&d.id)))
        .filter(|d| {
            snapshot.state.can_read(snapshot.principal.as_deref(), d.id)
                || (transaction.is_some()
                    && snapshot
                        .principal
                        .as_deref()
                        .is_none_or(|p| snapshot.state.grant(p, d.id).is_some_and(|g| g.write)))
        })
    {
        let (namespace, name) = relation
            .name
            .split_once('.')
            .expect("validated qualified name");
        if namespaces.insert(namespace.to_string()) {
            connection.execute(
                &format!("ATTACH DATABASE ':memory:' AS {}", quote(namespace)),
                [],
            )?;
        }
        let module_name = format!("liquid_{}", relation.id.0.simple());
        connection.create_module(
            &module_name,
            if transaction.is_some() {
                update_module::<LiquidTab>()
            } else {
                read_only_module::<LiquidTab>()
            },
            Some(Source {
                snapshot: snapshot.clone(),
                transaction: transaction.clone(),
                relation: snapshot.state.relations[&relation.id].clone(),
            }),
        )?;
        connection.execute(
            &format!(
                "CREATE VIRTUAL TABLE {}.{} USING {}",
                quote(namespace),
                quote(name),
                quote(&module_name)
            ),
            [],
        )?;
    }
    if transaction.is_some() {
        authorize_writes(&connection);
    } else {
        connection.execute_batch("PRAGMA query_only=ON;")?;
        authorize_reads(&connection);
    }
    install_live_progress(&connection, snapshot.live.clone());
    Ok(connection)
}
fn install_live_progress(connection: &Connection, live: crate::engine::LiveGuard) {
    connection.progress_handler(1000, Some(move || live.check().is_err()));
}

/// Explicit Liquid transaction with SQLite syntax. Drop rolls back; any SQL error
/// aborts the whole transaction (including earlier statements). Schema changes,
/// PRAGMA, ATTACH, savepoints and SQL BEGIN/COMMIT are not accepted here.
pub struct SqlTransaction {
    connection: Connection,
    transaction: Arc<Mutex<Transaction>>,
    failed: bool,
}
impl Session {
    pub fn sql_transaction(&self) -> Result<SqlTransaction> {
        let tx = self.transaction();
        let snapshot = tx.snapshot();
        let transaction = Arc::new(Mutex::new(tx));
        Ok(SqlTransaction {
            connection: connection(snapshot, Some(transaction.clone()), None)?,
            transaction,
            failed: false,
        })
    }
}
impl SqlTransaction {
    pub fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlTable> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let result = (|| {
            let snapshot = {
                let mut transaction = self.transaction.lock().map_err(|_| Error::Denied)?;
                let snapshot = transaction.snapshot();
                // DataFusion reads immutable authorized batches. Conservatively
                // record family/visibility dependencies before releasing them so
                // SQL transaction reads remain serializable with later writes.
                for relation in snapshot.relations() {
                    transaction.scan(&relation.name)?;
                }
                snapshot
            };
            snapshot.sql()?.query(sql, params)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn execute(&mut self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let result = (|| {
            let live = self
                .transaction
                .lock()
                .map_err(|_| Error::Denied)?
                .snapshot()
                .live;
            live.check()?;
            let mut statement = prepare_one(&self.connection, sql)?;
            if statement.readonly() || statement.column_count() != 0 {
                return Err(Error::Query(
                    "expected INSERT, UPDATE, or DELETE without RETURNING".into(),
                ));
            }
            let changed = statement.execute(rusqlite::params_from_iter(params))?;
            live.check()?;
            Ok(changed)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn commit(self) -> Result<u64> {
        if self.failed {
            return Err(Error::Aborted);
        }
        drop(self.connection);
        let tx = Arc::try_unwrap(self.transaction)
            .map_err(|_| Error::Storage("SQL transaction still borrowed".into()))?
            .into_inner()
            .map_err(|_| Error::Aborted)?;
        tx.commit()
    }
}
fn authorize_writes(connection: &Connection) {
    connection.authorizer(Some(|context: AuthContext<'_>| match context.action {
        AuthAction::Select
        | AuthAction::Read { .. }
        | AuthAction::Recursive
        | AuthAction::Insert { .. }
        | AuthAction::Update { .. }
        | AuthAction::Delete { .. } => Authorization::Allow,
        AuthAction::Function { function_name }
            if !function_name.eq_ignore_ascii_case("load_extension") =>
        {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }));
}
fn parse_id(value: SqlValue) -> rusqlite::Result<Id> {
    if let SqlValue::Text(s) = value {
        uuid::Uuid::parse_str(&s)
            .map(Id)
            .map_err(|_| sql_error(Error::Constraint("expected UUID identity".into())))
    } else {
        Err(sql_error(Error::Constraint(
            "explicit UUID id required for SQL insert".into(),
        )))
    }
}
impl LiquidTab {
    fn write_values(&self, args: &Values<'_>) -> rusqlite::Result<Vec<Value>> {
        self.source
            .relation
            .roles
            .iter()
            .enumerate()
            .map(|(i, role)| {
                liquid_value(args.get(i + 3)?, &role.data_type).ok_or_else(|| {
                    sql_error(Error::Constraint(format!(
                        "SQL value is not a lossless {} value",
                        role.name
                    )))
                })
            })
            .collect()
    }
}
impl UpdateVTab<'_> for LiquidTab {
    fn delete(&mut self, arg: ValueRef<'_>) -> rusqlite::Result<()> {
        let id = parse_id(arg.into())?;
        self.source
            .transaction
            .as_ref()
            .ok_or_else(|| sql_error(Error::Denied))?
            .lock()
            .map_err(|_| sql_error(Error::Aborted))?
            .delete(&self.source.relation.name, id)
            .map_err(sql_error)
    }
    fn insert(&mut self, args: &Values<'_>) -> rusqlite::Result<i64> {
        let id = parse_id(args.get(2)?)?;
        let values = self.write_values(args)?;
        self.source
            .transaction
            .as_ref()
            .ok_or_else(|| sql_error(Error::Denied))?
            .lock()
            .map_err(|_| sql_error(Error::Aborted))?
            .insert_with_id(&self.source.relation.name, id, values)
            .map_err(sql_error)?;
        Ok(0) // WITHOUT ROWID uses the UUID primary key, never this placeholder.
    }
    fn update(&mut self, args: &Values<'_>) -> rusqlite::Result<()> {
        let old = parse_id(args.get(0)?)?;
        let id = parse_id(args.get(2)?)?;
        if old != id {
            return Err(sql_error(Error::Constraint("identity is immutable".into())));
        }
        let values = self.write_values(args)?;
        self.source
            .transaction
            .as_ref()
            .ok_or_else(|| sql_error(Error::Denied))?
            .lock()
            .map_err(|_| sql_error(Error::Aborted))?
            .update(&self.source.relation.name, id, values)
            .map_err(sql_error)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueryLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
    /// SQLite VM instructions in compatibility mode; output cells in DataFusion.
    pub vm_steps: u64,
    /// Cooperative DataFusion execution deadline in milliseconds.
    pub timeout_ms: u64,
}
impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_rows: 1000,
            max_bytes: 1024 * 1024,
            vm_steps: 1_000_000,
            timeout_ms: 5_000,
        }
    }
}
impl SqliteSnapshot {
    /// Limits output buffering and SQLite VM work. Native policy traversal and
    /// schema registration are outside the VM-step budget.
    pub fn query_with_limits(
        &self,
        sql: &str,
        params: &[SqlValue],
        limits: QueryLimits,
    ) -> Result<SqlTable> {
        self.live.check()?;
        if limits.max_rows == 0 || limits.max_bytes == 0 || limits.vm_steps == 0 {
            return Err(Error::Query("query budgets must be positive".into()));
        }
        // Bound individual SQLite values too, before copying them into Rust output.
        let old_length = self.connection.set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            limits.max_bytes.min(i32::MAX as usize) as i32,
        );
        let mut remaining = limits.vm_steps;
        let live = self.live.clone();
        self.connection.progress_handler(
            100,
            Some(move || {
                remaining = remaining.saturating_sub(100);
                remaining == 0 || live.check().is_err()
            }),
        );
        let result = (|| {
            let mut query = self.prepare(sql)?;
            let columns = query
                .statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let count = columns.len();
            let mut size = columns.iter().map(|s| s.len()).sum::<usize>();
            if size > limits.max_bytes {
                return Err(Error::Query("query byte budget exceeded".into()));
            }
            let mut output = Vec::new();
            let mut rows = query.statement.query(rusqlite::params_from_iter(params))?;
            while let Some(row) = rows.next()? {
                if output.len() >= limits.max_rows {
                    return Err(Error::Query(
                        "query row budget exceeded; use LIMIT or pages".into(),
                    ));
                }
                let mut values = Vec::with_capacity(count);
                for i in 0..count {
                    let v: SqlValue = row.get(i)?;
                    size = size.saturating_add(match &v {
                        SqlValue::Text(s) => s.len(),
                        SqlValue::Blob(b) => b.len(),
                        _ => 8,
                    });
                    values.push(v);
                }
                if size > limits.max_bytes {
                    return Err(Error::Query("query byte budget exceeded".into()));
                }
                output.push(values);
            }
            self.live.check()?;
            Ok(SqlTable {
                columns,
                rows: output,
            })
        })();
        install_live_progress(&self.connection, self.live.clone());
        self.connection
            .set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, old_length);
        result
    }
}

impl Snapshot {
    /// Build a SQL catalog for exactly these readable families. Explicit selection
    /// avoids registering unrelated schemas; canonical IDs and authorization stay
    /// bound to this snapshot. Names omitted here are unavailable to this SQL handle.
    pub fn sqlite_compat_selected(&self, names: &[&str]) -> Result<SqliteSnapshot> {
        let selected = names
            .iter()
            .map(|name| self.resolve_read(name).map(|r| r.id))
            .collect::<Result<BTreeSet<_>>>()?;
        self.sqlite_for_relations(&selected)
    }
    /// Restrict SQL registration to explicitly selected contracts; native reference
    /// visibility still uses the complete authorized snapshot.
    pub(crate) fn sqlite_for_relations(&self, selected: &BTreeSet<Id>) -> Result<SqliteSnapshot> {
        self.check_live()?;
        Ok(SqliteSnapshot {
            live: self.live.clone(),
            connection: connection(self.clone(), None, Some(selected))?,
        })
    }
}

/// SQL reads use DataFusion over a principal-bound, immutable Arrow catalog.
/// SQLite remains an explicit compatibility adapter for its dialect and mutations.
pub struct SqlSnapshot {
    backend: crate::datafusion_sql::DfSnapshot,
}
pub struct SqlQuery<'a> {
    query: crate::datafusion_sql::DfQuery,
    owner: std::marker::PhantomData<&'a SqlSnapshot>,
}
impl Snapshot {
    pub fn sql(&self) -> Result<SqlSnapshot> {
        Ok(SqlSnapshot {
            backend: crate::datafusion_sql::DfSnapshot::new(self.clone(), None)?,
        })
    }
    pub fn sql_selected(&self, names: &[&str]) -> Result<SqlSnapshot> {
        let selected = names
            .iter()
            .map(|name| self.resolve_read(name).map(|r| r.id))
            .collect::<Result<BTreeSet<_>>>()?;
        self.sql_for_relations(&selected)
    }
    pub(crate) fn sql_for_relations(&self, selected: &BTreeSet<Id>) -> Result<SqlSnapshot> {
        Ok(SqlSnapshot {
            backend: crate::datafusion_sql::DfSnapshot::new(self.clone(), Some(selected))?,
        })
    }
}
impl SqlSnapshot {
    pub fn view(&mut self, name: &str, select: &str) -> Result<()> {
        self.backend.view(name, select)
    }
    pub fn prepare(&self, sql: &str) -> Result<SqlQuery<'_>> {
        Ok(SqlQuery {
            query: self.backend.prepare(sql)?,
            owner: std::marker::PhantomData,
        })
    }
    pub fn query(&self, sql: &str, params: &[SqlValue]) -> Result<SqlTable> {
        self.prepare(sql)?.query(params)
    }
    pub fn query_with_limits(
        &self,
        sql: &str,
        params: &[SqlValue],
        limits: QueryLimits,
    ) -> Result<SqlTable> {
        self.backend.prepare(sql)?.query(params, limits)
    }
}
impl SqlQuery<'_> {
    pub fn query(&mut self, params: &[SqlValue]) -> Result<SqlTable> {
        self.query.query(
            params,
            QueryLimits {
                max_rows: 1_000_000,
                max_bytes: 128 * 1024 * 1024,
                vm_steps: 10_000_000,
                timeout_ms: 30_000,
            },
        )
    }
}

#[cfg(test)]
mod live_revocation_tests {
    use super::*;
    #[test]
    fn revocation_during_sql_execution_discards_even_buffered_aggregate_results() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.grant("reader", "core.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let sql = db.session("reader").snapshot().sqlite_compat().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let signal = barrier.clone();
        let revoker = std::thread::spawn(move || {
            signal.wait();
            let mut tx = db.admin().transaction();
            tx.revoke("reader", "core.Person").unwrap();
            tx.commit().unwrap();
            signal.wait();
        });
        let mut first = true;
        sql.connection.progress_handler(
            50,
            Some(move || {
                if first {
                    first = false;
                    barrier.wait();
                    barrier.wait();
                }
                false
            }),
        );
        let result=sql.query("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000) SELECT count(*) FROM n, core.Person",&[]);
        assert!(result.is_err(), "in-flight result escaped revocation");
        revoker.join().unwrap();
    }
}
