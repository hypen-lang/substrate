//! Authorized, read-only DataFusion SQL over typed Arrow snapshot batches.
//! No raw DataFusion context or detached table provider escapes this module.
use crate::*;
mod checked;
mod planning;
mod provider;
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray,
            StringArray, StringViewArray,
        },
        datatypes::DataType as ArrowType,
    },
    catalog::{
        CatalogProvider,
        memory::{MemoryCatalogProvider, MemorySchemaProvider},
    },
    common::ScalarValue,
    dataframe::DataFrame,
    datasource::view::ViewTable,
    execution::{
        TaskContext,
        context::SQLOptions,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        runtime_env::RuntimeEnvBuilder,
    },
    logical_expr::LogicalPlan,
    physical_plan::execute_stream,
    prelude::{SessionConfig, SessionContext},
    sql::sqlparser::{
        ast::{BinaryOperator, Expr, Statement, TableFactor, Visit, Visitor},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::{Token, Tokenizer},
    },
};
use futures::StreamExt;
use rusqlite::types::Value as SqlValue;
use std::{
    collections::BTreeSet,
    future::Future,
    ops::ControlFlow,
    sync::{Arc, OnceLock, mpsc},
    time::Duration,
};

const QUERY_MEMORY: usize = 256 * 1024 * 1024;
const SETUP_TIMEOUT_MS: u64 = 30_000;

fn query_error(error: impl std::fmt::Display) -> Error {
    Error::Query(error.to_string())
}

/// Runs futures on a separate runtime rather than calling block_on inside a
/// caller's runtime. Cancels on timeout, and turns a panicked task into an error.
fn run<T: Send + 'static>(
    future: impl Future<Output = Result<T>> + Send + 'static,
    timeout_ms: u64,
) -> Result<T> {
    static RUNTIME: OnceLock<std::result::Result<tokio::runtime::Runtime, String>> =
        OnceLock::new();
    let runtime = RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("liquid-sql")
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(query_error)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let handle = runtime.spawn(async move {
        let result = tokio::time::timeout(Duration::from_millis(timeout_ms), future)
            .await
            .map_err(|_| Error::Query("SQL deadline exceeded".into()))
            .and_then(|r| r);
        let _ = sender.send(result);
    });
    match receiver
        .recv_timeout(Duration::from_millis(timeout_ms).saturating_add(Duration::from_millis(100)))
    {
        Ok(result) => result,
        Err(_) => {
            handle.abort();
            Err(Error::Query(
                "SQL task failed or exceeded its deadline".into(),
            ))
        }
    }
}

pub(crate) struct DfSnapshot {
    context: SessionContext,
    live: crate::engine::LiveGuard,
}
pub(crate) struct DfQuery {
    context: SessionContext,
    plan: LogicalPlan,
    parameter_count: usize,
    cache: planning::PlanCache,
    cacheable: bool,
    live: crate::engine::LiveGuard,
}
impl DfSnapshot {
    pub(crate) fn new(snapshot: Snapshot, selected: Option<&BTreeSet<Id>>) -> Result<Self> {
        snapshot.live.check_attached()?;
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_default_catalog_and_schema("liquid", "public")
            .with_information_schema(false);
        config.options_mut().sql_parser.enable_ident_normalization = false;
        config.options_mut().sql_parser.default_null_ordering = "nulls_min".into();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(QUERY_MEMORY, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
            .map_err(query_error)?;
        let context = SessionContext::new_with_config_rt(config, runtime);
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .map_err(query_error)?;
        context.register_catalog("liquid", catalog.clone());
        // relations() filters both grants and protected referenced type metadata.
        let declarations = snapshot.relations();
        let pages = Arc::new(provider::BatchCache::default());
        if let Some(selected) = selected
            && !selected
                .iter()
                .all(|id| declarations.iter().any(|r| r.id == *id))
        {
            return Err(Error::Denied);
        }
        for declaration in declarations {
            if selected.is_some_and(|ids| !ids.contains(&declaration.id)) {
                continue;
            }
            snapshot.live.check_attached()?;
            let (namespace, table) = declaration
                .name
                .split_once('.')
                .ok_or_else(|| Error::Schema("relation requires a namespace".into()))?;
            if catalog.schema(namespace).is_none() {
                catalog
                    .register_schema(namespace, Arc::new(MemorySchemaProvider::new()))
                    .map_err(query_error)?;
            }
            let provider = Arc::new(provider::LiquidTable::new(
                snapshot.clone(),
                declaration.clone(),
                pages.clone(),
            )?);
            catalog
                .schema(namespace)
                .ok_or_else(|| Error::Query("SQL schema disappeared".into()))?
                .register_table(table.to_owned(), provider)
                .map_err(query_error)?;
        }
        snapshot.live.check_attached()?;
        Ok(Self {
            context,
            live: snapshot.live.clone(),
        })
    }

    pub(crate) fn prepare(&self, sql: &str) -> Result<DfQuery> {
        self.live.check_attached()?;
        let (sql, parameter_count) = validated_sql(sql)?;
        let context = self.context.clone();
        let live = self.live.clone();
        let plan = run(
            async move {
                live.check_attached()?;
                let frame = context
                    .sql_with_options(&sql, read_options())
                    .await
                    .map_err(query_error)?;
                live.check_attached()?;
                checked::rewrite(frame.logical_plan().clone()).map_err(query_error)
            },
            SETUP_TIMEOUT_MS,
        )?;
        self.live.check_attached()?;
        Ok(DfQuery {
            context: self.context.clone(),
            cacheable: planning::cacheable(&plan),
            plan,
            parameter_count,
            cache: planning::PlanCache::default(),
            live: self.live.clone(),
        })
    }

    pub(crate) fn view(&mut self, name: &str, select: &str) -> Result<()> {
        self.live.check_attached()?;
        if !crate::model::identifier(name) {
            return Err(Error::Query("invalid view name".into()));
        }
        let prepared = self.prepare(select)?;
        if prepared.parameter_count != 0 {
            return Err(Error::Query("view parameters are not supported".into()));
        }
        if self.context.table_exist(name).map_err(query_error)? {
            return Err(Error::Query("SQL view already exists".into()));
        }
        let provider = ViewTable::new(prepared.plan, Some(select.to_owned()));
        self.context
            .register_table(name, Arc::new(provider))
            .map_err(query_error)?;
        self.live.check_attached()
    }
}

fn read_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

impl DfQuery {
    pub(crate) fn query(&mut self, params: &[SqlValue], limits: QueryLimits) -> Result<SqlTable> {
        self.live.check_attached()?;
        if limits.max_rows == 0
            || limits.max_bytes == 0
            || limits.vm_steps == 0
            || limits.timeout_ms == 0
        {
            return Err(Error::Query("query budgets must be positive".into()));
        }
        if params.len() != self.parameter_count {
            return Err(Error::Query("SQL parameter count mismatch".into()));
        }
        let parameter_bytes = params.iter().fold(0usize, |size, value| {
            size.saturating_add(match value {
                SqlValue::Text(value) => value.len(),
                SqlValue::Blob(value) => value.len(),
                _ => 8,
            })
        });
        if parameter_bytes > 1024 * 1024 {
            return Err(Error::Query("SQL parameter byte budget exceeded".into()));
        }
        let context = self.context.clone();
        let plan = self.plan.clone();
        let live = self.live.clone();
        let mut cache = std::mem::take(&mut self.cache);
        let cacheable = self.cacheable;
        let binding = params.to_vec();
        let result = run(
            async move {
                live.check_attached()?;
                let physical = if let Some(cached) = cache.get(&binding) {
                    if let Some(fresh) = cached.physical().map_err(query_error)? {
                        fresh
                    } else {
                        let state = context.state();
                        state
                            .query_planner()
                            .create_physical_plan(&cached.optimized, &state)
                            .await
                            .map_err(query_error)?
                    }
                } else {
                    let values = binding
                        .iter()
                        .map(|v| match v {
                            SqlValue::Null => ScalarValue::Null,
                            SqlValue::Integer(v) => ScalarValue::Int64(Some(*v)),
                            SqlValue::Real(v) => ScalarValue::Float64(Some(*v)),
                            SqlValue::Text(v) => ScalarValue::Utf8(Some(v.clone())),
                            SqlValue::Blob(v) => ScalarValue::Binary(Some(v.clone())),
                        })
                        .collect::<Vec<_>>();
                    let state = context.state();
                    let frame = DataFrame::new(state.clone(), plan)
                        .with_param_values(values)
                        .map_err(query_error)?;
                    let checked =
                        checked::rewrite(frame.logical_plan().clone()).map_err(query_error)?;
                    let optimized = state.optimize(&checked).map_err(query_error)?;
                    let physical = state
                        .query_planner()
                        .create_physical_plan(&optimized, &state)
                        .await
                        .map_err(query_error)?;
                    if cacheable {
                        // The stored template is never executed. Instantiate fresh state
                        // even for the first call; non-whitelisted plans are not retained.
                        cache.put(&binding, optimized, physical.clone());
                        match cache
                            .get(&binding)
                            .map(|entry| entry.physical())
                            .transpose()
                            .map_err(query_error)?
                            .flatten()
                        {
                            Some(fresh) => fresh,
                            None => physical,
                        }
                    } else {
                        physical
                    }
                };
                let columns = physical
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>();
                let task = TaskContext::from(&context);
                let config = task
                    .session_config()
                    .clone()
                    .with_extension(Arc::new(provider::SourceBudget::default()));
                let task = Arc::new(task.with_session_config(config));
                let mut bytes = columns.iter().map(|s| s.len()).sum::<usize>();
                if bytes > limits.max_bytes {
                    return Err(Error::Query("query byte budget exceeded".into()));
                }
                let mut stream = execute_stream(physical, task).map_err(query_error)?;
                let mut rows = Vec::new();
                let mut work = 0u64;
                while let Some(batch) = stream.next().await {
                    live.check_attached()?;
                    let batch = batch.map_err(query_error)?;
                    if batch.num_rows() > limits.max_rows.saturating_sub(rows.len()) {
                        return Err(Error::Query(
                            "query row budget exceeded; use LIMIT or pages".into(),
                        ));
                    }
                    for index in 0..batch.num_rows() {
                        work = work.saturating_add(batch.num_columns().max(1) as u64);
                        if work > limits.vm_steps {
                            return Err(Error::Query("query output-cell budget exceeded".into()));
                        }
                        let mut row = Vec::with_capacity(batch.num_columns());
                        for column in batch.columns() {
                            let length = cell_size(column, index)?;
                            if length > limits.max_bytes.saturating_sub(bytes) {
                                return Err(Error::Query("query byte budget exceeded".into()));
                            }
                            bytes += length;
                            let scalar =
                                ScalarValue::try_from_array(column, index).map_err(query_error)?;
                            row.push(sql_value(scalar)?);
                        }
                        rows.push(row);
                    }
                }
                live.check_attached()?;
                Ok((SqlTable { columns, rows }, cache))
            },
            limits.timeout_ms,
        );
        self.live.check_attached()?;
        match result {
            Ok((table, cache)) => {
                self.cache = cache;
                Ok(table)
            }
            Err(error) => {
                self.cache.clear();
                Err(error)
            }
        }
    }
}

/// Check size and supported scalar shape before cloning Arrow-owned buffers.
fn cell_size(array: &ArrayRef, row: usize) -> Result<usize> {
    if array.is_null(row) {
        return Ok(8);
    }
    macro_rules! length {
        ($kind:ty) => {
            array
                .as_any()
                .downcast_ref::<$kind>()
                .ok_or_else(|| Error::Query("invalid Arrow scalar representation".into()))?
                .value(row)
                .len()
        };
    }
    Ok(match array.data_type() {
        ArrowType::Utf8 => length!(StringArray),
        ArrowType::LargeUtf8 => length!(LargeStringArray),
        ArrowType::Utf8View => length!(StringViewArray),
        ArrowType::Binary => length!(BinaryArray),
        ArrowType::LargeBinary => length!(LargeBinaryArray),
        ArrowType::BinaryView => length!(BinaryViewArray),
        ArrowType::Null
        | ArrowType::Boolean
        | ArrowType::Int8
        | ArrowType::Int16
        | ArrowType::Int32
        | ArrowType::Int64
        | ArrowType::UInt8
        | ArrowType::UInt16
        | ArrowType::UInt32
        | ArrowType::UInt64
        | ArrowType::Float32
        | ArrowType::Float64
        | ArrowType::Decimal128(_, 0) => 8,
        other => {
            return Err(Error::Query(format!(
                "unsupported SQL result type: {other}"
            )));
        }
    })
}

fn sql_value(value: ScalarValue) -> Result<SqlValue> {
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    Ok(match value {
        ScalarValue::Boolean(Some(v)) => SqlValue::Integer(i64::from(v)),
        ScalarValue::Int8(Some(v)) => SqlValue::Integer(v.into()),
        ScalarValue::Int16(Some(v)) => SqlValue::Integer(v.into()),
        ScalarValue::Int32(Some(v)) => SqlValue::Integer(v.into()),
        ScalarValue::Int64(Some(v)) => SqlValue::Integer(v),
        ScalarValue::UInt8(Some(v)) => SqlValue::Integer(v.into()),
        ScalarValue::UInt16(Some(v)) => SqlValue::Integer(v.into()),
        ScalarValue::UInt32(Some(v)) => SqlValue::Integer(v.into()),
        ScalarValue::UInt64(Some(v)) => {
            SqlValue::Integer(v.try_into().map_err(|_| {
                Error::Query("SQL integer exceeds signed 64-bit result range".into())
            })?)
        }
        ScalarValue::Float32(Some(v)) if v.is_finite() => SqlValue::Real(v.into()),
        ScalarValue::Float64(Some(v)) if v.is_finite() => SqlValue::Real(v),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => SqlValue::Text(v),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v)) => SqlValue::Blob(v),
        ScalarValue::Decimal128(Some(v), _, 0) => {
            SqlValue::Integer(v.try_into().map_err(|_| {
                Error::Query("SQL decimal exceeds signed 64-bit result range".into())
            })?)
        }
        other => {
            return Err(Error::Query(format!(
                "unsupported SQL result type: {}",
                other.data_type()
            )));
        }
    })
}

/// Token rewriting preserves quoted strings/comments; no interpolation of values.
fn validated_sql(sql: &str) -> Result<(String, usize)> {
    if sql.len() > 1024 * 1024 {
        return Err(Error::Query("SQL text budget exceeded".into()));
    }
    let dialect = GenericDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(query_error)?;
    let mut parameter_count = 0usize;
    for token in &mut tokens {
        if let Token::Placeholder(text) = token {
            let number = if text == "?" {
                parameter_count + 1
            } else {
                text.strip_prefix('?')
                    .or_else(|| text.strip_prefix('$'))
                    .ok_or_else(|| Error::Query("use positional SQL parameters".into()))?
                    .parse::<usize>()
                    .map_err(|_| Error::Query("invalid SQL parameter".into()))?
            };
            if !(1..=1024).contains(&number) {
                return Err(Error::Query("SQL parameter limit is 1024".into()));
            }
            parameter_count = parameter_count.max(number);
            *text = format!("${number}");
        }
    }
    let normalized = tokens
        .into_iter()
        .map(|token| token.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    let statements = Parser::parse_sql(&dialect, &normalized).map_err(query_error)?;
    if statements.len() != 1 {
        return Err(Error::Query("exactly one SQL statement required".into()));
    }
    struct ReadOnly;
    impl Visitor for ReadOnly {
        type Break = ();
        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
            match statement {
                Statement::Query(_) | Statement::Explain { .. } => ControlFlow::Continue(()),
                _ => ControlFlow::Break(()),
            }
        }
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if matches!(
                expr,
                Expr::Overlay { .. }
                    | Expr::BinaryOp {
                        op: BinaryOperator::StringConcat,
                        ..
                    }
            ) {
                return ControlFlow::Break(());
            }
            if let Expr::Function(function) = expr {
                let Some(name) = function.name.0.last().and_then(|part| part.as_ident()) else {
                    return ControlFlow::Break(());
                };
                if matches!(
                    name.value.to_ascii_lowercase().as_str(),
                    "repeat"
                        | "overlay"
                        | "space"
                        | "lpad"
                        | "rpad"
                        | "array_repeat"
                        | "range"
                        | "generate_series"
                        | "string_agg"
                        | "array_agg"
                        | "list"
                        | "unnest"
                        | "concat"
                        | "concat_ws"
                        | "replace"
                        | "regexp_replace"
                        | "to_hex"
                ) {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
            match table {
                TableFactor::Table { args: None, .. }
                | TableFactor::Derived { .. }
                | TableFactor::NestedJoin { .. } => ControlFlow::Continue(()),
                _ => ControlFlow::Break(()),
            }
        }
    }
    if statements[0].visit(&mut ReadOnly).is_break() {
        return Err(Error::Query(
            "read-only SQL over registered relations required".into(),
        ));
    }
    Ok((normalized, parameter_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{CapabilityScope, RelationCapability};
    #[test]
    fn capability_expiry_invalidates_prepared_datafusion_query() {
        let db = Database::memory();
        db.clock.set(1000);
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.grant("owner", "core.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let cap = db
            .issue_capability_until(
                Id::new(),
                "owner",
                CapabilityScope {
                    relations: vec![RelationCapability {
                        relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                        grant: Grant::read_only(),
                    }],
                    schema_namespace: None,
                    delegations: 0,
                },
                Some(1010),
            )
            .unwrap();
        let sql = db
            .session("owner")
            .with_capability(cap.id)
            .unwrap()
            .snapshot()
            .sql()
            .unwrap();
        let mut query = sql.prepare("SELECT name FROM core.Person").unwrap();
        assert_eq!(query.query(&[]).unwrap().rows.len(), 1);
        db.clock.set(1010);
        assert!(matches!(query.query(&[]), Err(Error::Denied)));
    }
    #[test]
    fn planning_cache_is_binding_local_and_discards_failed_execution() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let ada = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        let grace = tx.insert("core.Person", vec!["Grace".into()]).unwrap();
        tx.commit().unwrap();
        let sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let mut query = sql
            .prepare("SELECT name FROM core.Person WHERE id=?1")
            .unwrap();
        let first = [SqlValue::Text(ada.to_string())];
        let second = [SqlValue::Text(grace.to_string())];
        assert!(query.cache.get(&first).is_none());
        for _ in 0..3 {
            assert_eq!(
                query.query(&first, QueryLimits::default()).unwrap().rows,
                vec![vec![SqlValue::Text("Ada".into())]]
            );
            assert!(
                query
                    .cache
                    .get(&first)
                    .unwrap()
                    .physical()
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            query.query(&second, QueryLimits::default()).unwrap().rows,
            vec![vec![SqlValue::Text("Grace".into())]]
        );
        assert!(query.cache.get(&first).is_none());
        assert!(query.cache.get(&second).is_some());
        assert!(
            query
                .query(
                    &second,
                    QueryLimits {
                        max_bytes: 1,
                        ..QueryLimits::default()
                    }
                )
                .is_err()
        );
        assert!(query.cache.get(&second).is_none());
        assert_eq!(
            query.query(&first, QueryLimits::default()).unwrap().rows,
            vec![vec![SqlValue::Text("Ada".into())]]
        );
    }

    #[test]
    fn context_dependent_queries_and_views_never_cache_folded_values() {
        let db = Database::memory();
        let mut sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        sql.view("clock_view", "SELECT CAST(now() AS VARCHAR) AS stamp")
            .unwrap();
        for text in [
            "SELECT random()",
            "SELECT CAST(now() AS VARCHAR)",
            "SELECT stamp FROM clock_view",
            "SELECT (SELECT CAST(now() AS VARCHAR))",
            "SELECT 1 WHERE EXISTS (SELECT 1 WHERE random() > 0)",
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM clock_view WHERE stamp IS NOT NULL)",
        ] {
            let mut query = sql.prepare(text).unwrap();
            assert!(!query.cacheable, "{text}");
            for _ in 0..2 {
                query.query(&[], QueryLimits::default()).unwrap();
                assert!(query.cache.get(&[]).is_none(), "{text}");
            }
        }
    }

    #[test]
    fn deterministic_subquery_cache_preserves_bindings_and_live_authority() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.insert("core.Person", vec!["Grace".into()]).unwrap();
        tx.grant("reader", "core.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").snapshot();
        let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
        let sqlite = snapshot.sqlite_compat().unwrap();
        for text in [
            "SELECT p.name FROM core.Person p WHERE EXISTS (SELECT 1 FROM core.Person q WHERE q.id=p.id AND q.name=?1) ORDER BY p.name",
            "SELECT p.name FROM core.Person p WHERE NOT EXISTS (SELECT 1 FROM core.Person q WHERE q.id=p.id AND q.name=?1) ORDER BY p.name",
            "SELECT p.name FROM core.Person p WHERE p.id IN (SELECT q.id FROM core.Person q WHERE q.name=?1) ORDER BY p.name",
            "SELECT p.name,(SELECT count(*) FROM core.Person q WHERE q.id=p.id AND q.name=?1) AS matches FROM core.Person p ORDER BY p.name",
        ] {
            let mut query = sql.prepare(text).unwrap();
            assert!(query.cacheable, "{text}");
            for value in [
                SqlValue::Text("Ada".into()),
                SqlValue::Text("Ada".into()),
                SqlValue::Text("Grace".into()),
                SqlValue::Text("Grace".into()),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Text("missing".into()),
                SqlValue::Text("Ada".into()),
            ] {
                let binding = [value];
                let expected = sqlite.query(text, &binding).unwrap();
                assert_eq!(
                    query.query(&binding, QueryLimits::default()).unwrap().rows,
                    expected.rows,
                    "{text}, {binding:?}"
                );
                // Non-null matching bindings retain joins/sorts and require
                // fresh physical plans. NULL can fold to an immutable empty
                // relation, which is safely eligible for template reuse.
                if matches!(&binding[0], SqlValue::Text(name) if name == "Ada" || name == "Grace") {
                    assert!(
                        query
                            .cache
                            .get(&binding)
                            .unwrap()
                            .physical()
                            .unwrap()
                            .is_none()
                    );
                }
            }
        }
        // A scalar subquery cardinality error must not leave a usable cached
        // plan behind; a later valid binding can still execute successfully.
        let mut scalar = sql
            .prepare("SELECT (SELECT name FROM core.Person WHERE name>=?1) AS name")
            .unwrap();
        let many = [SqlValue::Text("Ada".into())];
        assert!(scalar.query(&many, QueryLimits::default()).is_err());
        assert!(scalar.cache.get(&many).is_none());
        let one = [SqlValue::Text("Grace".into())];
        for _ in 0..2 {
            assert_eq!(
                scalar.query(&one, QueryLimits::default()).unwrap().rows,
                vec![vec![SqlValue::Text("Grace".into())]]
            );
        }
        let mut query = sql.prepare("SELECT p.name FROM core.Person p WHERE EXISTS (SELECT 1 FROM core.Person q WHERE q.id=p.id) ORDER BY p.name").unwrap();
        for _ in 0..2 {
            assert_eq!(
                query.query(&[], QueryLimits::default()).unwrap().rows.len(),
                2
            );
        }
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
        assert!(matches!(
            query.query(&[], QueryLimits::default()),
            Err(Error::Denied)
        ));
    }

    /// Manual release-mode diagnostic. This measures an executor round trip,
    /// not a subtractable profile of the complete point-query implementation.
    #[test]
    #[ignore = "manual release-mode performance probe"]
    fn sql_execution_overhead_probe() {
        use std::time::Instant;
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let id = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
        let mut point = sql
            .prepare("SELECT name FROM core.Person WHERE id=?1")
            .unwrap();
        let mut constant = sql.prepare("SELECT 1 AS n").unwrap();
        let parameters = [SqlValue::Text(id.to_string())];
        let measure = |name: &str, action: &mut dyn FnMut()| {
            for _ in 0..100 {
                action();
            }
            let mut samples = Vec::new();
            for _ in 0..9 {
                let start = Instant::now();
                for _ in 0..1000 {
                    action();
                }
                samples.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            samples.sort_by(f64::total_cmp);
            println!("{name}: {:.3} us/call (median of 9 x 1000)", samples[4]);
        };
        measure("executor_round_trip", &mut || {
            run(async { Ok(()) }, SETUP_TIMEOUT_MS).unwrap();
        });
        measure("prepared_constant", &mut || {
            std::hint::black_box(constant.query(&[], QueryLimits::default()).unwrap());
        });
        measure("prepared_identity", &mut || {
            std::hint::black_box(point.query(&parameters, QueryLimits::default()).unwrap());
        });
        measure("native_identity", &mut || {
            std::hint::black_box(snapshot.get("core.Person", id).unwrap());
        });
    }

    #[test]
    fn numeric_result_conversion_never_truncates() {
        assert!(sql_value(ScalarValue::UInt64(Some(u64::MAX))).is_err());
        assert!(sql_value(ScalarValue::Decimal128(Some(i128::MAX), 38, 0)).is_err());
        assert!(sql_value(ScalarValue::Decimal128(Some(125), 10, 2)).is_err());
        assert_eq!(
            sql_value(ScalarValue::Int64(Some(i64::MIN))).unwrap(),
            SqlValue::Integer(i64::MIN)
        );
    }
}
