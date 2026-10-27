//! SQL over authoritative paged storage. Scans yield bounded Arrow batches and
//! never build a resident copy of the database. Providers never escape this API.
use super::*;
use crate::datafusion_sql::{cell_size, checked, read_options, run, sql_value, validated_sql};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryArray, Int64Array, StringArray},
        datatypes::{DataType as ArrowType, Field, Schema, SchemaRef},
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    catalog::{
        CatalogProvider, Session, TableProvider,
        memory::{MemoryCatalogProvider, MemorySchemaProvider},
    },
    common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion},
    dataframe::DataFrame,
    datasource::view::ViewTable,
    execution::{
        TaskContext,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        runtime_env::RuntimeEnvBuilder,
    },
    logical_expr::{Expr, LogicalPlan, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, execute_stream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::{SessionConfig, SessionContext},
};
use futures::StreamExt;
use rusqlite::types::Value as SqlValue;
use std::{fmt, sync::atomic::AtomicUsize};

type DfResult<T> = std::result::Result<T, DataFusionError>;
mod aggregate;
mod join;
fn query_error(e: impl fmt::Display) -> Error {
    Error::Query(e.to_string())
}
fn external(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Read-only SQL catalog tied to one data revision and its live authorization.
pub struct PagedSqlSnapshot {
    snapshot: PagedSnapshot,
    context: SessionContext,
}
/// Reusable parsed/typed SQL plan. Parameters are bound without string interpolation.
pub struct PagedSqlQuery {
    #[cfg(test)]
    after_direct_test_hook: Option<Box<dyn FnOnce() + Send>>,
    snapshot: PagedSnapshot,
    context: SessionContext,
    plan: LogicalPlan,
    parameter_count: usize,
    direct: Option<DirectRead>,
    join: Option<join::IndexedJoin>,
    aggregate: Option<aggregate::ScalarAggregate>,
}
impl PagedSnapshot {
    pub(crate) fn sql_for_relations(&self, selected: &BTreeSet<Id>) -> Result<PagedSqlSnapshot> {
        let schemas = self.schemas()?;
        let names = selected
            .iter()
            .map(|id| {
                schemas
                    .iter()
                    .find(|r| r.id == *id)
                    .map(|r| r.name.as_str())
                    .ok_or(Error::Denied)
            })
            .collect::<Result<Vec<_>>>()?;
        self.sql_selected(&names)
    }
    pub fn sql(&self) -> Result<PagedSqlSnapshot> {
        PagedSqlSnapshot::new(self.clone(), None)
    }
    pub fn sql_selected(&self, names: &[&str]) -> Result<PagedSqlSnapshot> {
        PagedSqlSnapshot::new(self.clone(), Some(names))
    }
}
impl PagedSqlSnapshot {
    fn new(snapshot: PagedSnapshot, selected: Option<&[&str]>) -> Result<Self> {
        snapshot.fence()?;
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(256)
            .with_default_catalog_and_schema("liquid", "public")
            .with_information_schema(false);
        config.options_mut().sql_parser.enable_ident_normalization = false;
        config.options_mut().sql_parser.default_null_ordering = "nulls_min".into();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(256 * 1024 * 1024, 1.0)
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
        let declarations = snapshot.schemas()?;
        if selected.is_some_and(|names| {
            names
                .iter()
                .any(|name| !declarations.iter().any(|r| &r.name == name))
        }) {
            return Err(Error::Denied);
        }
        for declaration in declarations {
            if selected.is_some_and(|names| !names.contains(&declaration.name.as_str())) {
                continue;
            }
            let (namespace, name) = declaration
                .name
                .split_once('.')
                .ok_or_else(|| query_error("relation requires namespace"))?;
            if catalog.schema(namespace).is_none() {
                catalog
                    .register_schema(namespace, Arc::new(MemorySchemaProvider::new()))
                    .map_err(query_error)?;
            }
            catalog
                .schema(namespace)
                .ok_or_else(|| query_error("missing SQL namespace"))?
                .register_table(
                    name.to_owned(),
                    Arc::new(PagedTable::new(snapshot.clone(), declaration.clone())),
                )
                .map_err(query_error)?;
        }
        snapshot.fence()?;
        Ok(Self { snapshot, context })
    }
    pub fn prepare(&self, sql: &str) -> Result<PagedSqlQuery> {
        self.snapshot.fence()?;
        let (sql, parameter_count) = validated_sql(sql)?;
        let context = self.context.clone();
        let snapshot = self.snapshot.clone();
        let plan = run(
            async move {
                snapshot.fence()?;
                let frame = context
                    .sql_with_options(&sql, read_options())
                    .await
                    .map_err(query_error)?;
                snapshot.fence()?;
                checked::rewrite(frame.logical_plan().clone()).map_err(query_error)
            },
            30_000,
        )?;
        self.snapshot.fence()?;
        Ok(PagedSqlQuery {
            #[cfg(test)]
            after_direct_test_hook: None,
            snapshot: self.snapshot.clone(),
            context: self.context.clone(),
            direct: DirectRead::compile(&plan),
            join: join::IndexedJoin::compile(&plan),
            aggregate: aggregate::ScalarAggregate::compile(&plan),
            plan,
            parameter_count,
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
        self.prepare(sql)?.query_with_limits(params, limits)
    }
    pub fn view(&mut self, name: &str, select: &str) -> Result<()> {
        self.snapshot.fence()?;
        if !crate::model::identifier(name) {
            return Err(query_error("invalid view name"));
        }
        let prepared = self.prepare(select)?;
        if prepared.parameter_count != 0 || self.context.table_exist(name).map_err(query_error)? {
            return Err(query_error(
                "view requires parameter-free query and unused name",
            ));
        }
        self.context
            .register_table(
                name,
                Arc::new(ViewTable::new(prepared.plan, Some(select.into()))),
            )
            .map_err(query_error)?;
        self.snapshot.fence()
    }
}
impl PagedSqlQuery {
    pub fn query(&mut self, params: &[SqlValue]) -> Result<SqlTable> {
        self.query_with_limits(
            params,
            QueryLimits {
                max_rows: 1_000_000,
                max_bytes: 128 * 1024 * 1024,
                vm_steps: 10_000_000,
                timeout_ms: 30_000,
            },
        )
    }
    pub fn query_with_limits(
        &mut self,
        params: &[SqlValue],
        limits: QueryLimits,
    ) -> Result<SqlTable> {
        self.snapshot.fence()?;
        if limits.max_rows == 0
            || limits.max_bytes == 0
            || limits.vm_steps == 0
            || limits.timeout_ms == 0
        {
            return Err(query_error("query budgets must be positive"));
        }
        if params.len() != self.parameter_count {
            return Err(query_error("SQL parameter count mismatch"));
        }
        let parameter_bytes = params.iter().fold(0usize, |n, v| {
            n.saturating_add(match v {
                SqlValue::Text(v) => v.len(),
                SqlValue::Blob(v) => v.len(),
                _ => 8,
            })
        });
        if parameter_bytes > 1024 * 1024 {
            return Err(query_error("SQL parameter byte budget exceeded"));
        }
        if let Some(direct) = &self.direct {
            let result = direct.query(params, limits);
            #[cfg(test)]
            if let Some(hook) = self.after_direct_test_hook.take() {
                hook();
            }
            // Output conversion/budget errors can be data-dependent too.
            self.snapshot.fence()?;
            if let Some(result) = result? {
                return Ok(result);
            }
        }
        if let Some(join) = &self.join {
            let result = join.query(params, limits);
            self.snapshot.fence()?;
            if let Some(result) = result? {
                return Ok(result);
            }
        }
        if let Some(aggregate) = &self.aggregate {
            let result = aggregate.query(params, limits);
            // Match the ordinary executor's final authorization gate, including
            // errors computed from data before concurrent authority changes.
            self.snapshot.fence()?;
            if let Some(result) = result? {
                return Ok(result);
            }
        }
        let values = params
            .iter()
            .map(|v| match v {
                SqlValue::Null => ScalarValue::Null,
                SqlValue::Integer(v) => ScalarValue::Int64(Some(*v)),
                SqlValue::Real(v) => ScalarValue::Float64(Some(*v)),
                SqlValue::Text(v) => ScalarValue::Utf8(Some(v.clone())),
                SqlValue::Blob(v) => ScalarValue::Binary(Some(v.clone())),
            })
            .collect::<Vec<_>>();
        let context = self.context.clone();
        let snapshot = self.snapshot.clone();
        let plan = self.plan.clone();
        let budget = Arc::new(ScanBudget::new(limits.timeout_ms));
        let result = run(
            async move {
                snapshot.fence()?;
                let state = context.state();
                let frame = DataFrame::new(state.clone(), plan)
                    .with_param_values(values)
                    .map_err(query_error)?;
                let plan = checked::rewrite(frame.logical_plan().clone()).map_err(query_error)?;
                let optimized = state.optimize(&plan).map_err(query_error)?;
                let physical = state
                    .query_planner()
                    .create_physical_plan(&optimized, &state)
                    .await
                    .map_err(query_error)?;
                let columns = physical
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>();
                let mut bytes = columns.iter().map(String::len).sum::<usize>();
                if bytes > limits.max_bytes {
                    return Err(query_error("query byte budget exceeded"));
                }
                let task = TaskContext::from(&context);
                let config = task.session_config().clone().with_extension(budget.clone());
                let mut stream =
                    execute_stream(physical, Arc::new(task.with_session_config(config)))
                        .map_err(query_error)?;
                let mut rows = Vec::new();
                let mut work = 0u64;
                while let Some(batch) = stream.next().await {
                    snapshot.fence()?;
                    let batch = batch.map_err(query_error)?;
                    if batch.num_rows() > limits.max_rows.saturating_sub(rows.len()) {
                        return Err(query_error("query row budget exceeded; use LIMIT or pages"));
                    }
                    for index in 0..batch.num_rows() {
                        work = work.saturating_add(batch.num_columns().max(1) as u64);
                        if work > limits.vm_steps {
                            return Err(query_error("query output-cell budget exceeded"));
                        }
                        let mut row = Vec::with_capacity(batch.num_columns());
                        for column in batch.columns() {
                            let length = cell_size(column, index)?;
                            if length > limits.max_bytes.saturating_sub(bytes) {
                                return Err(query_error("query byte budget exceeded"));
                            }
                            bytes += length;
                            row.push(sql_value(
                                ScalarValue::try_from_array(column, index).map_err(query_error)?,
                            )?);
                        }
                        rows.push(row);
                    }
                }
                snapshot.fence()?;
                budget.check()?;
                Ok(SqlTable { columns, rows })
            },
            limits.timeout_ms,
        );
        self.snapshot.fence()?;
        result
    }
}

#[derive(Debug)]
struct ScanBudget {
    rows: AtomicUsize,
    start: std::time::Instant,
    timeout: std::time::Duration,
}
impl Default for ScanBudget {
    fn default() -> Self {
        Self::new(30_000)
    }
}
impl ScanBudget {
    fn new(timeout_ms: u64) -> Self {
        Self {
            rows: AtomicUsize::new(0),
            start: std::time::Instant::now(),
            timeout: std::time::Duration::from_millis(timeout_ms),
        }
    }
    fn check(&self) -> Result<()> {
        if self.start.elapsed() >= self.timeout {
            return Err(query_error("SQL deadline exceeded"));
        }
        Ok(())
    }
    fn charge(&self, rows: usize) -> DfResult<()> {
        if self
            .rows
            .fetch_add(rows, Ordering::Relaxed)
            .saturating_add(rows)
            > 50_000_000
        {
            return Err(DataFusionError::Execution(
                "SQL source row budget exceeded".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone)]
struct Source {
    snapshot: PagedSnapshot,
    declaration: Arc<Relation>,
}
struct PagedTable {
    source: Source,
    schema: SchemaRef,
}
impl fmt::Debug for PagedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PagedTable")
            .field("relation", &self.source.declaration.name)
            .finish()
    }
}
impl PagedTable {
    fn new(snapshot: PagedSnapshot, declaration: Relation) -> Self {
        let schema = Arc::new(Schema::new(
            std::iter::once(Field::new("id", ArrowType::Utf8, false))
                .chain(declaration.roles.iter().map(|role| {
                    Field::new(
                        &role.name,
                        crate::datafusion_sql::values::arrow_type(&role.data_type),
                        role.data_type.is_nullable(),
                    )
                }))
                .collect::<Vec<_>>(),
        ));
        Self {
            source: Source {
                snapshot,
                declaration: Arc::new(declaration),
            },
            schema,
        }
    }
}
#[derive(Clone)]
struct Predicate {
    column: usize,
    value: Value,
}
fn predicates(expr: &Expr, relation: &Relation) -> Option<Vec<Predicate>> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        let mut p = predicates(&binary.left, relation)?;
        p.extend(predicates(&binary.right, relation)?);
        return Some(p);
    }
    if binary.op != Operator::Eq {
        return None;
    }
    let (column, value) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(c)) => (c, v),
        _ => return None,
    };
    let text = match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
        _ => None,
    };
    let number = match value {
        ScalarValue::Int64(Some(n)) => Some(*n),
        _ => None,
    };
    let reference = || {
        let s = text?;
        let id = Id(s.parse::<uuid::Uuid>().ok()?);
        (id.to_string() == s).then_some(Value::Ref(id))
    };
    let (column, value) = if column.name == "id" {
        (0, reference()?)
    } else {
        let slot = relation
            .roles
            .iter()
            .position(|role| role.name == column.name)?;
        let v = match relation.roles[slot].data_type {
            DataType::Float | DataType::Nullable(_) => return None,
            DataType::Text => Value::Text(text?.into()),
            DataType::Ref(_) => reference()?,
            DataType::Int => Value::Int(number?),
            DataType::Timestamp => Value::Timestamp(number?),
            DataType::Decimal { .. } => Value::Decimal(number?),
            DataType::Bool => Value::Bool(match number? {
                0 => false,
                1 => true,
                _ => return None,
            }),
            DataType::Bytes => match value {
                ScalarValue::Binary(Some(v))
                | ScalarValue::LargeBinary(Some(v))
                | ScalarValue::BinaryView(Some(v)) => Value::Bytes(v.clone()),
                _ => return None,
            },
        };
        (slot + 1, v)
    };
    Some(vec![Predicate { column, value }])
}
#[derive(Clone)]
enum Access {
    Identity(Id),
    Equality(usize, Value),
    Scan,
}
#[async_trait]
impl TableProvider for PagedTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if predicates(expr, &self.source.declaration).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.source.snapshot.fence().map_err(external)?;
        let predicates = filters
            .iter()
            .map(|expr| {
                predicates(expr, &self.source.declaration).ok_or_else(|| {
                    DataFusionError::Plan("unsupported pushed paged predicate".into())
                })
            })
            .collect::<DfResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let access = if let Some(id) = predicates.iter().find_map(|p| match (&p.value, p.column) {
            (Value::Ref(id), 0) => Some(*id),
            _ => None,
        }) {
            Access::Identity(id)
        } else if let Some(p) = predicates.iter().min_by_key(|p| {
            let role = &self.source.declaration.roles[p.column - 1].name;
            usize::from(
                !self
                    .source
                    .declaration
                    .unique
                    .iter()
                    .any(|key| key.len() == 1 && key[0] == *role),
            )
        }) {
            Access::Equality(p.column - 1, p.value.clone())
        } else {
            Access::Scan
        };
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = Arc::new(self.schema.project(&projection)?);
        Ok(Arc::new(PagedScan::new(
            self.source.clone(),
            schema,
            projection,
            predicates,
            access,
            limit,
        )))
    }
}
struct PagedScan {
    source: Source,
    projection: Vec<usize>,
    predicates: Vec<Predicate>,
    access: Access,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}
impl PagedScan {
    fn new(
        source: Source,
        schema: SchemaRef,
        projection: Vec<usize>,
        predicates: Vec<Predicate>,
        access: Access,
        limit: Option<usize>,
    ) -> Self {
        Self {
            source,
            projection,
            predicates,
            access,
            limit,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}
impl fmt::Debug for PagedScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}
impl DisplayAs for PagedScan {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PagedScan: relation={}, access={}",
            self.source.declaration.name,
            match self.access {
                Access::Identity(_) => "identity",
                Access::Equality(..) => "role_equality",
                Access::Scan => "streaming_scan",
            }
        )
    }
}
impl ExecutionPlan for PagedScan {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &'static str {
        "PagedScan"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan("paged scan is a leaf".into()))
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution("invalid paged partition".into()));
        }
        self.source.snapshot.fence().map_err(external)?;
        let cursor = Cursor {
            source: self.source.clone(),
            schema: self.schema(),
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            access: self.access.clone(),
            remaining: self.limit.unwrap_or(usize::MAX),
            after: None,
            done: false,
            budget: context
                .session_config()
                .get_extension::<ScanBudget>()
                .unwrap_or_default(),
        };
        let stream = futures::stream::try_unfold(cursor, |mut cursor| async move {
            tokio::task::yield_now().await;
            cursor
                .next()
                .map(|batch| batch.map(|batch| (batch, cursor)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
struct Cursor {
    source: Source,
    schema: SchemaRef,
    projection: Vec<usize>,
    predicates: Vec<Predicate>,
    access: Access,
    remaining: usize,
    after: Option<Id>,
    done: bool,
    budget: Arc<ScanBudget>,
}
impl Cursor {
    fn next(&mut self) -> DfResult<Option<RecordBatch>> {
        self.source.snapshot.fence().map_err(external)?;
        if self.done || self.remaining == 0 {
            return Ok(None);
        }
        let count = 256
            .min(self.source.snapshot.db.inner.options.max_page_rows)
            .min(self.remaining);
        let mut check = || self.budget.check();
        let rows = match &self.access {
            Access::Identity(id) => {
                self.done = true;
                self.source
                    .snapshot
                    .get_checked(&self.source.declaration.name, *id, &mut check)
                    .map_err(external)?
                    .into_iter()
                    .collect()
            }
            Access::Equality(slot, value) => {
                let page = self
                    .source
                    .snapshot
                    .page_checked(
                        &self.source.declaration.name,
                        Some((&self.source.declaration.roles[*slot].name, value)),
                        self.after,
                        count,
                        &mut check,
                    )
                    .map_err(external)?;
                self.after = page.next;
                self.done = page.next.is_none();
                page.rows
            }
            Access::Scan => {
                let page = self
                    .source
                    .snapshot
                    .page_checked(
                        &self.source.declaration.name,
                        None,
                        self.after,
                        count,
                        &mut check,
                    )
                    .map_err(external)?;
                self.after = page.next;
                self.done = page.next.is_none();
                page.rows
            }
        };
        self.budget.charge(rows.len())?;
        let rows = rows
            .into_iter()
            .filter(|row| {
                self.predicates.iter().all(|p| {
                    if p.column == 0 {
                        p.value == Value::Ref(row.id)
                    } else {
                        p.value == row.values[p.column - 1]
                    }
                })
            })
            .collect::<Vec<_>>();
        self.remaining -= rows.len();
        let columns = self
            .projection
            .iter()
            .map(|&slot| -> DfResult<ArrayRef> {
                if slot == 0 {
                    return Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.id.to_string()),
                    )));
                }
                Ok(match self.source.declaration.roles[slot - 1].data_type {
                    DataType::Nullable(_) | DataType::Float => {
                        crate::datafusion_sql::values::extended_array(
                            &self.source.declaration.roles[slot - 1].data_type,
                            rows.iter().map(|r| &r.values[slot - 1]),
                        )?
                    }
                    DataType::Text => {
                        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| {
                            match &row.values[slot - 1] {
                                Value::Text(s) => s.as_str(),
                                _ => unreachable!("validated typed row"),
                            }
                        })))
                    }
                    DataType::Ref(_) => {
                        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| {
                            match &row.values[slot - 1] {
                                Value::Ref(id) => id.to_string(),
                                _ => unreachable!("validated typed row"),
                            }
                        })))
                    }
                    DataType::Bytes => {
                        Arc::new(BinaryArray::from_iter_values(rows.iter().map(|row| {
                            match &row.values[slot - 1] {
                                Value::Bytes(v) => v.as_slice(),
                                _ => unreachable!("validated typed row"),
                            }
                        })))
                    }
                    _ => Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| {
                        match &row.values[slot - 1] {
                            Value::Int(n) | Value::Decimal(n) | Value::Timestamp(n) => *n,
                            Value::Bool(v) => i64::from(*v),
                            _ => unreachable!("validated typed row"),
                        }
                    }))),
                })
            })
            .collect::<DfResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )?;
        self.source.snapshot.fence().map_err(external)?;
        self.budget.check().map_err(external)?;
        Ok(Some(batch))
    }
}

/// Rebind tiny indexed reads without re-running optimizer/physical planning.
/// DataFusion still parses/types the plan. Any coercion/shape outside this narrow
/// proof is left to its ordinary executor.
struct DirectRead {
    source: Source,
    probe: Option<super::probe::PointProbe>,
    projection: Vec<usize>,
    column: usize,
    binding: Binding,
    columns: Vec<String>,
    column_bytes: usize,
}
enum Binding {
    Literal(SqlValue),
    Parameter(usize),
}
struct DirectShape {
    source: Source,
    projection: Vec<usize>,
    predicate: Option<(usize, Binding)>,
}
impl DirectRead {
    fn compile(plan: &LogicalPlan) -> Option<Self> {
        let shape = Self::shape(plan)?;
        let (column, binding) = shape.predicate?;
        let columns: Vec<_> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let column_bytes = columns
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(c.len()))?;
        Some(Self {
            probe: super::probe::PointProbe::compile(
                &shape.source.snapshot,
                &shape.source.declaration.name,
                column,
            ),
            source: shape.source,
            projection: shape.projection,
            column,
            binding,
            columns,
            column_bytes,
        })
    }
    fn shape(plan: &LogicalPlan) -> Option<DirectShape> {
        match plan {
            LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
                let provider = datafusion::datasource::source_as_provider(&scan.source).ok()?;
                let table =
                    (provider.as_ref() as &dyn std::any::Any).downcast_ref::<PagedTable>()?;
                Some(DirectShape {
                    source: table.source.clone(),
                    projection: scan
                        .projection
                        .clone()
                        .unwrap_or_else(|| (0..table.schema.fields().len()).collect()),
                    predicate: None,
                })
            }
            LogicalPlan::SubqueryAlias(alias) => Self::shape(&alias.input),
            LogicalPlan::Projection(projection) => {
                let mut shape = Self::shape(&projection.input)?;
                shape.projection = projection
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = if let Expr::Alias(alias) = expr {
                            alias.expr.as_ref()
                        } else {
                            expr
                        };
                        let Expr::Column(column) = expr else {
                            return None;
                        };
                        shape
                            .projection
                            .get(projection.input.schema().index_of_column(column).ok()?)
                            .copied()
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(shape)
            }
            LogicalPlan::Filter(filter) => {
                let mut shape = Self::shape(&filter.input)?;
                if shape.predicate.is_some() {
                    return None;
                }
                let Expr::BinaryExpr(binary) = &filter.predicate else {
                    return None;
                };
                if binary.op != Operator::Eq {
                    return None;
                }
                let (column, value) = match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(column), value) | (value, Expr::Column(column)) => {
                        (column, value)
                    }
                    _ => return None,
                };
                let column = *shape
                    .projection
                    .get(filter.input.schema().index_of_column(column).ok()?)?;
                let binding = match value {
                    Expr::Placeholder(p) => Binding::Parameter(
                        p.id.strip_prefix('$')?
                            .parse::<usize>()
                            .ok()?
                            .checked_sub(1)?,
                    ),
                    Expr::Literal(value, _) => Binding::Literal(sql_value(value.clone()).ok()?),
                    _ => return None,
                };
                shape.predicate = Some((column, binding));
                Some(shape)
            }
            _ => None,
        }
    }
    fn query(&self, params: &[SqlValue], limits: QueryLimits) -> Result<Option<SqlTable>> {
        use crate::datafusion_sql::direct::{Key, key};
        let binding = match &self.binding {
            Binding::Literal(value) => value,
            Binding::Parameter(i) => params
                .get(*i)
                .ok_or_else(|| query_error("SQL parameter count mismatch"))?,
        };
        let mut bytes = self.column_bytes;
        if bytes > limits.max_bytes {
            return Err(query_error("query byte budget exceeded"));
        }
        let key = key(
            binding,
            self.column
                .checked_sub(1)
                .map(|slot| &self.source.declaration.roles[slot].data_type),
        );
        let value = match key {
            Key::Unsupported => return Ok(None),
            Key::Empty => {
                return Ok(Some(SqlTable {
                    columns: self.columns.clone(),
                    rows: vec![],
                }));
            }
            Key::Value(value) => value,
        };
        let budget = ScanBudget::new(limits.timeout_ms);
        let mut check = || budget.check();
        let mut result = SqlTable {
            columns: self.columns.clone(),
            rows: Vec::new(),
        };
        let mut work = 0u64;
        if let Some(probe) = &self.probe {
            if let Some(row) = probe.read(&value, &mut check)? {
                self.append(&row, &mut result, limits, &mut bytes, &mut work)?;
            }
            // The enclosing query gates both success and errors with its final fence.
            check()?;
            return Ok(Some(result));
        }
        let mut after = None;
        loop {
            self.source.snapshot.fence()?;
            check()?;
            let (rows, next) = if self.column == 0 {
                let Value::Ref(id) = value else {
                    return Err(query_error("invalid identity probe"));
                };
                (
                    self.source
                        .snapshot
                        .get_checked(&self.source.declaration.name, id, &mut check)?
                        .into_iter()
                        .collect::<Vec<_>>(),
                    None,
                )
            } else {
                let page = self.source.snapshot.page_checked(
                    &self.source.declaration.name,
                    Some((&self.source.declaration.roles[self.column - 1].name, &value)),
                    after,
                    256.min(self.source.snapshot.db.inner.options.max_page_rows),
                    &mut check,
                )?;
                (page.rows, page.next)
            };
            if rows.len() > limits.max_rows.saturating_sub(result.rows.len()) {
                return Err(query_error("query row budget exceeded; use LIMIT or pages"));
            }
            for row in rows {
                self.append(&row, &mut result, limits, &mut bytes, &mut work)?;
            }
            after = next;
            if after.is_none() {
                break;
            }
        }
        check()?;
        Ok(Some(result))
    }
    fn append(
        &self,
        row: &Row,
        result: &mut SqlTable,
        limits: QueryLimits,
        bytes: &mut usize,
        work: &mut u64,
    ) -> Result<()> {
        if result.rows.len() >= limits.max_rows {
            return Err(query_error("query row budget exceeded; use LIMIT or pages"));
        }
        *work = work.saturating_add(self.projection.len().max(1) as u64);
        if *work > limits.vm_steps {
            return Err(query_error("query output-cell budget exceeded"));
        }
        let mut output = Vec::with_capacity(self.projection.len());
        for &slot in &self.projection {
            let length = if slot == 0 {
                36
            } else {
                match &row.values[slot - 1] {
                    Value::Text(v) => v.len(),
                    Value::Bytes(v) => v.len(),
                    Value::Ref(_) => 36,
                    _ => 8,
                }
            };
            if length > limits.max_bytes.saturating_sub(*bytes) {
                return Err(query_error("query byte budget exceeded"));
            }
            *bytes += length;
            output.push(if slot == 0 {
                SqlValue::Text(row.id.to_string())
            } else {
                match &row.values[slot - 1] {
                    Value::Text(v) => SqlValue::Text(v.clone()),
                    Value::Bytes(v) => SqlValue::Blob(v.clone()),
                    Value::Null => SqlValue::Null,
                    Value::Float(v) => SqlValue::Real(v.get()),
                    Value::Ref(id) => SqlValue::Text(id.to_string()),
                    Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
                    Value::Int(n) | Value::Decimal(n) | Value::Timestamp(n) => {
                        SqlValue::Integer(*n)
                    }
                }
            });
        }
        if self.probe.is_some() {
            // A compiled point probe can return at most one row.
            result.rows.reserve_exact(1);
        }
        result.rows.push(output);
        Ok(())
    }
}

#[cfg(test)]
mod release_gate_tests {
    use super::*;

    #[test]
    fn outer_gate_rejects_revocation_after_direct_success_empty_and_budget_error() {
        for mode in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let db = PagedDatabase::open(dir.path().join("gate"), Default::default()).unwrap();
            let mut tx = db.admin().transaction().unwrap();
            tx.install_owned(
                Module::new(
                    "gate",
                    1,
                    vec![],
                    vec![Relation::new(
                        "gate.Person",
                        vec![Role::new("name", DataType::Text)],
                    )],
                ),
                "owner",
                Visibility::Private,
            )
            .unwrap();
            let id = tx
                .insert("gate.Person", vec![Value::Text("private payload".into())])
                .unwrap();
            tx.grant("reader", "gate.Person", Grant::read_only())
                .unwrap();
            tx.commit().unwrap();
            let sql = db
                .session("reader")
                .unwrap()
                .snapshot()
                .unwrap()
                .sql()
                .unwrap();
            let mut query = sql
                .prepare("SELECT name FROM gate.Person WHERE id=?1")
                .unwrap();
            assert!(
                query
                    .direct
                    .as_ref()
                    .is_some_and(|direct| direct.probe.is_some())
            );
            let parameter = if mode == 1 {
                SqlValue::Null
            } else {
                SqlValue::Text(id.to_string())
            };
            let limits = QueryLimits {
                max_bytes: if mode == 2 { 4 } else { 1024 },
                ..Default::default()
            };
            let before = query.query_with_limits(std::slice::from_ref(&parameter), limits);
            match mode {
                0 => assert_eq!(
                    before.unwrap().rows,
                    vec![vec![SqlValue::Text("private payload".into())]]
                ),
                1 => assert!(before.unwrap().rows.is_empty()),
                _ => assert!(matches!(before, Err(Error::Query(_)))),
            }
            query.after_direct_test_hook = Some(Box::new(move || {
                let mut tx = db.admin().transaction().unwrap();
                tx.revoke("reader", "gate.Person").unwrap();
                tx.commit().unwrap();
            }));
            assert!(matches!(
                query.query_with_limits(&[parameter], limits),
                Err(Error::Denied)
            ));
        }
    }
}
