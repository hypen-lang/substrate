//! Lazy, authorized native-index access for DataFusion. SQL predicates are only
//! claimed exact when their typed comparison agrees with the native value order.
use crate::{DataType, Error, Id, Relation, Row, Snapshot, Value, dx::NumericRange};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryArray, Int64Array, StringArray},
        compute::SortOptions,
        datatypes::{DataType as ArrowType, Field, Schema, SchemaRef},
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result, tree_node::TreeNodeRecursion},
    execution::TaskContext,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        sort_pushdown::SortOrderPushdownResult,
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    ops::Bound,
    sync::{Arc, Mutex},
};

const PAGE_ROWS: usize = 1000;
pub(super) const SOURCE_ROWS: usize = 1_000_000;
pub(super) const SOURCE_BYTES: usize = 128 * 1024 * 1024;
pub(super) const CELL_BYTES: usize = 1024 * 1024;

fn external(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
fn execution(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

/// A new instance belongs to one execution, shared by all its native table scans.
/// It must be attached to TaskContext, never retained by a cached physical plan.
#[derive(Debug, Default)]
pub(super) struct SourceBudget(Mutex<(usize, usize)>);
impl SourceBudget {
    pub(super) fn charge(&self, rows: usize, bytes: usize) -> Result<()> {
        let mut used = self
            .0
            .lock()
            .map_err(|_| execution("SQL source budget poisoned"))?;
        let next = (used.0.saturating_add(rows), used.1.saturating_add(bytes));
        if next.0 > SOURCE_ROWS || next.1 > SOURCE_BYTES {
            return Err(execution("SQL source row or byte budget exceeded"));
        }
        *used = next;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PageKey {
    relation: Id,
    projection: Vec<usize>,
    after: Option<Id>,
    range: Option<RangeKey>,
}
/// Values are typed native values, never strings/JSON used as executable filters.
/// Include all pushed predicates, not just the selected index bounds: additional
/// role predicates can change the emitted rows within the same candidate page.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RangeKey {
    slot: usize,
    lower: Bound<Value>,
    upper: Bound<Value>,
    descending: bool,
    predicates: Vec<Predicate>,
    after: Option<(Value, Id)>,
}
impl PageKey {
    fn heap_bytes(&self) -> usize {
        fn value_bytes(value: &Value) -> usize {
            match value {
                Value::Text(value) => value.capacity(),
                Value::Bytes(value) => value.capacity(),
                _ => 0,
            }
        }
        fn bound_bytes(bound: &Bound<Value>) -> usize {
            match bound {
                Bound::Included(value) | Bound::Excluded(value) => value_bytes(value),
                Bound::Unbounded => 0,
            }
        }
        let mut size = self
            .projection
            .capacity()
            .saturating_mul(std::mem::size_of::<usize>());
        if let Some(range) = &self.range {
            size = size.saturating_add(
                range
                    .predicates
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Predicate>()),
            );
            for predicate in &range.predicates {
                size = size.saturating_add(value_bytes(&predicate.value));
            }
            size = size
                .saturating_add(bound_bytes(&range.lower))
                .saturating_add(bound_bytes(&range.upper));
            if let Some((value, _)) = &range.after {
                size = size.saturating_add(value_bytes(value));
            }
        }
        size
    }
}
#[derive(Clone)]
struct CachedPage {
    batch: RecordBatch,
    next: Option<Id>,
    range_next: Option<(Value, Id)>,
    source_rows: usize,
    payload_bytes: usize,
    estimated_bytes: usize,
}
impl CachedPage {
    fn new(
        batch: RecordBatch,
        next: Option<Id>,
        range_next: Option<(Value, Id)>,
        source_rows: usize,
        payload_bytes: usize,
        key: &PageKey,
    ) -> Self {
        // Include bounded bookkeeping, ordered projection keys, and batch column
        // pointers in addition to Arrow buffer estimates. This is accounting,
        // not a hard RSS bound; shared schema metadata belongs to the SQL handle.
        let estimated_bytes = batch
            .get_array_memory_size()
            .saturating_add(key.heap_bytes().saturating_mul(2))
            .saturating_add(
                batch
                    .num_columns()
                    .saturating_mul(std::mem::size_of::<ArrayRef>()),
            )
            .saturating_add(std::mem::size_of::<PageKey>().saturating_mul(2))
            .saturating_add(std::mem::size_of::<CachedPage>())
            // HashMap buckets, VecDeque slots, and allocation bookkeeping.
            .saturating_add(256);
        Self {
            batch,
            next,
            range_next,
            source_rows,
            payload_bytes,
            estimated_bytes,
        }
    }
}
#[derive(Default)]
struct CachedPages {
    pages: HashMap<PageKey, CachedPage>,
    order: VecDeque<PageKey>,
    bytes: usize,
}
/// A bounded FIFO of immutable authorized pages, private to one SQL snapshot.
/// Full scans and unlimited numeric ranges can reuse projected pages. Identity
/// and equality indexes remain direct; no caller can detach cached Arrow data.
pub(super) struct BatchCache {
    inner: Mutex<CachedPages>,
    max_bytes: usize,
    max_entries: usize,
}
impl Default for BatchCache {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024, 4096)
    }
}
impl BatchCache {
    fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(CachedPages::default()),
            max_bytes,
            max_entries,
        }
    }
    fn get(&self, key: &PageKey) -> Result<Option<CachedPage>> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| execution("SQL page cache poisoned"))?
            .pages
            .get(key)
            .cloned())
    }
    fn insert(&self, key: PageKey, page: CachedPage) -> Result<()> {
        if page.estimated_bytes > self.max_bytes || self.max_entries == 0 {
            return Ok(());
        }
        let mut cache = self
            .inner
            .lock()
            .map_err(|_| execution("SQL page cache poisoned"))?;
        // Concurrent misses can compute the same page; retain just one charge.
        if cache.pages.contains_key(&key) {
            return Ok(());
        }
        while cache.pages.len() >= self.max_entries
            || cache.bytes.saturating_add(page.estimated_bytes) > self.max_bytes
        {
            let oldest = cache
                .order
                .pop_front()
                .ok_or_else(|| execution("SQL page cache accounting mismatch"))?;
            if let Some(removed) = cache.pages.remove(&oldest) {
                cache.bytes -= removed.estimated_bytes;
            }
        }
        cache.bytes = cache
            .bytes
            .checked_add(page.estimated_bytes)
            .ok_or_else(|| execution("SQL page cache byte overflow"))?;
        cache.order.push_back(key.clone());
        cache.pages.insert(key, page);
        Ok(())
    }
}
#[derive(Clone)]
struct Source {
    snapshot: Snapshot,
    declaration: Arc<Relation>,
    cache: Arc<BatchCache>,
}
pub(super) struct LiquidTable {
    source: Source,
    schema: SchemaRef,
}
impl fmt::Debug for LiquidTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiquidTable")
            .field("relation", &self.source.declaration.name)
            .finish_non_exhaustive()
    }
}
impl LiquidTable {
    pub(super) fn snapshot(&self) -> &Snapshot {
        &self.source.snapshot
    }
    pub(super) fn declaration(&self) -> &Relation {
        &self.source.declaration
    }
    pub(super) fn new(
        snapshot: Snapshot,
        declaration: Relation,
        cache: Arc<BatchCache>,
    ) -> crate::Result<Self> {
        snapshot.live.check_attached()?;
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
        Ok(Self {
            source: Source {
                snapshot,
                declaration: Arc::new(declaration),
                cache,
            },
            schema,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Predicate {
    column: usize,
    operator: Operator,
    value: Value,
}
impl Predicate {
    fn matches(&self, row: &Row) -> bool {
        let identity;
        let actual = if self.column == 0 {
            identity = Value::Ref(row.id);
            &identity
        } else {
            &row.values[self.column - 1]
        };
        match self.operator {
            Operator::Eq => actual == &self.value,
            Operator::Lt => actual < &self.value,
            Operator::LtEq => actual <= &self.value,
            Operator::Gt => actual > &self.value,
            Operator::GtEq => actual >= &self.value,
            _ => false,
        }
    }
}

/// None means no pushdown: never guess at DataFusion coercion or cast semantics.
fn predicates(expr: &Expr, declaration: &Relation) -> Option<Vec<Predicate>> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        let mut left = predicates(&binary.left, declaration)?;
        left.extend(predicates(&binary.right, declaration)?);
        return Some(left);
    }
    let (column, scalar, operator) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(scalar, _)) => (column, scalar, binary.op),
        (Expr::Literal(scalar, _), Expr::Column(column)) => (
            column,
            scalar,
            match binary.op {
                Operator::Eq => Operator::Eq,
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                _ => return None,
            },
        ),
        _ => return None,
    };
    if !matches!(
        operator,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return None;
    }
    let text = || match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(v.as_str()),
        _ => None,
    };
    let number = || match scalar {
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    };
    let (slot, value) = if column.name == "id" {
        if operator != Operator::Eq {
            return None;
        }
        let raw = text()?;
        let id = Id(raw.parse::<uuid::Uuid>().ok()?);
        // Native identity parsing accepts noncanonical UUID text; SQL text equality
        // does not. Leave noncanonical literals to DataFusion's residual filter.
        if id.to_string() != raw {
            return None;
        }
        (0, Value::Ref(id))
    } else {
        let slot = declaration
            .roles
            .iter()
            .position(|r| r.name == column.name)?;
        let value = match declaration.roles[slot].data_type {
            DataType::Text if operator == Operator::Eq => Value::Text(text()?.into()),
            DataType::Bytes if operator == Operator::Eq => match scalar {
                ScalarValue::Binary(Some(v))
                | ScalarValue::LargeBinary(Some(v))
                | ScalarValue::BinaryView(Some(v)) => Value::Bytes(v.clone()),
                _ => return None,
            },
            DataType::Ref(_) if operator == Operator::Eq => {
                let raw = text()?;
                let id = Id(raw.parse::<uuid::Uuid>().ok()?);
                if id.to_string() != raw {
                    return None;
                }
                Value::Ref(id)
            }
            DataType::Int => Value::Int(number()?),
            DataType::Timestamp => Value::Timestamp(number()?),
            DataType::Decimal { .. } => Value::Decimal(number()?),
            DataType::Bool => Value::Bool(match number()? {
                0 => false,
                1 => true,
                _ => return None,
            }),
            _ => return None,
        };
        (slot + 1, value)
    };
    Some(vec![Predicate {
        column: slot,
        operator,
        value,
    }])
}

#[derive(Clone)]
enum Access {
    Identity(Id),
    Equality { slot: usize, value: Value },
    Range(NumericRange),
    Scan,
}
impl Access {
    fn label(&self) -> &'static str {
        match self {
            Self::Identity(_) => "identity",
            Self::Equality { .. } => "role_equality",
            Self::Range(_) => "numeric_range",
            Self::Scan => "family_scan",
        }
    }
}
fn access(predicates: &[Predicate], table: &LiquidTable) -> Access {
    if let Some(id) = predicates
        .iter()
        .find_map(|p| match (&p.value, p.column, p.operator) {
            (Value::Ref(id), 0, Operator::Eq) => Some(*id),
            _ => None,
        })
    {
        return Access::Identity(id);
    }
    if let Some(p) = predicates
        .iter()
        .filter(|p| p.column > 0 && p.operator == Operator::Eq)
        .min_by_key(|p| {
            table
                .source
                .snapshot
                .state
                .index
                .get(&(table.source.declaration.id, p.column - 1, p.value.clone()))
                .map_or(0, |ids| ids.len())
        })
    {
        return Access::Equality {
            slot: p.column - 1,
            value: p.value.clone(),
        };
    }
    if let Some(p) = predicates
        .iter()
        .find(|p| p.column > 0 && p.operator != Operator::Eq)
    {
        let mut range = NumericRange {
            slot: p.column - 1,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            descending: false,
        };
        // Intersect all bounds for the selected role. Other roles are rechecked
        // on candidates; contradictory bounds produce an empty native range.
        for predicate in predicates.iter().filter(|other| other.column == p.column) {
            let (bound, lower) = match predicate.operator {
                Operator::Lt => (Bound::Excluded(predicate.value.clone()), false),
                Operator::LtEq => (Bound::Included(predicate.value.clone()), false),
                Operator::Gt => (Bound::Excluded(predicate.value.clone()), true),
                Operator::GtEq => (Bound::Included(predicate.value.clone()), true),
                _ => continue,
            };
            let target = if lower {
                &mut range.lower
            } else {
                &mut range.upper
            };
            if tighter(&bound, target, lower) {
                *target = bound;
            }
        }
        return Access::Range(range);
    }
    Access::Scan
}

fn tighter(candidate: &Bound<Value>, current: &Bound<Value>, lower: bool) -> bool {
    let (Bound::Included(value) | Bound::Excluded(value)) = candidate else {
        return false;
    };
    let (Bound::Included(old) | Bound::Excluded(old)) = current else {
        return true;
    };
    if value == old {
        matches!(candidate, Bound::Excluded(_)) && matches!(current, Bound::Included(_))
    } else if lower {
        value > old
    } else {
        value < old
    }
}

#[async_trait]
impl TableProvider for LiquidTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.source
            .snapshot
            .live
            .check_attached()
            .map_err(external)?;
        let predicates = filters
            .iter()
            .map(|expr| {
                predicates(expr, &self.source.declaration)
                    .ok_or_else(|| execution("unsupported pushed SQL filter"))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let access = access(&predicates, self);
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = Arc::new(self.schema.project(&projection)?);
        Ok(Arc::new(LiquidScanExec::new(
            self.source.clone(),
            schema,
            projection,
            predicates,
            access,
            limit,
        )))
    }
}

pub(super) struct LiquidScanExec {
    source: Source,
    projection: Vec<usize>,
    predicates: Vec<Predicate>,
    access: Access,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}
impl LiquidScanExec {
    /// A bound over authorized candidates, not hidden raw index cardinalities.
    /// It is used only for choosing bounded join probes, never exposed as stats.
    pub(super) fn selective_upper_bound(&self) -> Option<usize> {
        if self.limit.is_some() {
            return None;
        }
        match &self.access {
            Access::Identity(_) => Some(1),
            Access::Equality { slot, value } => {
                let page = self
                    .source
                    .snapshot
                    .page_shared(
                        &self.source.declaration.name,
                        Some((&self.source.declaration.roles[*slot].name, value)),
                        None,
                        super::joins::MAX_INPUT_ROWS + 1,
                    )
                    .ok()?;
                page.next.is_none().then_some(page.rows.len())
            }
            _ => None,
        }
    }
    pub(super) fn probe_capable(&self, output_column: usize) -> bool {
        self.limit.is_none()
            && self.projection.get(output_column).is_some_and(|column| {
                *column == 0
                    || !matches!(
                        self.source.declaration.roles[*column - 1].data_type,
                        DataType::Nullable(_) | DataType::Float
                    )
            })
    }
    pub(super) fn probe_result_upper_bound(&self, output_column: usize) -> Option<usize> {
        if !self.probe_capable(output_column) {
            return None;
        }
        if self.projection[output_column] == 0 {
            Some(1)
        } else {
            self.selective_upper_bound()
        }
    }
    /// Build a fresh scan for one exact join key. This never substitutes parsing
    /// or casts for SQL equality, and cannot cross a source LIMIT boundary.
    pub(super) fn probe_scan(
        &self,
        output_column: usize,
        value: &ScalarValue,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.source
            .snapshot
            .live
            .check_attached()
            .map_err(external)?;
        if !self.probe_capable(output_column) {
            return Err(execution("unsupported native join probe"));
        }
        if value.is_null() {
            return Ok(None);
        }
        if value.data_type() != *self.schema().field(output_column).data_type() {
            return Err(execution("native join probe type mismatch"));
        }
        let column = self.projection[output_column];
        let name = if column == 0 {
            "id"
        } else {
            &self.source.declaration.roles[column - 1].name
        };
        let expr = Expr::Column(datafusion::common::Column::new_unqualified(name))
            .eq(Expr::Literal(value.clone(), None));
        let Some(mut extra) = predicates(&expr, &self.source.declaration) else {
            // Canonical references and Bool's 0/1 domain cannot equal other
            // literal representations, even if their Arrow types agree.
            return Ok(None);
        };
        let key = extra[0].value.clone();
        let access = if column == 0 {
            let Value::Ref(id) = key else {
                return Err(execution("invalid identity probe"));
            };
            Access::Identity(id)
        } else {
            Access::Equality {
                slot: column - 1,
                value: key,
            }
        };
        let mut predicates = self.predicates.clone();
        predicates.append(&mut extra);
        Ok(Some(Arc::new(Self::new(
            self.source.clone(),
            self.schema(),
            self.projection.clone(),
            predicates,
            access,
            None,
        ))))
    }
    fn new(
        source: Source,
        schema: SchemaRef,
        projection: Vec<usize>,
        predicates: Vec<Predicate>,
        access: Access,
        limit: Option<usize>,
    ) -> Self {
        // Numeric ranges traverse (typed value, occurrence ID); plain/equality
        // scans traverse occurrence IDs. Filtering preserves their order. Only
        // advertise a surviving lexicographic prefix after projection: projecting
        // away a varying leading range value does not make IDs globally sorted.
        let ordered_columns = match &access {
            Access::Range(range) => vec![range.slot + 1, 0],
            _ => vec![0],
        };
        let descending = matches!(&access, Access::Range(range) if range.descending);
        let mut ordering = Vec::new();
        for column in ordered_columns {
            let Some(projected) = projection.iter().position(|index| *index == column) else {
                break;
            };
            ordering.push(PhysicalSortExpr::new(
                Arc::new(Column::new(schema.field(projected).name(), projected)),
                SortOptions {
                    descending,
                    nulls_first: true,
                },
            ));
        }
        let properties = EquivalenceProperties::new_with_orderings(schema, [ordering]);
        Self {
            source,
            projection,
            predicates,
            access,
            limit,
            properties: Arc::new(PlanProperties::new(
                properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}
impl fmt::Debug for LiquidScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}
impl DisplayAs for LiquidScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LiquidScanExec: relation={}, access={}, projection={:?}, limit={:?}",
            self.source.declaration.name,
            self.access.label(),
            self.projection,
            self.limit
        )
    }
}
impl ExecutionPlan for LiquidScanExec {
    fn name(&self) -> &'static str {
        "LiquidScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(execution("native table scan cannot have children"));
        }
        Ok(Arc::new(Self::new(
            self.source.clone(),
            self.schema(),
            self.projection.clone(),
            self.predicates.clone(),
            self.access.clone(),
            self.limit,
        )))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn fetch(&self) -> Option<usize> {
        self.limit
    }
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        // This leaf applies its own exact predicates before counting emitted
        // rows. Residual filters and offsets remain outside and DataFusion must
        // retain their barriers when deciding whether a fetch can reach us.
        let limit = match (self.limit, limit) {
            (Some(old), Some(new)) => Some(old.min(new)),
            (old, new) => old.or(new),
        };
        Some(Arc::new(Self::new(
            self.source.clone(),
            self.schema(),
            self.projection.clone(),
            self.predicates.clone(),
            self.access.clone(),
            limit,
        )))
    }
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // Reversing an already-limited source would choose a different subset.
        // The native range cursor can reverse both value and ID, but cannot
        // satisfy mixed directions or unrelated expressions/leading columns.
        let Access::Range(range) = &self.access else {
            return Ok(SortOrderPushdownResult::Unsupported);
        };
        if self.limit.is_some() || order.is_empty() || order.len() > 2 {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        let descending = order[0].options.descending;
        for (index, sort) in order.iter().enumerate() {
            let Some(column) = (sort.expr.as_ref() as &dyn std::any::Any).downcast_ref::<Column>()
            else {
                return Ok(SortOrderPushdownResult::Unsupported);
            };
            let expected = if index == 0 { range.slot + 1 } else { 0 };
            if self.projection.get(column.index()) != Some(&expected)
                || sort.options.descending != descending
                || self.schema().field(column.index()).is_nullable()
            {
                return Ok(SortOrderPushdownResult::Unsupported);
            }
        }
        let mut range = range.clone();
        range.descending = descending;
        Ok(SortOrderPushdownResult::Exact {
            inner: Arc::new(Self::new(
                self.source.clone(),
                self.schema(),
                self.projection.clone(),
                self.predicates.clone(),
                Access::Range(range),
                None,
            )),
        })
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(execution("invalid native scan partition"));
        }
        self.source
            .snapshot
            .live
            .check_attached()
            .map_err(external)?;
        let cursor = Cursor {
            source: self.source.clone(),
            schema: self.schema(),
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            access: self.access.clone(),
            remaining: self.limit.unwrap_or(usize::MAX),
            after: None,
            range_after: None,
            done: false,
            started: false,
            budget: context
                .session_config()
                .get_extension::<SourceBudget>()
                .unwrap_or_else(|| Arc::new(SourceBudget::default())),
            source_rows: MetricBuilder::new(&self.metrics).counter("source_rows", partition),
            output_rows: MetricBuilder::new(&self.metrics).counter("returned_rows", partition),
            cache_hits: MetricBuilder::new(&self.metrics).counter("cache_hits", partition),
            cacheable: (matches!(self.access, Access::Scan) && self.predicates.is_empty()
                || matches!(self.access, Access::Range(_)))
                && self.limit.is_none(),
        };
        let stream = futures::stream::try_unfold(cursor, |mut cursor| async move {
            if cursor.started && !cursor.done && cursor.remaining > 0 {
                tokio::task::yield_now().await;
            }
            cursor.started = true;
            cursor
                .next_batch()
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
    range_after: Option<(Value, Id)>,
    done: bool,
    started: bool,
    budget: Arc<SourceBudget>,
    source_rows: Count,
    output_rows: Count,
    cache_hits: Count,
    cacheable: bool,
}
impl Cursor {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.source
            .snapshot
            .live
            .check_attached()
            .map_err(external)?;
        if self.done || self.remaining == 0 {
            return Ok(None);
        }
        let cache_key = self.cacheable.then(|| PageKey {
            relation: self.source.declaration.id,
            projection: self.projection.clone(),
            after: self.after,
            range: match &self.access {
                Access::Range(range) => Some(RangeKey {
                    slot: range.slot,
                    lower: range.lower.clone(),
                    upper: range.upper.clone(),
                    descending: range.descending,
                    predicates: self.predicates.clone(),
                    after: self.range_after.clone(),
                }),
                _ => None,
            },
        });
        if let Some(key) = &cache_key
            && let Some(page) = self.source.cache.get(key)?
        {
            let rows = page.batch.num_rows();
            // A filtered range page can emit fewer rows than the native cursor
            // examined. Repeated executions must pay the original candidate charge.
            self.budget.charge(page.source_rows, page.payload_bytes)?;
            self.after = page.next;
            self.range_after = page.range_next;
            self.done = if matches!(self.access, Access::Range(_)) {
                self.range_after.is_none()
            } else {
                self.after.is_none()
            };
            self.remaining -= rows;
            self.source_rows.add(page.source_rows);
            self.output_rows.add(rows);
            self.cache_hits.add(1);
            self.source
                .snapshot
                .live
                .check_attached()
                .map_err(external)?;
            return Ok(Some(page.batch));
        }
        let page_size = PAGE_ROWS.min(self.remaining);
        let rows = match &self.access {
            Access::Identity(id) => {
                self.done = true;
                self.source
                    .snapshot
                    .get_shared(&self.source.declaration.name, *id)
                    .map_err(external)?
                    .into_iter()
                    .collect::<Vec<_>>()
            }
            Access::Equality { slot, value } => {
                let page = self
                    .source
                    .snapshot
                    .page_shared(
                        &self.source.declaration.name,
                        Some((&self.source.declaration.roles[*slot].name, value)),
                        self.after,
                        page_size,
                    )
                    .map_err(external)?;
                self.after = page.next;
                self.done = self.after.is_none();
                page.rows
            }
            Access::Range(range) => {
                let page = self
                    .source
                    .snapshot
                    .range_page_shared(
                        &self.source.declaration.name,
                        range,
                        self.range_after.as_ref(),
                        page_size,
                    )
                    .map_err(external)?;
                self.range_after = page.next;
                self.done = self.range_after.is_none();
                page.rows
            }
            Access::Scan => {
                let page = self
                    .source
                    .snapshot
                    .page_shared(&self.source.declaration.name, None, self.after, page_size)
                    .map_err(external)?;
                self.after = page.next;
                self.done = self.after.is_none();
                page.rows
            }
        };
        let candidate_rows = rows.len();
        self.source_rows.add(candidate_rows);
        self.budget.charge(candidate_rows, 0)?;
        let rows = rows
            .into_iter()
            .filter(|row| self.predicates.iter().all(|p| p.matches(row)))
            .take(self.remaining)
            .collect::<Vec<_>>();
        let (batch, payload_bytes) = batch(
            &self.schema,
            &self.projection,
            &self.source.declaration,
            &rows,
            &self.budget,
        )?;
        self.remaining -= rows.len();
        self.output_rows.add(rows.len());
        self.source
            .snapshot
            .live
            .check_attached()
            .map_err(external)?;
        if let Some(key) = cache_key {
            let page = CachedPage::new(
                batch.clone(),
                self.after,
                self.range_after.clone(),
                candidate_rows,
                payload_bytes,
                &key,
            );
            self.source.cache.insert(key, page)?;
        }
        Ok(Some(batch))
    }
}

fn batch(
    schema: &SchemaRef,
    projection: &[usize],
    declaration: &Relation,
    rows: &[Arc<Row>],
    budget: &SourceBudget,
) -> Result<(RecordBatch, usize)> {
    let mut bytes = 0usize;
    for row in rows {
        for &column in projection {
            let size = if column == 0 {
                40
            } else {
                match &row.values[column - 1] {
                    Value::Text(value) => {
                        if value.len() > CELL_BYTES {
                            return Err(execution("SQL source cell exceeds 1 MiB budget"));
                        }
                        value.len().saturating_add(4)
                    }
                    Value::Bytes(value) => {
                        if value.len() > CELL_BYTES {
                            return Err(execution("SQL source cell exceeds 1 MiB budget"));
                        }
                        value.len().saturating_add(4)
                    }
                    Value::Ref(_) => 40,
                    _ => 8,
                }
            };
            bytes = bytes.saturating_add(size);
        }
    }
    budget.charge(0, bytes)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &column in projection {
        let array: ArrayRef = if column == 0 {
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.id.to_string()).collect::<Vec<_>>(),
            ))
        } else {
            let slot = column - 1;
            match declaration.roles[slot].data_type {
                DataType::Nullable(_) | DataType::Float => {
                    crate::datafusion_sql::values::extended_array(
                        &declaration.roles[slot].data_type,
                        rows.iter().map(|r| &r.values[slot]),
                    )?
                }
                DataType::Text => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| match &r.values[slot] {
                            Value::Text(v) => Ok(v.as_str()),
                            _ => Err(execution("typed SQL source mismatch")),
                        })
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::Ref(_) => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| match &r.values[slot] {
                            Value::Ref(v) => Ok(v.to_string()),
                            _ => Err(execution("typed SQL source mismatch")),
                        })
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::Bytes => Arc::new(BinaryArray::from(
                    rows.iter()
                        .map(|r| match &r.values[slot] {
                            Value::Bytes(v) => Ok(v.as_slice()),
                            _ => Err(execution("typed SQL source mismatch")),
                        })
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| match r.values[slot] {
                            Value::Bool(v) => Ok(i64::from(v)),
                            Value::Int(v) | Value::Decimal(v) | Value::Timestamp(v) => Ok(v),
                            _ => Err(execution("typed SQL source mismatch")),
                        })
                        .collect::<Result<Vec<_>>>()?,
                )),
            }
        };
        arrays.push(array);
    }
    Ok((
        RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )?,
        bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_budget_is_shared_across_scans_and_counts_before_allocation() {
        let budget = Arc::new(SourceBudget::default());
        let second = budget.clone();
        budget.charge(SOURCE_ROWS - 1, SOURCE_BYTES - 1).unwrap();
        second.charge(1, 1).unwrap();
        assert!(budget.charge(1, 0).is_err());
        assert!(second.charge(0, 1).is_err());
        assert!(SourceBudget::default().charge(usize::MAX, 0).is_err());
    }

    fn empty_cached_page(key: &PageKey) -> CachedPage {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        CachedPage::new(batch, None, None, 1, 0, key)
    }
    fn page_key(number: u128) -> PageKey {
        PageKey {
            relation: Id::from_u128(number),
            projection: vec![],
            after: None,
            range: None,
        }
    }
    #[test]
    fn page_cache_fifo_bounds_duplicate_insert_and_oversized_pages() {
        let capacity = empty_cached_page(&page_key(1)).estimated_bytes * 2;
        let cache = BatchCache::new(capacity, 2);
        for number in 1..=3 {
            let key = page_key(number);
            cache.insert(key.clone(), empty_cached_page(&key)).unwrap();
        }
        assert!(cache.get(&page_key(1)).unwrap().is_none());
        assert!(cache.get(&page_key(2)).unwrap().is_some());
        assert!(cache.get(&page_key(3)).unwrap().is_some());
        let duplicate = page_key(2);
        cache
            .insert(duplicate.clone(), empty_cached_page(&duplicate))
            .unwrap();
        let state = cache.inner.lock().unwrap();
        assert_eq!(state.pages.len(), 2);
        assert_eq!(state.order.len(), 2);
        assert_eq!(state.bytes, capacity);
        drop(state);
        let mut oversized = empty_cached_page(&page_key(4));
        oversized.estimated_bytes = capacity + 1;
        cache.insert(page_key(4), oversized).unwrap();
        assert!(cache.get(&page_key(4)).unwrap().is_none());
        let entry_limited = BatchCache::new(usize::MAX, 1);
        for number in 1..=2 {
            let key = page_key(number);
            entry_limited
                .insert(key.clone(), empty_cached_page(&key))
                .unwrap();
        }
        assert!(entry_limited.get(&page_key(1)).unwrap().is_none());
        assert_eq!(entry_limited.inner.lock().unwrap().pages.len(), 1);
    }

    #[test]
    fn cached_page_reads_recharge_budgets_and_isolate_projection_order() {
        let db = crate::Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            crate::Module::new(
                "cache",
                1,
                vec![],
                vec![Relation::new(
                    "cache.Row",
                    vec![crate::Role::new("n", DataType::Int)],
                )],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        tx.insert("cache.Row", vec![Value::Int(7)]).unwrap();
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let table = LiquidTable::new(
            snapshot.clone(),
            snapshot
                .relations()
                .into_iter()
                .find(|r| r.name == "cache.Row")
                .unwrap(),
            Arc::new(BatchCache::default()),
        )
        .unwrap();
        let cursor = |budget| {
            let metrics = ExecutionPlanMetricsSet::new();
            Cursor {
                source: table.source.clone(),
                schema: Arc::new(table.schema.project(&[1]).unwrap()),
                projection: vec![1],
                predicates: vec![],
                access: Access::Scan,
                remaining: usize::MAX,
                after: None,
                range_after: None,
                done: false,
                started: false,
                budget,
                source_rows: MetricBuilder::new(&metrics).counter("source_rows", 0),
                output_rows: MetricBuilder::new(&metrics).counter("returned_rows", 0),
                cache_hits: MetricBuilder::new(&metrics).counter("cache_hits", 0),
                cacheable: true,
            }
        };
        let cold_budget = Arc::new(SourceBudget::default());
        cursor(cold_budget.clone()).next_batch().unwrap().unwrap();
        assert_eq!(*cold_budget.0.lock().unwrap(), (1, 8));
        assert_eq!(table.source.cache.inner.lock().unwrap().pages.len(), 1);
        let warm_budget = Arc::new(SourceBudget::default());
        let mut warm = cursor(warm_budget.clone());
        warm.next_batch().unwrap().unwrap();
        assert_eq!(warm.cache_hits.value(), 1);
        assert_eq!(*warm_budget.0.lock().unwrap(), (1, 8));
        let exhausted = Arc::new(SourceBudget::default());
        exhausted.charge(SOURCE_ROWS, 0).unwrap();
        assert!(cursor(exhausted).next_batch().is_err());
        let exhausted = Arc::new(SourceBudget::default());
        exhausted.charge(0, SOURCE_BYTES).unwrap();
        assert!(cursor(exhausted).next_batch().is_err());
        // Ordered projection participates in the key, even for equal-typed roles.
        let left = PageKey {
            projection: vec![0, 1],
            ..page_key(1)
        };
        let right = PageKey {
            projection: vec![1, 0],
            ..page_key(1)
        };
        table
            .source
            .cache
            .insert(left.clone(), empty_cached_page(&left))
            .unwrap();
        assert!(table.source.cache.get(&right).unwrap().is_none());
    }

    fn guarded_cursor(table: &LiquidTable) -> Cursor {
        let metrics = ExecutionPlanMetricsSet::new();
        Cursor {
            source: table.source.clone(),
            schema: Arc::new(table.schema.project(&[1]).unwrap()),
            projection: vec![1],
            predicates: vec![],
            access: Access::Scan,
            remaining: usize::MAX,
            after: None,
            range_after: None,
            done: false,
            started: false,
            budget: Arc::new(SourceBudget::default()),
            source_rows: MetricBuilder::new(&metrics).counter("source_rows", 0),
            output_rows: MetricBuilder::new(&metrics).counter("returned_rows", 0),
            cache_hits: MetricBuilder::new(&metrics).counter("cache_hits", 0),
            cacheable: true,
        }
    }
    fn authorized_fixture() -> crate::Database {
        let db = crate::Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            crate::Module::new(
                "cache",
                1,
                vec![],
                vec![Relation::new(
                    "cache.Row",
                    vec![crate::Role::new("n", DataType::Int)],
                )],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for n in 0..1001 {
            tx.insert("cache.Row", vec![Value::Int(n)]).unwrap();
        }
        tx.grant("reader", "cache.Row", crate::Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        db
    }
    fn warm_table(snapshot: Snapshot) -> LiquidTable {
        let table = LiquidTable::new(
            snapshot.clone(),
            snapshot
                .relations()
                .into_iter()
                .find(|r| r.name == "cache.Row")
                .unwrap(),
            Arc::new(BatchCache::default()),
        )
        .unwrap();
        let mut cold = guarded_cursor(&table);
        while cold.next_batch().unwrap().is_some() {}
        assert_eq!(table.source.cache.inner.lock().unwrap().pages.len(), 2);
        table
    }
    #[test]
    fn revocation_between_cached_pages_discards_further_reads() {
        let db = authorized_fixture();
        let table = warm_table(db.session("reader").snapshot());
        let mut cursor = guarded_cursor(&table);
        assert_eq!(cursor.next_batch().unwrap().unwrap().num_rows(), 1000);
        assert_eq!(cursor.cache_hits.value(), 1);
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "cache.Row").unwrap();
        tx.commit().unwrap();
        assert!(
            cursor
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("access denied")
        );
    }
    #[test]
    fn capability_expiry_between_cached_pages_discards_further_reads() {
        use crate::capability::{CapabilityScope, RelationCapability};
        let db = authorized_fixture();
        db.clock.set(1000);
        let cap = db
            .issue_capability_until(
                Id::new(),
                "reader",
                CapabilityScope {
                    relations: vec![RelationCapability {
                        relation: db.admin().snapshot().schema("cache.Row").unwrap().id,
                        grant: crate::Grant::read_only(),
                    }],
                    schema_namespace: None,
                    delegations: 0,
                },
                Some(1010),
            )
            .unwrap();
        let table = warm_table(
            db.session("reader")
                .with_capability(cap.id)
                .unwrap()
                .snapshot(),
        );
        let mut cursor = guarded_cursor(&table);
        assert_eq!(cursor.next_batch().unwrap().unwrap().num_rows(), 1000);
        assert_eq!(cursor.cache_hits.value(), 1);
        db.clock.set(1010);
        assert!(
            cursor
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("access denied")
        );
    }
    fn range_cursor(table: &LiquidTable) -> Cursor {
        let mut cursor = guarded_cursor(table);
        cursor.access = Access::Range(NumericRange {
            slot: 0,
            lower: Bound::Included(Value::Int(0)),
            upper: Bound::Unbounded,
            descending: false,
        });
        cursor.predicates = vec![Predicate {
            column: 1,
            operator: Operator::GtEq,
            value: Value::Int(0),
        }];
        cursor
    }

    #[test]
    fn range_cache_preserves_candidate_charges_and_empty_page_continuations() {
        let db = crate::Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            crate::Module::new(
                "cache",
                1,
                vec![],
                vec![Relation::new(
                    "cache.Row",
                    vec![
                        crate::Role::new("n", DataType::Int),
                        crate::Role::new("m", DataType::Int),
                    ],
                )],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for n in 0..1001 {
            tx.insert(
                "cache.Row",
                vec![Value::Int(n), Value::Int(i64::from(n == 1000))],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let table = LiquidTable::new(
            snapshot.clone(),
            snapshot
                .relations()
                .into_iter()
                .find(|r| r.name == "cache.Row")
                .unwrap(),
            Arc::new(BatchCache::default()),
        )
        .unwrap();
        let cursor = || {
            let mut cursor = range_cursor(&table);
            cursor.predicates.push(Predicate {
                column: 2,
                operator: Operator::Gt,
                value: Value::Int(0),
            });
            cursor
        };
        for warm in [false, true] {
            let mut current = cursor();
            assert_eq!(current.next_batch().unwrap().unwrap().num_rows(), 0);
            assert_eq!(*current.budget.0.lock().unwrap(), (1000, 0));
            assert_eq!(current.next_batch().unwrap().unwrap().num_rows(), 1);
            assert!(current.next_batch().unwrap().is_none());
            assert_eq!(*current.budget.0.lock().unwrap(), (1001, 8));
            assert_eq!(current.source_rows.value(), 1001);
            assert_eq!(current.cache_hits.value(), if warm { 2 } else { 0 });
        }
        for cold in [false, true] {
            let mut capped = cursor();
            if cold {
                capped.source.cache = Arc::new(BatchCache::default());
            }
            capped.budget.charge(SOURCE_ROWS - 999, 0).unwrap();
            assert!(capped.next_batch().is_err());
            if cold {
                assert!(capped.source.cache.inner.lock().unwrap().pages.is_empty());
            }
            let mut capped = cursor();
            if cold {
                capped.source.cache = Arc::new(BatchCache::default());
            }
            capped.budget.charge(0, SOURCE_BYTES).unwrap();
            assert_eq!(capped.next_batch().unwrap().unwrap().num_rows(), 0);
            assert!(capped.next_batch().is_err());
            if cold {
                // The successful empty page may be retained, but not the failed page.
                assert_eq!(capped.source.cache.inner.lock().unwrap().pages.len(), 1);
            }
        }
        let mut changed = cursor();
        changed.predicates[1].value = Value::Int(-1);
        assert_eq!(changed.next_batch().unwrap().unwrap().num_rows(), 1000);
        assert_eq!(changed.cache_hits.value(), 0);
    }

    #[test]
    fn range_cache_live_guards_recheck_between_pages() {
        use crate::capability::{CapabilityScope, RelationCapability};
        for expiry in [false, true] {
            let db = authorized_fixture();
            db.clock.set(1000);
            let snapshot = if expiry {
                let cap = db
                    .issue_capability_until(
                        Id::new(),
                        "reader",
                        CapabilityScope {
                            relations: vec![RelationCapability {
                                relation: db.admin().snapshot().schema("cache.Row").unwrap().id,
                                grant: crate::Grant::read_only(),
                            }],
                            schema_namespace: None,
                            delegations: 0,
                        },
                        Some(1010),
                    )
                    .unwrap();
                db.session("reader")
                    .with_capability(cap.id)
                    .unwrap()
                    .snapshot()
            } else {
                db.session("reader").snapshot()
            };
            let table = LiquidTable::new(
                snapshot.clone(),
                snapshot
                    .relations()
                    .into_iter()
                    .find(|r| r.name == "cache.Row")
                    .unwrap(),
                Arc::new(BatchCache::default()),
            )
            .unwrap();
            let mut cold = range_cursor(&table);
            while cold.next_batch().unwrap().is_some() {}
            let mut warm = range_cursor(&table);
            assert_eq!(warm.next_batch().unwrap().unwrap().num_rows(), 1000);
            assert_eq!(warm.cache_hits.value(), 1);
            if expiry {
                db.clock.set(1010);
            } else {
                let mut tx = db.admin().transaction();
                tx.revoke("reader", "cache.Row").unwrap();
                tx.commit().unwrap();
            }
            assert!(
                warm.next_batch()
                    .unwrap_err()
                    .to_string()
                    .contains("access denied")
            );
        }
    }

    #[test]
    fn range_cache_keys_account_for_typed_bounds_direction_and_continuation() {
        let base = PageKey {
            range: Some(RangeKey {
                slot: 0,
                lower: Bound::Included(Value::Int(0)),
                upper: Bound::Excluded(Value::Int(9)),
                descending: false,
                predicates: vec![],
                after: None,
            }),
            ..page_key(1)
        };
        let cache = BatchCache::new(4096, 1);
        cache
            .insert(base.clone(), empty_cached_page(&base))
            .unwrap();
        let mut changed = base.clone();
        changed.range.as_mut().unwrap().lower = Bound::Excluded(Value::Int(0));
        assert!(cache.get(&changed).unwrap().is_none());
        changed = base.clone();
        changed.range.as_mut().unwrap().descending = true;
        assert!(cache.get(&changed).unwrap().is_none());
        changed = base.clone();
        changed.range.as_mut().unwrap().after = Some((Value::Int(1), Id::from_u128(1)));
        assert!(cache.get(&changed).unwrap().is_none());
        cache
            .insert(changed.clone(), empty_cached_page(&changed))
            .unwrap();
        assert!(cache.get(&base).unwrap().is_none());
        assert_eq!(cache.inner.lock().unwrap().pages.len(), 1);
        // Dynamic predicate values count against capacity in both owned keys.
        let mut large = base;
        large.range.as_mut().unwrap().predicates.push(Predicate {
            column: 2,
            operator: Operator::Eq,
            value: Value::Text("x".repeat(4096)),
        });
        cache
            .insert(large.clone(), empty_cached_page(&large))
            .unwrap();
        assert!(cache.get(&large).unwrap().is_none());
        assert!(cache.get(&changed).unwrap().is_some());
    }
}
