//! Typed aggregates over authorized, bounded storage visitors. DataFusion owns
//! parsing/type checking and every unsupported expression, plan or coercion.
//! No result is cached: executions retain pinned data and live authority fences.
use super::*;
use datafusion::functions_aggregate::{
    count::Count,
    min_max::{Max, Min},
};

use crate::paged::encoding::{FixedPackedRow, PackedValue};

// A source chooses its representation once. Generic consumers keep indexed
// owned rows on their original direct Value path, without a per-access variant.
trait AggregateRecord {
    fn id(&self) -> Id;
    fn value(&self, slot: usize) -> Option<PackedValue<'_>>;
    fn owned_value(&self, _slot: usize) -> Option<&Value> {
        None
    }
    fn column(&self, column: usize) -> PackedValue<'_> {
        if column == 0 {
            PackedValue::Ref(self.id())
        } else {
            self.value(column - 1).expect("compiled aggregate column")
        }
    }
    fn number(&self, slot: usize) -> Option<i64> {
        match self.value(slot)? {
            PackedValue::Int(n) | PackedValue::Decimal(n) | PackedValue::Timestamp(n) => Some(n),
            PackedValue::Bool(v) => Some(i64::from(v)),
            _ => None,
        }
    }
    fn compare(&self, column: usize, value: &Value) -> std::cmp::Ordering {
        self.column(column).cmp_owned(value)
    }
}
impl AggregateRecord for Row {
    fn id(&self) -> Id {
        self.id
    }
    fn value(&self, slot: usize) -> Option<PackedValue<'_>> {
        self.values.get(slot).map(PackedValue::from_owned)
    }
    fn owned_value(&self, slot: usize) -> Option<&Value> {
        self.values.get(slot)
    }
    fn number(&self, slot: usize) -> Option<i64> {
        match self.values.get(slot)? {
            Value::Int(n) | Value::Decimal(n) | Value::Timestamp(n) => Some(*n),
            Value::Bool(v) => Some(i64::from(*v)),
            _ => None,
        }
    }
    fn compare(&self, column: usize, value: &Value) -> std::cmp::Ordering {
        if column == 0 {
            Value::Ref(self.id).cmp(value)
        } else {
            self.values[column - 1].cmp(value)
        }
    }
}
struct FixedRecord<'a> {
    id: Id,
    row: FixedPackedRow<'a, 'a>,
}
impl AggregateRecord for FixedRecord<'_> {
    #[inline]
    fn id(&self) -> Id {
        self.id
    }
    #[inline]
    fn value(&self, slot: usize) -> Option<PackedValue<'_>> {
        self.row.value(slot)
    }
    #[inline]
    fn number(&self, slot: usize) -> Option<i64> {
        self.row.number(slot)
    }
    #[inline]
    fn compare(&self, column: usize, value: &Value) -> std::cmp::Ordering {
        if column == 0 {
            PackedValue::Ref(self.id).cmp_owned(value)
        } else {
            self.row
                .compare(column - 1, value)
                .expect("compiled aggregate column")
        }
    }
}
trait AggregateVisitor {
    fn consume<R: AggregateRecord>(&mut self, row: &R) -> Result<()>;
}

struct RangeConsumer<'a, V>(&'a mut V);
impl<V: AggregateVisitor> crate::paged::packed_range::PackedRangeVisitor for RangeConsumer<'_, V> {
    fn owned(&mut self, row: &Row) -> Result<()> {
        self.0.consume(row)
    }
    #[inline(always)]
    fn packed(&mut self, id: Id, row: FixedPackedRow<'_, '_>) -> Result<()> {
        self.0.consume(&FixedRecord { id, row })
    }
}

const GROUP_STATE_BYTES: usize = 32 * 1024 * 1024;

pub(super) struct ScalarAggregate {
    source: Source,
    predicates: Vec<Condition>,
    groups: Vec<usize>,
    aggregates: Vec<Aggregate>,
    projection: Vec<usize>,
    columns: Vec<String>,
    order: Vec<(usize, bool, bool)>,
    unfiltered_scalar: bool,
}
struct Aggregate {
    operation: Operation,
    filter: Vec<Condition>,
}
#[derive(Clone, Copy)]
enum Operation {
    Count(bool),
    Sum(usize),
    Min(usize),
    Max(usize),
}
struct Condition {
    column: usize,
    operator: Operator,
    binding: Binding,
}
struct BoundCondition {
    column: usize,
    operator: Operator,
    value: Value,
}
struct Input {
    direct: DirectShape,
    predicates: Vec<Condition>,
}
struct Shape {
    source: Source,
    predicates: Vec<Condition>,
    groups: Vec<usize>,
    aggregates: Vec<Aggregate>,
    projection: Vec<usize>,
}

fn unalias(expr: &Expr) -> &Expr {
    if let Expr::Alias(alias) = expr {
        unalias(&alias.expr)
    } else {
        expr
    }
}
fn column(expr: &Expr, input: &LogicalPlan, source: &DirectShape) -> Option<usize> {
    let Expr::Column(column) = unalias(expr) else {
        return None;
    };
    source
        .projection
        .get(input.schema().index_of_column(column).ok()?)
        .copied()
}
fn numeric_slot(expr: &Expr, input: &LogicalPlan, source: &DirectShape) -> Option<usize> {
    let expr = match unalias(expr) {
        Expr::Cast(cast) if cast.field.data_type() == &ArrowType::Int64 => unalias(&cast.expr),
        expr => expr,
    };
    let slot = column(expr, input, source)?.checked_sub(1)?;
    matches!(
        source.source.declaration.roles.get(slot)?.data_type,
        DataType::Bool | DataType::Int | DataType::Decimal { .. } | DataType::Timestamp
    )
    .then_some(slot)
}
fn conditions(
    expr: &Expr,
    input: &LogicalPlan,
    source: &DirectShape,
    out: &mut Vec<Condition>,
) -> Option<()> {
    let Expr::BinaryExpr(binary) = unalias(expr) else {
        return None;
    };
    if binary.op == Operator::And {
        conditions(&binary.left, input, source, out)?;
        return conditions(&binary.right, input, source, out);
    }
    if !matches!(
        binary.op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    ) {
        return None;
    }
    let (slot, value, operator) = if let Some(slot) = column(&binary.left, input, source) {
        (slot, unalias(&binary.right), binary.op)
    } else {
        let slot = column(&binary.right, input, source)?;
        let operator = match binary.op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => other,
        };
        (slot, unalias(&binary.left), operator)
    };
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
    out.push(Condition {
        column: slot,
        operator,
        binding,
    });
    Some(())
}
fn input_shape(plan: &LogicalPlan) -> Option<Input> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut shape = input_shape(&filter.input)?;
            conditions(
                &filter.predicate,
                &filter.input,
                &shape.direct,
                &mut shape.predicates,
            )?;
            Some(shape)
        }
        LogicalPlan::Projection(projection) => {
            let mut shape = input_shape(&projection.input)?;
            shape.direct.projection = projection
                .expr
                .iter()
                .map(|expr| column(expr, &projection.input, &shape.direct))
                .collect::<Option<Vec<_>>>()?;
            Some(shape)
        }
        LogicalPlan::SubqueryAlias(alias) => input_shape(&alias.input),
        LogicalPlan::TableScan(_) => Some(Input {
            direct: DirectRead::shape(plan)?,
            predicates: Vec::new(),
        }),
        _ => None,
    }
}
fn operation(expr: &Expr, input: &LogicalPlan, source: &DirectShape) -> Option<Aggregate> {
    let Expr::AggregateFunction(function) = unalias(expr) else {
        return None;
    };
    let params = &function.params;
    if params.distinct
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || params.args.len() != 1
    {
        return None;
    }
    let mut filter = Vec::new();
    if let Some(expr) = &params.filter {
        conditions(expr, input, source, &mut filter)?;
    }
    let argument = &params.args[0];
    let implementation = function.func.inner().as_ref() as &dyn std::any::Any;
    let operation = if implementation.is::<Count>() {
        if column(argument, input, source).is_some() {
            Operation::Count(true)
        } else if let Expr::Literal(value, _) = unalias(argument) {
            Operation::Count(!value.is_null())
        } else {
            return None;
        }
    } else {
        let slot = numeric_slot(argument, input, source)?;
        if checked::reusable_sum(&function.func) {
            Operation::Sum(slot)
        } else if implementation.is::<Min>() {
            Operation::Min(slot)
        } else if implementation.is::<Max>() {
            Operation::Max(slot)
        } else {
            return None;
        }
    };
    Some(Aggregate { operation, filter })
}
fn shape(plan: &LogicalPlan) -> Option<Shape> {
    match plan {
        LogicalPlan::Aggregate(aggregate) if !aggregate.aggr_expr.is_empty() => {
            let input = input_shape(&aggregate.input)?;
            let groups = aggregate
                .group_expr
                .iter()
                .map(|expr| column(expr, &aggregate.input, &input.direct))
                .collect::<Option<Vec<_>>>()?;
            let aggregates = aggregate
                .aggr_expr
                .iter()
                .map(|expr| operation(expr, &aggregate.input, &input.direct))
                .collect::<Option<Vec<_>>>()?;
            Some(Shape {
                source: input.direct.source,
                predicates: input.predicates,
                projection: (0..groups.len() + aggregates.len()).collect(),
                groups,
                aggregates,
            })
        }
        LogicalPlan::SubqueryAlias(alias) => shape(&alias.input),
        LogicalPlan::Projection(projection) => {
            let mut shape = shape(&projection.input)?;
            shape.projection = projection
                .expr
                .iter()
                .map(|expr| {
                    let Expr::Column(column) = unalias(expr) else {
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
        _ => None,
    }
}

#[derive(Default)]
struct Accumulator {
    count: i64,
    sum: Option<i128>,
    extreme: Option<i64>,
}
fn integer(row: &impl AggregateRecord, slot: usize) -> Result<i64> {
    row.number(slot)
        .ok_or_else(|| query_error("invalid typed scalar aggregate input"))
}
#[cold]
#[inline(never)]
fn overflow() -> Error {
    query_error("signed 64-bit SQL arithmetic overflow or division by zero")
}
impl Accumulator {
    fn add(&mut self, operation: Operation, row: &impl AggregateRecord) -> Result<()> {
        match operation {
            Operation::Count(include) => {
                if include {
                    self.count = self.count.checked_add(1).ok_or_else(overflow)?;
                }
            }
            Operation::Sum(slot) => {
                self.sum = Some(
                    self.sum
                        .unwrap_or(0)
                        .checked_add(i128::from(integer(row, slot)?))
                        .ok_or_else(overflow)?,
                )
            }
            Operation::Min(slot) => {
                let n = integer(row, slot)?;
                self.extreme = Some(self.extreme.map_or(n, |v| v.min(n)));
            }
            Operation::Max(slot) => {
                let n = integer(row, slot)?;
                self.extreme = Some(self.extreme.map_or(n, |v| v.max(n)));
            }
        }
        Ok(())
    }
    fn finish(&self, operation: Operation) -> Result<SqlValue> {
        let n = match operation {
            Operation::Count(_) => Some(self.count),
            Operation::Sum(_) => self
                .sum
                .map(i64::try_from)
                .transpose()
                .map_err(|_| overflow())?,
            Operation::Min(_) | Operation::Max(_) => self.extreme,
        };
        Ok(n.map_or(SqlValue::Null, SqlValue::Integer))
    }
}

/// Groups have an explicit memory ceiling independent of database cardinality.
/// The estimate reserves tree/node/Vec overhead, owned keys and accumulators,
/// and the final projected output while group state is still alive. A temporary
/// next key is checked against the same ceiling before copying variable data.
struct GroupBudget {
    remaining: usize,
    groups: usize,
    max_groups: usize,
}
impl GroupBudget {
    fn new(limits: QueryLimits, width: usize) -> Self {
        Self {
            remaining: GROUP_STATE_BYTES.min(limits.max_bytes),
            groups: 0,
            max_groups: limits
                .max_rows
                .min((limits.vm_steps / width.max(1) as u64).min(usize::MAX as u64) as usize),
        }
    }
    fn check_key(&self, bytes: usize) -> Result<()> {
        if bytes > self.remaining {
            return Err(query_error("query aggregate state byte budget exceeded"));
        }
        Ok(())
    }
    fn reserve(&mut self, bytes: usize, limits: QueryLimits) -> Result<()> {
        if self.groups >= self.max_groups {
            return Err(query_error(if self.groups >= limits.max_rows {
                "query row budget exceeded; use LIMIT or pages"
            } else {
                "query output-cell budget exceeded"
            }));
        }
        self.check_key(bytes)?;
        self.remaining -= bytes;
        self.groups += 1;
        Ok(())
    }
}
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum GroupKey {
    One(Value),
    Many(Vec<Value>),
}
impl GroupKey {
    fn values(&self) -> &[Value] {
        match self {
            Self::One(v) => std::slice::from_ref(v),
            Self::Many(v) => v,
        }
    }
}
/// Single-field groups can probe owned keys by borrowing the row's value,
/// avoiding both a tree walk and a text/blob copy for every repeated key.
/// RandomState keeps externally controlled group values collision-resistant.
/// Multi-field keys retain the ordered map until a borrowed tuple is needed.
enum GroupIndex {
    One(std::collections::HashMap<Value, usize>),
    Many(BTreeMap<GroupKey, usize>),
}
impl GroupIndex {
    fn new(width: usize) -> Self {
        if width == 1 {
            Self::One(std::collections::HashMap::new())
        } else {
            Self::Many(BTreeMap::new())
        }
    }
    fn borrowed(&self, row: &impl AggregateRecord, column: usize) -> Option<usize> {
        let Self::One(groups) = self else { return None };
        // Borrow owned variable keys; fixed-width keys require no allocation.
        if column > 0
            && let Some(value) = row.owned_value(column - 1)
        {
            return groups.get(value).copied();
        }
        groups.get(&row.column(column).into_owned()).copied()
    }

    fn get(&self, key: &GroupKey) -> Option<usize> {
        match (self, key) {
            (Self::One(groups), GroupKey::One(key)) => groups.get(key).copied(),
            (Self::Many(groups), _) => groups.get(key).copied(),
            _ => None,
        }
    }
    fn insert(&mut self, key: GroupKey, index: usize) {
        match (self, key) {
            (Self::One(groups), GroupKey::One(key)) => {
                groups.insert(key, index);
            }
            (Self::Many(groups), key) => {
                groups.insert(key, index);
            }
            _ => unreachable!("compiled group width"),
        }
    }
    fn sorted(self) -> Vec<(GroupKey, usize)> {
        match self {
            Self::One(groups) => {
                let mut groups = groups
                    .into_iter()
                    .map(|(key, index)| (GroupKey::One(key), index))
                    .collect::<Vec<_>>();
                groups.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
                groups
            }
            Self::Many(groups) => groups.into_iter().collect(),
        }
    }
}
/// A bounded accelerator for repeated numeric group keys. It stores only
/// indices into separately owned accumulator vectors; collisions always fall
/// back to the group index. No pointer survives a map/vector insertion.
const GROUP_CACHE_SLOTS: usize = 64;
type GroupCache = [Option<(i64, usize)>; GROUP_CACHE_SLOTS];
const MAX_ADAPTIVE_GROUP_SLOTS: usize = 2048;

/// Grow only after repeated groups demonstrate that the small cache is missing.
/// First-seen groups never trigger growth. The optional accelerator remains
/// bounded independently of input cardinality and is charged at peak capacity.
struct NumericGroupCache {
    small: Box<GroupCache>,
    larger: Option<Vec<Option<(i64, usize)>>>,
    repeated_misses: usize,
}
impl NumericGroupCache {
    fn new() -> Self {
        Self {
            small: Box::new([None; GROUP_CACHE_SLOTS]),
            larger: None,
            repeated_misses: 0,
        }
    }

    #[inline]
    fn entries(&self) -> &[Option<(i64, usize)>] {
        self.larger.as_deref().unwrap_or(self.small.as_slice())
    }

    #[inline]
    fn get(&self, key: i64) -> Option<usize> {
        let entries = self.entries();
        entries[(key as u64 as usize) & (entries.len() - 1)]
            .and_then(|(stored, index)| (stored == key).then_some(index))
    }

    fn store(&mut self, key: i64, index: usize) {
        let entries = self
            .larger
            .as_deref_mut()
            .unwrap_or(self.small.as_mut_slice());
        let slot = (key as u64 as usize) & (entries.len() - 1);
        entries[slot] = Some((key, index));
    }

    fn store_repeated(&mut self, key: i64, index: usize, groups: usize, remaining: &mut usize) {
        let current_slots = self.entries().len();
        if groups > current_slots && current_slots < MAX_ADAPTIVE_GROUP_SLOTS {
            self.repeated_misses += 1;
            if self.repeated_misses == GROUP_CACHE_SLOTS {
                self.repeated_misses = 0;
                self.try_grow(groups, remaining);
            }
        }
        self.store(key, index);
    }

    fn try_grow(&mut self, groups: usize, remaining: &mut usize) {
        let slots = groups
            .min(MAX_ADAPTIVE_GROUP_SLOTS / 2)
            .next_power_of_two()
            .saturating_mul(2);
        if slots <= self.entries().len() {
            return;
        }
        let entry_bytes = std::mem::size_of::<Option<(i64, usize)>>();
        // Account for old and new tables coexisting during rehash. Insufficient
        // budget/allocation simply keeps canonical map lookup available.
        if slots * entry_bytes > *remaining {
            return;
        }
        let mut entries = Vec::new();
        if entries.try_reserve_exact(slots).is_err() {
            return;
        }
        let Some(bytes) = entries.capacity().checked_mul(entry_bytes) else {
            return;
        };
        if bytes > *remaining {
            return;
        }
        *remaining -= bytes;
        entries.resize(slots, None);
        for &(key, index) in self.entries().iter().flatten() {
            entries[(key as u64 as usize) & (slots - 1)] = Some((key, index));
        }
        let previous = self.larger.replace(entries);
        let reclaimed = previous
            .as_ref()
            .map_or(0, |old| old.capacity() * entry_bytes);
        drop(previous);
        *remaining += reclaimed;
    }

    fn release_larger(&mut self, remaining: &mut usize) {
        if let Some(entries) = self.larger.take() {
            // Group indices remain valid for the whole fold. Keep a bounded
            // subset warm without letting an optional accelerator consume the
            // memory needed to admit a previously unseen group.
            for &(key, index) in entries.iter().flatten() {
                self.small[(key as u64 as usize) & (GROUP_CACHE_SLOTS - 1)] = Some((key, index));
            }
            let reclaimed = entries.capacity() * std::mem::size_of::<Option<(i64, usize)>>();
            drop(entries);
            *remaining += reclaimed;
        }
        self.repeated_misses = 0;
    }
}
#[inline(always)]
fn numeric_group(row: &impl AggregateRecord, groups: &[usize]) -> Option<i64> {
    let [column] = groups else { return None };
    row.number(column.checked_sub(1)?)
}

#[cfg(test)]
fn cache_slot(key: i64) -> usize {
    (key as u64 as usize) & (GROUP_CACHE_SLOTS - 1)
}
#[cfg(test)]
fn cached_group(cache: &GroupCache, key: i64) -> Option<usize> {
    cache[cache_slot(key)].and_then(|(stored, index)| (stored == key).then_some(index))
}
fn field(row: &impl AggregateRecord, column: usize) -> Value {
    row.column(column).into_owned()
}
fn field_size(row: &impl AggregateRecord, column: usize) -> usize {
    match row.column(column) {
        PackedValue::Text(v) => v.len(),
        PackedValue::Bytes(v) => v.len(),
        PackedValue::Ref(_) => 36,
        _ => 8,
    }
}

fn output(value: &Value) -> SqlValue {
    match value {
        Value::Int(n) | Value::Decimal(n) | Value::Timestamp(n) => SqlValue::Integer(*n),
        Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
        Value::Ref(id) => SqlValue::Text(id.to_string()),
        Value::Text(v) => SqlValue::Text(v.clone()),
        Value::Bytes(v) => SqlValue::Blob(v.clone()),
        Value::Null => SqlValue::Null,
        Value::Float(v) => SqlValue::Real(v.get()),
    }
}
fn accepted(row: &impl AggregateRecord, predicates: &[BoundCondition]) -> bool {
    predicates.iter().all(|p| {
        let cmp = row.compare(p.column, &p.value);
        match p.operator {
            Operator::Eq => cmp.is_eq(),
            Operator::NotEq => !cmp.is_eq(),
            Operator::Lt => cmp.is_lt(),
            Operator::LtEq => !cmp.is_gt(),
            Operator::Gt => cmp.is_gt(),
            Operator::GtEq => !cmp.is_lt(),
            _ => false,
        }
    })
}
fn reserve_memory(remaining: &mut usize, bytes: usize) -> Result<()> {
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| query_error("query aggregate state byte budget exceeded"))?;
    Ok(())
}

// None means normal DataFusion coercion is needed. Some(None) is SQL UNKNOWN
// or an impossible typed equality; it excludes every row in this conjunction.
fn bind(
    conditions: &[Condition],
    params: &[SqlValue],
    relation: &Relation,
    memory: &mut usize,
) -> Result<Option<Option<Vec<BoundCondition>>>> {
    use crate::datafusion_sql::direct::{Key, key};
    reserve_memory(
        memory,
        conditions
            .len()
            .saturating_mul(std::mem::size_of::<BoundCondition>()),
    )?;
    let mut bound = Vec::with_capacity(conditions.len());
    let mut empty = false;
    for c in conditions {
        let value = match &c.binding {
            Binding::Literal(v) => v,
            Binding::Parameter(i) => params
                .get(*i)
                .ok_or_else(|| query_error("SQL parameter count mismatch"))?,
        };
        let kind = c
            .column
            .checked_sub(1)
            .map(|n| &relation.roles[n].data_type);
        reserve_memory(
            memory,
            match value {
                SqlValue::Text(v) => v.len(),
                SqlValue::Blob(v) => v.len(),
                _ => 0,
            },
        )?;
        let value = match key(value, kind) {
            Key::Unsupported => return Ok(None),
            Key::Empty if matches!(value, SqlValue::Null) || c.operator == Operator::Eq => {
                empty = true;
                continue;
            }
            Key::Empty => return Ok(None),
            Key::Value(v) => v,
        };
        bound.push(BoundCondition {
            column: c.column,
            operator: c.operator,
            value,
        });
    }
    Ok(Some((!empty).then_some(bound)))
}

/// Execution-local accounting: bounded polling without an atomic per source row.
struct AggregateBudget {
    remaining: usize,
    until_time_check: u8,
    start: std::time::Instant,
    timeout: std::time::Duration,
}
impl AggregateBudget {
    fn new(timeout_ms: u64) -> Self {
        Self::with_source_limit(timeout_ms, 50_000_000)
    }
    fn with_source_limit(timeout_ms: u64, source_limit: usize) -> Self {
        Self {
            remaining: source_limit,
            until_time_check: 0,
            start: std::time::Instant::now(),
            timeout: std::time::Duration::from_millis(timeout_ms),
        }
    }
    #[inline(always)]
    fn charge(&mut self) -> Result<()> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or_else(|| query_error("SQL source row budget exceeded"))?;
        if self.until_time_check == 0 {
            self.check_deadline()?;
            self.until_time_check = 255;
        } else {
            self.until_time_check -= 1;
        }
        Ok(())
    }
    fn check_deadline(&self) -> Result<()> {
        if self.start.elapsed() >= self.timeout {
            Err(query_error("SQL deadline exceeded"))
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct UnfilteredState {
    sum: Option<i128>,
    extreme: Option<i64>,
}
struct UnfilteredFold<'a> {
    plan: &'a ScalarAggregate,
    states: Vec<UnfilteredState>,
    count: i64,
}
impl UnfilteredFold<'_> {
    fn finish(&self, slot: usize) -> Result<SqlValue> {
        let state = &self.states[slot];
        let value = match self.plan.aggregates[slot].operation {
            Operation::Count(include) => Some(if include { self.count } else { 0 }),
            Operation::Sum(_) => state
                .sum
                .map(i64::try_from)
                .transpose()
                .map_err(|_| overflow())?,
            Operation::Min(_) | Operation::Max(_) => state.extreme,
        };
        Ok(value.map_or(SqlValue::Null, SqlValue::Integer))
    }
}

// WHERE still runs once per authorized row, but a query without aggregate
// FILTER clauses shares the compact count/state kernel of a whole-family fold.
struct PredicatedFold<'a> {
    predicates: &'a [BoundCondition],
    fold: UnfilteredFold<'a>,
}
impl AggregateVisitor for PredicatedFold<'_> {
    fn consume<R: AggregateRecord>(&mut self, row: &R) -> Result<()> {
        if accepted(row, self.predicates) {
            self.fold.consume(row)?;
        }
        Ok(())
    }
}

enum ScalarStates<'a> {
    Shared(UnfilteredFold<'a>),
    Filtered(Vec<Accumulator>),
}
impl AggregateVisitor for UnfilteredFold<'_> {
    #[inline(always)]
    fn consume<R: AggregateRecord>(&mut self, row: &R) -> Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(overflow)?;
        for (aggregate, state) in self.plan.aggregates.iter().zip(&mut self.states) {
            match aggregate.operation {
                Operation::Count(_) => {}
                Operation::Sum(slot) => {
                    state.sum = Some(
                        state
                            .sum
                            .unwrap_or(0)
                            .checked_add(i128::from(integer(row, slot)?))
                            .ok_or_else(overflow)?,
                    );
                }
                Operation::Min(slot) => {
                    let n = integer(row, slot)?;
                    state.extreme = Some(state.extreme.map_or(n, |old| old.min(n)));
                }
                Operation::Max(slot) => {
                    let n = integer(row, slot)?;
                    state.extreme = Some(state.extreme.map_or(n, |old| old.max(n)));
                }
            }
        }
        Ok(())
    }
}

struct ScalarFold<'a> {
    plan: &'a ScalarAggregate,
    predicates: &'a [BoundCondition],
    filters: &'a [Option<Vec<BoundCondition>>],
    accumulators: Vec<Accumulator>,
}
impl AggregateVisitor for ScalarFold<'_> {
    fn consume<R: AggregateRecord>(&mut self, row: &R) -> Result<()> {
        if accepted(row, self.predicates) {
            for ((a, state), filter) in self
                .plan
                .aggregates
                .iter()
                .zip(&mut self.accumulators)
                .zip(self.filters)
            {
                if filter.as_ref().is_some_and(|p| accepted(row, p)) {
                    state.add(a.operation, row)?;
                }
            }
        }
        Ok(())
    }
}

// Static dispatch chooses the group kernel once per query. Shared groups
// reuse scalar count/sum/extreme semantics without per-operation FILTER checks.
trait GroupState<'a>: Sized {
    fn new(plan: &'a ScalarAggregate) -> Self;
    fn add<R: AggregateRecord>(
        &mut self,
        plan: &ScalarAggregate,
        filters: &[Option<Vec<BoundCondition>>],
        row: &R,
    ) -> Result<()>;
    fn finish(&self, slot: usize, operation: Operation) -> Result<SqlValue>;
}
impl<'a> GroupState<'a> for UnfilteredFold<'a> {
    fn new(plan: &'a ScalarAggregate) -> Self {
        Self {
            plan,
            states: (0..plan.aggregates.len())
                .map(|_| UnfilteredState::default())
                .collect(),
            count: 0,
        }
    }
    #[inline(always)]
    fn add<R: AggregateRecord>(
        &mut self,
        _: &ScalarAggregate,
        _: &[Option<Vec<BoundCondition>>],
        row: &R,
    ) -> Result<()> {
        self.consume(row)
    }
    fn finish(&self, slot: usize, _: Operation) -> Result<SqlValue> {
        UnfilteredFold::finish(self, slot)
    }
}
impl<'a> GroupState<'a> for Vec<Accumulator> {
    fn new(plan: &'a ScalarAggregate) -> Self {
        (0..plan.aggregates.len())
            .map(|_| Accumulator::default())
            .collect()
    }
    fn add<R: AggregateRecord>(
        &mut self,
        plan: &ScalarAggregate,
        filters: &[Option<Vec<BoundCondition>>],
        row: &R,
    ) -> Result<()> {
        for ((aggregate, state), filter) in plan.aggregates.iter().zip(self).zip(filters) {
            if filter
                .as_ref()
                .is_some_and(|predicate| accepted(row, predicate))
            {
                state.add(aggregate.operation, row)?;
            }
        }
        Ok(())
    }
    fn finish(&self, slot: usize, operation: Operation) -> Result<SqlValue> {
        self[slot].finish(operation)
    }
}

struct GroupedFold<'a, S> {
    plan: &'a ScalarAggregate,
    predicates: &'a [BoundCondition],
    filters: &'a [Option<Vec<BoundCondition>>],
    groups: GroupIndex,
    accumulators: Vec<Option<S>>,
    state_budget: GroupBudget,
    cache: Option<NumericGroupCache>,
    cache_eligible: bool,
    first_group: bool,
    limits: QueryLimits,
}
impl<'a, S: GroupState<'a>> AggregateVisitor for GroupedFold<'a, S> {
    #[inline(always)]
    fn consume<R: AggregateRecord>(&mut self, row: &R) -> Result<()> {
        if !accepted(row, self.predicates) {
            return Ok(());
        }
        // An existing cache implies initialization and its memory reservation
        // already succeeded. Hits need neither map lookup nor repeated stores
        // of the same cache entry. Collisions still compare the complete key.
        if let Some(cache) = &self.cache
            && let Some(key) = numeric_group(row, &self.plan.groups)
            && let Some(index) = cache.get(key)
        {
            return self.accumulators[index]
                .as_mut()
                .expect("live group state")
                .add(self.plan, self.filters, row);
        }
        self.consume_slow(row)
    }
}
impl<'a, S: GroupState<'a>> GroupedFold<'a, S> {
    // Non-numeric/colliding keys legitimately use this on every row. Keep the
    // allocation/budget machinery out of the compact hot hit path, not cold.
    #[inline(never)]
    fn consume_slow<R: AggregateRecord>(&mut self, row: &R) -> Result<()> {
        if self.first_group {
            // Map containers allocate spare slots before they fill;
            // reserve initial capacity, not just one entry.
            reserve_memory(&mut self.state_budget.remaining, 2048)?;
            if self.cache_eligible && self.state_budget.remaining >= 16 * 1024 {
                reserve_memory(
                    &mut self.state_budget.remaining,
                    std::mem::size_of::<GroupCache>(),
                )?;
                self.cache = Some(NumericGroupCache::new());
            }
            self.first_group = false;
        }
        let number = self
            .cache
            .as_ref()
            .and_then(|_| numeric_group(row, &self.plan.groups));
        // Entry reaches this helper only after a cache miss, or before
        // initialization creates an empty cache. Do not repeat that lookup.
        if let Some(index) = match self.plan.groups.as_slice() {
            [column] => self.groups.borrowed(row, *column),
            _ => None,
        } {
            if let (Some(cache), Some(key)) = (&mut self.cache, number) {
                cache.store_repeated(
                    key,
                    index,
                    self.accumulators.len(),
                    &mut self.state_budget.remaining,
                );
            }
            self.accumulators[index]
                .as_mut()
                .expect("live group state")
                .add(self.plan, self.filters, row)?;
            return Ok(());
        }
        if let Some(cache) = &mut self.cache {
            cache.release_larger(&mut self.state_budget.remaining);
        }
        let key_bytes = self
            .plan
            .groups
            .iter()
            .fold(std::mem::size_of::<GroupKey>(), |n, col| {
                n.saturating_add(std::mem::size_of::<Value>())
                    .saturating_add(field_size(row, *col))
            });
        self.state_budget.check_key(key_bytes)?;
        let key = if let [col] = self.plan.groups.as_slice() {
            GroupKey::One(field(row, *col))
        } else {
            GroupKey::Many(
                self.plan
                    .groups
                    .iter()
                    .map(|col| field(row, *col))
                    .collect(),
            )
        };
        let index = match self.groups.get(&key) {
            Some(index) => index,
            None => {
                let output_bytes = self.plan.projection.iter().fold(0usize, |n, slot| {
                    n.saturating_add(std::mem::size_of::<SqlValue>())
                        .saturating_add(if *slot < self.plan.groups.len() {
                            field_size(row, self.plan.groups[*slot])
                        } else {
                            8
                        })
                });
                let reserve = 512usize
                    .saturating_add(key_bytes)
                    .saturating_add(
                        self.plan
                            .aggregates
                            .len()
                            .saturating_mul(std::mem::size_of::<Accumulator>()),
                    )
                    .saturating_add(output_bytes);
                self.state_budget.reserve(reserve, self.limits)?;
                // The per-group 512-byte allowance covers table
                // resize (old + new), vector headers/capacity and
                // the sorted key/index vector used during output.
                let index = self.accumulators.len();
                self.accumulators.push(Some(S::new(self.plan)));
                self.groups.insert(key, index);
                index
            }
        };
        if let (Some(cache), Some(key)) = (&mut self.cache, number) {
            cache.store(key, index);
        }
        self.accumulators[index]
            .as_mut()
            .expect("live group state")
            .add(self.plan, self.filters, row)?;
        Ok(())
    }
}

impl ScalarAggregate {
    pub(super) fn compile(plan: &LogicalPlan) -> Option<Self> {
        let (core, order) = if let LogicalPlan::Sort(sort) = plan {
            if sort.fetch.is_some() {
                return None;
            }
            let order = sort
                .expr
                .iter()
                .map(|s| {
                    let Expr::Column(c) = unalias(&s.expr) else {
                        return None;
                    };
                    Some((
                        sort.input.schema().index_of_column(c).ok()?,
                        s.asc,
                        s.nulls_first,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            (sort.input.as_ref(), order)
        } else {
            (plan, Vec::new())
        };
        let shape = shape(core)?;
        // Nullable/float aggregates use DataFusion SQL semantics until the
        // specialized integer fold implements null counts and floating sums.
        if shape
            .source
            .declaration
            .roles
            .iter()
            .any(|r| matches!(r.data_type, DataType::Nullable(_) | DataType::Float))
        {
            return None;
        }
        // Pruned aggregates remain the optimizer's responsibility: evaluating a
        // discarded SUM could expose overflow that ordinary execution removes.
        let retained = shape
            .projection
            .iter()
            .filter_map(|n| n.checked_sub(shape.groups.len()))
            .collect::<BTreeSet<_>>();
        if retained.len() != shape.aggregates.len() {
            return None;
        }
        if core.schema().fields().iter().any(|f| {
            !matches!(
                f.data_type(),
                ArrowType::Int64 | ArrowType::Utf8 | ArrowType::Binary
            )
        }) {
            return None;
        }
        let unfiltered_scalar = shape.groups.is_empty()
            && shape.predicates.is_empty()
            && shape
                .aggregates
                .iter()
                .all(|aggregate| aggregate.filter.is_empty());
        Some(Self {
            unfiltered_scalar,
            source: shape.source,
            predicates: shape.predicates,
            groups: shape.groups,
            aggregates: shape.aggregates,
            projection: shape.projection,
            columns: plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            order,
        })
    }
    /// Keep the original whole-family path compact: one shared count, no
    /// binding/filter vectors and no empty-predicate checks for every row/op.
    /// Authorization and source-work checks still run in the storage visitor.
    fn query_unfiltered(&self, limits: QueryLimits) -> Result<SqlTable> {
        self.source.snapshot.fence()?;
        let mut budget = AggregateBudget::new(limits.timeout_ms);
        if self.projection.len().max(1) as u64 > limits.vm_steps {
            return Err(query_error("query output-cell budget exceeded"));
        }
        let mut bytes = self
            .columns
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(c.len()))
            .ok_or_else(|| query_error("query byte budget exceeded"))?;
        if bytes > limits.max_bytes {
            return Err(query_error("query byte budget exceeded"));
        }
        let mut memory = GROUP_STATE_BYTES;
        reserve_memory(
            &mut memory,
            self.aggregates
                .len()
                .saturating_mul(std::mem::size_of::<UnfilteredState>()),
        )?;
        let mut fold = UnfilteredFold {
            plan: self,
            states: (0..self.aggregates.len())
                .map(|_| UnfilteredState::default())
                .collect(),
            count: 0,
        };
        self.visit_scan(&mut || budget.charge(), &mut fold)?;
        let mut row = Vec::with_capacity(self.projection.len());
        for slot in &self.projection {
            if 8 > limits.max_bytes.saturating_sub(bytes) {
                return Err(query_error("query byte budget exceeded"));
            }
            bytes += 8;
            row.push(fold.finish(*slot)?);
        }
        self.source.snapshot.fence()?;
        budget.check_deadline()?;
        Ok(SqlTable {
            columns: self.columns.clone(),
            rows: vec![row],
        })
    }

    fn visit_scan(
        &self,
        check: &mut impl FnMut() -> Result<()>,
        visitor: &mut impl AggregateVisitor,
    ) -> Result<()> {
        if self.source.snapshot.visit_fixed_authorized_rows(
            &self.source.declaration.name,
            check,
            &mut |id, row| visitor.consume(&FixedRecord { id, row }),
        )? {
            return Ok(());
        }
        self.source.snapshot.visit_authorized_rows(
            &self.source.declaration.name,
            check,
            &mut |row| visitor.consume(row),
        )
    }

    fn visit_range(
        &self,
        role: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
        check: &mut impl FnMut() -> Result<()>,
        visitor: &mut impl AggregateVisitor,
    ) -> Result<()> {
        if self.source.snapshot.visit_fixed_authorized_range(
            &self.source.declaration.name,
            role,
            lower,
            upper,
            check,
            &mut RangeConsumer(visitor),
        )? {
            return Ok(());
        }
        self.source.snapshot.visit_authorized_range(
            &self.source.declaration.name,
            role,
            lower,
            upper,
            check,
            &mut |row| visitor.consume(row),
        )
    }

    fn visit(
        &self,
        predicates: &[BoundCondition],
        budget: &mut AggregateBudget,
        visitor: &mut impl AggregateVisitor,
    ) -> Result<()> {
        // Typed index bounds narrow candidates before residual predicates.
        // Every selected occurrence still passes the same visibility checks.
        if let Some(p) = predicates.iter().find(|p| p.operator == Operator::Eq) {
            if p.column == 0 {
                let Value::Ref(id) = p.value else {
                    return Err(query_error("invalid identity predicate"));
                };
                if let Some(row) = self.source.snapshot.get_checked(
                    &self.source.declaration.name,
                    id,
                    &mut || budget.charge(),
                )? {
                    visitor.consume(&row)?;
                }
                return Ok(());
            }
            return self.visit_range(
                &self.source.declaration.roles[p.column - 1].name,
                Some((&p.value, true)),
                Some((&p.value, true)),
                &mut || budget.charge(),
                visitor,
            );
        }
        for lower in predicates
            .iter()
            .filter(|p| p.column > 0 && matches!(p.operator, Operator::Gt | Operator::GtEq))
        {
            if let Some(upper) = predicates.iter().find(|p| {
                p.column == lower.column && matches!(p.operator, Operator::Lt | Operator::LtEq)
            }) {
                return self.visit_range(
                    &self.source.declaration.roles[lower.column - 1].name,
                    Some((&lower.value, lower.operator == Operator::GtEq)),
                    Some((&upper.value, upper.operator == Operator::LtEq)),
                    &mut || budget.charge(),
                    visitor,
                );
            }
        }
        if let Some(bound) = predicates.iter().find(|p| {
            p.column > 0
                && matches!(
                    p.operator,
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
                )
        }) {
            let is_lower = matches!(bound.operator, Operator::Gt | Operator::GtEq);
            let inclusive = matches!(bound.operator, Operator::GtEq | Operator::LtEq);
            let role = &self.source.declaration.roles[bound.column - 1].name;
            // Endpoint estimates are planner hints only. Packed indexed reads
            // avoid materializing fixed roots; general paths retain their
            // existing 50% cutoff. Broad selections still stream full families.
            let use_index = matches!(self.source.snapshot.estimate_one_sided_range(
                &self.source.declaration.name,
                role,
                &bound.value,
                is_lower,
                inclusive,
            )?, Some(fraction) if fraction <= 0.5);
            if use_index {
                let bounds = Some((&bound.value, inclusive));
                return self.visit_range(
                    role,
                    if is_lower { bounds } else { None },
                    if is_lower { None } else { bounds },
                    &mut || budget.charge(),
                    visitor,
                );
            }
        }
        self.visit_scan(&mut || budget.charge(), visitor)
    }
    fn query_grouped<'a, S: GroupState<'a>>(
        &'a self,
        predicates: Option<&'a [BoundCondition]>,
        filters: &'a [Option<Vec<BoundCondition>>],
        limits: QueryLimits,
        budget: &mut AggregateBudget,
        allocation: (usize, &mut usize),
    ) -> Result<Vec<Vec<SqlValue>>> {
        let (memory, bytes) = allocation;
        let mut rows = Vec::new();
        let mut state_budget = GroupBudget::new(limits, self.projection.len());
        state_budget.remaining = state_budget.remaining.min(memory).saturating_sub(*bytes);
        // Tight queries retain the uncached path; the fold reserves cache
        // capacity only when it admits its first visible matching group.
        let cache_eligible = matches!(self.groups.as_slice(), [column] if *column > 0
                && matches!(self.source.declaration.roles[*column - 1].data_type,
                    DataType::Bool | DataType::Int | DataType::Decimal { .. } | DataType::Timestamp));
        let mut fold = GroupedFold::<S> {
            plan: self,
            predicates: predicates.unwrap_or(&[]),
            filters,
            groups: GroupIndex::new(self.groups.len()),
            accumulators: Vec::new(),
            state_budget,
            cache: None,
            cache_eligible,
            first_group: true,
            limits,
        };
        if let Some(predicates) = predicates {
            self.visit(predicates, budget, &mut fold)?;
        }
        let GroupedFold {
            groups,
            mut accumulators,
            ..
        } = fold;
        for (key, index) in groups.sorted() {
            budget.charge()?;
            // Release each group's state as its output is built. The
            // reservation also covers simultaneously retained output.
            let states = accumulators[index].take().expect("live group state");
            let mut row = Vec::with_capacity(self.projection.len());
            for slot in &self.projection {
                let (length, value) = if *slot < self.groups.len() {
                    let v = &key.values()[*slot];
                    let length = match v {
                        Value::Text(v) => v.len(),
                        Value::Bytes(v) => v.len(),
                        Value::Ref(_) => 36,
                        _ => 8,
                    };
                    // Check before cloning a possibly large output value.
                    if length > limits.max_bytes.saturating_sub(*bytes) {
                        return Err(query_error("query byte budget exceeded"));
                    }
                    (length, output(v))
                } else {
                    let n = *slot - self.groups.len();
                    (8, states.finish(n, self.aggregates[n].operation)?)
                };
                if length > limits.max_bytes.saturating_sub(*bytes) {
                    return Err(query_error("query byte budget exceeded"));
                }
                *bytes += length;
                row.push(value);
            }
            rows.push(row);
        }
        Ok(rows)
    }
    pub(super) fn query(
        &self,
        params: &[SqlValue],
        limits: QueryLimits,
    ) -> Result<Option<SqlTable>> {
        if self.unfiltered_scalar {
            return self.query_unfiltered(limits).map(Some);
        }
        self.source.snapshot.fence()?;
        let mut budget = AggregateBudget::new(limits.timeout_ms);
        let mut memory = GROUP_STATE_BYTES;
        let Some(predicates) = bind(
            &self.predicates,
            params,
            &self.source.declaration,
            &mut memory,
        )?
        else {
            return Ok(None);
        };
        reserve_memory(
            &mut memory,
            self.aggregates
                .len()
                .saturating_mul(std::mem::size_of::<Option<Vec<BoundCondition>>>()),
        )?;
        let mut filters = Vec::with_capacity(self.aggregates.len());
        for a in &self.aggregates {
            let Some(filter) = bind(&a.filter, params, &self.source.declaration, &mut memory)?
            else {
                return Ok(None);
            };
            filters.push(filter);
        }
        let mut bytes = self
            .columns
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(c.len()))
            .ok_or_else(|| query_error("query byte budget exceeded"))?;
        if bytes > limits.max_bytes {
            return Err(query_error("query byte budget exceeded"));
        }
        let mut rows = Vec::new();
        if self.groups.is_empty() {
            if self.projection.len().max(1) as u64 > limits.vm_steps {
                return Err(query_error("query output-cell budget exceeded"));
            }
            reserve_memory(
                &mut memory,
                self.aggregates
                    .len()
                    .saturating_mul(std::mem::size_of::<Accumulator>()),
            )?;
            let states = if filters
                .iter()
                .all(|filter| filter.as_ref().is_some_and(Vec::is_empty))
            {
                // The existing Accumulator reservation also bounds this smaller
                // state representation; there is no second per-row allocation.
                let mut fold = PredicatedFold {
                    predicates: predicates.as_deref().unwrap_or(&[]),
                    fold: UnfilteredFold {
                        plan: self,
                        states: (0..self.aggregates.len())
                            .map(|_| UnfilteredState::default())
                            .collect(),
                        count: 0,
                    },
                };
                if let Some(predicates) = &predicates {
                    self.visit(predicates, &mut budget, &mut fold)?;
                }
                ScalarStates::Shared(fold.fold)
            } else {
                let mut fold = ScalarFold {
                    plan: self,
                    predicates: predicates.as_deref().unwrap_or(&[]),
                    filters: &filters,
                    accumulators: (0..self.aggregates.len())
                        .map(|_| Accumulator::default())
                        .collect(),
                };
                if let Some(predicates) = &predicates {
                    self.visit(predicates, &mut budget, &mut fold)?;
                }
                ScalarStates::Filtered(fold.accumulators)
            };
            let mut row = Vec::with_capacity(self.projection.len());
            for slot in &self.projection {
                if 8 > limits.max_bytes.saturating_sub(bytes) {
                    return Err(query_error("query byte budget exceeded"));
                }
                bytes += 8;
                row.push(match &states {
                    ScalarStates::Shared(fold) => fold.finish(*slot)?,
                    ScalarStates::Filtered(states) => {
                        states[*slot].finish(self.aggregates[*slot].operation)?
                    }
                });
            }
            rows.push(row);
        } else {
            rows = if filters
                .iter()
                .all(|filter| filter.as_ref().is_some_and(Vec::is_empty))
            {
                self.query_grouped::<UnfilteredFold<'_>>(
                    predicates.as_deref(),
                    &filters,
                    limits,
                    &mut budget,
                    (memory, &mut bytes),
                )?
            } else {
                self.query_grouped::<Vec<Accumulator>>(
                    predicates.as_deref(),
                    &filters,
                    limits,
                    &mut budget,
                    (memory, &mut bytes),
                )?
            };
        }
        // In-place sorting has logarithmic stack space; group reservations
        // already cover all output rows. Only direct output columns compile.
        if !self.order.is_empty() {
            rows.sort_unstable_by(|a, b| {
                for (column, asc, nulls_first) in &self.order {
                    let (a, b) = (&a[*column], &b[*column]);
                    let mut comparison = match (a, b) {
                        (SqlValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
                        (SqlValue::Null, _) => {
                            if *nulls_first {
                                std::cmp::Ordering::Less
                            } else {
                                std::cmp::Ordering::Greater
                            }
                        }
                        (_, SqlValue::Null) => {
                            if *nulls_first {
                                std::cmp::Ordering::Greater
                            } else {
                                std::cmp::Ordering::Less
                            }
                        }
                        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
                        (SqlValue::Text(a), SqlValue::Text(b)) => a.cmp(b),
                        (SqlValue::Blob(a), SqlValue::Blob(b)) => a.cmp(b),
                        _ => unreachable!("compiled typed aggregate output"),
                    };
                    if !*asc && !matches!(a, SqlValue::Null) && !matches!(b, SqlValue::Null) {
                        comparison = comparison.reverse();
                    }
                    if !comparison.is_eq() {
                        return comparison;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        self.source.snapshot.fence()?;
        budget.check_deadline()?;
        Ok(Some(SqlTable {
            columns: self.columns.clone(),
            rows,
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{CapabilityScope, RelationCapability};

    #[test]
    fn packed_range_crossover_indexes_half_and_streams_broad_selections() {
        for format in [PagedFormat::V2, PagedFormat::V3] {
            let dir = tempfile::tempdir().unwrap();
            let db = crate::paged::tests::empty_format(&dir.path().join("db"), format);
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "agg",
                1,
                vec![],
                vec![Relation::new(
                    "agg.Item",
                    vec![Role::new("n", DataType::Int)],
                )],
            ))
            .unwrap();
            for n in 0..100i64 {
                tx.insert("agg.Item", vec![n.into()]).unwrap();
            }
            tx.commit().unwrap();
            let (snapshot, metrics) = db
                .with_read_metrics(db.admin().snapshot().unwrap())
                .unwrap();
            let sql = snapshot.sql().unwrap();
            let mut query = sql
                .prepare("SELECT COUNT(*),SUM(n) FROM agg.Item WHERE n>=?1")
                .unwrap();
            for (threshold, count, sum) in [(50, 50, 3725), (90, 10, 945), (10, 90, 4905)] {
                let before = metrics.snapshot();
                assert_eq!(
                    query.query(&[SqlValue::Integer(threshold)]).unwrap().rows,
                    vec![vec![SqlValue::Integer(count), SqlValue::Integer(sum)]]
                );
                let work = metrics.snapshot().delta_since(before);
                if threshold == 10 {
                    assert_eq!(work.root_scan_rows, 100);
                    assert_eq!(work.index_candidates, 0);
                } else {
                    assert_eq!(work.root_scan_rows, count as u64);
                    assert_eq!(work.index_candidates, count as u64);
                }
            }
        }
    }

    #[test]
    fn grouped_hot_hits_match_collisions_and_non_numeric_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "cache",
            1,
            vec![],
            [
                ("cache.Hit", DataType::Int),
                ("cache.Collision", DataType::Int),
                ("cache.Text", DataType::Text),
            ]
            .into_iter()
            .map(|(name, kind)| {
                Relation::new(
                    name,
                    vec![Role::new("bucket", kind), Role::new("n", DataType::Int)],
                )
            })
            .collect(),
        ))
        .unwrap();
        for n in -64i64..64 {
            let bucket = n.rem_euclid(2);
            for (name, value) in [
                ("cache.Hit", Value::Int(bucket)),
                (
                    "cache.Collision",
                    Value::Int(bucket * GROUP_CACHE_SLOTS as i64),
                ),
                (
                    "cache.Text",
                    Value::Text(if bucket == 0 { "a" } else { "b" }.into()),
                ),
            ] {
                tx.insert(name, vec![value, Value::Int(n)]).unwrap();
            }
        }
        tx.commit().unwrap();
        let sql = db.admin().snapshot().unwrap().sql().unwrap();
        for threshold in [i64::MIN, 0, 64] {
            for filtered in [false, true] {
                let aggregates = ["COUNT(*)", "COUNT(NULL)", "SUM(n)", "MIN(n)", "MAX(n)"]
                    .map(|operation| {
                        if filtered {
                            format!("{operation} FILTER (WHERE n>=?2)")
                        } else {
                            operation.into()
                        }
                    })
                    .join(",");
                let params = if filtered {
                    vec![SqlValue::Integer(threshold), SqlValue::Integer(0)]
                } else {
                    vec![SqlValue::Integer(threshold)]
                };
                let mut reference = None;
                for name in ["cache.Hit", "cache.Collision", "cache.Text"] {
                    let mut query = sql.prepare(&format!("SELECT bucket,{aggregates} FROM {name} WHERE n>=?1 GROUP BY bucket ORDER BY bucket")).unwrap();
                    assert!(query.aggregate.is_some());
                    let mut rows = query.query(&params).unwrap().rows;
                    for row in &mut rows {
                        row[0] = match (&row[0], name) {
                            (SqlValue::Integer(n), "cache.Collision") => {
                                SqlValue::Integer(n / GROUP_CACHE_SLOTS as i64)
                            }
                            (SqlValue::Text(text), "cache.Text") => {
                                SqlValue::Integer(i64::from(text == "b"))
                            }
                            (value, _) => value.clone(),
                        };
                    }
                    if let Some(expected) = &reference {
                        assert_eq!(&rows, expected, "{name}/{threshold}/{filtered}");
                    } else {
                        reference = Some(rows);
                    }
                }
            }
        }
    }

    #[test]
    fn shared_groups_match_filtered_states_and_retain_overflow_and_null_filter_semantics() {
        for format in [PagedFormat::V2, PagedFormat::V3] {
            let dir = tempfile::tempdir().unwrap();
            let db = crate::paged::tests::empty_format(&dir.path().join("db"), format);
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "groups",
                1,
                vec![],
                vec![Relation::new(
                    "groups.Item",
                    vec![
                        Role::new("bucket", DataType::Int),
                        Role::new("n", DataType::Int),
                    ],
                )],
            ))
            .unwrap();
            for (bucket, n) in [
                (0, i64::MIN),
                (0, i64::MAX),
                (0, i64::MAX),
                (1, 2),
                (1, -2),
                (64, i64::MAX),
                (64, i64::MAX),
            ] {
                tx.insert("groups.Item", vec![Value::Int(bucket), Value::Int(n)])
                    .unwrap();
            }
            tx.commit().unwrap();
            let sql = db.admin().snapshot().unwrap().sql().unwrap();
            let mut shared = sql.prepare("SELECT bucket,COUNT(*),COUNT(NULL),SUM(n),MIN(n),MAX(n) FROM groups.Item WHERE bucket<?1 GROUP BY bucket ORDER BY bucket").unwrap();
            let mut filtered = sql.prepare("SELECT bucket,COUNT(*) FILTER (WHERE n>=?2),COUNT(NULL) FILTER (WHERE n>=?2),SUM(n) FILTER (WHERE n>=?2),MIN(n) FILTER (WHERE n>=?2),MAX(n) FILTER (WHERE n>=?2) FROM groups.Item WHERE bucket<?1 GROUP BY bucket ORDER BY bucket").unwrap();
            assert!(shared.aggregate.is_some());
            assert!(filtered.aggregate.is_some());
            for threshold in [
                SqlValue::Null,
                SqlValue::Integer(0),
                SqlValue::Integer(1),
                SqlValue::Integer(64),
                SqlValue::Integer(65),
            ] {
                let actual = shared.query(std::slice::from_ref(&threshold));
                let expected = filtered.query(&[threshold.clone(), SqlValue::Integer(i64::MIN)]);
                assert_eq!(
                    actual.map(|t| t.rows).map_err(|e| e.to_string()),
                    expected.map(|t| t.rows).map_err(|e| e.to_string())
                );
            }
            assert_eq!(
                shared.query(&[SqlValue::Integer(1)]).unwrap().rows,
                vec![vec![
                    SqlValue::Integer(0),
                    SqlValue::Integer(3),
                    SqlValue::Integer(0),
                    SqlValue::Integer(i64::MAX - 1),
                    SqlValue::Integer(i64::MIN),
                    SqlValue::Integer(i64::MAX),
                ]]
            );
            assert!(shared.query(&[SqlValue::Integer(65)]).is_err());
            // UNKNOWN aggregate FILTER must keep its groups but feed no values.
            assert_eq!(
                filtered
                    .query(&[SqlValue::Integer(1), SqlValue::Null])
                    .unwrap()
                    .rows,
                vec![vec![
                    SqlValue::Integer(0),
                    SqlValue::Integer(0),
                    SqlValue::Integer(0),
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                ]]
            );
            for limits in [
                QueryLimits {
                    max_rows: 1,
                    ..Default::default()
                },
                QueryLimits {
                    max_bytes: 64,
                    ..Default::default()
                },
                QueryLimits {
                    vm_steps: 1,
                    ..Default::default()
                },
            ] {
                assert!(
                    shared
                        .query_with_limits(&[SqlValue::Integer(64)], limits)
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn predicated_shared_fold_matches_filtered_kernel_for_empty_extreme_and_overflow_cases() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "agg",
            1,
            vec![],
            vec![Relation::new(
                "agg.Item",
                vec![Role::new("n", DataType::Int)],
            )],
        ))
        .unwrap();
        for n in [i64::MIN, -1, 0, 1, i64::MAX, i64::MAX] {
            tx.insert("agg.Item", vec![n.into()]).unwrap();
        }
        tx.commit().unwrap();
        let sql = db.admin().snapshot().unwrap().sql().unwrap();
        let mut shared = sql
            .prepare("SELECT COUNT(*),SUM(n),MIN(n),MAX(n) FROM agg.Item WHERE n>=?1")
            .unwrap();
        let mut general = sql.prepare("SELECT COUNT(*) FILTER (WHERE n>=?2),SUM(n) FILTER (WHERE n>=?2),MIN(n) FILTER (WHERE n>=?2),MAX(n) FILTER (WHERE n>=?2) FROM agg.Item WHERE n>=?1").unwrap();
        assert!(shared.aggregate.is_some());
        assert!(general.aggregate.is_some());
        for threshold in [
            SqlValue::Null,
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(0),
            SqlValue::Integer(i64::MAX),
        ] {
            let fast = shared.query(std::slice::from_ref(&threshold));
            let reference = general.query(&[threshold.clone(), SqlValue::Integer(i64::MIN)]);
            assert_eq!(
                fast.map(|t| t.rows).map_err(|e| e.to_string()),
                reference.map(|t| t.rows).map_err(|e| e.to_string())
            );
        }
        assert_eq!(
            shared.query(&[SqlValue::Null]).unwrap().rows,
            vec![vec![
                SqlValue::Integer(0),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Null
            ]]
        );
        assert!(shared.query(&[SqlValue::Integer(i64::MAX)]).is_err());
        assert!(
            shared
                .query_with_limits(
                    &[SqlValue::Null],
                    QueryLimits {
                        max_bytes: 1,
                        ..Default::default()
                    }
                )
                .is_err()
        );
    }

    #[test]
    fn generic_aggregate_records_match_filters_groups_and_overflow() {
        use crate::compiled::CompiledRelation;
        use crate::paged::encoding::{FixedPackedLayout, encode_row};

        let def = CompiledRelation::new(Relation::new(
            "test.Fixed",
            vec![
                Role::new("number", DataType::Int),
                Role::new("flag", DataType::Bool),
                Role::new("amount", DataType::Decimal { scale: 2 }),
                Role::new("time", DataType::Timestamp),
                Role::new("person", DataType::Ref("test.Person".into())),
            ],
        ));
        let owned = Row {
            id: Id::from_u128(99),
            values: vec![
                Value::Int(i64::MAX),
                Value::Bool(true),
                Value::Decimal(-17),
                Value::Timestamp(i64::MIN),
                Value::Ref(Id::from_u128(11)),
            ]
            .into_boxed_slice(),
        };
        let bytes = encode_row(&def, &owned, 1).unwrap();
        let layout = FixedPackedLayout::compile(&def).unwrap();
        let fixed = FixedRecord {
            id: owned.id,
            row: layout.read(&bytes).unwrap(),
        };
        for column in 0..=owned.values.len() {
            assert_eq!(field(&owned, column), field(&fixed, column));
            assert_eq!(field_size(&owned, column), field_size(&fixed, column));
            assert_eq!(
                numeric_group(&owned, &[column]),
                numeric_group(&fixed, &[column])
            );
            let mut groups = GroupIndex::new(1);
            groups.insert(GroupKey::One(field(&owned, column)), column);
            assert_eq!(groups.borrowed(&owned, column), Some(column));
            assert_eq!(groups.borrowed(&fixed, column), Some(column));
            for value in &owned.values {
                for operator in [
                    Operator::Eq,
                    Operator::NotEq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ] {
                    let predicate = [BoundCondition {
                        column,
                        operator,
                        value: value.clone(),
                    }];
                    assert_eq!(accepted(&owned, &predicate), accepted(&fixed, &predicate));
                }
            }
        }
        for operation in [
            Operation::Count(true),
            Operation::Count(false),
            Operation::Sum(0),
            Operation::Min(1),
            Operation::Max(2),
            Operation::Sum(3),
        ] {
            let mut a = Accumulator::default();
            let mut b = Accumulator::default();
            for _ in 0..2 {
                a.add(operation, &owned).unwrap();
                b.add(operation, &fixed).unwrap();
            }
            assert_eq!(
                a.finish(operation).map_err(|e| e.to_string()),
                b.finish(operation).map_err(|e| e.to_string())
            );
        }
        assert!(integer(&owned, 4).is_err());
        assert!(integer(&fixed, 4).is_err());
    }

    #[test]
    fn adaptive_group_cache_requires_reuse_and_accounts_peak_memory() {
        let mut cache = NumericGroupCache::new();
        let mut remaining = 1_000_000;
        for index in 0..1024 {
            cache.store(index as i64 - 512, index);
        }
        assert!(
            cache.larger.is_none(),
            "first-seen groups must not grow cache"
        );
        for index in 0..GROUP_CACHE_SLOTS - 1 {
            cache.store_repeated(index as i64 - 512, index, 1024, &mut remaining);
        }
        assert!(cache.larger.is_none(), "wait for observed repeated misses");
        cache.store_repeated(-449, 63, 1024, &mut remaining);
        assert_eq!(cache.entries().len(), MAX_ADAPTIVE_GROUP_SLOTS);
        let allocated =
            cache.larger.as_ref().unwrap().capacity() * std::mem::size_of::<Option<(i64, usize)>>();
        assert_eq!(remaining, 1_000_000 - allocated);
        for index in 0..1024 {
            cache.store(index as i64 - 512, index);
        }
        for index in 0..1024 {
            assert_eq!(cache.get(index as i64 - 512), Some(index));
        }
        for key in [i64::MIN, i64::MAX, -1, 0, 2048, -2048] {
            cache.store(key, 9999);
            assert_eq!(cache.get(key), Some(9999));
            assert_ne!(cache.get(key.wrapping_add(2048)), Some(9999));
        }
        cache.try_grow(usize::MAX, &mut remaining);
        assert_eq!(cache.entries().len(), MAX_ADAPTIVE_GROUP_SLOTS);
        assert_eq!(remaining, 1_000_000 - allocated);
    }

    #[test]
    fn adaptive_group_cache_budget_fallback_and_resize_refund_are_exact() {
        let mut cache = NumericGroupCache::new();
        let entry_bytes = std::mem::size_of::<Option<(i64, usize)>>();
        let mut remaining = 256 * entry_bytes - 1;
        cache.store(7, 3);
        cache.try_grow(128, &mut remaining);
        assert!(cache.larger.is_none());
        assert_eq!(remaining, 256 * entry_bytes - 1);
        assert_eq!(cache.get(7), Some(3));
        remaining = 1_000_000;
        cache.try_grow(128, &mut remaining);
        assert_eq!(cache.entries().len(), 256);
        let first_bytes = cache.larger.as_ref().unwrap().capacity() * entry_bytes;
        assert_eq!(remaining, 1_000_000 - first_bytes);
        // Growth must have room for the entire new allocation before the old
        // allocation can be released, rather than only the capacity difference.
        let mut tight = 2048 * entry_bytes - 1;
        cache.try_grow(1024, &mut tight);
        assert_eq!(cache.entries().len(), 256);
        assert_eq!(tight, 2048 * entry_bytes - 1);
        cache.try_grow(1024, &mut remaining);
        assert_eq!(cache.entries().len(), 2048);
        assert_eq!(cache.get(7), Some(3));
        let final_bytes = cache.larger.as_ref().unwrap().capacity() * entry_bytes;
        assert_eq!(remaining, 1_000_000 - final_bytes);
    }

    #[test]
    fn novel_group_reclaims_adaptive_cache_before_tight_budget_admission() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "reclaim",
            1,
            vec![],
            vec![Relation::new(
                "reclaim.Item",
                vec![Role::new("bucket", DataType::Int)],
            )],
        ))
        .unwrap();
        tx.commit().unwrap();
        let sql = db.admin().snapshot().unwrap().sql().unwrap();
        let prepared = sql
            .prepare("SELECT bucket,COUNT(*) FROM reclaim.Item GROUP BY bucket")
            .unwrap();
        let plan = prepared.aggregate.as_ref().unwrap();
        let limits = QueryLimits::default();
        let mut fold = GroupedFold::<UnfilteredFold<'_>> {
            plan,
            predicates: &[],
            filters: &[],
            groups: GroupIndex::new(1),
            accumulators: Vec::new(),
            state_budget: GroupBudget::new(limits, 2),
            cache: None,
            cache_eligible: true,
            first_group: true,
            limits,
        };
        let row = |bucket| Row {
            id: Id::from_u128(1),
            values: vec![Value::Int(bucket)].into_boxed_slice(),
        };
        for key in 0..128 {
            fold.consume(&row(key)).unwrap();
        }
        // Give optional growth exactly the remaining capacity, reproducing a
        // near-limit query where the next new group otherwise cannot fit.
        let growth_bytes = 256 * std::mem::size_of::<Option<(i64, usize)>>();
        fold.state_budget.remaining = growth_bytes;
        for key in 0..64 {
            fold.consume(&row(key)).unwrap();
        }
        assert_eq!(fold.state_budget.remaining, 0);
        assert_eq!(fold.cache.as_ref().unwrap().entries().len(), 256);
        fold.consume(&row(127)).unwrap();
        assert_eq!(fold.cache.as_ref().unwrap().entries().len(), 256);
        assert_eq!(fold.state_budget.remaining, 0);

        fold.consume(&row(128)).unwrap();
        let cache = fold.cache.as_ref().unwrap();
        assert!(cache.larger.is_none());
        assert_eq!(cache.repeated_misses, 0);
        assert_eq!(cache.get(128), Some(128));
        assert!(fold.state_budget.remaining > 0);
        assert!(fold.state_budget.remaining < growth_bytes);
        assert_eq!(fold.state_budget.groups, 129);
        assert_eq!(fold.accumulators[0].as_ref().unwrap().count, 2);
        assert_eq!(fold.accumulators[127].as_ref().unwrap().count, 2);
        assert_eq!(fold.accumulators[128].as_ref().unwrap().count, 1);
        // Any retained small-cache entry must still refer to the matching group.
        for &(key, index) in cache.entries().iter().flatten() {
            assert_eq!(index, key as usize);
        }
    }

    #[test]
    fn wider_numeric_groups_preserve_filtered_folds_and_output_limits() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "wide",
            1,
            vec![],
            vec![Relation::new(
                "wide.Item",
                vec![
                    Role::new("bucket", DataType::Int),
                    Role::new("n", DataType::Int),
                ],
            )],
        ))
        .unwrap();
        tx.commit().unwrap();
        for batch in 0..12 {
            let mut tx = db.admin().transaction().unwrap();
            for n in batch * 256..(batch + 1) * 256 {
                tx.insert_with_id(
                    "wide.Item",
                    Id::from_u128(n as u128 + 1),
                    vec![Value::Int(n % 1024 - 512), Value::Int(n)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        let sql = db.admin().snapshot().unwrap().sql().unwrap();
        for filtered in [false, true] {
            let suffix = if filtered {
                " FILTER (WHERE n>=1024)"
            } else {
                ""
            };
            let mut query = sql.prepare(&format!(
                "SELECT bucket,COUNT(*){suffix},SUM(n){suffix} FROM wide.Item WHERE n>=0 GROUP BY bucket ORDER BY bucket"
            )).unwrap();
            assert!(query.aggregate.is_some());
            let limits = QueryLimits {
                max_rows: 1024,
                max_bytes: 4 * 1024 * 1024,
                ..Default::default()
            };
            let rows = query.query_with_limits(&[], limits).unwrap().rows;
            let count = if filtered { 2 } else { 3 };
            let expected: Vec<_> = (0..1024i64)
                .map(|bucket| {
                    vec![
                        SqlValue::Integer(bucket - 512),
                        SqlValue::Integer(count),
                        SqlValue::Integer(count * bucket + 3072),
                    ]
                })
                .collect();
            assert_eq!(rows, expected);
            let error = query
                .query_with_limits(
                    &[],
                    QueryLimits {
                        max_rows: 1023,
                        ..limits
                    },
                )
                .unwrap_err();
            assert!(matches!(error, Error::Query(message) if message.contains("row budget")));
        }
    }

    #[test]
    fn numeric_group_cache_collisions_and_extremes_never_alias() {
        let mut cache = [None; GROUP_CACHE_SLOTS];
        let keys = [0, 64, 128, -64, i64::MIN, i64::MAX, -1, 63];
        for (index, key) in keys.iter().copied().enumerate() {
            assert_eq!(cached_group(&cache, key), None);
            cache[cache_slot(key)] = Some((key, index));
            assert_eq!(cached_group(&cache, key), Some(index));
            for earlier in &keys[..index] {
                if cache_slot(*earlier) == cache_slot(key) {
                    assert_eq!(cached_group(&cache, *earlier), None);
                }
            }
        }
        assert!(std::mem::size_of::<GroupCache>() <= 2048);
    }

    #[test]
    fn local_aggregate_source_budget_has_an_exact_limit_without_underflow() {
        let mut budget = AggregateBudget::with_source_limit(30_000, 2);
        assert!(budget.charge().is_ok());
        assert!(budget.charge().is_ok());
        assert!(
            matches!(budget.charge(), Err(Error::Query(message)) if message.contains("source row budget"))
        );
        assert_eq!(budget.remaining, 0);
        assert!(budget.charge().is_err());
        assert_eq!(AggregateBudget::new(30_000).remaining, 50_000_000);
    }

    #[test]
    fn local_aggregate_checks_deadline_before_first_work_and_final_release() {
        let mut budget = AggregateBudget::with_source_limit(30_000, 1024);
        assert!(budget.charge().is_ok());
        budget.start = std::time::Instant::now() - std::time::Duration::from_secs(60);
        // A short final partial interval is allowed to finish its work, but
        // must never release an aggregate computed after the query deadline.
        assert!(budget.charge().is_ok());
        assert!(
            matches!(budget.check_deadline(), Err(Error::Query(message)) if message.contains("deadline"))
        );
        // The next periodic check must occur within 256 examined nodes, even
        // when no visible rows have reached an aggregate accumulator.
        for _ in 0..254 {
            budget.charge().unwrap();
        }
        assert!(budget.charge().is_err());
        let mut first = AggregateBudget::with_source_limit(0, 1);
        assert!(first.charge().is_err());
    }

    #[test]
    fn repeated_large_bindings_are_charged_before_copying_payloads() {
        let relation = Relation::new("app.Item", vec![Role::new("tag", DataType::Text)]);
        let predicates = (0..2)
            .map(|_| Condition {
                column: 1,
                operator: Operator::Eq,
                binding: Binding::Parameter(0),
            })
            .collect::<Vec<_>>();
        let params = [SqlValue::Text("x".repeat(1024))];
        let mut remaining = 2 * std::mem::size_of::<BoundCondition>() + 1024;
        assert!(
            matches!(bind(&predicates, &params, &relation, &mut remaining), Err(Error::Query(message)) if message.contains("state byte budget"))
        );
        assert_eq!(remaining, 0);
        let mut remaining = 1;
        assert!(reserve_memory(&mut remaining, usize::MAX).is_err());
        assert_eq!(remaining, 1);
    }

    #[test]
    fn grouped_state_budget_is_bounded_before_allocating_a_new_group() {
        let limits = QueryLimits {
            max_rows: 2,
            max_bytes: 1024,
            vm_steps: 4,
            timeout_ms: 1000,
        };
        let mut budget = GroupBudget::new(limits, 2);
        assert_eq!(budget.remaining, 1024);
        budget.reserve(512, limits).unwrap();
        budget.reserve(512, limits).unwrap();
        assert_eq!(budget.remaining, 0);
        assert!(
            matches!(budget.check_key(1), Err(Error::Query(m)) if m.contains("state byte budget"))
        );
        assert!(
            matches!(budget.reserve(0, limits), Err(Error::Query(m)) if m.contains("row budget"))
        );
        let limits = QueryLimits {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
            vm_steps: u64::MAX,
            timeout_ms: 1000,
        };
        let mut budget = GroupBudget::new(limits, 1);
        assert_eq!(budget.remaining, GROUP_STATE_BYTES);
        assert!(budget.reserve(GROUP_STATE_BYTES + 1, limits).is_err());
        assert_eq!(budget.remaining, GROUP_STATE_BYTES);
        assert_eq!(budget.groups, 0);
    }

    #[test]
    fn scalar_compiler_and_live_credential_fences() {
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
        tx.grant("reader", "app.Item", Grant::read_only()).unwrap();
        tx.commit().unwrap();
        let cap = Id::new();
        db.issue_capability_until(
            cap,
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: tid,
                    grant: Grant {
                        filter: RowFilter::Equals {
                            role: "n".into(),
                            value: 1.into(),
                        },
                        ..Grant::read_only()
                    },
                }],
                schema_namespace: None,
                delegations: 0,
            },
            Some(200),
        )
        .unwrap();
        let credential = db.issue_credential("reader", cap, 150).unwrap();
        let sql = db
            .authenticate(&credential.token)
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        let mut prepared = sql
            .prepare("SELECT COUNT(*),SUM(n),MIN(n),MAX(n) FROM app.Item")
            .unwrap();
        assert!(
            prepared.aggregate.is_some(),
            "fixture must exercise bounded scalar path"
        );
        assert_eq!(
            prepared.query(&[]).unwrap().rows,
            vec![vec![SqlValue::Integer(1); 4]]
        );
        assert!(prepared.aggregate.as_ref().unwrap().unfiltered_scalar);
        let compact = prepared.query(&[]).unwrap();
        prepared.aggregate.as_mut().unwrap().unfiltered_scalar = false;
        assert_eq!(prepared.query(&[]).unwrap().rows, compact.rows);
        prepared.aggregate.as_mut().unwrap().unfiltered_scalar = true;
        for text in [
            "SELECT AVG(n) FROM app.Item",
            "SELECT COUNT(DISTINCT n) FROM app.Item",
            "SELECT COUNT(*) FROM app.Item LIMIT 0",
            "SELECT c FROM (SELECT COUNT(*) AS c,SUM(n) AS s FROM app.Item) q",
        ] {
            assert!(
                sql.prepare(text).unwrap().aggregate.is_none(),
                "fallback: {text}"
            );
        }
        for text in [
            "SELECT COUNT(*),SUM(n) FROM app.Item WHERE n>=?1 AND n<?2",
            "SELECT n,COUNT(*) FROM app.Item WHERE n>=?1 GROUP BY n ORDER BY n DESC",
            "SELECT COUNT(*) FILTER (WHERE n=?1), SUM(n) FILTER (WHERE n>?2) FROM app.Item",
            "SELECT n,COUNT(*) AS c FROM app.Item GROUP BY n ORDER BY c DESC,n",
        ] {
            assert!(
                sql.prepare(text).unwrap().aggregate.is_some(),
                "compiled: {text}"
            );
        }
        let fresh = db.rotate_credential(credential.id, 150).unwrap();
        assert!(matches!(prepared.query(&[]), Err(Error::Denied)));
        let sql = db
            .authenticate(&fresh.token)
            .unwrap()
            .snapshot()
            .unwrap()
            .sql()
            .unwrap();
        let mut prepared = sql.prepare("SELECT COUNT(*),SUM(n) FROM app.Item").unwrap();
        assert!(prepared.aggregate.is_some());
        assert_eq!(
            prepared.query(&[]).unwrap().rows,
            vec![vec![SqlValue::Integer(1); 2]]
        );
        db.inner.clock.set(150);
        assert!(matches!(prepared.query(&[]), Err(Error::Denied)));
    }
}
