//! Compiled simple reads over authorized native indexes. DataFusion still parses
//! and validates SQL; unsupported plans or parameter coercions use its executor.
//! No SQL text matching and no parameter-specific optimizer state is retained.
use super::{
    provider::{CELL_BYTES, LiquidTable, SourceBudget},
    sql_value,
};
use crate::{
    DataType, Error, Id, QueryLimits, Relation, Result, Snapshot, SqlTable, Value, dx::NumericRange,
};
use datafusion::{
    common::ScalarValue,
    datasource::source_as_provider,
    logical_expr::{
        Expr, LogicalPlan, Operator,
        logical_plan::{FetchType, SkipType},
    },
};
use rusqlite::types::Value as SqlValue;
use std::{
    any::Any,
    ops::Bound,
    time::{Duration, Instant},
};

#[derive(Clone)]
enum Binding {
    Literal(SqlValue),
    Parameter(usize),
}
pub(super) struct DirectRead {
    snapshot: Snapshot,
    relation: Relation,
    columns: Vec<String>,
    projection: Vec<usize>,
    source_projection: Vec<usize>,
    selection: Selection,
    skip: usize,
    fetch: Option<usize>,
}
struct Shape {
    snapshot: Snapshot,
    relation: Relation,
    projection: Vec<usize>,
    predicates: Vec<(usize, Operator, Binding)>,
    order: Vec<(usize, bool)>,
    skip: usize,
    fetch: Option<usize>,
    limited: bool,
}

impl DirectRead {
    pub(super) fn compile(plan: &LogicalPlan) -> Option<Self> {
        let shape = shape(plan)?;
        // SQL NULL ordering and floating coercions stay in DataFusion.
        if shape
            .relation
            .roles
            .iter()
            .any(|r| matches!(r.data_type, DataType::Nullable(_) | DataType::Float))
        {
            return None;
        }
        let [(column, operator, binding), rest @ ..] = shape.predicates.as_slice() else {
            return None;
        };
        let selection = if *operator == Operator::Eq && rest.is_empty() {
            // Equality indexes emit IDs ascending. The key itself is constant;
            // ordering is vacuous for an identity lookup.
            if *column != 0
                && shape
                    .order
                    .iter()
                    .any(|(col, asc)| *col != *column && !(*col == 0 && *asc))
            {
                return None;
            }
            Selection::Equality {
                column: *column,
                binding: binding.clone(),
            }
        } else {
            // Broad unbounded reads retain DataFusion's bounded Arrow page cache.
            // The direct path targets explicitly bounded application reads.
            shape.fetch?;
            let slot = column.checked_sub(1)?;
            if !matches!(
                shape.relation.roles[slot].data_type,
                DataType::Int | DataType::Decimal { .. } | DataType::Timestamp
            ) || shape
                .predicates
                .iter()
                .any(|(col, op, _)| *col != *column || !range_operator(*op))
            {
                return None;
            }
            // The native index orders (value, occurrence ID), both in the same
            // direction. Never infer ID ordering when the leading value varies.
            let descending = match shape.order.as_slice() {
                [] => false,
                [(first, asc)] if first == column => !asc,
                [(first, asc), (0, id_asc)] if first == column && asc == id_asc => !asc,
                _ => return None,
            };
            Selection::Range {
                column: *column,
                bounds: shape
                    .predicates
                    .iter()
                    .map(|(_, op, binding)| (*op, binding.clone()))
                    .collect(),
                descending,
            }
        };
        let mut source_projection = shape.projection.clone();
        source_projection.sort_unstable();
        source_projection.dedup();
        Some(Self {
            snapshot: shape.snapshot,
            relation: shape.relation,
            columns: plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            projection: shape.projection,
            source_projection,
            selection,
            skip: shape.skip,
            fetch: shape.fetch,
        })
    }

    /// None means the binding requires DataFusion coercion, not an empty result.
    pub(super) fn query(
        &self,
        params: &[SqlValue],
        limits: QueryLimits,
    ) -> Result<Option<SqlTable>> {
        let Some(access) = self.selection.bind(params, &self.relation)? else {
            return Ok(None);
        };
        let start = Instant::now();
        let check = || -> Result<()> {
            self.snapshot.live.check_attached()?;
            if start.elapsed() >= Duration::from_millis(limits.timeout_ms) {
                return Err(Error::Query("SQL deadline exceeded".into()));
            }
            Ok(())
        };
        check()?;
        let mut bytes = self.columns.iter().map(String::len).sum::<usize>();
        if bytes > limits.max_bytes {
            return Err(Error::Query("query byte budget exceeded".into()));
        }
        let mut result = SqlTable {
            columns: self.columns.clone(),
            rows: Vec::new(),
        };
        if matches!(access, BoundSelection::Empty) {
            check()?;
            return Ok(Some(result));
        };
        let mut remaining = self.fetch.unwrap_or(usize::MAX);
        let mut skip = self.skip;
        let mut after = None;
        let mut range_after = None;
        let mut work = 0u64;
        let budget = SourceBudget::default();
        while remaining > 0 {
            check()?;
            let count = 1000.min(remaining.saturating_add(skip));
            let (rows, more) = match &access {
                BoundSelection::Equality {
                    column: 0,
                    value: Value::Ref(id),
                } => (
                    self.snapshot
                        .get_shared(&self.relation.name, *id)?
                        .into_iter()
                        .collect::<Vec<_>>(),
                    false,
                ),
                BoundSelection::Equality { column, value } if *column > 0 => {
                    let page = self.snapshot.page_shared(
                        &self.relation.name,
                        Some((&self.relation.roles[column - 1].name, value)),
                        after,
                        count,
                    )?;
                    after = page.next;
                    (page.rows, after.is_some())
                }
                BoundSelection::Range(range) => {
                    let page = self.snapshot.range_page_shared(
                        &self.relation.name,
                        range,
                        range_after.as_ref(),
                        count,
                    )?;
                    range_after = page.next;
                    (page.rows, range_after.is_some())
                }
                _ => return Err(Error::Query("invalid compiled selection".into())),
            };
            budget.charge(rows.len(), 0).map_err(super::query_error)?;
            for row in rows {
                // Native pages and the final result release are live-authority
                // fences. Output construction publishes nothing, so only the
                // deadline needs checking for each row, not the head mutex.
                if start.elapsed() >= Duration::from_millis(limits.timeout_ms) {
                    return Err(Error::Query("SQL deadline exceeded".into()));
                }
                // Charge selected native payload before allocating output values.
                let source_bytes =
                    self.source_projection
                        .iter()
                        .try_fold(0usize, |total, &column| {
                            source_size(&row, &self.relation, column)
                                .map(|size| total.saturating_add(size))
                        })?;
                budget.charge(0, source_bytes).map_err(super::query_error)?;
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if result.rows.len() == limits.max_rows {
                    return Err(Error::Query(
                        "query row budget exceeded; use LIMIT or pages".into(),
                    ));
                }
                work = work.saturating_add(self.projection.len().max(1) as u64);
                if work > limits.vm_steps {
                    return Err(Error::Query("query output-cell budget exceeded".into()));
                }
                let mut values = Vec::with_capacity(self.projection.len());
                for &column in &self.projection {
                    let size = output_size(&row, column);
                    if size > limits.max_bytes.saturating_sub(bytes) {
                        return Err(Error::Query("query byte budget exceeded".into()));
                    }
                    bytes += size;
                    values.push(output_value(&row, column));
                }
                result.rows.push(values);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
            if !more {
                break;
            }
        }
        check()?;
        Ok(Some(result))
    }
}

fn shape(plan: &LogicalPlan) -> Option<Shape> {
    match plan {
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
            let provider = source_as_provider(&scan.source).ok()?;
            let table = (provider.as_ref() as &dyn Any).downcast_ref::<LiquidTable>()?;
            Some(Shape {
                snapshot: table.snapshot().clone(),
                relation: table.declaration().clone(),
                projection: scan
                    .projection
                    .clone()
                    .unwrap_or_else(|| (0..table.declaration().roles.len() + 1).collect()),
                predicates: Vec::new(),
                order: Vec::new(),
                skip: 0,
                fetch: None,
                limited: false,
            })
        }
        LogicalPlan::SubqueryAlias(alias) => shape(&alias.input),
        LogicalPlan::Projection(projection) => {
            let mut input = shape(&projection.input)?;
            input.projection = projection
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
                    input
                        .projection
                        .get(projection.input.schema().index_of_column(column).ok()?)
                        .copied()
                })
                .collect::<Option<_>>()?;
            Some(input)
        }
        LogicalPlan::Filter(filter) => {
            let mut input = shape(&filter.input)?;
            if input.limited {
                return None;
            }
            collect_predicates(
                &filter.predicate,
                &filter.input,
                &input.projection,
                &mut input.predicates,
            )?;
            Some(input)
        }
        LogicalPlan::Sort(sort) => {
            let mut input = shape(&sort.input)?;
            if input.limited || sort.fetch.is_some() {
                return None;
            }
            input.order = sort
                .expr
                .iter()
                .map(|ordering| {
                    let Expr::Column(column) = &ordering.expr else {
                        return None;
                    };
                    Some((
                        *input
                            .projection
                            .get(sort.input.schema().index_of_column(column).ok()?)?,
                        ordering.asc,
                    ))
                })
                .collect::<Option<_>>()?;
            Some(input)
        }
        LogicalPlan::Limit(limit) => {
            let mut input = shape(&limit.input)?;
            if input.limited {
                return None;
            }
            let SkipType::Literal(skip) = limit.get_skip_type().ok()? else {
                return None;
            };
            let FetchType::Literal(fetch) = limit.get_fetch_type().ok()? else {
                return None;
            };
            input.skip = skip;
            input.fetch = fetch;
            input.limited = true;
            Some(input)
        }
        _ => None,
    }
}

fn range_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

/// Accept only plain column comparisons and AND. All casts, expressions and
/// other Boolean logic retain DataFusion's coercion/evaluation semantics.
fn collect_predicates(
    expr: &Expr,
    input: &LogicalPlan,
    projection: &[usize],
    out: &mut Vec<(usize, Operator, Binding)>,
) -> Option<()> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        collect_predicates(&binary.left, input, projection, out)?;
        return collect_predicates(&binary.right, input, projection, out);
    }
    if binary.op != Operator::Eq && !range_operator(binary.op) {
        return None;
    }
    let (column, value, op) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), value) => (column, value, binary.op),
        (value, Expr::Column(column)) => (
            column,
            value,
            match binary.op {
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                op => op,
            },
        ),
        _ => return None,
    };
    let column = *projection.get(input.schema().index_of_column(column).ok()?)?;
    let binding = match value {
        Expr::Placeholder(p) => Binding::Parameter(
            p.id.strip_prefix('$')?
                .parse::<usize>()
                .ok()?
                .checked_sub(1)?,
        ),
        Expr::Literal(value, _) => Binding::Literal(match value {
            ScalarValue::Null
            | ScalarValue::Utf8(None)
            | ScalarValue::Int64(None)
            | ScalarValue::Binary(None) => SqlValue::Null,
            ScalarValue::Int64(_) | ScalarValue::Utf8(_) | ScalarValue::Binary(_) => {
                sql_value(value.clone()).ok()?
            }
            _ => return None,
        }),
        _ => return None,
    };
    out.push((column, op, binding));
    Some(())
}

enum Selection {
    Equality {
        column: usize,
        binding: Binding,
    },
    Range {
        column: usize,
        bounds: Vec<(Operator, Binding)>,
        descending: bool,
    },
}
enum BoundSelection {
    Empty,
    Equality { column: usize, value: Value },
    Range(NumericRange),
}
impl Binding {
    fn value<'a>(&'a self, params: &'a [SqlValue]) -> Result<&'a SqlValue> {
        match self {
            Self::Literal(value) => Ok(value),
            Self::Parameter(index) => params
                .get(*index)
                .ok_or_else(|| Error::Query("SQL parameter count mismatch".into())),
        }
    }
}
impl Selection {
    fn bind(&self, params: &[SqlValue], relation: &Relation) -> Result<Option<BoundSelection>> {
        match self {
            Self::Equality { column, binding } => Ok(
                match key(
                    binding.value(params)?,
                    column
                        .checked_sub(1)
                        .map(|slot| &relation.roles[slot].data_type),
                ) {
                    Key::Unsupported => None,
                    Key::Empty => Some(BoundSelection::Empty),
                    Key::Value(value) => Some(BoundSelection::Equality {
                        column: *column,
                        value,
                    }),
                },
            ),
            Self::Range {
                column,
                bounds,
                descending,
            } => {
                let mut range = NumericRange {
                    slot: column - 1,
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                    descending: *descending,
                };
                let mut empty = false;
                // Inspect every binding before returning an empty range: NULL
                // must not hide another parameter's unsupported coercion.
                for (op, binding) in bounds {
                    let value = match key(
                        binding.value(params)?,
                        Some(&relation.roles[column - 1].data_type),
                    ) {
                        Key::Unsupported => return Ok(None),
                        Key::Empty => {
                            empty = true;
                            continue;
                        }
                        Key::Value(value) => value,
                    };
                    let (bound, lower) = match op {
                        Operator::Lt => (Bound::Excluded(value), false),
                        Operator::LtEq => (Bound::Included(value), false),
                        Operator::Gt => (Bound::Excluded(value), true),
                        Operator::GtEq => (Bound::Included(value), true),
                        _ => return Err(Error::Query("invalid compiled range operator".into())),
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
                Ok(Some(if empty {
                    BoundSelection::Empty
                } else {
                    BoundSelection::Range(range)
                }))
            }
        }
    }
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

pub(crate) enum Key {
    Empty,
    Unsupported,
    Value(Value),
}
pub(crate) fn key(value: &SqlValue, kind: Option<&DataType>) -> Key {
    if matches!(value, SqlValue::Null) {
        return Key::Empty;
    }
    let value = match (kind.map(DataType::base_type), value) {
        (Some(DataType::Float), SqlValue::Real(v)) => match crate::FiniteFloat::new(*v) {
            Ok(v) => Value::Float(v),
            Err(_) => return Key::Unsupported,
        },
        (None | Some(DataType::Ref(_)), SqlValue::Text(text)) => {
            let Ok(uuid) = uuid::Uuid::parse_str(text) else {
                return Key::Empty;
            };
            let id = Id(uuid);
            if id.to_string() != *text {
                return Key::Empty;
            }
            Value::Ref(id)
        }
        (Some(DataType::Text), SqlValue::Text(value)) => Value::Text(value.clone()),
        (Some(DataType::Bytes), SqlValue::Blob(value)) => Value::Bytes(value.clone()),
        (Some(DataType::Int), SqlValue::Integer(value)) => Value::Int(*value),
        (Some(DataType::Timestamp), SqlValue::Integer(value)) => Value::Timestamp(*value),
        (Some(DataType::Decimal { .. }), SqlValue::Integer(value)) => Value::Decimal(*value),
        (Some(DataType::Bool), SqlValue::Integer(0)) => Value::Bool(false),
        (Some(DataType::Bool), SqlValue::Integer(1)) => Value::Bool(true),
        (Some(DataType::Bool), SqlValue::Integer(_)) => return Key::Empty,
        _ => return Key::Unsupported,
    };
    Key::Value(value)
}
pub(super) fn source_size(row: &crate::Row, relation: &Relation, column: usize) -> Result<usize> {
    if column == 0 {
        return Ok(40);
    }
    let value = row
        .values
        .get(column - 1)
        .filter(|value| relation.roles[column - 1].data_type.accepts(value))
        .ok_or_else(|| Error::Query("typed SQL source mismatch".into()))?;
    Ok(match value {
        Value::Text(value) => {
            if value.len() > CELL_BYTES {
                return Err(Error::Query("SQL source cell exceeds 1 MiB budget".into()));
            }
            value.len().saturating_add(4)
        }
        Value::Bytes(value) => {
            if value.len() > CELL_BYTES {
                return Err(Error::Query("SQL source cell exceeds 1 MiB budget".into()));
            }
            value.len().saturating_add(4)
        }
        Value::Ref(_) => 40,
        _ => 8,
    })
}
pub(super) fn output_size(row: &crate::Row, column: usize) -> usize {
    if column == 0 {
        return 36;
    }
    match &row.values[column - 1] {
        Value::Text(v) => v.len(),
        Value::Bytes(v) => v.len(),
        Value::Ref(_) => 36,
        _ => 8,
    }
}
pub(super) fn output_value(row: &crate::Row, column: usize) -> SqlValue {
    if column == 0 {
        return SqlValue::Text(row.id.to_string());
    }
    match &row.values[column - 1] {
        Value::Text(v) => SqlValue::Text(v.clone()),
        Value::Bytes(v) => SqlValue::Blob(v.clone()),
        Value::Ref(v) => SqlValue::Text(v.to_string()),
        Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
        Value::Null => SqlValue::Null,
        Value::Float(v) => SqlValue::Real(v.get()),
        Value::Int(v) | Value::Timestamp(v) | Value::Decimal(v) => SqlValue::Integer(*v),
    }
}

#[cfg(test)]
mod tests {
    use super::super::DfSnapshot;
    use super::*;
    use crate::*;

    fn fixture() -> Database {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "direct",
                1,
                vec![],
                vec![Relation::new(
                    "direct.Item",
                    vec![
                        Role::new("n", DataType::Int),
                        Role::new("tag", DataType::Text),
                        Role::new("enabled", DataType::Bool),
                        Role::new("payload", DataType::Bytes),
                    ],
                )],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for n in 0..30 {
            tx.insert_with_id(
                "direct.Item",
                Id::from_u128(n + 1),
                vec![
                    Value::Int((n % 3) as i64),
                    Value::Text(format!("tag{n}")),
                    Value::Bool(n % 2 == 0),
                    Value::Bytes(vec![n as u8]),
                ],
            )
            .unwrap();
        }
        tx.grant(
            "even",
            "direct.Item",
            Grant {
                filter: RowFilter::Equals {
                    role: "enabled".into(),
                    value: Value::Bool(true),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        db
    }

    #[test]
    fn compiled_lookups_rebind_unseen_values_without_planning_or_result_reuse() {
        let db = fixture();
        let snapshot = db.admin().snapshot();
        let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
        let sqlite = snapshot.sqlite_compat().unwrap();
        for (text, values) in [
            (
                "SELECT id,tag AS label,tag FROM direct.Item WHERE id=?1",
                (1..=35)
                    .map(|id| SqlValue::Text(Id::from_u128(id).to_string()))
                    .collect::<Vec<_>>(),
            ),
            (
                "SELECT tag,n FROM direct.Item WHERE n=?1 ORDER BY id LIMIT 4 OFFSET 2",
                (0..7).map(SqlValue::Integer).collect(),
            ),
            (
                "SELECT id,n FROM direct.Item WHERE ?1=n ORDER BY n DESC,id",
                (0..7).map(SqlValue::Integer).collect(),
            ),
            (
                "SELECT id FROM direct.Item WHERE payload=?1 ORDER BY id",
                (0..35).map(|n| SqlValue::Blob(vec![n])).collect(),
            ),
            (
                "SELECT id FROM direct.Item WHERE enabled=?1 ORDER BY id",
                vec![
                    SqlValue::Integer(0),
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Null,
                ],
            ),
        ] {
            let mut query = sql.prepare(text).unwrap();
            assert!(query.direct.is_some(), "{text}");
            for value in values {
                let args = [value];
                let expected = sqlite.query(text, &args).unwrap();
                let actual = query.query(&args, QueryLimits::default()).unwrap();
                assert_eq!(actual.columns, expected.columns, "{text}");
                assert_eq!(actual.rows, expected.rows, "{text}");
                assert!(
                    query.cache.get(&args).is_none(),
                    "simple bindings must not optimize/cache individually"
                );
            }
        }
    }

    #[test]
    fn compiled_lookup_coercions_and_plan_barriers_fall_back_without_changing_results() {
        let db = fixture();
        let sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        for (text, args) in [
            (
                "SELECT id FROM direct.Item WHERE n=?1 ORDER BY id",
                vec![SqlValue::Real(1.)],
            ),
            (
                "SELECT id FROM direct.Item WHERE id=?1",
                vec![SqlValue::Integer(1)],
            ),
            (
                "SELECT id FROM direct.Item WHERE tag=?1",
                vec![SqlValue::Integer(1)],
            ),
            (
                "SELECT tag FROM direct.Item WHERE id=?1",
                vec![SqlValue::Text("00000000000000000000000000000001".into())],
            ),
            (
                "SELECT tag FROM direct.Item WHERE id=?1",
                vec![SqlValue::Null],
            ),
        ] {
            let mut specialized = sql.prepare(text).unwrap();
            assert!(specialized.direct.is_some(), "{text}");
            let mut generic = sql.prepare(text).unwrap();
            generic.direct = None;
            let actual = specialized.query(&args, QueryLimits::default());
            let expected = generic.query(&args, QueryLimits::default());
            match (actual, expected) {
                (Ok(a), Ok(b)) => assert_eq!(a.rows, b.rows, "{text}"),
                (Err(_), Err(_)) => {}
                (a, b) => panic!("{text}: {a:?} != {b:?}"),
            }
        }
        for text in [
            "SELECT tag FROM (SELECT * FROM direct.Item LIMIT 1) t WHERE n=1",
            "SELECT tag FROM direct.Item WHERE n=1 ORDER BY tag DESC",
            "SELECT tag FROM direct.Item WHERE n=1 OR n=2",
            "SELECT count(*) FROM direct.Item WHERE n=1",
        ] {
            assert!(sql.prepare(text).unwrap().direct.is_none(), "{text}");
        }
    }

    #[test]
    fn direct_reads_preserve_budgets_pinned_data_and_live_grants() {
        let db = fixture();
        let snapshot = db.session("even").snapshot();
        let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let text = "SELECT id,tag FROM direct.Item WHERE n=?1 ORDER BY id";
        let mut query = sql.prepare(text).unwrap();
        assert!(query.direct.is_some());
        let args = [SqlValue::Integer(1)];
        let old = query.query(&args, QueryLimits::default()).unwrap();
        assert_eq!(old.rows, sqlite.query(text, &args).unwrap().rows);
        for limits in [
            QueryLimits {
                max_rows: 1,
                ..QueryLimits::default()
            },
            QueryLimits {
                max_bytes: 1,
                ..QueryLimits::default()
            },
            QueryLimits {
                vm_steps: 1,
                ..QueryLimits::default()
            },
        ] {
            assert!(query.query(&args, limits).is_err());
            assert_eq!(
                query.query(&args, QueryLimits::default()).unwrap().rows,
                old.rows
            );
        }
        let mut tx = db.admin().transaction();
        tx.insert(
            "direct.Item",
            vec![
                Value::Int(1),
                "later".into(),
                Value::Bool(true),
                Value::Bytes(vec![]),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            query.query(&args, QueryLimits::default()).unwrap().rows,
            old.rows
        );
        let mut tx = db.admin().transaction();
        tx.revoke("even", "direct.Item").unwrap();
        tx.commit().unwrap();
        assert!(matches!(
            query.query(&args, QueryLimits::default()),
            Err(Error::Denied)
        ));
    }

    #[test]
    fn compiled_ranges_rebind_unseen_bounds_with_sqlite_parity() {
        let db = fixture();
        for snapshot in [db.admin().snapshot(), db.session("even").snapshot()] {
            let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
            let sqlite = snapshot.sqlite_compat().unwrap();
            for text in [
                "SELECT id,n AS number,tag FROM direct.Item WHERE n>=?1 AND n<?2 ORDER BY n,id LIMIT 100",
                "SELECT id,n AS number,tag FROM direct.Item WHERE ?1<=n AND ?2>n ORDER BY number DESC,id DESC LIMIT 7 OFFSET 3",
                "SELECT n,id FROM direct.Item WHERE n>?1 AND n<=?2 ORDER BY n,id LIMIT 4 OFFSET 12",
                "SELECT id,n FROM direct.Item WHERE n>=?1 AND n<=?2 AND n>0 AND n<3 ORDER BY n DESC,id DESC LIMIT 100",
                "SELECT id,n FROM direct.Item WHERE n>?1 AND n<?2 ORDER BY n,id LIMIT 0",
                "SELECT id,n FROM direct.Item WHERE n>=?1 AND n<=?2 ORDER BY n NULLS FIRST,id NULLS LAST LIMIT 100",
            ] {
                let mut query = sql.prepare(text).unwrap();
                assert!(query.direct.is_some(), "{text}");
                for lower in [i64::MIN, -4, -1, 0, 1, 2, 3, 7, i64::MAX] {
                    for upper in [i64::MIN, -2, 0, 1, 2, 3, 8, i64::MAX] {
                        let args = [SqlValue::Integer(lower), SqlValue::Integer(upper)];
                        let expected = sqlite.query(text, &args).unwrap();
                        let actual = query.query(&args, QueryLimits::default()).unwrap();
                        assert_eq!(actual.columns, expected.columns, "{text}");
                        assert_eq!(actual.rows, expected.rows, "{text}: {args:?}");
                        assert!(
                            query.cache.get(&args).is_none(),
                            "range bindings must not replan: {text}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn compiled_ranges_preserve_coercion_and_plan_barriers() {
        let db = fixture();
        let sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let text = "SELECT id,n FROM direct.Item WHERE n>=?1 AND n<=?2 ORDER BY n,id LIMIT 100";
        for args in [
            vec![SqlValue::Null, SqlValue::Integer(2)],
            vec![SqlValue::Integer(0), SqlValue::Null],
            vec![SqlValue::Real(0.5), SqlValue::Integer(2)],
            vec![SqlValue::Integer(0), SqlValue::Text("2".into())],
            vec![SqlValue::Null, SqlValue::Text("not a number".into())],
            vec![SqlValue::Blob(vec![0]), SqlValue::Integer(2)],
        ] {
            let mut specialized = sql.prepare(text).unwrap();
            assert!(specialized.direct.is_some(), "{text}");
            let mut generic = sql.prepare(text).unwrap();
            generic.direct = None;
            let actual = specialized.query(&args, QueryLimits::default());
            let expected = generic.query(&args, QueryLimits::default());
            match (actual, expected) {
                (Ok(a), Ok(b)) => assert_eq!(a.rows, b.rows, "{args:?}"),
                (Err(_), Err(_)) => {}
                (a, b) => panic!("{args:?}: {a:?} != {b:?}"),
            }
        }
        for text in [
            "SELECT id FROM direct.Item WHERE n>=0 ORDER BY id",
            "SELECT id FROM direct.Item WHERE n>=0 ORDER BY n DESC,id",
            "SELECT id FROM direct.Item WHERE n>=0 AND enabled=1 ORDER BY n,id",
            "SELECT id FROM direct.Item WHERE n>=0 OR n<2 ORDER BY n,id",
            "SELECT id FROM direct.Item WHERE n+1>=0 ORDER BY n,id",
            "SELECT id FROM direct.Item WHERE enabled>0 ORDER BY enabled,id",
            "SELECT id FROM direct.Item WHERE tag>'a' ORDER BY tag,id",
            "SELECT id FROM (SELECT * FROM direct.Item ORDER BY n LIMIT 1) t WHERE n>=0",
            "SELECT id FROM (SELECT * FROM direct.Item WHERE n>=0 ORDER BY n LIMIT 1) t ORDER BY n DESC",
        ] {
            let text = format!("{text} LIMIT 100");
            assert!(sql.prepare(&text).unwrap().direct.is_none(), "{text}");
        }
    }

    #[test]
    fn unbounded_ranges_retain_generic_cached_execution() {
        let db = fixture();
        for snapshot in [db.admin().snapshot(), db.session("even").snapshot()] {
            let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
            let sqlite = snapshot.sqlite_compat().unwrap();
            for text in [
                "SELECT id,n FROM direct.Item WHERE n>=?1 ORDER BY n,id",
                "SELECT id,n FROM direct.Item WHERE n>=?1 ORDER BY n DESC,id DESC OFFSET 2",
            ] {
                let mut query = sql.prepare(text).unwrap();
                assert!(query.direct.is_none(), "{text}");
                for bound in [-1, 0, 1, 2, 3, -1] {
                    let args = [SqlValue::Integer(bound)];
                    let sqlite_text = text.replace("OFFSET 2", "LIMIT -1 OFFSET 2");
                    let expected = sqlite.query(&sqlite_text, &args).unwrap();
                    for _ in 0..2 {
                        let actual = query.query(&args, QueryLimits::default()).unwrap();
                        assert_eq!(actual.columns, expected.columns, "{text}");
                        assert_eq!(actual.rows, expected.rows, "{text}: {args:?}");
                        assert!(
                            query.cache.get(&args).is_some(),
                            "unbounded range must retain its reusable DataFusion plan"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn compiled_numeric_ranges_page_ties_and_preserve_reference_visibility() {
        let db = fixture();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "ranges",
                1,
                vec![("direct".into(), 1)],
                vec![Relation::new(
                    "ranges.Link",
                    vec![
                        Role::new("n", DataType::Int),
                        Role::new("amount", DataType::Decimal { scale: 2 }),
                        Role::new("instant", DataType::Timestamp),
                        Role::new("target", DataType::Ref("direct.Item".into())),
                    ],
                )],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for n in 0..2406u128 {
            let value = (n % 7) as i64 - 3;
            tx.insert_with_id(
                "ranges.Link",
                Id::from_u128(100 + n),
                vec![
                    Value::Int(value),
                    Value::Decimal(value),
                    Value::Timestamp(value),
                    Value::Ref(Id::from_u128(n % 30 + 1)),
                ],
            )
            .unwrap();
        }
        tx.grant("even", "ranges.Link", Grant::read_only()).unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("even").snapshot();
        let sql = DfSnapshot::new(snapshot.clone(), None).unwrap();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let generous = QueryLimits {
            max_rows: 3000,
            ..QueryLimits::default()
        };
        for role in ["n", "amount", "instant"] {
            for (order, suffix) in [
                ("ASC", " LIMIT 3000"),
                ("DESC", " LIMIT 3000"),
                ("DESC", " LIMIT 13 OFFSET 1001"),
            ] {
                let text = format!(
                    "SELECT id,{role} AS value,target FROM ranges.Link WHERE {role}>=?1 AND {role}<=?2 ORDER BY value {order},id {order}{suffix}"
                );
                let args = [SqlValue::Integer(-3), SqlValue::Integer(3)];
                let mut query = sql.prepare(&text).unwrap();
                assert!(query.direct.is_some(), "{text}");
                let expected = sqlite.query(&text, &args).unwrap();
                let actual = query.query(&args, generous).unwrap();
                assert_eq!(actual.rows, expected.rows, "{text}");
                assert_eq!(
                    actual.rows.len(),
                    if suffix.contains("OFFSET") { 13 } else { 1203 }
                );
                assert!(query.cache.get(&args).is_none());
            }
        }
        let mut query = sql
            .prepare("SELECT id,n FROM ranges.Link WHERE n>=?1 ORDER BY n,id LIMIT 3000")
            .unwrap();
        let args = [SqlValue::Integer(-3)];
        let old = query.query(&args, generous).unwrap();
        for limits in [
            QueryLimits {
                max_rows: 2,
                ..QueryLimits::default()
            },
            QueryLimits {
                max_bytes: 32,
                ..QueryLimits::default()
            },
            QueryLimits {
                vm_steps: 2,
                ..QueryLimits::default()
            },
            QueryLimits {
                timeout_ms: 0,
                ..QueryLimits::default()
            },
        ] {
            assert!(query.query(&args, limits).is_err());
            assert_eq!(query.query(&args, generous).unwrap().rows, old.rows);
        }
        let mut tx = db.admin().transaction();
        tx.insert(
            "ranges.Link",
            vec![
                Value::Int(1),
                Value::Decimal(1),
                Value::Timestamp(1),
                Value::Ref(Id::from_u128(1)),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(query.query(&args, generous).unwrap().rows, old.rows);
        let mut tx = db.admin().transaction();
        tx.revoke("even", "direct.Item").unwrap();
        tx.commit().unwrap();
        // Revoking the referenced relation invalidates attached authority even
        // when the range relation's own read grant is unchanged.
        assert!(matches!(query.query(&args, generous), Err(Error::Denied)));
    }

    #[test]
    fn direct_projection_does_not_copy_unselected_large_cells() {
        let db = fixture();
        let mut tx = db.admin().transaction();
        let id = tx
            .insert(
                "direct.Item",
                vec![
                    Value::Int(9),
                    Value::Text("x".repeat(CELL_BYTES + 1)),
                    Value::Bool(true),
                    Value::Bytes(vec![]),
                ],
            )
            .unwrap();
        tx.commit().unwrap();
        let sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let args = [SqlValue::Text(id.to_string())];
        let mut small = sql
            .prepare("SELECT n FROM direct.Item WHERE id=?1")
            .unwrap();
        assert_eq!(
            small.query(&args, QueryLimits::default()).unwrap().rows,
            vec![vec![SqlValue::Integer(9)]]
        );
        let mut large = sql
            .prepare("SELECT tag FROM direct.Item WHERE id=?1")
            .unwrap();
        assert!(
            large
                .query(
                    &args,
                    QueryLimits {
                        max_bytes: CELL_BYTES * 3,
                        ..QueryLimits::default()
                    }
                )
                .unwrap_err()
                .to_string()
                .contains("source cell")
        );
    }
}
