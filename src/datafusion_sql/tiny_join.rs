//! Bounded native execution for validated, selective inner equijoins.
//!
//! Immutable descriptors retain no rows, permissions or parameter-specific state.
//! Unknown expressions, coercions and large fanouts fall back to DataFusion.
use super::{
    direct::{self, Key},
    provider::{LiquidTable, SourceBudget},
    sql_value,
};
use crate::{DataType, Error, QueryLimits, Relation, Result, Row, Snapshot, SqlTable, Value};
use datafusion::{
    common::{JoinType, NullEquality, ScalarValue},
    datasource::source_as_provider,
    logical_expr::{
        Expr, LogicalPlan, Operator,
        logical_plan::{FetchType, SkipType},
    },
};
use rusqlite::types::Value as SqlValue;
use std::{
    any::Any,
    borrow::Cow,
    cmp::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_TABLES: usize = 8;
const MAX_ROWS: usize = 128;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Column {
    table: usize,
    field: usize,
}
#[derive(Clone)]
enum Binding {
    Literal(SqlValue),
    Parameter(usize),
}
#[derive(Clone)]
enum Predicate {
    Columns(Column, Column),
    Bound(Column, Binding),
}
struct Table {
    snapshot: Snapshot,
    relation: Relation,
    needed: Vec<usize>,
}
struct Shape {
    tables: Vec<Table>,
    columns: Vec<Column>,
    predicates: Vec<Predicate>,
    order: Vec<(Column, bool)>,
    skip: usize,
    fetch: Option<usize>,
    limited: bool,
}
enum KeySource {
    Bound(usize),
    Column(Column),
}
struct Stage {
    table: usize,
    field: usize,
    key: KeySource,
}
pub(super) struct TinyJoin {
    tables: Vec<Table>,
    projection: Vec<Column>,
    predicates: Vec<Predicate>,
    stages: Vec<Stage>,
    order: Vec<(Column, bool)>,
    skip: usize,
    fetch: Option<usize>,
    names: Vec<String>,
}
type Tuple = Vec<Option<Arc<Row>>>;

impl TinyJoin {
    pub(super) fn compile(plan: &LogicalPlan) -> Option<Self> {
        let mut shape = shape(plan)?;
        if !(2..=MAX_TABLES).contains(&shape.tables.len()) {
            return None;
        }
        for predicate in &shape.predicates {
            if let Predicate::Columns(a, b) = predicate
                && !compatible(kind(&shape.tables, *a), kind(&shape.tables, *b))
            {
                return None;
            }
        }
        let mut selected = vec![false; shape.tables.len()];
        let mut stages = Vec::new();
        while stages.len() < selected.len() {
            let mut choices = Vec::new();
            for (index, predicate) in shape.predicates.iter().enumerate() {
                match predicate {
                    Predicate::Bound(c, _) if !selected[c.table] => choices.push(Stage {
                        table: c.table,
                        field: c.field,
                        key: KeySource::Bound(index),
                    }),
                    Predicate::Columns(a, b) if selected[a.table] && !selected[b.table] => choices
                        .push(Stage {
                            table: b.table,
                            field: b.field,
                            key: KeySource::Column(*a),
                        }),
                    Predicate::Columns(a, b) if selected[b.table] && !selected[a.table] => choices
                        .push(Stage {
                            table: a.table,
                            field: a.field,
                            key: KeySource::Column(*b),
                        }),
                    _ => {}
                }
            }
            // Identity probes are unique; all other equality probes have a
            // strict per-execution fanout bound rather than an estimated count.
            let stage = choices
                .into_iter()
                .min_by_key(|s| (s.field != 0, s.table, s.field))?;
            selected[stage.table] = true;
            stages.push(stage);
        }
        for c in shape
            .columns
            .iter()
            .copied()
            .chain(shape.order.iter().map(|(c, _)| *c))
        {
            shape.tables[c.table].needed.push(c.field);
        }
        for p in &shape.predicates {
            match p {
                Predicate::Columns(a, b) => {
                    shape.tables[a.table].needed.push(a.field);
                    shape.tables[b.table].needed.push(b.field);
                }
                Predicate::Bound(c, _) => shape.tables[c.table].needed.push(c.field),
            }
        }
        for table in &mut shape.tables {
            table.needed.sort_unstable();
            table.needed.dedup();
        }
        Some(Self {
            tables: shape.tables,
            projection: shape.columns,
            predicates: shape.predicates,
            stages,
            order: shape.order,
            skip: shape.skip,
            fetch: shape.fetch,
            names: plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        })
    }

    /// None requests ordinary execution. Materialized work is bounded to eight
    /// stages of at most 128 input tuples and 129 visible rows per probe. Native
    /// authorization can still inspect additional denied rows or references;
    /// these limits are not a strict CPU bound. No results are cached, and every
    /// row comes through the authorized native boundary.
    pub(super) fn query(
        &self,
        params: &[SqlValue],
        limits: QueryLimits,
        budget: &SourceBudget,
    ) -> Result<Option<SqlTable>> {
        let start = Instant::now();
        let deadline = || -> Result<()> {
            if start.elapsed() >= Duration::from_millis(limits.timeout_ms) {
                return Err(Error::Query("SQL deadline exceeded".into()));
            }
            Ok(())
        };
        let check = || -> Result<()> {
            for table in &self.tables {
                table.snapshot.live.check_attached()?;
            }
            deadline()
        };
        check()?;
        let mut bindings = Vec::with_capacity(self.predicates.len());
        let mut empty = false;
        for predicate in &self.predicates {
            bindings.push(if let Predicate::Bound(column, binding) = predicate {
                let value = match binding {
                    Binding::Literal(v) => v,
                    Binding::Parameter(i) => params
                        .get(*i)
                        .ok_or_else(|| Error::Query("SQL parameter count mismatch".into()))?,
                };
                match direct::key(value, kind(&self.tables, *column)) {
                    Key::Unsupported => return Ok(None),
                    Key::Empty => {
                        empty = true;
                        None
                    }
                    Key::Value(value) => Some(value),
                }
            } else {
                None
            });
        }
        let mut bytes = self.names.iter().map(String::len).sum::<usize>();
        if bytes > limits.max_bytes {
            return Err(Error::Query("query byte budget exceeded".into()));
        }
        let mut output = SqlTable {
            columns: self.names.clone(),
            rows: Vec::new(),
        };
        if empty || self.fetch == Some(0) {
            check()?;
            return Ok(Some(output));
        }
        let mut tuples = vec![vec![None; self.tables.len()]];
        for stage in &self.stages {
            let table = &self.tables[stage.table];
            let mut next = Vec::new();
            for tuple in tuples {
                deadline()?;
                let key = match &stage.key {
                    KeySource::Bound(i) => Cow::Borrowed(
                        bindings[*i]
                            .as_ref()
                            .ok_or_else(|| Error::Query("invalid compiled join binding".into()))?,
                    ),
                    KeySource::Column(c) => cell(&tuple, *c)
                        .ok_or_else(|| Error::Query("invalid compiled join dependency".into()))?,
                };
                let (rows, too_many) = if stage.field == 0 {
                    let Value::Ref(id) = key.as_ref() else {
                        return Err(Error::Query("invalid compiled identity probe".into()));
                    };
                    (
                        table
                            .snapshot
                            .get_shared(&table.relation.name, *id)?
                            .into_iter()
                            .collect::<Vec<_>>(),
                        false,
                    )
                } else {
                    let page = table.snapshot.page_shared(
                        &table.relation.name,
                        Some((&table.relation.roles[stage.field - 1].name, key.as_ref())),
                        None,
                        MAX_ROWS + 1,
                    )?;
                    let too_many = page.next.is_some() || page.rows.len() > MAX_ROWS;
                    (page.rows, too_many)
                };
                budget.charge(rows.len(), 0).map_err(super::query_error)?;
                drop(key);
                for row in rows {
                    deadline()?;
                    if too_many {
                        continue;
                    }
                    let mut candidate = tuple.clone();
                    candidate[stage.table] = Some(row);
                    if !self.matches(&candidate, &bindings) {
                        continue;
                    }
                    // As in the generic provider, candidate probes count as
                    // source work, but rejected rows do not undergo payload
                    // conversion or incur its cell/byte limits.
                    let row = candidate[stage.table]
                        .as_ref()
                        .ok_or_else(|| Error::Query("incomplete compiled join candidate".into()))?;
                    let source_bytes = table.needed.iter().try_fold(0usize, |n, &field| {
                        direct::source_size(row, &table.relation, field)
                            .map(|m| n.saturating_add(m))
                    })?;
                    budget.charge(0, source_bytes).map_err(super::query_error)?;
                    if next.len() == MAX_ROWS {
                        check()?;
                        return Ok(None);
                    }
                    next.push(candidate);
                }
                if too_many {
                    check()?;
                    return Ok(None);
                }
            }
            tuples = next;
            if tuples.is_empty() {
                break;
            }
        }
        check()?;
        if !self.order.is_empty() {
            tuples.sort_by(|a, b| {
                for (column, asc) in &self.order {
                    let order = cell(a, *column).cmp(&cell(b, *column));
                    if order != Ordering::Equal {
                        return if *asc { order } else { order.reverse() };
                    }
                }
                Ordering::Equal
            });
        }
        let mut work = 0u64;
        for tuple in tuples
            .into_iter()
            .skip(self.skip)
            .take(self.fetch.unwrap_or(usize::MAX))
        {
            deadline()?;
            if output.rows.len() == limits.max_rows {
                return Err(Error::Query(
                    "query row budget exceeded; use LIMIT or pages".into(),
                ));
            }
            work = work.saturating_add(self.projection.len().max(1) as u64);
            if work > limits.vm_steps {
                return Err(Error::Query("query output-cell budget exceeded".into()));
            }
            let mut values = Vec::with_capacity(self.projection.len());
            for column in &self.projection {
                let row = tuple[column.table]
                    .as_ref()
                    .ok_or_else(|| Error::Query("incomplete compiled join result".into()))?;
                let size = direct::output_size(row, column.field);
                if size > limits.max_bytes.saturating_sub(bytes) {
                    return Err(Error::Query("query byte budget exceeded".into()));
                }
                bytes += size;
                values.push(direct::output_value(row, column.field));
            }
            output.rows.push(values);
        }
        check()?;
        Ok(Some(output))
    }
    fn matches(&self, tuple: &Tuple, bindings: &[Option<Value>]) -> bool {
        self.predicates.iter().enumerate().all(|(i, p)| match p {
            Predicate::Columns(a, b) => match (cell(tuple, *a), cell(tuple, *b)) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            },
            Predicate::Bound(c, _) => {
                cell(tuple, *c).is_none_or(|v| bindings[i].as_ref() == Some(v.as_ref()))
            }
        })
    }
}
fn cell(tuple: &Tuple, column: Column) -> Option<Cow<'_, Value>> {
    let row = tuple.get(column.table)?.as_ref()?;
    if column.field == 0 {
        Some(Cow::Owned(Value::Ref(row.id)))
    } else {
        row.values.get(column.field - 1).map(Cow::Borrowed)
    }
}
fn kind(tables: &[Table], c: Column) -> Option<&DataType> {
    c.field
        .checked_sub(1)
        .map(|i| &tables[c.table].relation.roles[i].data_type)
}
fn compatible(a: Option<&DataType>, b: Option<&DataType>) -> bool {
    match (a, b) {
        (None | Some(DataType::Ref(_)), None | Some(DataType::Ref(_))) => true,
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}
fn column(expr: &Expr, plan: &LogicalPlan, columns: &[Column]) -> Option<Column> {
    let Expr::Column(c) = expr else { return None };
    columns.get(plan.schema().index_of_column(c).ok()?).copied()
}
fn binding(expr: &Expr) -> Option<Binding> {
    Some(match expr {
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
    })
}
fn predicates(
    expr: &Expr,
    plan: &LogicalPlan,
    columns: &[Column],
    out: &mut Vec<Predicate>,
) -> Option<()> {
    let Expr::BinaryExpr(b) = expr else {
        return None;
    };
    if b.op == Operator::And {
        predicates(&b.left, plan, columns, out)?;
        return predicates(&b.right, plan, columns, out);
    }
    if b.op != Operator::Eq {
        return None;
    }
    match (
        column(&b.left, plan, columns),
        column(&b.right, plan, columns),
    ) {
        (Some(a), Some(b)) => out.push(Predicate::Columns(a, b)),
        (Some(c), None) => out.push(Predicate::Bound(c, binding(&b.right)?)),
        (None, Some(c)) => out.push(Predicate::Bound(c, binding(&b.left)?)),
        _ => return None,
    }
    Some(())
}
fn shape(plan: &LogicalPlan) -> Option<Shape> {
    match plan {
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
            let provider = source_as_provider(&scan.source).ok()?;
            let table = (provider.as_ref() as &dyn Any).downcast_ref::<LiquidTable>()?;
            Some(Shape {
                columns: scan
                    .projection
                    .clone()
                    .unwrap_or_else(|| (0..table.declaration().roles.len() + 1).collect())
                    .into_iter()
                    .map(|field| Column { table: 0, field })
                    .collect(),
                tables: vec![Table {
                    snapshot: table.snapshot().clone(),
                    relation: table.declaration().clone(),
                    needed: vec![],
                }],
                predicates: vec![],
                order: vec![],
                skip: 0,
                fetch: None,
                limited: false,
            })
        }
        LogicalPlan::SubqueryAlias(a) => shape(&a.input),
        LogicalPlan::Join(join)
            if join.join_type == JoinType::Inner
                && join.null_equality == NullEquality::NullEqualsNothing
                && !join.null_aware =>
        {
            let mut left = shape(&join.left)?;
            let mut right = shape(&join.right)?;
            if left.limited
                || right.limited
                || !left.order.is_empty()
                || !right.order.is_empty()
                || left.tables.len() + right.tables.len() > MAX_TABLES
            {
                return None;
            }
            let offset = left.tables.len();
            let shift = |c: &mut Column| c.table += offset;
            for c in &mut right.columns {
                shift(c);
            }
            for p in &mut right.predicates {
                match p {
                    Predicate::Columns(a, b) => {
                        shift(a);
                        shift(b);
                    }
                    Predicate::Bound(c, _) => shift(c),
                }
            }
            for (a, b) in &join.on {
                left.predicates.push(Predicate::Columns(
                    column(a, &join.left, &left.columns)?,
                    column(b, &join.right, &right.columns)?,
                ));
            }
            left.tables.extend(right.tables);
            left.columns.extend(right.columns);
            left.predicates.extend(right.predicates);
            if let Some(filter) = &join.filter {
                predicates(filter, plan, &left.columns, &mut left.predicates)?;
            }
            Some(left)
        }
        LogicalPlan::Filter(filter) => {
            let mut s = shape(&filter.input)?;
            if s.limited {
                return None;
            }
            predicates(
                &filter.predicate,
                &filter.input,
                &s.columns,
                &mut s.predicates,
            )?;
            Some(s)
        }
        LogicalPlan::Projection(p) => {
            let mut s = shape(&p.input)?;
            s.columns = p
                .expr
                .iter()
                .map(|e| {
                    column(
                        if let Expr::Alias(a) = e { &a.expr } else { e },
                        &p.input,
                        &s.columns,
                    )
                })
                .collect::<Option<_>>()?;
            Some(s)
        }
        LogicalPlan::Sort(sort) => {
            let mut s = shape(&sort.input)?;
            if s.limited || sort.fetch.is_some() {
                return None;
            }
            s.order = sort
                .expr
                .iter()
                .map(|o| Some((column(&o.expr, &sort.input, &s.columns)?, o.asc)))
                .collect::<Option<_>>()?;
            Some(s)
        }
        LogicalPlan::Limit(limit) => {
            let mut s = shape(&limit.input)?;
            if s.limited {
                return None;
            }
            let SkipType::Literal(skip) = limit.get_skip_type().ok()? else {
                return None;
            };
            let FetchType::Literal(fetch) = limit.get_fetch_type().ok()? else {
                return None;
            };
            s.skip = skip;
            s.fetch = fetch;
            s.limited = true;
            Some(s)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::DfSnapshot;
    use super::*;
    use crate::{Database, Grant, Id, Module, Relation, Role, RowFilter, demo};

    fn fixture() -> (Database, Vec<Id>) {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), crate::Visibility::Private)
            .unwrap();
        tx.install_with_visibility(demo::fitness(), crate::Visibility::Private)
            .unwrap();
        tx.install_with_visibility(demo::crm(), crate::Visibility::Private)
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..24 {
            let p = tx
                .insert("core.Person", vec![Value::Text(format!("Person{i}"))])
                .unwrap();
            ids.push(p);
            for j in 0..2 {
                let w = tx
                    .insert("fitness.Workout", vec![Value::Int(i + j + 30)])
                    .unwrap();
                tx.insert("fitness.Performed", vec![p.into(), w.into()])
                    .unwrap();
                let d = tx
                    .insert("crm.Deal", vec![Value::Decimal(i * 100 + j)])
                    .unwrap();
                tx.insert("crm.CustomerDeal", vec![p.into(), d.into()])
                    .unwrap();
            }
        }
        for r in [
            "core.Person",
            "fitness.Workout",
            "fitness.Performed",
            "crm.Deal",
            "crm.CustomerDeal",
        ] {
            tx.grant("reader", r, Grant::read_only()).unwrap();
        }
        tx.commit().unwrap();
        (db, ids)
    }
    const RELATIONSHIP: &str = "SELECT f.id,w.id,w.durationMinutes FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout WHERE f.athlete=?1 ORDER BY f.id";
    const FIVE: &str = "SELECT p.id,f.id,w.durationMinutes,c.id,d.amount FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout JOIN crm.CustomerDeal c ON c.customer=p.id JOIN crm.Deal d ON d.id=c.deal WHERE p.id=?1 ORDER BY f.id,c.id";

    #[test]
    fn benchmark_joins_compile_and_rebind_without_executor_or_cached_results() {
        let (db, ids) = fixture();
        for session in [db.admin(), db.session("reader")] {
            let snapshot = session.snapshot();
            let sqlite = snapshot.sqlite_compat().unwrap();
            let sql = DfSnapshot::new(snapshot, None).unwrap();
            for text in [RELATIONSHIP, FIVE] {
                let mut query = sql.prepare(text).unwrap();
                assert!(query.tiny_join.is_some(), "{text}");
                let mut generic = sql.prepare(text).unwrap();
                generic.tiny_join = None;
                for id in ids
                    .iter()
                    .cycle()
                    .take(70)
                    .chain(std::iter::once(&Id::from_u128(999)))
                {
                    let args = [SqlValue::Text(id.to_string())];
                    let expected = sqlite.query(text, &args).unwrap().rows;
                    assert_eq!(
                        query.query(&args, QueryLimits::default()).unwrap().rows,
                        expected
                    );
                    assert!(
                        query.cache.get(&args).is_none(),
                        "tiny execution must not enter general planner"
                    );
                    assert_eq!(
                        generic.query(&args, QueryLimits::default()).unwrap().rows,
                        expected
                    );
                }
            }
        }
    }

    fn scalar_fixture(count: usize) -> Database {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "tiny",
                1,
                vec![],
                vec![
                    Relation::new(
                        "tiny.A",
                        vec![
                            Role::new("bucket", DataType::Int),
                            Role::new("k", DataType::Text),
                        ],
                    ),
                    Relation::new(
                        "tiny.B",
                        vec![
                            Role::new("k", DataType::Text),
                            Role::new("n", DataType::Int),
                        ],
                    ),
                ],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for (bucket, key) in [(1, "x"), (1, "x"), (1, "y"), (2, "none")] {
            tx.insert("tiny.A", vec![Value::Int(bucket), key.into()])
                .unwrap();
        }
        for i in 0..count {
            tx.insert(
                "tiny.B",
                vec![
                    if i % 3 == 0 { "y".into() } else { "x".into() },
                    Value::Int(i as i64 % 4),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        db
    }
    #[test]
    fn duplicates_aliases_multiple_keys_reordered_sources_and_order_limits_match() {
        let db = scalar_fixture(8);
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let sql = DfSnapshot::new(snapshot, None).unwrap();
        for text in [
            "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k WHERE a.bucket=?1 ORDER BY b.n DESC,a.k LIMIT 5 OFFSET 2",
            "SELECT a.k,b.n FROM tiny.B b JOIN tiny.A a ON a.k=b.k WHERE a.bucket=?1 ORDER BY b.n,a.k",
            "SELECT x.k,y.n FROM (SELECT k,bucket FROM tiny.A) x JOIN tiny.B y ON x.k=y.k AND x.bucket=y.n WHERE x.bucket=?1 ORDER BY y.n,x.k",
            "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k WHERE a.bucket=?1 AND b.n=1 ORDER BY b.n,a.k",
            "SELECT a.k,b.k FROM tiny.A a JOIN tiny.A b ON a.bucket=b.bucket WHERE a.bucket=?1 ORDER BY a.k,b.k",
            "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k WHERE a.bucket=?1 ORDER BY a.k,b.n LIMIT 0",
        ] {
            let mut query = sql.prepare(text).unwrap();
            assert!(query.tiny_join.is_some(), "{text}");
            for value in [1, 2, 3, 1] {
                let args = [SqlValue::Integer(value)];
                assert_eq!(
                    query.query(&args, QueryLimits::default()).unwrap().rows,
                    sqlite.query(text, &args).unwrap().rows,
                    "{text}"
                );
            }
        }
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(24))]
        #[test]
        fn randomized_bags_and_alias_shapes_match_both_executors(
            left in proptest::collection::vec((-2i64..3, 0u8..4), 0..18),
            right in proptest::collection::vec((0u8..4, -2i64..3), 0..18),
            selected in -3i64..4,
        ) {
            let db = scalar_fixture(0);
            let mut tx = db.admin().transaction();
            // Replace the fixed left-side fixture with the generated bag.
            for row in tx.scan("tiny.A").unwrap() { tx.delete("tiny.A", row.id).unwrap(); }
            for (bucket, key) in left { tx.insert("tiny.A", vec![bucket.into(), Value::Text(format!("k{key}"))]).unwrap(); }
            for (key, number) in right { tx.insert("tiny.B", vec![Value::Text(format!("k{key}")), number.into()]).unwrap(); }
            tx.commit().unwrap();
            let snapshot = db.admin().snapshot();
            let sqlite = snapshot.sqlite_compat().unwrap();
            let sql = DfSnapshot::new(snapshot, None).unwrap();
            for text in [
                "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k WHERE a.bucket=?1 ORDER BY a.k,b.n",
                "SELECT a.k,b.n FROM tiny.B b JOIN tiny.A a ON a.k=b.k AND a.bucket=b.n WHERE a.bucket=?1 ORDER BY a.k,b.n",
                "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b USING(k) WHERE a.bucket=?1 ORDER BY a.k,b.n",
                "SELECT x.key,y.n FROM (SELECT k AS key,bucket FROM tiny.A) x JOIN tiny.B y ON x.key=y.k WHERE x.bucket=?1 ORDER BY x.key,y.n",
            ] {
                let mut query = sql.prepare(text).unwrap();
                proptest::prop_assert!(query.tiny_join.is_some(), "{text}");
                let mut generic = sql.prepare(text).unwrap();
                generic.tiny_join = None;
                for value in [selected, 99] {
                    let args = [SqlValue::Integer(value)];
                    let expected = sqlite.query(text, &args).unwrap().rows;
                    proptest::prop_assert_eq!(query.query(&args, QueryLimits::default()).unwrap().rows, expected.clone(), "{}", text);
                    proptest::prop_assert_eq!(generic.query(&args, QueryLimits::default()).unwrap().rows, expected, "{}", text);
                }
            }
        }
    }

    #[test]
    fn unsupported_shapes_coercions_and_large_fanout_fall_back() {
        let db = scalar_fixture(300);
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let sql = DfSnapshot::new(snapshot, None).unwrap();
        for text in [
            "SELECT a.k,b.n FROM tiny.A a LEFT JOIN tiny.B b ON a.k=b.k WHERE a.bucket=1 ORDER BY a.k,b.n",
            "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k WHERE a.bucket=1 AND b.n>0 ORDER BY a.k,b.n",
            "SELECT a.k,b.n FROM (SELECT k FROM tiny.A LIMIT 1) a JOIN tiny.B b ON a.k=b.k ORDER BY a.k,b.n",
            "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k ORDER BY a.k,b.n",
        ] {
            let mut query = sql.prepare(text).unwrap();
            assert!(query.tiny_join.is_none(), "{text}");
            assert_eq!(
                query.query(&[], QueryLimits::default()).unwrap().rows,
                sqlite.query(text, &[]).unwrap().rows
            );
        }
        let text = "SELECT a.k,b.n FROM tiny.A a JOIN tiny.B b ON a.k=b.k WHERE a.bucket=?1 ORDER BY a.k,b.n";
        let mut query = sql.prepare(text).unwrap();
        assert!(query.tiny_join.is_some());
        for value in [
            SqlValue::Integer(1),
            SqlValue::Real(1.),
            SqlValue::Text("1".into()),
            SqlValue::Null,
            SqlValue::Integer(2),
        ] {
            let args = [value];
            let mut generic = sql.prepare(text).unwrap();
            generic.tiny_join = None;
            let expected = generic.query(&args, QueryLimits::default());
            let actual = query.query(&args, QueryLimits::default());
            match (actual, expected) {
                (Ok(a), Ok(b)) => assert_eq!(a.rows, b.rows),
                (Err(_), Err(_)) => {}
                pair => panic!("coercion parity: {pair:?}"),
            }
        }
        let spent = SourceBudget::default();
        assert!(
            query
                .tiny_join
                .as_ref()
                .unwrap()
                .query(&[SqlValue::Integer(1)], QueryLimits::default(), &spent)
                .unwrap()
                .is_none()
        );
        // Fanout fallback does not reset already read candidate charges. The
        // caller passes this very budget into the generic execution task.
        assert!(
            spent
                .charge(super::super::provider::SOURCE_ROWS - 128, 0)
                .is_err()
        );
    }
    #[test]
    fn tiny_budgets_errors_and_live_revocation_preserve_pinned_rows() {
        let (db, ids) = fixture();
        let snapshot = db.session("reader").snapshot();
        let sql = DfSnapshot::new(snapshot, None).unwrap();
        let mut query = sql.prepare(FIVE).unwrap();
        let args = [SqlValue::Text(ids[0].to_string())];
        let expected = query.query(&args, QueryLimits::default()).unwrap();
        assert_eq!(expected.rows.len(), 4);
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
                expected.rows
            );
        }
        let mut tx = db.admin().transaction();
        let w = tx.insert("fitness.Workout", vec![99.into()]).unwrap();
        tx.insert("fitness.Performed", vec![ids[0].into(), w.into()])
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            query.query(&args, QueryLimits::default()).unwrap().rows,
            expected.rows
        );
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "fitness.Workout").unwrap();
        tx.commit().unwrap();
        assert!(matches!(
            query.query(&args, QueryLimits::default()),
            Err(Error::Denied)
        ));
    }
    #[test]
    fn private_reference_closure_row_filters_and_noncanonical_keys_remain_enforced() {
        let (db, ids) = fixture();
        let mut tx = db.admin().transaction();
        tx.grant(
            "reader",
            "fitness.Workout",
            Grant {
                filter: RowFilter::Equals {
                    role: "durationMinutes".into(),
                    value: Value::Int(30),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let sql = DfSnapshot::new(snapshot, None).unwrap();
        let mut query = sql.prepare(FIVE).unwrap();
        assert!(query.tiny_join.is_some());
        for id in [
            ids[0].to_string(),
            ids[1].to_string(),
            ids[0].to_string().to_uppercase(),
            "not-an-id".into(),
        ] {
            let args = [SqlValue::Text(id)];
            let actual = query.query(&args, QueryLimits::default()).unwrap().rows;
            assert_eq!(actual, sqlite.query(FIVE, &args).unwrap().rows);
        }
        let args = [SqlValue::Text(ids[0].to_string())];
        assert_eq!(
            query
                .query(&args, QueryLimits::default())
                .unwrap()
                .rows
                .len(),
            2
        );
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
        assert!(matches!(
            query.query(&args, QueryLimits::default()),
            Err(Error::Denied)
        ));
    }
    #[test]
    fn exact_scalar_join_keys_and_selected_payload_budgets_match_generic_execution() {
        let cases = [
            (DataType::Text, Value::Text("héllo".into())),
            (DataType::Bytes, Value::Bytes(vec![0, 255, 17])),
            (DataType::Bool, Value::Bool(true)),
            (DataType::Int, Value::Int(i64::MIN)),
            (DataType::Timestamp, Value::Timestamp(i64::MAX)),
            (DataType::Decimal { scale: 2 }, Value::Decimal(i64::MAX)),
        ];
        for (kind, key) in cases {
            let db = Database::memory();
            let mut tx = db.admin().transaction();
            tx.install_with_visibility(
                Module::new(
                    "scalar",
                    1,
                    vec![],
                    vec![
                        Relation::new(
                            "scalar.A",
                            vec![
                                Role::new("bucket", DataType::Int),
                                Role::new("k", kind.clone()),
                            ],
                        ),
                        Relation::new(
                            "scalar.B",
                            vec![Role::new("k", kind), Role::new("payload", DataType::Bytes)],
                        ),
                    ],
                ),
                crate::Visibility::Private,
            )
            .unwrap();
            tx.insert("scalar.A", vec![1.into(), key.clone()]).unwrap();
            tx.insert(
                "scalar.B",
                vec![
                    key,
                    Value::Bytes(vec![7; super::super::provider::CELL_BYTES + 1]),
                ],
            )
            .unwrap();
            tx.commit().unwrap();
            let sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
            let text = "SELECT a.k,b.k FROM scalar.A a JOIN scalar.B b ON a.k=b.k WHERE a.bucket=?1 ORDER BY a.k";
            let mut query = sql.prepare(text).unwrap();
            assert!(query.tiny_join.is_some());
            let mut generic = sql.prepare(text).unwrap();
            generic.tiny_join = None;
            let args = [SqlValue::Integer(1)];
            assert_eq!(
                query.query(&args, QueryLimits::default()).unwrap().rows,
                generic.query(&args, QueryLimits::default()).unwrap().rows
            );
            let text =
                "SELECT b.payload FROM scalar.A a JOIN scalar.B b ON a.k=b.k WHERE a.bucket=1";
            let mut query = sql.prepare(text).unwrap();
            assert!(query.tiny_join.is_some());
            assert!(query.query(&[], QueryLimits::default()).is_err());
        }
    }

    #[test]
    fn rejected_candidate_does_not_convert_unused_oversized_payload() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "reject",
                1,
                vec![],
                vec![
                    Relation::new("reject.A", vec![Role::new("k", DataType::Text)]),
                    Relation::new(
                        "reject.B",
                        vec![
                            Role::new("k", DataType::Text),
                            Role::new("payload", DataType::Bytes),
                        ],
                    ),
                ],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        tx.insert("reject.A", vec!["x".into()]).unwrap();
        tx.insert(
            "reject.B",
            vec![
                "x".into(),
                Value::Bytes(vec![0; super::super::provider::CELL_BYTES + 1]),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        let sql = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let text = "SELECT b.payload FROM reject.A a JOIN reject.B b ON a.k=b.k WHERE a.k=?1 AND b.k='none'";
        let mut query = sql.prepare(text).unwrap();
        assert!(query.tiny_join.is_some());
        let mut generic = sql.prepare(text).unwrap();
        generic.tiny_join = None;
        let args = [SqlValue::Text("x".into())];
        let expected = generic.query(&args, QueryLimits::default()).unwrap();
        assert!(expected.rows.is_empty());
        assert_eq!(
            query.query(&args, QueryLimits::default()).unwrap().rows,
            expected.rows
        );
    }

    #[test]
    fn capability_scope_expiry_and_missing_references_cannot_be_bypassed() {
        let (db, ids) = fixture();
        db.clock.set(1000);
        let cap = db
            .issue_capability_until(
                Id::new(),
                "reader",
                crate::capability::CapabilityScope {
                    relations: ["core.Person", "fitness.Workout", "fitness.Performed"]
                        .into_iter()
                        .map(|relation| crate::capability::RelationCapability {
                            relation: db.admin().snapshot().schema(relation).unwrap().id,
                            grant: Grant::read_only(),
                        })
                        .collect(),
                    schema_namespace: None,
                    delegations: 0,
                },
                Some(1010),
            )
            .unwrap();
        let session = db.session("reader").with_capability(cap.id).unwrap();
        let sql = DfSnapshot::new(session.snapshot(), None).unwrap();
        assert!(
            sql.prepare(FIVE).is_err(),
            "credential scope must not gain unrelated CRM tables"
        );
        let mut query = sql.prepare(RELATIONSHIP).unwrap();
        assert!(query.tiny_join.is_some());
        let args = [SqlValue::Text(ids[0].to_string())];
        assert_eq!(
            query
                .query(&args, QueryLimits::default())
                .unwrap()
                .rows
                .len(),
            2
        );
        db.clock.set(1010);
        assert!(matches!(
            query.query(&args, QueryLimits::default()),
            Err(Error::Denied)
        ));
    }
}
