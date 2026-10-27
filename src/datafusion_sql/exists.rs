//! Ordered, bounded correlated EXISTS through authorized native index probes.
//! The compiled descriptor contains no rows or parameter-dependent query state.
//! Unsupported syntax/coercion and excessive visible/probe work use the general executor.
//! Candidate paging checks deadlines and source budgets, including denied rows.
//! A single native reference-closure traversal is not preemptible by that callback.
pub(super) mod top;

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
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_TABLES: usize = 5;
const MAX_PROBES: usize = 16_384;
const MAX_OUTER: usize = 4096;
#[derive(Clone, Copy, PartialEq, Eq)]
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
enum Operand {
    Column(Column),
    Bound(Binding),
}
struct Predicate {
    left: Column,
    op: Operator,
    right: Operand,
}
struct Table {
    snapshot: Snapshot,
    relation: Relation,
    needed: Vec<usize>,
}
struct Inner {
    tables: Vec<Table>,
    columns: Vec<Column>,
    predicates: Vec<Predicate>,
}
struct Outer {
    table: Table,
    columns: Vec<Column>,
    inner: Option<Inner>,
    ordered: bool,
    skip: usize,
    fetch: Option<usize>,
}
struct Stage {
    table: usize,
    field: usize,
    source: Column,
}
pub(super) struct ExistsRead {
    tables: Vec<Table>,
    predicates: Vec<Predicate>,
    stages: Vec<Stage>,
    projection: Vec<Column>,
    output_needed: Vec<usize>,
    names: Vec<String>,
    skip: usize,
    fetch: usize,
}

impl ExistsRead {
    pub(super) fn compile(plan: &LogicalPlan) -> Option<Self> {
        let outer = outer(plan)?;
        let mut inner = outer.inner?;
        let fetch = outer.fetch.filter(|n| *n <= 1000)?;
        if !outer.ordered || outer.skip > 1000 {
            return None;
        }
        let mut tables = vec![outer.table];
        tables.append(&mut inner.tables);
        if tables.iter().any(|table| {
            table
                .relation
                .roles
                .iter()
                .any(|r| matches!(r.data_type, DataType::Nullable(_) | DataType::Float))
        }) {
            return None;
        }
        if tables.len() > MAX_TABLES {
            return None;
        }
        let stages = compile_stages(&tables, &inner.predicates, 0)?;
        // Projection is charged only after the EXISTS predicate and OFFSET.
        // Rejected candidates must not copy or convert unused outer payloads.
        let mut output_needed = outer.columns.iter().map(|c| c.field).collect::<Vec<_>>();
        output_needed.sort_unstable();
        output_needed.dedup();
        for p in &inner.predicates {
            tables[p.left.table].needed.push(p.left.field);
            if let Operand::Column(c) = p.right {
                tables[c.table].needed.push(c.field);
            }
        }
        for t in &mut tables {
            t.needed.sort_unstable();
            t.needed.dedup();
        }
        Some(Self {
            tables,
            predicates: inner.predicates,
            stages,
            projection: outer.columns,
            output_needed,
            names: plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            skip: outer.skip,
            fetch,
        })
    }

    pub(super) fn query(
        &self,
        params: &[SqlValue],
        limits: QueryLimits,
        budget: &SourceBudget,
    ) -> Result<Option<SqlTable>> {
        let start = Instant::now();
        let check = || {
            for t in &self.tables {
                t.snapshot.live.check_attached()?;
            }
            if start.elapsed() >= Duration::from_millis(limits.timeout_ms) {
                return Err(Error::Query("SQL deadline exceeded".into()));
            }
            Ok(())
        };
        check()?;
        let mut bindings = Vec::new();
        let mut empty = false;
        for p in &self.predicates {
            bindings.push(if let Operand::Bound(binding) = &p.right {
                let value = match binding {
                    Binding::Literal(v) => v,
                    Binding::Parameter(i) => params
                        .get(*i)
                        .ok_or_else(|| Error::Query("SQL parameter count mismatch".into()))?,
                };
                match direct::key(value, kind(&self.tables, p.left)) {
                    Key::Unsupported => return Ok(None),
                    Key::Empty => {
                        empty = true;
                        None
                    }
                    Key::Value(v) => Some(v),
                }
            } else {
                None
            });
        }
        let mut output = SqlTable {
            columns: self.names.clone(),
            rows: vec![],
        };
        let mut bytes = self.names.iter().map(String::len).sum::<usize>();
        if bytes > limits.max_bytes {
            return Err(Error::Query("query byte budget exceeded".into()));
        }
        if empty || self.fetch == 0 {
            check()?;
            return Ok(Some(output));
        }
        let mut context = Execution {
            tables: &self.tables,
            predicates: &self.predicates,
            stages: &self.stages,
            bindings: &bindings,
            budget,
            start,
            limits,
            probes: 0,
        };
        let mut after = None;
        let mut visited = 0;
        let mut skip = self.skip;
        let mut work = 0u64;
        loop {
            check()?;
            let page = self.tables[0].snapshot.page_shared_checked(
                &self.tables[0].relation.name,
                None,
                after,
                128,
                &mut || context.candidate(),
            )?;
            after = page.next;
            for row in page.rows {
                context.charge(0, &row)?;
                visited += 1;
                if visited > MAX_OUTER {
                    return Ok(None);
                }
                let mut tuple = vec![None; self.tables.len()];
                tuple[0] = Some(row.clone());
                let Some(found) = context.probe(0, &mut tuple)? else {
                    return Ok(None);
                };
                if !found {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if output.rows.len() == limits.max_rows {
                    return Err(Error::Query(
                        "query row budget exceeded; use LIMIT or pages".into(),
                    ));
                }
                work = work.saturating_add(self.projection.len().max(1) as u64);
                if work > limits.vm_steps {
                    return Err(Error::Query("query output-cell budget exceeded".into()));
                }
                let source_bytes =
                    self.output_needed
                        .iter()
                        .try_fold(0usize, |bytes, &column| {
                            direct::source_size(&row, &self.tables[0].relation, column)
                                .map(|size| bytes.saturating_add(size))
                        })?;
                budget.charge(0, source_bytes).map_err(super::query_error)?;
                let mut values = Vec::with_capacity(self.projection.len());
                for c in &self.projection {
                    let size = direct::output_size(&row, c.field);
                    if size > limits.max_bytes.saturating_sub(bytes) {
                        return Err(Error::Query("query byte budget exceeded".into()));
                    }
                    bytes += size;
                    values.push(direct::output_value(&row, c.field));
                }
                output.rows.push(values);
                if output.rows.len() == self.fetch {
                    check()?;
                    return Ok(Some(output));
                }
            }
            if after.is_none() {
                check()?;
                return Ok(Some(output));
            }
        }
    }
}
fn compile_stages(tables: &[Table], predicates: &[Predicate], root: usize) -> Option<Vec<Stage>> {
    for p in predicates {
        let kind = kind(tables, p.left);
        if let Operand::Column(right) = p.right {
            if p.op != Operator::Eq || !compatible(kind, self::kind(tables, right)) {
                return None;
            }
        } else if p.op != Operator::Eq
            && !matches!(
                kind,
                Some(DataType::Int | DataType::Decimal { .. } | DataType::Timestamp)
            )
        {
            return None;
        }
    }
    let mut selected = vec![false; tables.len()];
    selected[root] = true;
    let mut stages = vec![];
    while stages.len() + 1 < tables.len() {
        let mut choices = vec![];
        for p in predicates {
            let Operand::Column(right) = p.right else {
                continue;
            };
            if p.op != Operator::Eq {
                continue;
            }
            for (a, b) in [(p.left, right), (right, p.left)] {
                if selected[a.table] && !selected[b.table] {
                    choices.push(Stage {
                        table: b.table,
                        field: b.field,
                        source: a,
                    });
                }
            }
        }
        let stage = choices
            .into_iter()
            .min_by_key(|s| (s.field != 0, s.table, s.field))?;
        selected[stage.table] = true;
        stages.push(stage);
    }
    Some(stages)
}
struct Execution<'a> {
    tables: &'a [Table],
    predicates: &'a [Predicate],
    stages: &'a [Stage],
    bindings: &'a [Option<Value>],
    budget: &'a SourceBudget,
    start: Instant,
    limits: QueryLimits,
    probes: usize,
}
impl Execution<'_> {
    fn candidate(&self) -> Result<()> {
        if self.start.elapsed() >= Duration::from_millis(self.limits.timeout_ms) {
            return Err(Error::Query("SQL deadline exceeded".into()));
        }
        self.budget.charge(1, 0).map_err(super::query_error)
    }
    fn charge(&self, table: usize, row: &Row) -> Result<()> {
        if self.start.elapsed() >= Duration::from_millis(self.limits.timeout_ms) {
            return Err(Error::Query("SQL deadline exceeded".into()));
        }
        let t = &self.tables[table];
        let bytes = t.needed.iter().try_fold(0usize, |n, &c| {
            direct::source_size(row, &t.relation, c).map(|size| n.saturating_add(size))
        })?;
        self.budget.charge(0, bytes).map_err(super::query_error)
    }
    fn probe(&mut self, stage: usize, tuple: &mut [Option<Arc<Row>>]) -> Result<Option<bool>> {
        if stage == self.stages.len() {
            return Ok(Some(true));
        }
        self.probes += 1;
        if self.probes > MAX_PROBES {
            return Ok(None);
        }
        let s = &self.stages[stage];
        let t = &self.tables[s.table];
        let key = cell(tuple, s.source)
            .ok_or_else(|| Error::Query("invalid EXISTS probe dependency".into()))?;
        let mut after = None;
        loop {
            let (rows, next) = if s.field == 0 {
                let Value::Ref(id) = key else {
                    return Err(Error::Query("invalid EXISTS identity key".into()));
                };
                self.candidate()?;
                (
                    t.snapshot
                        .get_shared(&t.relation.name, id)?
                        .into_iter()
                        .collect::<Vec<_>>(),
                    None,
                )
            } else {
                let page = t.snapshot.page_shared_checked(
                    &t.relation.name,
                    Some((&t.relation.roles[s.field - 1].name, &key)),
                    after,
                    32,
                    &mut || self.candidate(),
                )?;
                (page.rows, page.next)
            };
            for row in rows {
                self.charge(s.table, &row)?;
                tuple[s.table] = Some(row);
                if matches_predicates(self.predicates, self.bindings, tuple) {
                    match self.probe(stage + 1, tuple)? {
                        Some(false) => {}
                        outcome => {
                            tuple[s.table] = None;
                            return Ok(outcome);
                        }
                    }
                }
                tuple[s.table] = None;
            }
            let Some(next) = next else {
                return Ok(Some(false));
            };
            after = Some(next);
            self.probes += 1;
            if self.probes > MAX_PROBES {
                return Ok(None);
            }
        }
    }
}
fn cell(tuple: &[Option<Arc<Row>>], c: Column) -> Option<Value> {
    let row = tuple.get(c.table)?.as_ref()?;
    Some(if c.field == 0 {
        Value::Ref(row.id)
    } else {
        row.values.get(c.field - 1)?.clone()
    })
}
fn matches_predicates(
    predicates: &[Predicate],
    bindings: &[Option<Value>],
    tuple: &[Option<Arc<Row>>],
) -> bool {
    predicates.iter().enumerate().all(|(i, p)| {
        let Some(left) = cell(tuple, p.left) else {
            return true;
        };
        let right = match p.right {
            Operand::Column(c) => cell(tuple, c),
            Operand::Bound(_) => bindings[i].clone(),
        };
        let Some(right) = right else {
            return true;
        };
        match p.op {
            Operator::Eq => left == right,
            Operator::Lt => left < right,
            Operator::LtEq => left <= right,
            Operator::Gt => left > right,
            Operator::GtEq => left >= right,
            _ => false,
        }
    })
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
    let Expr::Column(c) = expr else {
        return None;
    };
    columns.get(plan.schema().index_of_column(c).ok()?).copied()
}
fn table(plan: &LogicalPlan, offset: usize) -> Option<Inner> {
    let LogicalPlan::TableScan(scan) = plan else {
        return None;
    };
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return None;
    }
    let provider = source_as_provider(&scan.source).ok()?;
    let t = (provider.as_ref() as &dyn Any).downcast_ref::<LiquidTable>()?;
    Some(Inner {
        tables: vec![Table {
            snapshot: t.snapshot().clone(),
            relation: t.declaration().clone(),
            needed: vec![],
        }],
        columns: scan
            .projection
            .clone()
            .unwrap_or_else(|| (0..t.declaration().roles.len() + 1).collect())
            .into_iter()
            .map(|field| Column {
                table: offset,
                field,
            })
            .collect(),
        predicates: vec![],
    })
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
    outer: &LogicalPlan,
    outer_columns: &[Column],
    out: &mut Vec<Predicate>,
) -> Option<()> {
    let Expr::BinaryExpr(b) = expr else {
        return None;
    };
    if b.op == Operator::And {
        predicates(&b.left, plan, columns, outer, outer_columns, out)?;
        return predicates(&b.right, plan, columns, outer, outer_columns, out);
    }
    if !matches!(
        b.op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return None;
    }
    let resolve = |e: &Expr| {
        if let Expr::OuterReferenceColumn(_, c) = e {
            outer_columns
                .get(outer.schema().index_of_column(c).ok()?)
                .copied()
        } else {
            column(e, plan, columns)
        }
    };
    let (left, right, op) = if let Some(left) = resolve(&b.left) {
        (left, b.right.as_ref(), b.op)
    } else {
        (
            resolve(&b.right)?,
            b.left.as_ref(),
            match b.op {
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                op => op,
            },
        )
    };
    out.push(Predicate {
        left,
        op,
        right: resolve(right)
            .map(Operand::Column)
            .or_else(|| binding(right).map(Operand::Bound))?,
    });
    Some(())
}
fn inner(
    plan: &LogicalPlan,
    offset: usize,
    outer: &LogicalPlan,
    outer_columns: &[Column],
) -> Option<Inner> {
    match plan {
        LogicalPlan::TableScan(_) => table(plan, offset),
        LogicalPlan::SubqueryAlias(a) => inner(&a.input, offset, outer, outer_columns),
        LogicalPlan::Filter(f) => {
            let mut s = inner(&f.input, offset, outer, outer_columns)?;
            predicates(
                &f.predicate,
                &f.input,
                &s.columns,
                outer,
                outer_columns,
                &mut s.predicates,
            )?;
            Some(s)
        }
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner
                && j.null_equality == NullEquality::NullEqualsNothing
                && !j.null_aware =>
        {
            let mut l = inner(&j.left, offset, outer, outer_columns)?;
            let r = inner(&j.right, offset + l.tables.len(), outer, outer_columns)?;
            if l.tables.len() + r.tables.len() >= MAX_TABLES {
                return None;
            }
            for (a, b) in &j.on {
                l.predicates.push(Predicate {
                    left: column(a, &j.left, &l.columns)?,
                    op: Operator::Eq,
                    right: Operand::Column(column(b, &j.right, &r.columns)?),
                });
            }
            l.tables.extend(r.tables);
            l.columns.extend(r.columns);
            l.predicates.extend(r.predicates);
            if let Some(f) = &j.filter {
                predicates(f, plan, &l.columns, outer, outer_columns, &mut l.predicates)?;
            }
            Some(l)
        }
        _ => None,
    }
}
fn outer(plan: &LogicalPlan) -> Option<Outer> {
    match plan {
        LogicalPlan::TableScan(_) => {
            let mut s = table(plan, 0)?;
            Some(Outer {
                table: s.tables.remove(0),
                columns: s.columns,
                inner: None,
                ordered: false,
                skip: 0,
                fetch: None,
            })
        }
        LogicalPlan::SubqueryAlias(a) => outer(&a.input),
        LogicalPlan::Filter(f) => {
            let mut s = outer(&f.input)?;
            if s.inner.is_some() || s.fetch.is_some() {
                return None;
            }
            let Expr::Exists(e) = &f.predicate else {
                return None;
            };
            if e.negated {
                return None;
            }
            let LogicalPlan::Projection(p) = e.subquery.subquery.as_ref() else {
                return None;
            };
            if p.expr.len() != 1
                || !matches!(&p.expr[0], Expr::Literal(ScalarValue::Int64(Some(1)), _))
            {
                return None;
            }
            s.inner = Some(inner(&p.input, 1, &f.input, &s.columns)?);
            Some(s)
        }
        LogicalPlan::Projection(p) => {
            let mut s = outer(&p.input)?;
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
            let mut s = outer(&sort.input)?;
            if s.ordered || s.fetch.is_some() || sort.fetch.is_some() || sort.expr.len() != 1 {
                return None;
            }
            let order = &sort.expr[0];
            let c = column(&order.expr, &sort.input, &s.columns)?;
            if c.table != 0 || c.field != 0 || !order.asc {
                return None;
            }
            s.ordered = true;
            Some(s)
        }
        LogicalPlan::Limit(l) => {
            let mut s = outer(&l.input)?;
            if s.fetch.is_some() {
                return None;
            }
            let SkipType::Literal(skip) = l.get_skip_type().ok()? else {
                return None;
            };
            let FetchType::Literal(Some(fetch)) = l.get_fetch_type().ok()? else {
                return None;
            };
            s.skip = skip;
            s.fetch = Some(fetch);
            Some(s)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::DfSnapshot;
    use super::*;
    use crate::{Database, Grant, Id, RowFilter, demo};
    const SQL: &str = "SELECT p.id,p.name FROM core.Person p WHERE EXISTS (SELECT 1 FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout WHERE f.athlete=p.id AND w.durationMinutes>?1) ORDER BY p.id LIMIT 10 OFFSET 2";
    fn fixture(people: usize) -> Database {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), crate::Visibility::Private)
            .unwrap();
        tx.install_with_visibility(demo::fitness(), crate::Visibility::Private)
            .unwrap();
        for i in 0..people {
            let p = Id::from_u128(i as u128 + 1);
            tx.insert_with_id("core.Person", p, vec![Value::Text(format!("Person{i}"))])
                .unwrap();
            for j in 0..3 {
                let w = tx
                    .insert(
                        "fitness.Workout",
                        vec![Value::Int(((i * 7 + j * 19) % 120) as i64)],
                    )
                    .unwrap();
                tx.insert("fitness.Performed", vec![p.into(), w.into()])
                    .unwrap();
            }
        }
        for name in ["core.Person", "fitness.Workout", "fitness.Performed"] {
            tx.grant("reader", name, Grant::read_only()).unwrap();
        }
        tx.commit().unwrap();
        db
    }
    #[test]
    fn correlated_probes_match_generic_sql_and_sqlite_and_rebind() {
        let db = fixture(100);
        for session in [db.admin(), db.session("reader")] {
            let snapshot = session.snapshot();
            let sqlite = snapshot.sqlite_compat().unwrap();
            let df = DfSnapshot::new(snapshot, None).unwrap();
            let mut query = df.prepare(SQL).unwrap();
            assert!(query.exists.is_some(), "{}", query.plan.display_indent());
            let mut generic = df.prepare(SQL).unwrap();
            generic.exists = None;
            for n in [110, 30, 119, 120, -1, 110] {
                let args = [SqlValue::Integer(n)];
                let expected = sqlite.query(SQL, &args).unwrap().rows;
                assert_eq!(
                    query.query(&args, QueryLimits::default()).unwrap().rows,
                    expected
                );
                assert_eq!(
                    generic.query(&args, QueryLimits::default()).unwrap().rows,
                    expected
                );
                assert!(query.cache.get(&args).is_none());
            }
            let args = [SqlValue::Null];
            assert!(
                query
                    .query(&args, QueryLimits::default())
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
    }
    #[test]
    fn filters_aliases_duplicate_matches_limits_and_supported_operators() {
        let db = fixture(30);
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        for text in [
            "SELECT p.name AS label,p.id FROM core.Person p WHERE EXISTS (SELECT 1 FROM fitness.Workout w JOIN fitness.Performed f ON f.workout=w.id WHERE p.id=f.athlete AND w.durationMinutes>=60 AND 100>w.durationMinutes) ORDER BY p.id LIMIT 12",
            "SELECT p.id FROM core.Person p WHERE EXISTS (SELECT 1 FROM fitness.Performed f WHERE f.athlete=p.id) ORDER BY p.id LIMIT 5",
            "SELECT p.id FROM core.Person p WHERE EXISTS (SELECT 1 FROM fitness.Workout w JOIN fitness.Performed f ON f.workout=w.id WHERE p.id=f.athlete AND w.durationMinutes<=60 AND w.durationMinutes=50) ORDER BY p.id LIMIT 12",
            "SELECT p.id FROM core.Person p WHERE EXISTS (SELECT 1 FROM fitness.Performed f WHERE f.athlete=p.id) ORDER BY p.id LIMIT 0",
        ] {
            let mut q = df.prepare(text).unwrap();
            assert!(q.exists.is_some(), "{}", q.plan.display_indent());
            assert_eq!(
                q.query(&[], QueryLimits::default()).unwrap().rows,
                sqlite.query(text, &[]).unwrap().rows,
                "{text}"
            );
        }
    }
    #[test]
    fn unsupported_shapes_and_numeric_coercion_use_general_executor() {
        let db = fixture(30);
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        for text in [
            SQL.replace("WHERE EXISTS", "WHERE NOT EXISTS"),
            SQL.replace("ORDER BY p.id", "ORDER BY p.id DESC"),
            SQL.replace("ORDER BY p.id", "ORDER BY p.name"),
            SQL.replace("SELECT 1 FROM", "SELECT random() FROM"),
            SQL.replace(
                "w.durationMinutes>?1",
                "w.durationMinutes>?1 OR w.durationMinutes<10",
            ),
            SQL.replace("JOIN fitness.Workout", "LEFT JOIN fitness.Workout"),
        ] {
            let q = df.prepare(&text);
            if let Ok(q) = q {
                assert!(q.exists.is_none(), "{text}");
            }
        }
        let mut q = df.prepare(SQL).unwrap();
        let args = [SqlValue::Real(50.5)];
        assert!(
            q.exists
                .as_ref()
                .unwrap()
                .query(&args, QueryLimits::default(), &SourceBudget::default())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            q.query(&args, QueryLimits::default()).unwrap().rows,
            sqlite.query(SQL, &args).unwrap().rows
        );
    }
    #[test]
    fn row_filters_reference_visibility_and_live_revocation_remain_enforced() {
        let db = fixture(300);
        let mut tx = db.admin().transaction();
        tx.grant(
            "reader",
            "fitness.Workout",
            Grant {
                read: true,
                write: false,
                reference: false,
                filter: RowFilter::Equals {
                    role: "durationMinutes".into(),
                    value: Value::Int(113),
                },
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
        assert!(q.exists.is_some());
        let args = [SqlValue::Integer(90)];
        let actual = q.query(&args, QueryLimits::default()).unwrap().rows;
        assert!(!actual.is_empty());
        assert_eq!(actual, sqlite.query(SQL, &args).unwrap().rows);
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "fitness.Workout").unwrap();
        tx.commit().unwrap();
        assert!(q.query(&args, QueryLimits::default()).is_err());
    }
    #[test]
    fn capability_scope_expiry_and_dropped_database_fail_closed() {
        let db = fixture(30);
        db.clock.set(1000);
        let issue = |names: &[&str]| {
            db.issue_capability_until(
                Id::new(),
                "reader",
                crate::capability::CapabilityScope {
                    relations: names
                        .iter()
                        .map(|name| crate::capability::RelationCapability {
                            relation: db.admin().snapshot().schema(name).unwrap().id,
                            grant: Grant::read_only(),
                        })
                        .collect(),
                    schema_namespace: None,
                    delegations: 0,
                },
                Some(1010),
            )
            .unwrap()
        };
        let narrow = issue(&["core.Person", "fitness.Performed"]);
        let narrow_session = db.session("reader").with_capability(narrow.id).unwrap();
        let df = DfSnapshot::new(narrow_session.snapshot(), None).unwrap();
        assert!(
            df.prepare(SQL).is_err(),
            "scope must not expose referenced workouts"
        );
        let cap = issue(&["core.Person", "fitness.Workout", "fitness.Performed"]);
        let session = db.session("reader").with_capability(cap.id).unwrap();
        let df = DfSnapshot::new(session.snapshot(), None).unwrap();
        let mut query = df.prepare(SQL).unwrap();
        assert!(query.exists.is_some());
        assert!(
            !query
                .query(&[SqlValue::Integer(0)], QueryLimits::default())
                .unwrap()
                .rows
                .is_empty()
        );
        db.clock.set(1010);
        assert!(matches!(
            query.query(&[SqlValue::Integer(0)], QueryLimits::default()),
            Err(Error::Denied)
        ));
        let df = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let mut query = df.prepare(SQL).unwrap();
        drop(session);
        drop(narrow_session);
        drop(db);
        assert!(matches!(
            query.query(&[SqlValue::Integer(0)], QueryLimits::default()),
            Err(Error::Denied)
        ));
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(24))]
        #[test]
        fn randomized_relationship_bags_preserve_exists_multiplicity(
            rows in proptest::collection::vec((0u8..12, -5i64..6), 0..70),
            bound in -6i64..7,
            skip in 0usize..5,
            take in 0usize..12,
        ) {
            let db = Database::memory(); let mut tx = db.admin().transaction();
            tx.install(demo::core()).unwrap(); tx.install(demo::fitness()).unwrap();
            let people = (0..12).map(|i| tx.insert_with_id("core.Person", Id::from_u128(i+1), vec![Value::Text(format!("P{i}"))]).unwrap()).collect::<Vec<_>>();
            for (person, value) in rows {
                let workout=tx.insert("fitness.Workout", vec![Value::Int(value)]).unwrap();
                // Duplicate relationship occurrences must not duplicate an outer row.
                for _ in 0..2 {tx.insert("fitness.Performed", vec![people[person as usize].into(),workout.into()]).unwrap();}
            }
            tx.commit().unwrap();
            let snapshot=db.admin().snapshot(); let sqlite=snapshot.sqlite_compat().unwrap(); let df=DfSnapshot::new(snapshot,None).unwrap();
            let text=SQL.replace("LIMIT 10 OFFSET 2", &format!("LIMIT {take} OFFSET {skip}"));
            let mut q=df.prepare(&text).unwrap(); proptest::prop_assert!(q.exists.is_some());
            let mut generic=df.prepare(&text).unwrap(); generic.exists=None;
            for n in [bound, 7, -6] {
                let args=[SqlValue::Integer(n)]; let expected=sqlite.query(&text,&args).unwrap().rows;
                proptest::prop_assert_eq!(q.query(&args,QueryLimits::default()).unwrap().rows,expected.clone());
                proptest::prop_assert_eq!(generic.query(&args,QueryLimits::default()).unwrap().rows,expected);
            }
        }
    }
    #[test]
    fn denied_candidates_are_interruptible_and_charge_source_work() {
        let db = fixture(30);
        let mut tx = db.admin().transaction();
        tx.grant(
            "reader",
            "core.Person",
            Grant {
                filter: RowFilter::Equals {
                    role: "name".into(),
                    value: "absent".into(),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").snapshot();
        let mut checked = 0;
        let result = snapshot.page_shared_checked("core.Person", None, None, 10, &mut || {
            checked += 1;
            if checked == 4 {
                Err(Error::Query("cancel candidate scan".into()))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert_eq!(checked, 4);
        let df = DfSnapshot::new(snapshot, None).unwrap();
        let q = df.prepare(SQL).unwrap();
        let budget = SourceBudget::default();
        budget
            .charge(super::super::provider::SOURCE_ROWS - 2, 0)
            .unwrap();
        assert!(
            q.exists
                .unwrap()
                .query(&[SqlValue::Integer(0)], QueryLimits::default(), &budget)
                .is_err()
        );
    }
    #[test]
    fn rejected_outer_payload_is_not_converted_but_returned_payload_is_bounded() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.install(demo::fitness()).unwrap();
        let person = tx
            .insert(
                "core.Person",
                vec![Value::Text(
                    "x".repeat(super::super::provider::CELL_BYTES + 1),
                )],
            )
            .unwrap();
        tx.commit().unwrap();
        let df = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
        assert!(q.exists.is_some());
        assert!(
            q.query(&[SqlValue::Integer(0)], QueryLimits::default())
                .unwrap()
                .rows
                .is_empty()
        );
        let mut tx = db.admin().transaction();
        let workout = tx.insert("fitness.Workout", vec![Value::Int(40)]).unwrap();
        tx.insert("fitness.Performed", vec![person.into(), workout.into()])
            .unwrap();
        tx.commit().unwrap();
        let df = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let mut q = df.prepare(&SQL.replace("OFFSET 2", "OFFSET 0")).unwrap();
        assert!(
            q.query(&[SqlValue::Integer(0)], QueryLimits::default())
                .is_err()
        );
    }
    #[test]
    fn output_limits_and_large_probe_fallback_preserve_semantics() {
        let db = fixture(1400);
        let snapshot = db.admin().snapshot();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
        let args = [SqlValue::Integer(0)];
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
            assert!(q.query(&args, limits).is_err());
        }
        // Zero matches inspect all rows; larger fixtures exercise an explicit work fallback.
        let db = fixture(MAX_OUTER + 1);
        let df = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
        let args = [SqlValue::Integer(120)];
        assert!(
            q.exists
                .as_ref()
                .unwrap()
                .query(&args, QueryLimits::default(), &SourceBudget::default())
                .unwrap()
                .is_none()
        );
        assert!(
            q.query(&args, QueryLimits::default())
                .unwrap()
                .rows
                .is_empty()
        );
    }
}
