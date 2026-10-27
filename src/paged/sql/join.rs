//! Narrow inner equijoins compiled once from DataFusion's typed logical plan.
//! Every source must be reachable from a bound index predicate. Unsupported
//! expressions remain on DataFusion; this path never falls back to a table scan.
use super::*;
use datafusion::common::{JoinType, NullEquality};
use std::borrow::Cow;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Column {
    table: usize,
    field: usize,
}
enum Test {
    Bound(Column, Binding),
    Columns(Column, Column),
}
struct Shape {
    sources: Vec<Source>,
    projection: Vec<Column>,
    tests: Vec<Test>,
}
pub(super) struct IndexedJoin {
    shape: Shape,
    columns: Vec<String>,
    column_bytes: usize,
    // (source, field to probe, predicate providing the probe value).
    order: Vec<(usize, Column, usize)>,
    probes: Vec<Option<super::super::probe::PointProbe>>,
}
fn column(expr: &Expr, plan: &LogicalPlan, projection: &[Column]) -> Option<Column> {
    let Expr::Column(column) = expr else {
        return None;
    };
    projection
        .get(plan.schema().index_of_column(column).ok()?)
        .copied()
}
fn tests(
    expr: &Expr,
    plan: &LogicalPlan,
    projection: &[Column],
    out: &mut Vec<Test>,
) -> Option<()> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        tests(&binary.left, plan, projection, out)?;
        return tests(&binary.right, plan, projection, out);
    }
    if binary.op != Operator::Eq {
        return None;
    }
    let left = column(&binary.left, plan, projection);
    let right = column(&binary.right, plan, projection);
    let (col, value) = match (left, right) {
        (Some(a), Some(b)) => {
            out.push(Test::Columns(a, b));
            return Some(());
        }
        (Some(col), None) => (col, &binary.right),
        (None, Some(col)) => (col, &binary.left),
        _ => return None,
    };
    let binding = match value.as_ref() {
        Expr::Literal(value, _) => Binding::Literal(sql_value(value.clone()).ok()?),
        Expr::Placeholder(p) => Binding::Parameter(
            p.id.strip_prefix('$')?
                .parse::<usize>()
                .ok()?
                .checked_sub(1)?,
        ),
        _ => return None,
    };
    out.push(Test::Bound(col, binding));
    Some(())
}
fn shape(plan: &LogicalPlan) -> Option<Shape> {
    match plan {
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
            let provider = datafusion::datasource::source_as_provider(&scan.source).ok()?;
            let table = (provider.as_ref() as &dyn std::any::Any).downcast_ref::<PagedTable>()?;
            Some(Shape {
                sources: vec![table.source.clone()],
                projection: scan
                    .projection
                    .clone()
                    .unwrap_or_else(|| (0..table.schema.fields().len()).collect())
                    .into_iter()
                    .map(|field| Column { table: 0, field })
                    .collect(),
                tests: vec![],
            })
        }
        LogicalPlan::SubqueryAlias(alias) => shape(&alias.input),
        LogicalPlan::Projection(p) => {
            let mut s = shape(&p.input)?;
            s.projection = p
                .expr
                .iter()
                .map(|expr| {
                    column(
                        if let Expr::Alias(a) = expr {
                            &a.expr
                        } else {
                            expr
                        },
                        &p.input,
                        &s.projection,
                    )
                })
                .collect::<Option<_>>()?;
            Some(s)
        }
        LogicalPlan::Filter(f) => {
            let mut s = shape(&f.input)?;
            tests(&f.predicate, &f.input, &s.projection, &mut s.tests)?;
            Some(s)
        }
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner
                && j.null_equality == NullEquality::NullEqualsNothing
                && !j.null_aware =>
        {
            let mut left = shape(&j.left)?;
            let mut right = shape(&j.right)?;
            if left.sources.len() + right.sources.len() > 4 {
                return None;
            }
            let offset = left.sources.len();
            for c in &mut right.projection {
                c.table += offset;
            }
            for test in &mut right.tests {
                match test {
                    Test::Bound(c, _) => c.table += offset,
                    Test::Columns(a, b) => {
                        a.table += offset;
                        b.table += offset;
                    }
                }
            }
            for (a, b) in &j.on {
                left.tests.push(Test::Columns(
                    column(a, &j.left, &left.projection)?,
                    column(b, &j.right, &right.projection)?,
                ));
            }
            left.sources.extend(right.sources);
            left.projection.extend(right.projection);
            left.tests.extend(right.tests);
            if let Some(f) = &j.filter {
                tests(f, plan, &left.projection, &mut left.tests)?;
            }
            Some(left)
        }
        _ => None,
    }
}
impl Shape {
    fn kind(&self, c: Column) -> Option<&DataType> {
        c.field
            .checked_sub(1)
            .map(|i| &self.sources[c.table].declaration.roles[i].data_type)
    }
    fn rank(&self, c: Column) -> usize {
        if c.field == 0 {
            return 0;
        }
        let relation = &self.sources[c.table].declaration;
        if relation
            .unique
            .iter()
            .any(|key| key.len() == 1 && key[0] == relation.roles[c.field - 1].name)
        {
            1
        } else {
            2
        }
    }
}
impl IndexedJoin {
    pub(super) fn compile(plan: &LogicalPlan) -> Option<Self> {
        let shape = shape(plan)?;
        if shape.sources.iter().any(|source| {
            source
                .declaration
                .roles
                .iter()
                .any(|r| matches!(r.data_type, DataType::Nullable(_) | DataType::Float))
        }) {
            return None;
        }
        if !(2..=4).contains(&shape.sources.len()) {
            return None;
        }
        // Value equality must exactly match SQL equality. The broader SQL
        // coercions (including Text-versus-UUID) stay in DataFusion.
        for test in &shape.tests {
            if let Test::Columns(a, b) = test {
                let compatible = match (shape.kind(*a), shape.kind(*b)) {
                    (None | Some(DataType::Ref(_)), None | Some(DataType::Ref(_))) => true,
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                };
                if !compatible {
                    return None;
                }
            }
        }
        let mut seen = vec![false; shape.sources.len()];
        let mut order = Vec::new();
        while order.len() < seen.len() {
            let mut choices = Vec::new();
            for (index, test) in shape.tests.iter().enumerate() {
                match test {
                    Test::Bound(c, _) if !seen[c.table] => choices.push((*c, index)),
                    Test::Columns(a, b) if seen[a.table] && !seen[b.table] => {
                        choices.push((*b, index))
                    }
                    Test::Columns(a, b) if seen[b.table] && !seen[a.table] => {
                        choices.push((*a, index))
                    }
                    _ => {}
                }
            }
            let (col, index) = choices.into_iter().min_by_key(|(c, _)| shape.rank(*c))?;
            seen[col.table] = true;
            order.push((col.table, col, index));
        }
        let probes = order
            .iter()
            .map(|(table, column, _)| {
                let source = &shape.sources[*table];
                super::super::probe::PointProbe::compile(
                    &source.snapshot,
                    &source.declaration.name,
                    column.field,
                )
            })
            .collect();
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
            shape,
            order,
            probes,
            columns,
            column_bytes,
        })
    }
    pub(super) fn query(
        &self,
        params: &[SqlValue],
        limits: QueryLimits,
    ) -> Result<Option<SqlTable>> {
        use crate::datafusion_sql::direct::{Key, key};
        let mut bound = Vec::with_capacity(self.shape.tests.len());
        let mut empty = false;
        for test in &self.shape.tests {
            bound.push(match test {
                Test::Columns(..) => None,
                Test::Bound(c, binding) => {
                    let value = match binding {
                        Binding::Literal(v) => v,
                        Binding::Parameter(i) => params
                            .get(*i)
                            .ok_or_else(|| query_error("SQL parameter count mismatch"))?,
                    };
                    match key(value, self.shape.kind(*c)) {
                        Key::Unsupported => return Ok(None),
                        Key::Empty => {
                            empty = true;
                            None
                        }
                        Key::Value(v) => Some(v),
                    }
                }
            });
        }
        let bytes = self.column_bytes;
        if bytes > limits.max_bytes {
            return Err(query_error("query byte budget exceeded"));
        }
        let mut execution = Execution {
            join: self,
            bound,
            rows: std::array::from_fn(|_| None),
            result: SqlTable {
                columns: self.columns.clone(),
                rows: vec![],
            },
            limits,
            bytes,
            work: 0,
            budget: ScanBudget::new(limits.timeout_ms),
        };
        if !empty {
            execution.visit(0)?;
        }
        for source in &self.shape.sources {
            source.snapshot.fence()?;
        }
        execution.budget.check()?;
        Ok(Some(execution.result))
    }
}
struct Execution<'a> {
    join: &'a IndexedJoin,
    bound: Vec<Option<Value>>,
    // Compilation admits at most four sources.
    rows: [Option<Arc<Row>>; 4],
    result: SqlTable,
    limits: QueryLimits,
    bytes: usize,
    work: u64,
    budget: ScanBudget,
}
impl Execution<'_> {
    fn value(&self, c: Column) -> Option<Cow<'_, Value>> {
        let row = self.rows[c.table].as_ref()?;
        Some(if c.field == 0 {
            Cow::Owned(Value::Ref(row.id))
        } else {
            Cow::Borrowed(&row.values[c.field - 1])
        })
    }
    fn matches(&self) -> Result<bool> {
        for (i, test) in self.join.shape.tests.iter().enumerate() {
            self.budget.check()?;
            let matches = match test {
                Test::Bound(c, _) => self
                    .value(*c)
                    .is_none_or(|v| self.bound[i].as_ref() == Some(v.as_ref())),
                Test::Columns(a, b) => match (self.value(*a), self.value(*b)) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                },
            };
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn visit(&mut self, depth: usize) -> Result<()> {
        self.budget.check()?;
        if depth == self.join.order.len() {
            return self.emit();
        }
        let (table, col, test) = self.join.order[depth];
        let value = match &self.join.shape.tests[test] {
            Test::Bound(..) => self.bound[test].clone(),
            Test::Columns(a, b) => self
                .value(if a.table == table { *b } else { *a })
                .map(Cow::into_owned),
        }
        .ok_or_else(|| query_error("missing indexed join binding"))?;
        let join = self.join;
        let source = &join.shape.sources[table];
        if let Some(probe) = &join.probes[depth] {
            let mut check = || self.budget.check();
            if let Some(row) = probe.read(&value, &mut check)? {
                self.budget.charge(1).map_err(query_error)?;
                self.rows[table] = Some(row);
                if self.matches()? {
                    self.visit(depth + 1)?;
                }
            }
            self.rows[table] = None;
            return Ok(());
        }
        let mut after = None;
        loop {
            source.snapshot.fence()?;
            let mut check = || self.budget.check();
            let (rows, next) = if col.field == 0 {
                let Value::Ref(id) = value else {
                    return Err(query_error("invalid identity join probe"));
                };
                (
                    source
                        .snapshot
                        .get_checked(&source.declaration.name, id, &mut check)?
                        .into_iter()
                        .collect::<Vec<_>>(),
                    None,
                )
            } else {
                let page = source.snapshot.page_checked(
                    &source.declaration.name,
                    Some((&source.declaration.roles[col.field - 1].name, &value)),
                    after,
                    256.min(source.snapshot.db.inner.options.max_page_rows),
                    &mut check,
                )?;
                (page.rows, page.next)
            };
            self.budget.charge(rows.len()).map_err(query_error)?;
            for row in rows {
                self.rows[table] = Some(Arc::new(row));
                if self.matches()? {
                    self.visit(depth + 1)?;
                }
            }
            self.rows[table] = None;
            after = next;
            if after.is_none() {
                break;
            }
        }
        Ok(())
    }
    fn emit(&mut self) -> Result<()> {
        if self.result.rows.len() == self.limits.max_rows {
            return Err(query_error("query row budget exceeded; use LIMIT or pages"));
        }
        self.work = self
            .work
            .saturating_add(self.join.shape.projection.len().max(1) as u64);
        if self.work > self.limits.vm_steps {
            return Err(query_error("query output-cell budget exceeded"));
        }
        let mut row = Vec::with_capacity(self.join.shape.projection.len());
        for c in &self.join.shape.projection {
            self.budget.check()?;
            let value = self
                .value(*c)
                .ok_or_else(|| query_error("missing indexed join row"))?;
            let len = match value.as_ref() {
                Value::Text(v) => v.len(),
                Value::Bytes(v) => v.len(),
                Value::Ref(_) => 36,
                _ => 8,
            };
            if len > self.limits.max_bytes.saturating_sub(self.bytes) {
                return Err(query_error("query byte budget exceeded"));
            }
            let cell = match value.as_ref() {
                Value::Text(v) => SqlValue::Text(v.clone()),
                Value::Bytes(v) => SqlValue::Blob(v.clone()),
                Value::Null => SqlValue::Null,
                Value::Float(v) => SqlValue::Real(v.get()),
                Value::Ref(id) => SqlValue::Text(id.to_string()),
                Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
                Value::Int(n) | Value::Decimal(n) | Value::Timestamp(n) => SqlValue::Integer(*n),
            };
            self.bytes += len;
            row.push(cell);
        }
        self.result.rows.push(row);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiler_selects_only_anchored_typed_equijoins() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "core",
            1,
            vec![],
            vec![
                Relation::new("core.Person", vec![Role::new("age", DataType::Int)]),
                Relation::new(
                    "core.Link",
                    vec![
                        Role::new("person", DataType::Ref("core.Person".into())),
                        Role::new("score", DataType::Int),
                    ],
                ),
            ],
        ))
        .unwrap();
        tx.commit().unwrap();
        let sql = db.admin().snapshot().unwrap().sql().unwrap();
        for statement in [
            "SELECT p.id,l.score FROM core.Person p JOIN core.Link l ON p.id=l.person WHERE p.age=?1",
            "SELECT p.id,l.score FROM core.Link l JOIN core.Person p ON p.id=l.person WHERE l.score=?1",
            "SELECT a.score,b.score FROM core.Person p JOIN core.Link a ON a.person=p.id JOIN core.Link b ON b.person=p.id WHERE p.age=?1 AND a.score=b.score",
            "SELECT p.id,q.id FROM core.Person p JOIN core.Person q ON p.age=q.age WHERE p.age=?1",
        ] {
            assert!(
                sql.prepare(statement).unwrap().join.is_some(),
                "{statement}"
            );
        }
        for statement in [
            "SELECT p.id,l.score FROM core.Person p JOIN core.Link l ON p.id=l.person",
            "SELECT p.id,l.score FROM core.Person p LEFT JOIN core.Link l ON p.id=l.person WHERE p.age=7",
            "SELECT p.id,l.score FROM core.Person p JOIN core.Link l ON p.id=l.person WHERE p.age=7 OR p.age=8",
            "SELECT p.id,l.score FROM core.Person p JOIN core.Link l ON p.id=l.person WHERE p.age=7 AND l.score>5",
            "SELECT p.id,l.score FROM core.Person p JOIN core.Link l ON p.id=l.person WHERE p.age=7 LIMIT 1",
        ] {
            assert!(
                sql.prepare(statement).unwrap().join.is_none(),
                "{statement}"
            );
        }
    }
}
