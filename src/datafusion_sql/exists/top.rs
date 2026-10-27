//! Bounded top-one-per-identity windows through authorized join probes.
//! Eligibility proves complete partitions and deterministic ROW_NUMBER ordering.
//! Wide/unsupported shapes retain DataFusion and its ordinary window execution.
use super::*;
use datafusion::{
    functions_window::row_number::RowNumber, logical_expr::expr::WindowFunctionDefinition,
};
use std::cmp::Ordering;

const MAX_PARTITION_TUPLES: usize = 256;
#[derive(Clone, Copy)]
enum Field {
    Native(Column),
    Rank,
}
struct Shape {
    inner: Inner,
    columns: Vec<Field>,
    partition: Column,
    order: Vec<(Column, bool)>,
    filtered: bool,
    ordered: bool,
    skip: usize,
    fetch: Option<usize>,
}
pub(in crate::datafusion_sql) struct PartitionTop {
    tables: Vec<Table>,
    predicates: Vec<Predicate>,
    stages: Vec<Stage>,
    root: usize,
    projection: Vec<Column>,
    output_needed: Vec<Column>,
    order: Vec<(Column, bool)>,
    names: Vec<String>,
    skip: usize,
    fetch: usize,
}
impl PartitionTop {
    pub(in crate::datafusion_sql) fn compile(plan: &LogicalPlan) -> Option<Self> {
        let shape = shape(plan)?;
        // Native tuple ordering/equality currently assumes non-null fields.
        // Leave SQL NULL ordering, three-valued predicates and floating
        // coercions to DataFusion until this executor implements them.
        if shape.inner.tables.iter().any(|table| {
            table
                .relation
                .roles
                .iter()
                .any(|role| matches!(role.data_type, DataType::Nullable(_) | DataType::Float))
        }) {
            return None;
        }
        let fetch = shape.fetch.filter(|n| *n <= 1000)?;
        if !shape.filtered
            || !shape.ordered
            || shape.skip > 1000
            || shape.inner.tables.len() > MAX_TABLES
        {
            return None;
        }
        let root = shape.partition.table;
        let stages = compile_stages(&shape.inner.tables, &shape.inner.predicates, root)?;
        // Every non-unique probe can multiply a partition. Its identity must be
        // in ORDER BY; subsequent identity probes are functionally determined.
        // This makes all ordering ties identical tuples, preserving ROW_NUMBER.
        if stages.iter().any(|stage| {
            stage.field != 0
                && !shape
                    .order
                    .iter()
                    .any(|(c, _)| c.table == stage.table && c.field == 0)
        }) {
            return None;
        }
        let projection = shape
            .columns
            .iter()
            .map(|field| match field {
                Field::Native(c) => Some(*c),
                Field::Rank => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let mut output_needed = projection.clone();
        output_needed.sort_by_key(|c| (c.table, c.field));
        output_needed.dedup();
        let mut tables = shape.inner.tables;
        for p in &shape.inner.predicates {
            tables[p.left.table].needed.push(p.left.field);
            if let Operand::Column(c) = p.right {
                tables[c.table].needed.push(c.field);
            }
        }
        for (c, _) in &shape.order {
            tables[c.table].needed.push(c.field);
        }
        for t in &mut tables {
            t.needed.sort_unstable();
            t.needed.dedup();
        }
        Some(Self {
            tables,
            predicates: shape.inner.predicates,
            stages,
            root,
            projection,
            output_needed,
            order: shape.order,
            names: plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            skip: shape.skip,
            fetch,
        })
    }
    pub(in crate::datafusion_sql) fn query(
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
        let mut bindings = Vec::with_capacity(self.predicates.len());
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
        let mut execution = Execution {
            tables: &self.tables,
            predicates: &self.predicates,
            stages: &self.stages,
            bindings: &bindings,
            budget,
            start,
            limits,
            probes: 0,
        };
        let root = &self.tables[self.root];
        let mut after = None;
        let mut visited = 0;
        let mut skip = self.skip;
        let mut work = 0u64;
        loop {
            check()?;
            let page = root.snapshot.page_shared_checked(
                &root.relation.name,
                None,
                after,
                128,
                &mut || execution.candidate(),
            )?;
            after = page.next;
            for row in page.rows {
                visited += 1;
                if visited > MAX_OUTER {
                    return Ok(None);
                }
                execution.charge(self.root, &row)?;
                let mut tuple = vec![None; self.tables.len()];
                tuple[self.root] = Some(row);
                let mut best = None;
                let mut count = 0;
                // Filters on the partition table are evaluated before probing.
                if !matches_predicates(&self.predicates, &bindings, &tuple) {
                    continue;
                }
                if !execution.best(0, &mut tuple, &self.order, &mut best, &mut count)? {
                    return Ok(None);
                }
                let Some(best) = best else {
                    continue;
                };
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
                let source_bytes = self.output_needed.iter().try_fold(0usize, |bytes, c| {
                    let row = best[c.table]
                        .as_ref()
                        .ok_or_else(|| Error::Query("invalid window output dependency".into()))?;
                    direct::source_size(row, &self.tables[c.table].relation, c.field)
                        .map(|n| bytes.saturating_add(n))
                })?;
                budget
                    .charge(0, source_bytes)
                    .map_err(super::super::query_error)?;
                let mut values = Vec::with_capacity(self.projection.len());
                for c in &self.projection {
                    let row = best[c.table]
                        .as_ref()
                        .ok_or_else(|| Error::Query("invalid window output dependency".into()))?;
                    let size = direct::output_size(row, c.field);
                    if size > limits.max_bytes.saturating_sub(bytes) {
                        return Err(Error::Query("query byte budget exceeded".into()));
                    }
                    bytes += size;
                    values.push(direct::output_value(row, c.field));
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
type Tuple = Vec<Option<Arc<Row>>>;
impl Execution<'_> {
    fn best(
        &mut self,
        stage: usize,
        tuple: &mut Tuple,
        order: &[(Column, bool)],
        best: &mut Option<Tuple>,
        count: &mut usize,
    ) -> Result<bool> {
        if stage == self.stages.len() {
            *count += 1;
            if *count > MAX_PARTITION_TUPLES {
                return Ok(false);
            }
            if best
                .as_ref()
                .is_none_or(|previous| compare(tuple, previous, order) == Ordering::Less)
            {
                *best = Some(tuple.clone());
            }
            return Ok(true);
        }
        self.probes += 1;
        if self.probes > MAX_PROBES {
            return Ok(false);
        }
        let s = &self.stages[stage];
        let t = &self.tables[s.table];
        let key = cell(tuple, s.source)
            .ok_or_else(|| Error::Query("invalid window probe dependency".into()))?;
        let mut after = None;
        loop {
            let (rows, next) = if s.field == 0 {
                let Value::Ref(id) = key else {
                    return Err(Error::Query("invalid window identity key".into()));
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
                if matches_predicates(self.predicates, self.bindings, tuple)
                    && !self.best(stage + 1, tuple, order, best, count)?
                {
                    tuple[s.table] = None;
                    return Ok(false);
                }
                tuple[s.table] = None;
            }
            let Some(next) = next else {
                return Ok(true);
            };
            after = Some(next);
            self.probes += 1;
            if self.probes > MAX_PROBES {
                return Ok(false);
            }
        }
    }
}
fn compare(a: &[Option<Arc<Row>>], b: &[Option<Arc<Row>>], order: &[(Column, bool)]) -> Ordering {
    for (c, ascending) in order {
        let order = cell(a, *c).cmp(&cell(b, *c));
        if order != Ordering::Equal {
            return if *ascending { order } else { order.reverse() };
        }
    }
    Ordering::Equal
}
fn field(expr: &Expr, plan: &LogicalPlan, columns: &[Field]) -> Option<Field> {
    let Expr::Column(c) = expr else {
        return None;
    };
    columns.get(plan.schema().index_of_column(c).ok()?).copied()
}
fn shape(plan: &LogicalPlan) -> Option<Shape> {
    match plan {
        LogicalPlan::Window(w) => {
            if w.window_expr.len() != 1 {
                return None;
            }
            let expr = &w.window_expr[0];
            let Expr::WindowFunction(function) = (if let Expr::Alias(a) = expr {
                a.expr.as_ref()
            } else {
                expr
            }) else {
                return None;
            };
            let WindowFunctionDefinition::WindowUDF(fun) = &function.fun else {
                return None;
            };
            if !(fun.inner().as_ref() as &dyn Any).is::<RowNumber>()
                || !function.params.args.is_empty()
                || function.params.filter.is_some()
                || function.params.distinct
                || function.params.null_treatment.is_some()
                || function.params.partition_by.len() != 1
                || function.params.order_by.is_empty()
            {
                return None;
            }
            let inner = inner(&w.input, 0, &w.input, &[])?;
            let partition = column(&function.params.partition_by[0], &w.input, &inner.columns)?;
            if partition.field != 0 {
                return None;
            }
            let order = function
                .params
                .order_by
                .iter()
                .map(|s| Some((column(&s.expr, &w.input, &inner.columns)?, s.asc)))
                .collect::<Option<Vec<_>>>()?;
            let mut columns = inner
                .columns
                .iter()
                .copied()
                .map(Field::Native)
                .collect::<Vec<_>>();
            columns.push(Field::Rank);
            Some(Shape {
                inner,
                columns,
                partition,
                order,
                filtered: false,
                ordered: false,
                skip: 0,
                fetch: None,
            })
        }
        LogicalPlan::SubqueryAlias(a) => shape(&a.input),
        LogicalPlan::Projection(p) => {
            let mut s = shape(&p.input)?;
            s.columns = p
                .expr
                .iter()
                .map(|e| {
                    field(
                        if let Expr::Alias(a) = e { &a.expr } else { e },
                        &p.input,
                        &s.columns,
                    )
                })
                .collect::<Option<_>>()?;
            Some(s)
        }
        LogicalPlan::Filter(f) => {
            let mut s = shape(&f.input)?;
            if s.filtered || s.fetch.is_some() {
                return None;
            }
            let Expr::BinaryExpr(b) = &f.predicate else {
                return None;
            };
            let (rank, value, op) =
                if matches!(field(&b.left, &f.input, &s.columns), Some(Field::Rank)) {
                    (&b.left, &b.right, b.op)
                } else {
                    (
                        &b.right,
                        &b.left,
                        match b.op {
                            Operator::GtEq => Operator::LtEq,
                            Operator::LtEq => Operator::GtEq,
                            Operator::Gt => Operator::Lt,
                            Operator::Lt => Operator::Gt,
                            op => op,
                        },
                    )
                };
            if !matches!(field(rank, &f.input, &s.columns), Some(Field::Rank))
                || !matches!(op, Operator::Eq | Operator::LtEq)
                || !matches!(
                    value.as_ref(),
                    Expr::Literal(ScalarValue::Int64(Some(1)), _)
                )
            {
                return None;
            }
            s.filtered = true;
            Some(s)
        }
        LogicalPlan::Sort(sort) => {
            let mut s = shape(&sort.input)?;
            if s.ordered || s.fetch.is_some() || sort.fetch.is_some() || sort.expr.len() != 1 {
                return None;
            }
            let order = &sort.expr[0];
            let Some(Field::Native(c)) = field(&order.expr, &sort.input, &s.columns) else {
                return None;
            };
            if c != s.partition || !order.asc {
                return None;
            }
            s.ordered = true;
            Some(s)
        }
        LogicalPlan::Limit(limit) => {
            let mut s = shape(&limit.input)?;
            if s.fetch.is_some() {
                return None;
            }
            let SkipType::Literal(skip) = limit.get_skip_type().ok()? else {
                return None;
            };
            let FetchType::Literal(Some(fetch)) = limit.get_fetch_type().ok()? else {
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
    use super::*;
    use crate::datafusion_sql::DfSnapshot;
    use crate::{Database, Grant, Id, RowFilter, demo};
    const SQL: &str = "SELECT person_id,link_id,workout_id,minutes FROM (SELECT p.id AS person_id,f.id AS link_id,w.id AS workout_id,w.durationMinutes AS minutes,ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY w.durationMinutes DESC,f.id) AS rn FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout WHERE w.durationMinutes>=?1) ranked WHERE rn<=1 ORDER BY person_id LIMIT 10 OFFSET 2";
    fn fixture(people: usize) -> Database {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), crate::Visibility::Private)
            .unwrap();
        tx.install_with_visibility(demo::fitness(), crate::Visibility::Private)
            .unwrap();
        for i in 0..people {
            let p = tx
                .insert_with_id(
                    "core.Person",
                    Id::from_u128(i as u128 + 1),
                    vec![Value::Text(format!("P{i}"))],
                )
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
    fn nullable_window_order_and_equality_use_sql_semantics() {
        use crate::{Module, Relation, Role};
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "win",
            1,
            vec![],
            vec![
                Relation::new(
                    "win.Parent",
                    vec![Role::new("key", DataType::Int.nullable())],
                ),
                Relation::new(
                    "win.Child",
                    vec![
                        Role::new("parent", DataType::Ref("win.Parent".into())),
                        Role::new("key", DataType::Int.nullable()),
                        Role::new("score", DataType::Float.nullable()),
                    ],
                ),
            ],
        ))
        .unwrap();
        for n in 0..2u128 {
            let parent = Id::from_u128(n + 1);
            let key = if n == 0 { Value::Null } else { Value::Int(1) };
            tx.insert_with_id("win.Parent", parent, vec![key.clone()])
                .unwrap();
            for j in 0..2u128 {
                let score = if j == 0 {
                    Value::Null
                } else {
                    Value::try_from(1.5).unwrap()
                };
                tx.insert_with_id(
                    "win.Child",
                    Id::from_u128(10 + n * 2 + j),
                    vec![parent.into(), key.clone(), score],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        for (null_order, score) in [("FIRST", SqlValue::Null), ("LAST", SqlValue::Real(1.5))] {
            for (predicate, expected_rows) in [("", 2), ("WHERE c.key=p.key", 1)] {
                let sql = format!(
                    "SELECT parent_id,score FROM (SELECT p.id AS parent_id,c.score,ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY c.score DESC NULLS {null_order},c.id) AS rn FROM win.Parent p JOIN win.Child c ON c.parent=p.id {predicate}) ranked WHERE rn<=1 ORDER BY parent_id LIMIT 10"
                );
                let mut prepared = df.prepare(&sql).unwrap();
                assert!(prepared.partition_top.is_none());
                let actual = prepared.query(&[], QueryLimits::default()).unwrap().rows;
                assert_eq!(actual, sqlite.query(&sql, &[]).unwrap().rows, "{sql}");
                assert_eq!(actual.len(), expected_rows, "{sql}");
                assert!(actual.iter().all(|row| row[1] == score), "{sql}");
            }
        }
    }
    #[test]
    fn ordered_partition_probes_match_both_executors_and_rebind() {
        let db = fixture(100);
        for session in [db.admin(), db.session("reader")] {
            let snapshot = session.snapshot();
            let sqlite = snapshot.sqlite_compat().unwrap();
            let df = DfSnapshot::new(snapshot, None).unwrap();
            let mut q = df.prepare(SQL).unwrap();
            assert!(q.partition_top.is_some(), "{}", q.plan.display_indent());
            let mut generic = df.prepare(SQL).unwrap();
            generic.partition_top = None;
            for bound in [0, 30, 110, 119, 120, 30] {
                let args = [SqlValue::Integer(bound)];
                let expected = sqlite.query(SQL, &args).unwrap().rows;
                assert_eq!(
                    q.query(&args, QueryLimits::default()).unwrap().rows,
                    expected
                );
                assert_eq!(
                    generic.query(&args, QueryLimits::default()).unwrap().rows,
                    expected
                );
                assert!(q.cache.get(&args).is_none());
            }
            assert!(
                q.query(&[SqlValue::Null], QueryLimits::default())
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
    }
    #[test]
    fn identity_partitions_and_unique_order_are_required() {
        let db = fixture(20);
        let df = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        for text in [
            SQL.replace("rn<=1", "1<=rn"),
            SQL.replace("rn<=1", "rn<=2"),
            SQL.replace("ROW_NUMBER()", "RANK()"),
            SQL.replace("PARTITION BY p.id", "PARTITION BY p.name"),
            SQL.replace("DESC,f.id", "DESC"),
            SQL.replace("ORDER BY person_id", "ORDER BY minutes"),
            SQL.replace("JOIN fitness.Workout", "LEFT JOIN fitness.Workout"),
            SQL.replace("rn<=1", "rn<=1 AND minutes>40"),
            SQL.replace("rn<=1", "rn<=?2"),
        ] {
            let q = df.prepare(&text).unwrap();
            assert!(q.partition_top.is_none(), "{text}");
        }
        for text in [
            SQL.replace("rn<=1", "1>=rn"),
            SQL.replace("rn<=1", "rn=1"),
            SQL.replace("LIMIT 10 OFFSET 2", "LIMIT 0"),
            SQL.replace(
                "ORDER BY w.durationMinutes DESC,f.id",
                "ORDER BY w.durationMinutes ASC,f.id DESC",
            ),
        ] {
            let mut q = df.prepare(&text).unwrap();
            assert!(q.partition_top.is_some(), "{}", q.plan.display_indent());
            let mut generic = df.prepare(&text).unwrap();
            generic.partition_top = None;
            assert_eq!(
                q.query(&[SqlValue::Integer(30)], QueryLimits::default())
                    .unwrap()
                    .rows,
                generic
                    .query(&[SqlValue::Integer(30)], QueryLimits::default())
                    .unwrap()
                    .rows
            );
        }
    }
    #[test]
    fn reordered_joins_and_aliases_keep_partition_and_order_meaning() {
        let db = fixture(20);
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        let text=SQL.replace("core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout","fitness.Workout w JOIN fitness.Performed f ON w.id=f.workout JOIN core.Person p ON p.id=f.athlete");
        let mut q = df.prepare(&text).unwrap();
        assert!(q.partition_top.is_some(), "{}", q.plan.display_indent());
        assert_eq!(
            q.query(&[SqlValue::Integer(20)], QueryLimits::default())
                .unwrap()
                .rows,
            sqlite.query(&text, &[SqlValue::Integer(20)]).unwrap().rows
        );
    }
    #[test]
    fn filtered_rows_scopes_and_live_revocation_remain_enforced() {
        let db = fixture(300);
        let mut tx = db.admin().transaction();
        tx.grant(
            "reader",
            "fitness.Workout",
            Grant {
                filter: RowFilter::Equals {
                    role: "durationMinutes".into(),
                    value: Value::Int(113),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
        assert!(q.partition_top.is_some());
        let args = [SqlValue::Integer(30)];
        let actual = q.query(&args, QueryLimits::default()).unwrap().rows;
        assert!(!actual.is_empty());
        assert_eq!(actual, sqlite.query(SQL, &args).unwrap().rows);
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "fitness.Workout").unwrap();
        tx.commit().unwrap();
        assert!(q.query(&args, QueryLimits::default()).is_err());
        let mut tx = db.admin().transaction();
        tx.grant("reader", "fitness.Workout", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        db.clock.set(1000);
        let cap = db
            .issue_capability_until(
                Id::new(),
                "reader",
                crate::capability::CapabilityScope {
                    relations: ["core.Person", "fitness.Workout", "fitness.Performed"]
                        .into_iter()
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
            .unwrap();
        let session = db.session("reader").with_capability(cap.id).unwrap();
        let df = DfSnapshot::new(session.snapshot(), None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
        assert!(q.partition_top.is_some());
        assert!(
            !q.query(&args, QueryLimits::default())
                .unwrap()
                .rows
                .is_empty()
        );
        db.clock.set(1010);
        assert!(matches!(
            q.query(&args, QueryLimits::default()),
            Err(Error::Denied)
        ));
    }
    #[test]
    fn partition_fanout_falls_back_and_budgets_are_enforced() {
        let db = fixture(1);
        let p = Id::from_u128(1);
        let mut tx = db.admin().transaction();
        for i in 0..300 {
            let w = tx.insert("fitness.Workout", vec![Value::Int(i)]).unwrap();
            tx.insert("fitness.Performed", vec![p.into(), w.into()])
                .unwrap();
        }
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let df = DfSnapshot::new(snapshot, None).unwrap();
        let text = SQL.replace("OFFSET 2", "OFFSET 0");
        let mut q = df.prepare(&text).unwrap();
        let args = [SqlValue::Integer(0)];
        assert!(
            q.partition_top
                .as_ref()
                .unwrap()
                .query(&args, QueryLimits::default(), &SourceBudget::default())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            q.query(&args, QueryLimits::default()).unwrap().rows,
            sqlite.query(&text, &args).unwrap().rows
        );
        let db = fixture(20);
        let df = DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        let mut q = df.prepare(SQL).unwrap();
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
        let args = [SqlValue::Real(30.5)];
        assert!(
            q.partition_top
                .as_ref()
                .unwrap()
                .query(&args, QueryLimits::default(), &SourceBudget::default())
                .unwrap()
                .is_none()
        );
        let mut generic = df.prepare(SQL).unwrap();
        generic.partition_top = None;
        assert_eq!(
            q.query(&args, QueryLimits::default()).unwrap().rows,
            generic.query(&args, QueryLimits::default()).unwrap().rows
        );
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(24))]
        #[test]
        fn randomized_tied_partitions_match_sqlite_and_generic(
            rows in proptest::collection::vec((0u8..12,-2i64..3),0..70),
            bound in -3i64..4,
            take in 0usize..12,
            skip in 0usize..5,
        ) {
            let db=Database::memory();let mut tx=db.admin().transaction();tx.install(demo::core()).unwrap();tx.install(demo::fitness()).unwrap();
            let people=(0..12).map(|i|tx.insert_with_id("core.Person",Id::from_u128(i+1),vec![Value::Text(format!("P{i}"))]).unwrap()).collect::<Vec<_>>();
            for(person,value)in rows {let w=tx.insert("fitness.Workout",vec![Value::Int(value)]).unwrap();for _ in 0..2{tx.insert("fitness.Performed",vec![people[person as usize].into(),w.into()]).unwrap();}}tx.commit().unwrap();
            let snapshot=db.admin().snapshot();let sqlite=snapshot.sqlite_compat().unwrap();let df=DfSnapshot::new(snapshot,None).unwrap();let text=SQL.replace("LIMIT 10 OFFSET 2",&format!("LIMIT {take} OFFSET {skip}"));
            let mut q=df.prepare(&text).unwrap();proptest::prop_assert!(q.partition_top.is_some());let mut generic=df.prepare(&text).unwrap();generic.partition_top=None;
            for value in [bound,-3,4]{let args=[SqlValue::Integer(value)];let expected=sqlite.query(&text,&args).unwrap().rows;proptest::prop_assert_eq!(q.query(&args,QueryLimits::default()).unwrap().rows,expected.clone());proptest::prop_assert_eq!(generic.query(&args,QueryLimits::default()).unwrap().rows,expected);}
        }
    }
}
