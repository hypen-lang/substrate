//! Bounded, snapshot-local planning reuse without reusing execution state.
//!
//! DataFusion's generic `reset_plan_states` explicitly excludes dynamic filters
//! and recursive plans. We instead keep an unexecuted template only for a narrow
//! set of concrete operators and expressions whose state is rebuilt here. Other
//! deterministic queries may reuse their optimized logical plan, but still obtain
//! a newly planned physical tree.
use datafusion::{
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    logical_expr::{Expr, LogicalPlan, Volatility},
    physical_expr::{
        PhysicalExpr,
        expressions::{
            BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr, TryCastExpr,
        },
    },
    physical_plan::{
        ExecutionPlan,
        aggregates::AggregateExec,
        coop::CooperativeExec,
        empty::EmptyExec,
        execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions},
        filter::FilterExec,
        limit::{GlobalLimitExec, LocalLimitExec},
        placeholder_row::PlaceholderRowExec,
        projection::ProjectionExec,
    },
};
use rusqlite::types::Value as SqlValue;
use std::{any::Any, sync::Arc};

use super::{joins::LiquidIndexJoinExec, provider::LiquidScanExec};

mod analytic;

#[derive(Clone, PartialEq, Eq)]
enum Parameter {
    Null,
    Integer(i64),
    Real(u64),
    Text(String),
    Blob(Vec<u8>),
}

fn key(params: &[SqlValue]) -> Option<Vec<Parameter>> {
    params
        .iter()
        .map(|value| {
            Some(match value {
                SqlValue::Null => Parameter::Null,
                SqlValue::Integer(value) => Parameter::Integer(*value),
                SqlValue::Real(value) if value.is_finite() => Parameter::Real(value.to_bits()),
                SqlValue::Real(_) => return None,
                SqlValue::Text(value) => Parameter::Text(value.clone()),
                SqlValue::Blob(value) => Parameter::Blob(value.clone()),
            })
        })
        .collect()
}

/// One last-used binding per prepared query. Parameter payload bounds are checked
/// by the public query entry point before this cache is accessed.
#[derive(Default)]
pub(super) struct PlanCache {
    entry: Option<(Vec<Parameter>, CachedPlan)>,
}

#[derive(Clone)]
pub(super) struct CachedPlan {
    pub(super) optimized: LogicalPlan,
    template: Option<Arc<dyn ExecutionPlan>>,
}

impl PlanCache {
    pub(super) fn get(&self, params: &[SqlValue]) -> Option<CachedPlan> {
        let (saved, plan) = self.entry.as_ref()?;
        (saved.len() == params.len()
            && saved
                .iter()
                .zip(params)
                .all(|(saved, value)| match (saved, value) {
                    (Parameter::Null, SqlValue::Null) => true,
                    (Parameter::Integer(a), SqlValue::Integer(b)) => a == b,
                    (Parameter::Real(a), SqlValue::Real(b)) => b.is_finite() && *a == b.to_bits(),
                    (Parameter::Text(a), SqlValue::Text(b)) => a == b,
                    (Parameter::Blob(a), SqlValue::Blob(b)) => a == b,
                    _ => false,
                }))
        .then(|| plan.clone())
    }

    /// Call only after `cacheable` accepts the original, unoptimized plan. An
    /// optimized plan may have already folded a stable function such as now().
    /// The supplied physical tree must never have been executed.
    pub(super) fn put(
        &mut self,
        params: &[SqlValue],
        optimized: LogicalPlan,
        physical: Arc<dyn ExecutionPlan>,
    ) {
        self.entry = key(params).map(|key| {
            let template = reusable(&physical).then_some(physical);
            (
                key,
                CachedPlan {
                    optimized,
                    template,
                },
            )
        });
    }

    pub(super) fn clear(&mut self) {
        self.entry = None;
    }
}

impl CachedPlan {
    /// Construct a fresh execution tree, including fresh leaf metrics. No cursor,
    /// memory reservation, source budget, task context or output is cached.
    pub(super) fn physical(&self) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.template.as_ref().map(fresh).transpose()
    }
}

/// Fail closed for context-dependent functions and plan forms not considered
/// here. Checking before optimization prevents capturing a folded stable value.
pub(super) fn cacheable(plan: &LogicalPlan) -> bool {
    logical_cacheable(plan, 0)
}

fn logical_cacheable(plan: &LogicalPlan, depth: usize) -> bool {
    if depth > 32 {
        return false;
    }
    let mut allowed = true;
    let result = plan.apply(|node| {
        if !matches!(
            node,
            LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::Aggregate(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Join(_)
                | LogicalPlan::Union(_)
                | LogicalPlan::TableScan(_)
                | LogicalPlan::EmptyRelation(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Values(_)
                | LogicalPlan::Distinct(_)
        ) {
            allowed = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::TableScan(scan) = node
            && let Some(view) = scan.source.get_logical_plan()
            && !logical_cacheable(view.as_ref(), depth + 1)
        {
            allowed = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                let safe = match expr {
                    Expr::ScalarFunction(function) => {
                        function.func.signature().volatility == Volatility::Immutable
                    }
                    Expr::AggregateFunction(function) => {
                        function.func.signature().volatility == Volatility::Immutable
                    }
                    Expr::WindowFunction(function) => {
                        function.fun.signature().volatility == Volatility::Immutable
                    }
                    // Inspect raw subquery plans before optimization can fold
                    // execution-time functions. Correlation is deterministic
                    // for this pinned snapshot and exact parameter binding.
                    // Physical reuse has its own audited operator whitelist.
                    Expr::Exists(expr) => logical_cacheable(&expr.subquery.subquery, depth + 1),
                    Expr::InSubquery(expr) => logical_cacheable(&expr.subquery.subquery, depth + 1),
                    Expr::ScalarSubquery(query) => logical_cacheable(&query.subquery, depth + 1),
                    Expr::OuterReferenceColumn(..) => depth > 0,
                    Expr::ScalarVariable(..)
                    | Expr::SetComparison(_)
                    | Expr::HigherOrderFunction(_)
                    | Expr::Lambda(_)
                    | Expr::LambdaVariable(_)
                    | Expr::Unnest(_) => false,
                    _ => true,
                };
                if safe {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    allowed = false;
                    Ok(TreeNodeRecursion::Stop)
                }
            })
        })?;
        Ok(if allowed {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    result.is_ok() && allowed
}

fn reusable(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(allowed) = analytic::reusable(plan) {
        return allowed && plan.children().into_iter().all(reusable);
    }
    // Exact concrete types: do not accept an unknown wrapper merely because it
    // delegates DataFusion downcasts to one of the supported inner operators.
    let kind = plan.as_ref() as &dyn Any;
    let plain_aggregate = kind
        .downcast_ref::<AggregateExec>()
        .is_some_and(reusable_aggregate);
    if !(kind.is::<LiquidScanExec>()
        || kind.is::<LiquidIndexJoinExec>()
        || plain_aggregate
        || kind.is::<CooperativeExec>()
        || kind.is::<ProjectionExec>()
        || kind.is::<FilterExec>()
        || kind.is::<GlobalLimitExec>()
        || kind.is::<LocalLimitExec>()
        || kind.is::<EmptyExec>()
        || kind.is::<PlaceholderRowExec>())
    {
        return false;
    }
    let mut safe = true;
    let result = plan.apply_expressions(&mut |expr| {
        safe &= reusable_expression(expr);
        Ok(if safe {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    result.is_ok() && safe && plan.children().into_iter().all(reusable)
}

fn reusable_aggregate(aggregate: &AggregateExec) -> bool {
    use datafusion::functions_aggregate::{
        average::Avg,
        count::Count,
        min_max::{Max, Min},
        sum::Sum,
    };
    // UDF volatility alone cannot prove absence of mutable implementation state.
    // These exact upstream builtins hold only signatures/configuration; their
    // accumulators (including DISTINCT sets) are newly created by each stream.
    aggregate.dynamic_expressions_produced().is_empty()
        && aggregate.limit_options().is_none()
        && !aggregate.group_expr().has_grouping_set()
        && aggregate.aggr_expr().iter().all(|expression| {
            let implementation = expression.fun().inner().as_ref() as &dyn Any;
            expression.fun().signature().volatility == Volatility::Immutable
                && (implementation.is::<Count>()
                    || implementation.is::<Sum>()
                    || implementation.is::<Min>()
                    || implementation.is::<Max>()
                    || implementation.is::<Avg>()
                    || super::checked::reusable_sum(expression.fun()))
        })
}

fn reusable_expression(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let kind = expr.as_ref() as &dyn Any;
    (kind.is::<Column>()
        || kind.is::<Literal>()
        || kind.is::<BinaryExpr>()
        || kind.is::<CastExpr>()
        || kind.is::<TryCastExpr>()
        || kind.is::<IsNullExpr>()
        || kind.is::<IsNotNullExpr>()
        || kind.is::<NotExpr>())
        && expr.children().into_iter().all(reusable_expression)
}

fn fresh(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    // These leaves are immutable but their upstream replacement returns `self`.
    // Copy them explicitly so even the first execution does not use the template.
    if let Some(empty) = (plan.as_ref() as &dyn Any).downcast_ref::<EmptyExec>() {
        return Ok(Arc::new(empty.clone()));
    }
    if let Some(row) = (plan.as_ref() as &dyn Any).downcast_ref::<PlaceholderRowExec>() {
        return Ok(Arc::new(row.clone()));
    }
    let children = plan
        .children()
        .into_iter()
        .map(fresh)
        .collect::<Result<Vec<_>>>()?;
    if let Some(plan) = analytic::fresh(plan, &children)? {
        return Ok(plan);
    }
    if let Some(join) = (plan.as_ref() as &dyn Any).downcast_ref::<LiquidIndexJoinExec>() {
        return join.fresh_execution(children);
    }
    if (plan.as_ref() as &dyn Any).is::<AggregateExec>() {
        // Upstream Recompute calls try_new_with_schema: fresh metrics and plan
        // properties, preserving optimized output names/metadata. No accumulator
        // lives in AggregateExec; the audited builtin descriptors are immutable.
        // Its copied dynamic filter is guaranteed absent by reusable_aggregate.
        return Arc::clone(plan).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        );
    }
    Arc::clone(plan).replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::datatypes::Schema, logical_expr::LogicalPlanBuilder,
        physical_plan::union::UnionExec, prelude::SessionContext,
    };

    fn logical() -> LogicalPlan {
        LogicalPlanBuilder::empty(false).build().unwrap()
    }
    fn empty() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())))
    }

    #[test]
    fn single_binding_cache_uses_exact_types_and_replaces_old_binding() {
        let mut cache = PlanCache::default();
        cache.put(&[SqlValue::Integer(1)], logical(), empty());
        assert!(cache.get(&[SqlValue::Integer(1)]).is_some());
        assert!(cache.get(&[SqlValue::Real(1.)]).is_none());
        assert!(cache.get(&[SqlValue::Text("1".into())]).is_none());
        cache.put(&[SqlValue::Real(-0.)], logical(), empty());
        assert!(cache.get(&[SqlValue::Integer(1)]).is_none());
        assert!(cache.get(&[SqlValue::Real(0.)]).is_none());
        assert!(cache.get(&[SqlValue::Real(-0.)]).is_some());
        cache.clear();
        assert!(cache.get(&[SqlValue::Real(-0.)]).is_none());
        cache.put(&[SqlValue::Real(f64::NAN)], logical(), empty());
        assert!(cache.entry.is_none());
    }

    #[test]
    fn every_template_execution_gets_fresh_nodes_and_unknown_operators_replan() {
        let original =
            Arc::new(GlobalLimitExec::new(empty(), 0, Some(1))) as Arc<dyn ExecutionPlan>;
        let mut cache = PlanCache::default();
        cache.put(&[], logical(), original.clone());
        let cached = cache.get(&[]).unwrap();
        let first = cached.physical().unwrap().unwrap();
        let second = cached.physical().unwrap().unwrap();
        assert!(!Arc::ptr_eq(&original, &first));
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(first.children()[0], second.children()[0]));
        // Deliberately unsupported even though this particular union is simple.
        let unknown = UnionExec::try_new(vec![empty(), empty()]).unwrap();
        cache.put(&[], logical(), unknown);
        assert!(cache.get(&[]).unwrap().physical().unwrap().is_none());
    }

    #[test]
    fn subqueries_are_checked_recursively_before_function_folding() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let context = SessionContext::new();
            for sql in [
                "SELECT now()",
                "SELECT random()",
                "SELECT (SELECT now())",
                "SELECT 1 WHERE EXISTS (SELECT 1 WHERE random() > 0)",
                "SELECT 1 WHERE 1 IN (SELECT 1 WHERE now() = now())",
                "SELECT 1 WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT 1 WHERE random() > 0))",
            ] {
                let frame = context.sql(sql).await.unwrap();
                assert!(!cacheable(frame.logical_plan()), "{sql}");
            }
            let frame = context.sql("SELECT 1 + 2 AS x").await.unwrap();
            assert!(cacheable(frame.logical_plan()));
            for sql in [
                "SELECT (SELECT 1)",
                "SELECT 1 WHERE EXISTS (SELECT 1)",
                "SELECT 1 WHERE 1 IN (SELECT 1)",
                "SELECT x FROM (VALUES (1), (2)) AS t(x) WHERE EXISTS (SELECT 1 WHERE t.x=1)",
            ] {
                let frame = context.sql(sql).await.unwrap();
                assert!(cacheable(frame.logical_plan()), "{sql}");
            }
            let view = context
                .sql("SELECT now() AS clock")
                .await
                .unwrap()
                .into_view();
            context.register_table("clock_view", view).unwrap();
            let frame = context.sql("SELECT * FROM clock_view").await.unwrap();
            assert!(!cacheable(frame.logical_plan()));
            let frame = context
                .sql("SELECT 1 WHERE EXISTS (SELECT 1 FROM clock_view WHERE clock IS NOT NULL)")
                .await
                .unwrap();
            assert!(!cacheable(frame.logical_plan()));
        });
    }

    #[test]
    fn aggregate_templates_rebuild_accumulators_and_recover_from_memory_errors() {
        use crate::{DataType, Database, Grant, Module, Relation, Role, Value};
        use datafusion::{
            common::ScalarValue,
            execution::{TaskContext, runtime_env::RuntimeEnvBuilder},
            physical_plan::collect,
        };
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "reuse",
                1,
                vec![],
                vec![Relation::new(
                    "reuse.Rows",
                    vec![
                        Role::new("bucket", DataType::Int),
                        Role::new("n", DataType::Int),
                    ],
                )],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for n in 0..512 {
            tx.insert("reuse.Rows", vec![Value::Int(n % 4), Value::Int(n)])
                .unwrap();
        }
        tx.grant("reader", "reuse.Rows", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let snapshot =
            super::super::DfSnapshot::new(db.session("reader").snapshot(), None).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let queries = [
                "SELECT COUNT(*),SUM(n),MIN(n),MAX(n),AVG(n),COUNT(DISTINCT n) FROM reuse.Rows",
                "SELECT bucket,COUNT(*),SUM(n) FROM reuse.Rows GROUP BY bucket ORDER BY bucket",
                "SELECT COUNT(*) FILTER (WHERE n>100),SUM(n) FILTER (WHERE n<100) FROM reuse.Rows",
            ];
            for query in queries {
                let frame = snapshot.context.sql(query).await.unwrap();
                assert!(cacheable(frame.logical_plan()));
                let checked = super::super::checked::rewrite(frame.logical_plan().clone()).unwrap();
                let frame = datafusion::dataframe::DataFrame::new(
                    snapshot.context.state(),
                    checked.clone(),
                );
                let template = frame.create_physical_plan().await.unwrap();
                let mut cache = PlanCache::default();
                cache.put(&[], checked, template.clone());
                let cached = cache.get(&[]).unwrap();
                let first = cached
                    .physical()
                    .unwrap()
                    .expect("audited aggregate must retain physical template");
                let second = cached.physical().unwrap().unwrap();
                assert!(!Arc::ptr_eq(&first, &second));
                assert!(!Arc::ptr_eq(&first, &template));
                let task = Arc::new(TaskContext::from(&snapshot.context));
                let (first, second) =
                    futures::join!(collect(first, task.clone()), collect(second, task.clone()));
                let expected = first.unwrap();
                assert_eq!(expected, second.unwrap());
                assert_eq!(
                    expected,
                    collect(cached.physical().unwrap().unwrap(), task.clone())
                        .await
                        .unwrap()
                );
                if query == queries[0] {
                    assert_eq!(
                        ScalarValue::try_from_array(expected[0].column(0), 0).unwrap(),
                        ScalarValue::Int64(Some(512))
                    );
                    assert_eq!(
                        ScalarValue::try_from_array(expected[0].column(1), 0).unwrap(),
                        ScalarValue::Int64(Some(130816))
                    );
                }
                let constrained = RuntimeEnvBuilder::new()
                    .with_memory_limit(1, 1.0)
                    .build_arc()
                    .unwrap();
                let pool = constrained.memory_pool.clone();
                let constrained_task =
                    Arc::new(TaskContext::from(&snapshot.context).with_runtime(constrained));
                let failure = collect(cached.physical().unwrap().unwrap(), constrained_task).await;
                if query == queries[1] {
                    assert!(
                        failure.is_err(),
                        "group aggregate must enforce memory budget: {query}"
                    );
                }
                assert_eq!(
                    pool.reserved(),
                    0,
                    "failure must release memory reservations"
                );
                assert_eq!(
                    expected,
                    collect(cached.physical().unwrap().unwrap(), task.clone())
                        .await
                        .unwrap()
                );
            }
            let frame = snapshot
                .context
                .sql("SELECT COUNT(*),SUM(n) FROM reuse.Rows")
                .await
                .unwrap();
            let checked = super::super::checked::rewrite(frame.logical_plan().clone()).unwrap();
            let template = datafusion::dataframe::DataFrame::new(snapshot.context.state(), checked)
                .create_physical_plan()
                .await
                .unwrap();
            assert!(reusable(&template));
            let mut tx = db.admin().transaction();
            tx.revoke("reader", "reuse.Rows").unwrap();
            tx.commit().unwrap();
            assert!(
                collect(
                    fresh(&template).unwrap(),
                    Arc::new(TaskContext::from(&snapshot.context))
                )
                .await
                .is_err()
            );
        });
    }

    #[test]
    fn unknown_immutable_aggregate_with_builtin_name_is_not_reusable() {
        use datafusion::{arrow::datatypes::DataType, logical_expr::create_udaf};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let context = SessionContext::new();
            context.register_udaf(create_udaf(
                "sum",
                vec![DataType::Int64],
                Arc::new(DataType::Int64),
                Volatility::Immutable,
                Arc::new(|_| {
                    Err(datafusion::common::DataFusionError::Execution(
                        "must never execute".into(),
                    ))
                }),
                Arc::new(vec![DataType::Int64]),
            ));
            let frame = context
                .sql("SELECT SUM(column1) FROM (VALUES (1), (2))")
                .await
                .unwrap();
            assert!(cacheable(frame.logical_plan()));
            let plan = frame.create_physical_plan().await.unwrap();
            let mut found = false;
            plan.apply(|node| {
                if let Some(aggregate) = (node.as_ref() as &dyn Any).downcast_ref::<AggregateExec>()
                {
                    assert!(!reusable_aggregate(aggregate));
                    found = true;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            assert!(found);
            assert!(!reusable(&plan));
        });
    }
}
