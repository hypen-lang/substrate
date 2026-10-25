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

use super::provider::LiquidScanExec;

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

fn logical_cacheable(plan: &LogicalPlan, view_depth: usize) -> bool {
    if view_depth > 32 {
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
            && !logical_cacheable(view.as_ref(), view_depth + 1)
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
                    Expr::ScalarVariable(..)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::ScalarSubquery(_)
                    | Expr::SetComparison(_)
                    | Expr::OuterReferenceColumn(..)
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
    // Exact concrete types: do not accept an unknown wrapper merely because it
    // delegates DataFusion downcasts to one of the supported inner operators.
    let kind = plan.as_ref() as &dyn Any;
    if !(kind.is::<LiquidScanExec>()
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
        .collect::<Result<_>>()?;
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
    fn stable_volatile_subquery_and_view_functions_are_not_cached() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let context = SessionContext::new();
            for sql in ["SELECT now()", "SELECT random()", "SELECT (SELECT 1)"] {
                let frame = context.sql(sql).await.unwrap();
                assert!(!cacheable(frame.logical_plan()), "{sql}");
            }
            let frame = context.sql("SELECT 1 + 2 AS x").await.unwrap();
            assert!(cacheable(frame.logical_plan()));
            let view = context
                .sql("SELECT now() AS clock")
                .await
                .unwrap()
                .into_view();
            context.register_table("clock_view", view).unwrap();
            let frame = context.sql("SELECT * FROM clock_view").await.unwrap();
            assert!(!cacheable(frame.logical_plan()));
        });
    }
}
