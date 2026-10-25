//! Bounded join-driven native probes. SQL pair matching remains DataFusion's
//! responsibility; this adapter only restricts the probe input to matching keys.
use super::provider::LiquidScanExec;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::{
        DataFusionError, JoinType, NullEquality, Result, ScalarValue, tree_node::TreeNodeRecursion,
    },
    datasource::memory::MemorySourceConfig,
    execution::{TaskContext, memory_pool::MemoryConsumer},
    physical_expr::{EquivalenceProperties, PhysicalExpr, expressions::Column},
    physical_plan::{
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
        coop::CooperativeExec,
        execution_plan::{Boundedness, EmissionType},
        filter::FilterExec,
        joins::{HashJoinExec, PartitionMode},
        limit::{GlobalLimitExec, LocalLimitExec},
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        projection::ProjectionExec,
        sorts::sort::SortExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, TryStreamExt};
use std::{any::Any, collections::HashSet, fmt, sync::Arc};

pub(super) const MAX_INPUT_ROWS: usize = 128;

/// Appended after DataFusion's normal physical rules so EXPLAIN records the
/// same native-probe plan that execution receives.
#[derive(Debug)]
pub(super) struct IndexJoinRule;

impl datafusion::physical_optimizer::PhysicalOptimizerRule for IndexJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &datafusion::common::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        optimize(plan)
    }

    fn name(&self) -> &str {
        "liquid_index_join"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn native(plan: &Arc<dyn ExecutionPlan>) -> Option<&LiquidScanExec> {
    let value = plan.as_ref() as &dyn Any;
    if let Some(scan) = value.downcast_ref::<LiquidScanExec>() {
        Some(scan)
    } else if let Some(coop) = value.downcast_ref::<CooperativeExec>() {
        native(coop.input())
    } else {
        None
    }
}

/// Internal eligibility only. No hidden-population counts are exposed in plans.
fn small_bound(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let value = plan.as_ref() as &dyn Any;
    let bound = if let Some(scan) = native(plan) {
        scan.selective_upper_bound()
    } else if let Some(join) = value.downcast_ref::<LiquidIndexJoinExec>() {
        join.output_bound
    } else if let Some(coop) = value.downcast_ref::<CooperativeExec>() {
        small_bound(coop.input())
    } else if let Some(projection) = value.downcast_ref::<ProjectionExec>() {
        small_bound(projection.input())
    } else if let Some(filter) = value.downcast_ref::<FilterExec>() {
        small_bound(filter.input())
    } else if let Some(limit) = value.downcast_ref::<GlobalLimitExec>() {
        limit.fetch().or_else(|| small_bound(limit.input()))
    } else if let Some(limit) = value.downcast_ref::<LocalLimitExec>() {
        Some(limit.fetch())
    } else if let Some(sort) = value.downcast_ref::<SortExec>() {
        sort.fetch().or_else(|| small_bound(sort.input()))
    } else {
        None
    };
    bound.filter(|bound| *bound <= MAX_INPUT_ROWS)
}

/// Run after DataFusion planning. Any advertised ordering is restored explicitly
/// so existing parent ordering/distribution assumptions remain valid.
pub(super) fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let original_children = plan.children();
    let mut changed = false;
    let mut children = Vec::with_capacity(original_children.len());
    for child in original_children {
        let replacement = optimize(child.clone())?;
        changed |= !Arc::ptr_eq(child, &replacement);
        children.push(replacement);
    }
    let plan = if changed {
        plan.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?
    } else {
        plan
    };
    let Some(join) = (plan.as_ref() as &dyn Any).downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    if join.join_type() != &JoinType::Inner
        || join.on().len() != 1
        || join.null_equality() != NullEquality::NullEqualsNothing
        || (join.properties().output_ordering().is_some() && join.fetch().is_some())
        || join
            .left()
            .properties()
            .output_partitioning()
            .partition_count()
            != 1
        || join
            .right()
            .properties()
            .output_partitioning()
            .partition_count()
            != 1
    {
        return Ok(plan);
    }
    let Some(scan) = native(join.right()) else {
        return Ok(plan);
    };
    let (left, right) = &join.on()[0];
    let (Some(left), Some(right)) = (
        (left.as_ref() as &dyn Any).downcast_ref::<Column>(),
        (right.as_ref() as &dyn Any).downcast_ref::<Column>(),
    ) else {
        return Ok(plan);
    };
    let left_schema = join.left().schema();
    let right_schema = join.right().schema();
    let (Some(left_field), Some(right_field)) = (
        left_schema.fields().get(left.index()),
        right_schema.fields().get(right.index()),
    ) else {
        return Ok(plan);
    };
    if !scan.probe_capable(right.index()) || left_field.data_type() != right_field.data_type() {
        return Ok(plan);
    }
    // The authorized cardinality probe is deliberately last among eligibility
    // checks: unsupported right inputs must not trigger needless native reads.
    let Some(input_bound) = small_bound(join.left()) else {
        return Ok(plan);
    };
    let output_bound = scan
        .probe_result_upper_bound(right.index())
        .and_then(|bound| bound.checked_mul(input_bound));
    let left_column = left.index();
    let right_column = right.index();
    let ordering = join.properties().output_ordering().cloned();
    let replacement: Arc<dyn ExecutionPlan> = Arc::new(LiquidIndexJoinExec::new(
        plan,
        left_column,
        right_column,
        input_bound,
        output_bound,
    )?);
    // Probes run in first-seen left-key order, whereas HashJoin can advertise
    // its original right-side scan ordering. Restore that contract rather than
    // relying on a parent Sort (which an optimizer may already have removed).
    // Ordered joins with pushed fetch are excluded above: sorting after an
    // early truncated join would not preserve its top-k semantics.
    Ok(match ordering {
        Some(ordering) => Arc::new(SortExec::new(ordering, replacement)),
        None => replacement,
    })
}

pub(super) struct LiquidIndexJoinExec {
    // Never executed: each invocation builds a fresh native HashJoin with new
    // inputs. Its filters, projection, null mode and fetch retain SQL semantics.
    template: Arc<dyn ExecutionPlan>,
    left_column: usize,
    right_column: usize,
    input_bound: usize,
    output_bound: Option<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}
impl LiquidIndexJoinExec {
    /// Instantiate only from the planning cache's structurally identical fresh
    /// children. Snapshot, predicates and bindings are unchanged, so the proven
    /// bounds remain valid. General optimizer replacement still revalidates.
    pub(super) fn fresh_execution(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = (self.template.as_ref() as &dyn Any)
            .downcast_ref::<HashJoinExec>()
            .ok_or_else(|| error("invalid indexed join template"))?;
        let template = Arc::new(
            join.builder()
                .reset_state()
                .with_new_children(children)?
                .build()?,
        );
        Ok(Arc::new(Self::new(
            template,
            self.left_column,
            self.right_column,
            self.input_bound,
            self.output_bound,
        )?))
    }

    fn new(
        template: Arc<dyn ExecutionPlan>,
        left_column: usize,
        right_column: usize,
        input_bound: usize,
        output_bound: Option<usize>,
    ) -> Result<Self> {
        // Probe execution deliberately builds a fresh HashJoin without its
        // unused upstream dynamic filter. Remove that state from the retained
        // descriptor too; child expressions remain independently checked by
        // the planning cache before reuse.
        let join = (template.as_ref() as &dyn Any)
            .downcast_ref::<HashJoinExec>()
            .ok_or_else(|| error("invalid indexed join template"))?;
        let template: Arc<dyn ExecutionPlan> = Arc::new(join.builder().reset_state().build()?);
        let properties = properties(template.schema());
        Ok(Self {
            template,
            left_column,
            right_column,
            input_bound,
            output_bound,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}
fn properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}
impl fmt::Debug for LiquidIndexJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}
impl DisplayAs for LiquidIndexJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LiquidIndexJoinExec: inner, key={} -> {}, native_probes=true",
            self.left_column, self.right_column
        )
    }
}
impl ExecutionPlan for LiquidIndexJoinExec {
    fn name(&self) -> &'static str {
        "LiquidIndexJoinExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.template.children()
    }
    fn apply_expressions(
        &self,
        visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.template.apply_expressions(visitor)
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
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Re-evaluate boundedness/eligibility if an external optimizer replaces
        // inputs. Never carry a stale small-input assumption to another child.
        let template = self.template.clone().replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        optimize(template)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(error("invalid indexed join partition"));
        }
        let template = self.template.clone();
        let left_column = self.left_column;
        let right_column = self.right_column;
        let input_bound = self.input_bound;
        let keys_metric = MetricBuilder::new(&self.metrics).counter("probe_keys", partition);
        let rows_metric = MetricBuilder::new(&self.metrics).counter("probe_rows", partition);
        let schema = self.schema();
        let output = futures::stream::once(async move {
            let join = (template.as_ref() as &dyn Any)
                .downcast_ref::<HashJoinExec>()
                .ok_or_else(|| error("invalid indexed join template"))?;
            let mut input = join.left().execute(0, context.clone())?;
            let reservation =
                MemoryConsumer::new("LiquidIndexJoinInput").register(context.memory_pool());
            let mut batches = Vec::new();
            let mut keys = Vec::new();
            let mut seen = HashSet::new();
            let mut rows = 0usize;
            while let Some(batch) = input.try_next().await? {
                rows = rows.saturating_add(batch.num_rows());
                if rows > input_bound {
                    return Err(error("indexed join input exceeded its bounded plan"));
                }
                reservation.try_grow(batch.get_array_memory_size())?;
                let column = batch
                    .columns()
                    .get(left_column)
                    .ok_or_else(|| error("indexed join key is outside input schema"))?;
                for row in 0..batch.num_rows() {
                    let key = ScalarValue::try_from_array(column, row)?;
                    if !key.is_null() && !seen.contains(&key) {
                        // Keys are owned by both the dedup set and probe list;
                        // account their retained payload instead of hiding it
                        // outside the DataFusion memory pool.
                        reservation.try_grow(key.size().saturating_mul(2).saturating_add(64))?;
                        seen.insert(key.clone());
                        keys.push(key);
                    }
                }
                batches.push(batch);
            }
            drop(seen);
            keys_metric.add(keys.len());
            let left = MemorySourceConfig::try_new_exec(&[batches], join.left().schema(), None)?;
            let right: Arc<dyn ExecutionPlan> = Arc::new(ProbeKeysExec {
                source: join.right().clone(),
                column: right_column,
                keys,
                properties: properties(join.right().schema()),
                rows: rows_metric,
            });
            let fresh = join
                .builder()
                .reset_state()
                .with_new_children(vec![left, right])?
                .with_partition_mode(PartitionMode::CollectLeft)
                .build()?;
            let stream = fresh.execute(0, context)?;
            // Keep the temporary left-buffer reservation alive until execution
            // finishes or is cancelled. No batch or cursor survives in the plan.
            Ok::<_, DataFusionError>(stream.map(move |batch| {
                let _keep = &reservation;
                batch
            }))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

struct ProbeKeysExec {
    source: Arc<dyn ExecutionPlan>,
    column: usize,
    keys: Vec<ScalarValue>,
    properties: Arc<PlanProperties>,
    rows: Count,
}
impl fmt::Debug for ProbeKeysExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}
impl DisplayAs for ProbeKeysExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LiquidProbeKeysExec")
    }
}
impl ExecutionPlan for ProbeKeysExec {
    fn name(&self) -> &'static str {
        "LiquidProbeKeysExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(error("indexed probe source cannot have children"));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(error("invalid indexed probe partition"));
        }
        let source = self.source.clone();
        let column = self.column;
        let rows = self.rows.clone();
        let stream = futures::stream::iter(self.keys.clone())
            .enumerate()
            .then(move |(index, key)| {
                let source = source.clone();
                let context = context.clone();
                async move {
                    // Native sources yield between data pages. Yield between
                    // bounded groups of small probes too, without scheduling
                    // another task for every single-row identity lookup.
                    if index > 0 && index % 16 == 0 {
                        tokio::task::yield_now().await;
                    }
                    let scan =
                        native(&source).ok_or_else(|| error("indexed probe source disappeared"))?;
                    let stream: SendableRecordBatchStream = match scan.probe_scan(column, &key)? {
                        Some(plan) => plan.execute(0, context)?,
                        None => Box::pin(RecordBatchStreamAdapter::new(
                            source.schema(),
                            futures::stream::empty::<Result<RecordBatch>>(),
                        )),
                    };
                    Ok::<_, DataFusionError>(stream)
                }
            })
            .try_flatten()
            .map(move |batch| {
                if let Ok(batch) = &batch {
                    rows.add(batch.num_rows());
                }
                batch
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataType, Database, Module, Relation, Role, Value};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    #[test]
    fn owned_probe_keys_respect_memory_pool_and_error_releases_reservation() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(
            Module::new(
                "memory_probe",
                1,
                vec![],
                vec![
                    Relation::new(
                        "memory_probe.Left",
                        vec![
                            Role::new("bucket", DataType::Int),
                            Role::new("key", DataType::Text),
                        ],
                    ),
                    Relation::new("memory_probe.Right", vec![Role::new("key", DataType::Text)]),
                ],
            ),
            crate::Visibility::Private,
        )
        .unwrap();
        for n in 0..16 {
            let key = format!("{n:04}{}", "x".repeat(508));
            tx.insert(
                "memory_probe.Left",
                vec![Value::Int(1), Value::Text(key.clone())],
            )
            .unwrap();
            tx.insert("memory_probe.Right", vec![Value::Text(key)])
                .unwrap();
        }
        tx.commit().unwrap();
        let sql = super::super::DfSnapshot::new(db.admin().snapshot(), None).unwrap();
        super::super::run(async move {
            let frame = sql.context.sql("SELECT l.key,r.key AS right_key FROM memory_probe.Left l JOIN memory_probe.Right r ON l.key=r.key WHERE l.bucket=1").await.map_err(super::super::query_error)?;
            let plan = optimize(frame.create_physical_plan().await.map_err(super::super::query_error)?).map_err(super::super::query_error)?;
            assert!(datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string().contains("LiquidIndexJoinExec"));
            let runtime = RuntimeEnvBuilder::new().with_memory_limit(16 * 1024, 1.0).build_arc().unwrap();
            let pool = runtime.memory_pool.clone();
            let task = Arc::new(TaskContext::from(&sql.context).with_runtime(runtime));
            let failure = datafusion::physical_plan::collect(plan, task).await.unwrap_err();
            assert!(failure.to_string().contains("LiquidIndexJoinInput"), "{failure}");
            assert_eq!(pool.reserved(), 0, "failed key allocation must release all temporary reservations");
            Ok(())
        }, super::super::SETUP_TIMEOUT_MS).unwrap();
    }
}
