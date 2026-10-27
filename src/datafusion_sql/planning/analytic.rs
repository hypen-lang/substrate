//! Fresh physical templates for audited DataFusion 55.2 analytic operators.
//!
//! A producer's dynamic filter is only an optimization when no descendant
//! consumes it. Replacing such producers is safe; sharing a filter with a scan
//! or predicate across executions is not. Inspect real keys/predicates rather
//! than skipping every expression that happens to be a producer.
use super::reusable_expression;
use datafusion::{
    common::{DataFusionError, Result, tree_node::TreeNodeRecursion},
    functions_window::row_number::RowNumber,
    logical_expr::Volatility,
    physical_plan::{
        ExecutionPlan,
        execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions},
        joins::HashJoinExec,
        sorts::sort::SortExec,
        windows::{
            BoundedWindowAggExec, StandardWindowExpr, WindowAggExec, WindowExpr, WindowUDFExpr,
        },
    },
};
use std::{any::Any, sync::Arc};

/// None means another module owns the operator. The parent must additionally
/// validate every child operator before retaining the complete template.
pub(super) fn reusable(plan: &Arc<dyn ExecutionPlan>) -> Option<bool> {
    let kind = plan.as_ref() as &dyn Any;
    if kind.is::<HashJoinExec>() || kind.is::<SortExec>() {
        return Some(no_dynamic_consumers(plan));
    }
    if let Some(window) = kind.downcast_ref::<WindowAggExec>() {
        return Some(row_number_only(window.window_expr()) && no_dynamic_consumers(plan));
    }
    if let Some(window) = kind.downcast_ref::<BoundedWindowAggExec>() {
        return Some(
            window.state_observer().is_none()
                && row_number_only(window.window_expr())
                && no_dynamic_consumers(plan),
        );
    }
    None
}

fn row_number_only(expressions: &[Arc<dyn WindowExpr>]) -> bool {
    !expressions.is_empty()
        && expressions.iter().all(|expr| {
            // These two upstream descriptor traits expose as_any rather than Any.
            // They are produced by our private, builtin-only DataFusion planner;
            // arbitrary host-supplied physical/window implementations are not an API.
            let Some(standard) = expr.as_any().downcast_ref::<StandardWindowExpr>() else {
                return false;
            };
            let Some(function) = standard
                .get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
            else {
                return false;
            };
            (function.fun().inner().as_ref() as &dyn Any).is::<RowNumber>()
                && function.fun().signature().volatility == Volatility::Immutable
                && function.args().is_empty()
                && expr.expressions().is_empty()
                && expr.partition_by().iter().all(reusable_expression)
                && expr
                    .order_by()
                    .iter()
                    .all(|key| reusable_expression(&key.expr))
        })
}

fn expressions_without_producers(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let kind = plan.as_ref() as &dyn Any;
    if let Some(join) = kind.downcast_ref::<HashJoinExec>() {
        return join
            .on()
            .iter()
            .all(|(left, right)| reusable_expression(left) && reusable_expression(right))
            && join
                .filter()
                .is_none_or(|filter| reusable_expression(filter.expression()));
    }
    if let Some(sort) = kind.downcast_ref::<SortExec>() {
        return sort.expr().iter().all(|key| reusable_expression(&key.expr));
    }
    let mut safe = true;
    let result = plan.apply_expressions(&mut |expression| {
        safe &= reusable_expression(expression);
        Ok(if safe {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    result.is_ok() && safe
}

fn no_dynamic_consumers(plan: &Arc<dyn ExecutionPlan>) -> bool {
    expressions_without_producers(plan) && plan.children().into_iter().all(no_dynamic_consumers)
}

/// Called only for a validated template, with recursively fresh children.
pub(super) fn fresh(
    plan: &Arc<dyn ExecutionPlan>,
    children: &[Arc<dyn ExecutionPlan>],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let kind = plan.as_ref() as &dyn Any;
    if let Some(join) = kind.downcast_ref::<HashJoinExec>() {
        // Audited builder preserves projection, null-aware/null-equality mode,
        // join type, partition mode and fetch. reset_state clears OnceAsync build
        // state, the unused filter producer and all metrics, after replacement.
        return join
            .builder()
            .with_new_children(children.to_vec())?
            .reset_state()
            .build_exec()
            .map(Some);
    }
    if let Some(sort) = kind.downcast_ref::<SortExec>() {
        let [input] = children else {
            return Err(DataFusionError::Internal(
                "fresh SortExec requires one input".into(),
            ));
        };
        // Starting from new (not clone/with_fetch on the template) makes the
        // TopK filter expression, threshold, emitter counters and metrics fresh.
        return Ok(Some(Arc::new(
            SortExec::new(sort.expr().clone(), input.clone())
                .with_preserve_partitioning(sort.preserve_partitioning())
                .with_fetch(sort.fetch()),
        )));
    }
    if kind.is::<WindowAggExec>() || kind.is::<BoundedWindowAggExec>() {
        let [input] = children else {
            return Err(DataFusionError::Internal(
                "fresh window requires one input".into(),
            ));
        };
        if input.schema() != plan.children()[0].schema() {
            return Err(DataFusionError::Internal(
                "fresh window input schema changed".into(),
            ));
        }
        // Recursive fresh() preserves the audited child operators, expressions,
        // partitioning and ordering. Their property Arcs may be newly allocated;
        // upstream has_same_children_properties tests pointer identity only and
        // therefore cannot validate this semantic-preserving reconstruction.
        // Keep preserves private can_repartition/order-mode configuration (the
        // unbounded Recompute path hardcodes can_repartition=true). Both audited
        // implementations allocate new metrics; partition evaluators and buffers
        // are allocated by execute, never stored in these immutable descriptors.
        return plan
            .clone()
            .replace_children(
                children.to_vec(),
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
            )
            .map(Some);
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, Error, Grant, Id, QueryLimits, RowFilter, Value, demo};
    use datafusion::{
        arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        common::{NullEquality, ScalarValue},
        datasource::memory::MemorySourceConfig,
        execution::TaskContext,
        logical_expr::{JoinType, WindowFrame},
        physical_expr::{
            LexOrdering, PhysicalExpr, PhysicalSortExpr,
            expressions::{CastExpr, Column, DynamicFilterPhysicalExpr, Literal},
        },
        physical_plan::{
            InputOrderMode, collect, filter::FilterExec, joins::PartitionMode,
            windows::create_udwf_window_expr,
        },
    };
    use rusqlite::types::Value as SqlValue;

    fn col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }
    fn input(values: &[Option<i64>]) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }
    fn texts(values: &[&str]) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }
    fn execute(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(collect(plan, Arc::new(TaskContext::default())))
    }
    fn scalars(batches: &[RecordBatch], column: usize) -> Vec<ScalarValue> {
        batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(move |r| ScalarValue::try_from_array(b.column(column), r).unwrap())
            })
            .collect()
    }
    fn sort(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            SortExec::new(
                LexOrdering::new(vec![PhysicalSortExpr::new_default(col("k", 0))]).unwrap(),
                input,
            )
            .with_fetch(fetch),
        )
    }
    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        key: Arc<dyn PhysicalExpr>,
        kind: JoinType,
        null_aware: bool,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(key, col("k", 0))],
                None,
                &kind,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                null_aware,
            )
            .unwrap(),
        )
    }

    #[test]
    fn finite_sort_has_fresh_threshold_metrics_and_fetch() {
        let template = sort(input(&[Some(3), Some(1), Some(2)]), Some(1));
        assert_eq!(reusable(&template), Some(true));
        let first = fresh(&template, &[input(&[Some(3), Some(1), Some(2)])])
            .unwrap()
            .unwrap();
        let second = fresh(&template, &[input(&[Some(30), Some(10), Some(20)])])
            .unwrap()
            .unwrap();
        assert!(!Arc::ptr_eq(
            &first.dynamic_expressions_produced()[0],
            &second.dynamic_expressions_produced()[0]
        ));
        assert_eq!(
            scalars(&execute(first.clone()).unwrap(), 0),
            vec![ScalarValue::Int64(Some(1))]
        );
        assert_eq!(
            scalars(&execute(second.clone()).unwrap(), 0),
            vec![ScalarValue::Int64(Some(10))]
        );
        assert_eq!(template.metrics().unwrap().iter().count(), 0);
        assert!(first.metrics().unwrap().iter().count() > 0);
        assert!(second.metrics().unwrap().iter().count() > 0);
        assert!(fresh(&template, &[]).is_err());
    }

    #[test]
    fn hash_join_rebuilds_success_and_failed_build_state_and_preserves_nulls() {
        let template = join(
            input(&[Some(1), None]),
            input(&[Some(1), None]),
            col("k", 0),
            JoinType::Inner,
            false,
        );
        assert_eq!(reusable(&template), Some(true));
        let first = fresh(
            &template,
            &[input(&[Some(1), None]), input(&[Some(1), None])],
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            execute(first)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        let second = fresh(&template, &[input(&[Some(2)]), input(&[Some(2)])])
            .unwrap()
            .unwrap();
        assert_eq!(
            scalars(&execute(second).unwrap(), 0),
            vec![ScalarValue::Int64(Some(2))]
        );
        assert_eq!(template.metrics().unwrap().iter().count(), 0);

        let cast = Arc::new(CastExpr::new(col("k", 0), DataType::Int64, None));
        let failing = join(
            texts(&["bad"]),
            input(&[Some(2)]),
            cast,
            JoinType::Inner,
            false,
        );
        assert_eq!(reusable(&failing), Some(true));
        for _ in 0..2 {
            assert!(
                execute(
                    fresh(&failing, &[texts(&["bad"]), input(&[Some(2)])])
                        .unwrap()
                        .unwrap()
                )
                .is_err()
            );
        }
        let recovered = fresh(&failing, &[texts(&["2"]), input(&[Some(2)])])
            .unwrap()
            .unwrap();
        assert_eq!(
            scalars(&execute(recovered).unwrap(), 0),
            vec![ScalarValue::Utf8(Some("2".into()))]
        );

        let anti = join(
            input(&[Some(1), Some(2)]),
            input(&[None]),
            col("k", 0),
            JoinType::LeftAnti,
            true,
        );
        let anti = fresh(&anti, &[input(&[Some(1), Some(2)]), input(&[None])])
            .unwrap()
            .unwrap();
        assert_eq!(
            execute(anti)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }

    #[test]
    fn pushed_dynamic_consumers_are_rejected_but_unused_producers_are_safe() {
        let source = input(&[Some(1)]);
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("k", 0)],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        let filter = Arc::new(FilterExec::try_new(dynamic, source.clone()).unwrap());
        assert_eq!(reusable(&sort(filter.clone(), Some(1))), Some(false));
        let joined = join(source, filter, col("k", 0), JoinType::Inner, false);
        assert_eq!(reusable(&joined), Some(false));
    }

    #[test]
    fn row_number_windows_make_fresh_evaluators_and_reject_other_functions() {
        let source = sort(input(&[Some(2), Some(1), Some(1)]), None);
        let row_number = Arc::new(datafusion::logical_expr::WindowUDF::new_from_impl(
            RowNumber::new(),
        ));
        let expr = create_udwf_window_expr(
            &row_number,
            &[],
            source.schema().as_ref(),
            "row_number".into(),
            false,
        )
        .unwrap();
        let window: Arc<dyn WindowExpr> = Arc::new(StandardWindowExpr::new(
            expr,
            &[col("k", 0)],
            &[PhysicalSortExpr::new_default(col("k", 0))],
            Arc::new(WindowFrame::new(Some(true))),
        ));
        for bounded in [false, true] {
            let template: Arc<dyn ExecutionPlan> = if bounded {
                Arc::new(
                    BoundedWindowAggExec::try_new(
                        vec![window.clone()],
                        source.clone(),
                        InputOrderMode::Sorted,
                        false,
                    )
                    .unwrap(),
                )
            } else {
                Arc::new(
                    WindowAggExec::try_new(vec![window.clone()], source.clone(), false).unwrap(),
                )
            };
            assert_eq!(reusable(&template), Some(true));
            for _ in 0..3 {
                let new = fresh(&template, std::slice::from_ref(&source))
                    .unwrap()
                    .unwrap();
                assert_eq!(new.schema(), template.schema());
                assert_eq!(
                    format!("{:?}", new.input_distribution_requirements()),
                    format!("{:?}", template.input_distribution_requirements())
                );
                assert_eq!(
                    scalars(&execute(new).unwrap(), 1),
                    vec![
                        ScalarValue::UInt64(Some(1)),
                        ScalarValue::UInt64(Some(2)),
                        ScalarValue::UInt64(Some(1))
                    ]
                );
            }
            assert_eq!(template.metrics().unwrap().iter().count(), 0);
        }
        struct Observer;
        impl datafusion::physical_plan::windows::WindowStateObserver for Observer {
            fn finalize_window_aggregate(
                &self,
                _: usize,
                _: &Arc<dyn WindowExpr>,
                _: &datafusion::physical_expr::window::PartitionKey,
                _: Vec<ScalarValue>,
            ) -> Result<()> {
                Ok(())
            }
        }
        let observed: Arc<dyn ExecutionPlan> = Arc::new(
            BoundedWindowAggExec::try_new(
                vec![window],
                source.clone(),
                InputOrderMode::Sorted,
                false,
            )
            .unwrap()
            .with_state_observer(Some(Arc::new(Observer)))
            .unwrap(),
        );
        assert_eq!(reusable(&observed), Some(false));
        let rank = datafusion::functions_window::rank::rank_udwf();
        let expr =
            create_udwf_window_expr(&rank, &[], source.schema().as_ref(), "rank".into(), false)
                .unwrap();
        let rank = Arc::new(StandardWindowExpr::new(
            expr,
            &[],
            &[],
            Arc::new(WindowFrame::new(None)),
        ));
        let rank: Arc<dyn ExecutionPlan> =
            Arc::new(WindowAggExec::try_new(vec![rank], source, false).unwrap());
        assert_eq!(reusable(&rank), Some(false));
    }

    fn fixture() -> Database {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), crate::Visibility::Private)
            .unwrap();
        tx.install_with_visibility(demo::fitness(), crate::Visibility::Private)
            .unwrap();
        tx.install_with_visibility(demo::crm(), crate::Visibility::Private)
            .unwrap();
        for i in 0..32 {
            let person = tx
                .insert_with_id(
                    "core.Person",
                    Id::from_u128(i + 1),
                    vec![Value::Text(format!("Person{i:03}"))],
                )
                .unwrap();
            for j in 0..2 {
                let workout = tx
                    .insert(
                        "fitness.Workout",
                        vec![Value::Int(((i * 7 + j * 19) % 120) as i64)],
                    )
                    .unwrap();
                tx.insert("fitness.Performed", vec![person.into(), workout.into()])
                    .unwrap();
                let deal = tx
                    .insert(
                        "crm.Deal",
                        vec![Value::Decimal(((i + 1) * 100 + j * 17) as i64)],
                    )
                    .unwrap();
                tx.insert("crm.CustomerDeal", vec![person.into(), deal.into()])
                    .unwrap();
            }
        }
        for relation in [
            "core.Person",
            "fitness.Workout",
            "fitness.Performed",
            "crm.Deal",
            "crm.CustomerDeal",
        ] {
            tx.grant("reader", relation, Grant::read_only()).unwrap();
        }
        tx.commit().unwrap();
        db
    }
    fn distinct_nodes(a: &Arc<dyn ExecutionPlan>, b: &Arc<dyn ExecutionPlan>) {
        assert!(!Arc::ptr_eq(a, b));
        assert_eq!(a.children().len(), b.children().len());
        for (a, b) in a.children().into_iter().zip(b.children()) {
            distinct_nodes(a, b);
        }
    }

    #[test]
    fn benchmark_group_and_window_retain_fresh_templates_and_preserve_authority() {
        let db = fixture();
        let snapshot = db.session("reader").snapshot();
        let sqlite = snapshot.sqlite_compat().unwrap();
        let sql = crate::datafusion_sql::DfSnapshot::new(snapshot, None).unwrap();
        let queries = [
            "SELECT p.id,COUNT(*),SUM(w.durationMinutes),SUM(d.amount) FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout JOIN crm.CustomerDeal c ON c.customer=p.id JOIN crm.Deal d ON d.id=c.deal GROUP BY p.id ORDER BY SUM(d.amount) DESC,p.id LIMIT 25",
            "SELECT person_id,link_id,workout_id,minutes FROM (SELECT p.id AS person_id,f.id AS link_id,w.id AS workout_id,w.durationMinutes AS minutes,ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY w.durationMinutes DESC,f.id) AS rn FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout WHERE w.durationMinutes>=30) ranked WHERE rn<=1 ORDER BY person_id LIMIT 100",
        ];
        let mut retained = Vec::new();
        for text in queries {
            let expected = sqlite.query(text, &[]).unwrap().rows;
            let mut query = sql.prepare(text).unwrap();
            // Exercise generic window-template isolation even when the bounded
            // partition-top probe executor also recognizes this SQL shape.
            query.partition_top = None;
            for _ in 0..3 {
                assert_eq!(
                    query.query(&[], QueryLimits::default()).unwrap().rows,
                    expected
                );
                let cached = query.cache.get(&[]).unwrap();
                let first = cached
                    .physical()
                    .unwrap()
                    .expect("benchmark must retain physical template");
                let second = cached.physical().unwrap().unwrap();
                distinct_nodes(&first, &second);
                let task = Arc::new(TaskContext::from(&sql.context));
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let (left, right) =
                        futures::join!(collect(first, task.clone()), collect(second, task.clone()));
                    assert_eq!(left.unwrap(), right.unwrap());
                    // A dropped consumer must not leave shared hash/window state
                    // that changes the next fresh execution's rows.
                    let interrupted = cached.physical().unwrap().unwrap();
                    let mut stream = interrupted.execute(0, task.clone()).unwrap();
                    use futures::StreamExt;
                    assert!(stream.next().await.transpose().unwrap().is_some());
                    drop(stream);
                    let retry = collect(cached.physical().unwrap().unwrap(), task)
                        .await
                        .unwrap();
                    assert_eq!(
                        retry.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        expected.len()
                    );
                });
            }
            assert!(
                query
                    .query(
                        &[],
                        QueryLimits {
                            max_rows: 1,
                            ..QueryLimits::default()
                        }
                    )
                    .is_err()
            );
            assert!(query.cache.get(&[]).is_none());
            assert_eq!(
                query.query(&[], QueryLimits::default()).unwrap().rows,
                expected
            );
            retained.push(query);
        }
        let parameterized = "SELECT p.name,ROW_NUMBER() OVER (ORDER BY p.name) AS rn FROM core.Person p WHERE p.name>=?1 ORDER BY p.name LIMIT 3";
        let mut parameterized_query = sql.prepare(parameterized).unwrap();
        for bound in ["Person000", "Person030", "Person000"] {
            let args = [SqlValue::Text(bound.into())];
            let expected = sqlite.query(parameterized, &args).unwrap().rows;
            for _ in 0..2 {
                assert_eq!(
                    parameterized_query
                        .query(&args, QueryLimits::default())
                        .unwrap()
                        .rows,
                    expected
                );
                assert!(
                    parameterized_query
                        .cache
                        .get(&args)
                        .unwrap()
                        .physical()
                        .unwrap()
                        .is_some()
                );
            }
        }
        let mut tx = db.admin().transaction();
        tx.grant(
            "reader",
            "core.Person",
            Grant {
                filter: RowFilter::Equals {
                    role: "name".into(),
                    value: "Person000".into(),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        for mut query in retained {
            assert!(matches!(
                query.query(&[], QueryLimits::default()),
                Err(Error::Denied)
            ));
        }
        assert!(matches!(
            parameterized_query.query(
                &[SqlValue::Text("Person000".into())],
                QueryLimits::default()
            ),
            Err(Error::Denied)
        ));
    }
}
