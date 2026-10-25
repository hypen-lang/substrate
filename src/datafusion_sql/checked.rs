//! Replace wrapping integer operators before DataFusion's optimizer can fold them.
use datafusion::{
    arrow::{
        array::{ArrayRef, Decimal128Array, Int64Array, UInt64Array},
        datatypes::DataType,
    },
    common::{
        DataFusionError, Result, ScalarValue,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{
        Accumulator, AggregateUDF, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator,
        Volatility, create_udaf, create_udf,
        expr::{Cast, WindowFunctionDefinition},
        expr_rewriter::NamePreserver,
        utils::merge_schema,
    },
};
use std::sync::{Arc, OnceLock};

fn overflow() -> DataFusionError {
    DataFusionError::Execution("signed 64-bit SQL arithmetic overflow or division by zero".into())
}

pub(super) fn rewrite(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|plan| {
        let schema = merge_schema(&plan.inputs());
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = names.save(&expr);
            let transformed = expr.transform_up(|expr| match expr {
                Expr::BinaryExpr(binary) if matches!(binary.op,
                    Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo
                ) => {
                    let left = binary.left.get_type(&schema).ok();
                    let right = binary.right.get_type(&schema).ok();
                    if left.as_ref().is_some_and(signed) && right.as_ref().is_some_and(signed) {
                        let result_type = Expr::BinaryExpr(binary.clone()).get_type(&schema)?;
                        let expression = arithmetic(binary.op).call(vec![wide(*binary.left), wide(*binary.right)]);
                        Ok(Transformed::yes(cast(expression, result_type)))
                    } else if left.as_ref().is_some_and(unsupported_number) || right.as_ref().is_some_and(unsupported_number) {
                        Err(DataFusionError::Plan("unsigned and decimal SQL arithmetic require an explicit BIGINT or DOUBLE cast".into()))
                    } else {
                        Ok(Transformed::no(Expr::BinaryExpr(binary)))
                    }
                }
                Expr::Negative(inner) => {
                    let kind = inner.get_type(&schema).ok();
                    if kind.as_ref().is_some_and(signed) {
                        let expression = arithmetic(Operator::Minus).call(vec![
                            Expr::Literal(ScalarValue::Int64(Some(0)), None), wide(*inner)]);
                        Ok(Transformed::yes(cast(expression, kind.unwrap())))
                    } else if kind.as_ref().is_some_and(unsupported_number) {
                        Err(DataFusionError::Plan("unsigned and decimal SQL negation require an explicit BIGINT or DOUBLE cast".into()))
                    } else {
                        Ok(Transformed::no(Expr::Negative(inner)))
                    }
                }
                Expr::AggregateFunction(mut function) if function.func.name() == "sum" => {
                    let Some(argument) = function.params.args.first() else {
                        return Ok(Transformed::no(Expr::AggregateFunction(function)));
                    };
                    match argument.get_type(&schema).ok() {
                        Some(kind) if signed(&kind) => {
                            if function.params.distinct {
                                return Err(DataFusionError::Plan(
                                    "SUM DISTINCT is not yet supported by checked SQL accumulation"
                                        .into(),
                                ));
                            }
                            function.params.args[0] = wide(function.params.args[0].clone());
                            function.func = checked_sum();
                            Ok(Transformed::yes(Expr::AggregateFunction(function)))
                        }
                        Some(DataType::Float64 | DataType::Float32 | DataType::Null) | None => {
                            Ok(Transformed::no(Expr::AggregateFunction(function)))
                        }
                        _ => Err(DataFusionError::Plan(
                            "SUM requires signed 64-bit or floating-point input".into(),
                        )),
                    }
                }
                Expr::WindowFunction(mut function) if function.fun.name() == "sum" => {
                    let Some(argument) = function.params.args.first() else {
                        return Ok(Transformed::no(Expr::WindowFunction(function)));
                    };
                    match argument.get_type(&schema).ok() {
                        Some(kind) if signed(&kind) => {
                            if function.params.distinct {
                                return Err(DataFusionError::Plan(
                                    "window SUM DISTINCT is not supported".into(),
                                ));
                            }
                            function.params.args[0] = wide(function.params.args[0].clone());
                            function.fun = WindowFunctionDefinition::AggregateUDF(checked_sum());
                            Ok(Transformed::yes(Expr::WindowFunction(function)))
                        }
                        Some(DataType::Float64 | DataType::Float32 | DataType::Null) | None => {
                            Ok(Transformed::no(Expr::WindowFunction(function)))
                        }
                        _ => Err(DataFusionError::Plan(
                            "window SUM requires signed 64-bit or floating-point input".into(),
                        )),
                    }
                }
                other => Ok(Transformed::no(other)),
            })?;
            Ok(transformed.update_data(|expr| name.restore(expr)))
        })?
        .map_data(|plan| plan.recompute_schema())
    })
    .map(|plan| plan.data)
}

fn signed(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}
fn unsupported_number(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}
fn cast(expr: Expr, kind: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), kind))
}
fn wide(expr: Expr) -> Expr {
    cast(expr, DataType::Int64)
}

fn arithmetic(operator: Operator) -> datafusion::logical_expr::ScalarUDF {
    let name = match operator {
        Operator::Plus => "__liquid_add",
        Operator::Minus => "__liquid_subtract",
        Operator::Multiply => "__liquid_multiply",
        Operator::Divide => "__liquid_divide",
        _ => "__liquid_modulo",
    };
    create_udf(
        name,
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(move |args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let (Some(left), Some(right)) = (
                arrays
                    .first()
                    .and_then(|a| a.as_any().downcast_ref::<Int64Array>()),
                arrays
                    .get(1)
                    .and_then(|a| a.as_any().downcast_ref::<Int64Array>()),
            ) else {
                return Err(DataFusionError::Execution(
                    "invalid checked arithmetic arguments".into(),
                ));
            };
            let result = left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(a), Some(b)) => match operator {
                        Operator::Plus => a.checked_add(b),
                        Operator::Minus => a.checked_sub(b),
                        Operator::Multiply => a.checked_mul(b),
                        Operator::Divide => a.checked_div(b),
                        Operator::Modulo => a.checked_rem(b),
                        _ => None,
                    }
                    .map(Some)
                    .ok_or_else(overflow),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
        }),
    )
}

fn checked_sum() -> Arc<AggregateUDF> {
    static FUNCTION: OnceLock<Arc<AggregateUDF>> = OnceLock::new();
    FUNCTION
        .get_or_init(|| {
            Arc::new(create_udaf(
                "sum",
                vec![DataType::Int64],
                Arc::new(DataType::Int64),
                Volatility::Immutable,
                Arc::new(|args| {
                    if args.is_distinct {
                        return Err(DataFusionError::Plan(
                            "SUM DISTINCT is not supported".into(),
                        ));
                    }
                    Ok(Box::new(CheckedSum::default()))
                }),
                Arc::new(vec![DataType::Decimal128(38, 0), DataType::UInt64]),
            ))
        })
        .clone()
}

/// Only our static, audited factory is eligible for physical template reuse.
/// A user-defined aggregate with the same name/signature is not sufficient.
pub(super) fn reusable_sum(function: &AggregateUDF) -> bool {
    Arc::ptr_eq(function.inner(), checked_sum().inner())
}

#[derive(Default, Debug)]
struct CheckedSum {
    value: Option<i128>,
    count: u64,
}
impl CheckedSum {
    fn add(&mut self, value: i128) -> Result<()> {
        self.value = Some(
            self.value
                .unwrap_or(0)
                .checked_add(value)
                .ok_or_else(overflow)?,
        );
        Ok(())
    }
}
impl Accumulator for CheckedSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values
            .first()
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| DataFusionError::Execution("invalid checked SUM input".into()))?;
        for value in array.iter().flatten() {
            self.add(i128::from(value))?;
            self.count = self.count.checked_add(1).ok_or_else(overflow)?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(
            self.value
                .map(i64::try_from)
                .transpose()
                .map_err(|_| overflow())?,
        ))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Decimal128(self.value, 38, 0),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states
            .first()
            .and_then(|a| a.as_any().downcast_ref::<Decimal128Array>())
            .ok_or_else(|| DataFusionError::Execution("invalid checked SUM state".into()))?;
        let counts = states
            .get(1)
            .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DataFusionError::Execution("invalid checked SUM count state".into()))?;
        for (value, count) in array.iter().zip(counts.iter()) {
            if let Some(value) = value {
                self.add(value)?;
            }
            self.count = self
                .count
                .checked_add(count.unwrap_or(0))
                .ok_or_else(overflow)?;
        }
        Ok(())
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values
            .first()
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| DataFusionError::Execution("invalid checked SUM retraction".into()))?;
        for value in array.iter().flatten() {
            self.count = self.count.checked_sub(1).ok_or_else(overflow)?;
            self.value = Some(
                self.value
                    .unwrap_or(0)
                    .checked_sub(i128::from(value))
                    .ok_or_else(overflow)?,
            );
            if self.count == 0 {
                self.value = None;
            }
        }
        Ok(())
    }
}
