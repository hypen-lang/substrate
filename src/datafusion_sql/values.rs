//! Shared Arrow conversion for newly declared nullable and floating columns.
//! Legacy non-nullable columns keep their existing batch construction paths.
use crate::{DataType, Value};
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray},
        datatypes::DataType as ArrowType,
    },
    common::{DataFusionError, Result},
};
use std::sync::Arc;

pub(crate) fn arrow_type(kind: &DataType) -> ArrowType {
    match kind.base_type() {
        DataType::Text | DataType::Ref(_) => ArrowType::Utf8,
        DataType::Bytes => ArrowType::Binary,
        DataType::Float => ArrowType::Float64,
        _ => ArrowType::Int64,
    }
}

pub(crate) fn extended_array<'a>(
    kind: &DataType,
    values: impl Iterator<Item = &'a Value>,
) -> Result<ArrayRef> {
    let mismatch = || DataFusionError::Execution("typed SQL source mismatch".into());
    let values = values.map(|v| {
        if kind.accepts(v) {
            Ok(v)
        } else {
            Err(mismatch())
        }
    });
    Ok(match kind.base_type() {
        DataType::Text => Arc::new(StringArray::from(
            values
                .map(|v| match v? {
                    Value::Null => Ok(None),
                    Value::Text(v) => Ok(Some(v.as_str())),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Ref(_) => Arc::new(StringArray::from(
            values
                .map(|v| match v? {
                    Value::Null => Ok(None),
                    Value::Ref(v) => Ok(Some(v.to_string())),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Bytes => Arc::new(BinaryArray::from(
            values
                .map(|v| match v? {
                    Value::Null => Ok(None),
                    Value::Bytes(v) => Ok(Some(v.as_slice())),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Float => Arc::new(Float64Array::from(
            values
                .map(|v| match v? {
                    Value::Null => Ok(None),
                    Value::Float(v) => Ok(Some(v.get())),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        _ => Arc::new(Int64Array::from(
            values
                .map(|v| match v? {
                    Value::Null => Ok(None),
                    Value::Bool(v) => Ok(Some(i64::from(*v))),
                    Value::Int(v) | Value::Timestamp(v) | Value::Decimal(v) => Ok(Some(*v)),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
    })
}
