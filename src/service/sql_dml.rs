//! Bounded SQL mutation compiler. DataFusion remains read-only; every accepted
//! mutation is executed by the same authorized transaction API as native writes.
use super::*;
use datafusion::sql::sqlparser::{
    ast::{
        AssignmentTarget, BinaryOperator, Expr, FromTable, ObjectName, Query, SetExpr, Statement,
        TableFactor, TableObject, TableWithJoins, UnaryOperator, Value as Literal,
    },
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use std::collections::BTreeSet;

pub(super) const FEATURES: &[&str] = &[
    "insert_values",
    "update_by_id",
    "delete_by_id",
    "atomic_100_rows",
    "bound_parameters",
];
fn error(message: impl Into<String>) -> Error {
    Error::Query(message.into())
}
fn unsupported() -> Error {
    error(
        "SQL write supports INSERT VALUES, UPDATE SET WHERE id =/IN, and DELETE WHERE id =/IN; other clauses are unsupported",
    )
}

#[derive(Clone)]
enum Atom {
    Null,
    Bool(bool),
    Number(String),
    Text(String),
    Parameter(usize),
}
impl Atom {
    fn parse(expr: &Expr) -> Result<Self> {
        match expr {
            Expr::Nested(expr) => Self::parse(expr),
            Expr::Value(value) => match &value.value {
                Literal::Null => Ok(Self::Null),
                Literal::Boolean(value) => Ok(Self::Bool(*value)),
                Literal::Number(value, false) => Ok(Self::Number(value.clone())),
                Literal::SingleQuotedString(value) => Ok(Self::Text(value.clone())),
                Literal::Placeholder(value) => Ok(Self::Parameter(
                    value
                        .strip_prefix('$')
                        .ok_or_else(unsupported)?
                        .parse::<usize>()
                        .map_err(|_| unsupported())?
                        .checked_sub(1)
                        .ok_or_else(unsupported)?,
                )),
                _ => Err(unsupported()),
            },
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => match Self::parse(expr)? {
                Self::Number(value) if !value.starts_with('-') => {
                    Ok(Self::Number(format!("-{value}")))
                }
                _ => Err(unsupported()),
            },
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr,
            } => match Self::parse(expr)? {
                value @ Self::Number(_) => Ok(value),
                _ => Err(unsupported()),
            },
            _ => Err(unsupported()),
        }
    }
    fn typed(&self, data_type: &DataType, params: &[SqlWireValue]) -> Result<Value> {
        if matches!(self, Self::Null)
            || matches!(self, Self::Parameter(i) if matches!(params.get(*i), Some(SqlWireValue::Null)))
        {
            return if data_type.is_nullable() {
                Ok(Value::Null)
            } else {
                Err(error("NULL requires a nullable role"))
            };
        }
        let parameter = if let Self::Parameter(i) = self {
            Some(
                params
                    .get(*i)
                    .ok_or_else(|| error("SQL parameter count mismatch"))?,
            )
        } else {
            None
        };
        let integer = || -> Result<i64> {
            match (self, parameter) {
                (Self::Number(value), _) | (_, Some(SqlWireValue::Integer(value))) => value
                    .parse()
                    .map_err(|_| error("expected exact signed 64-bit integer")),
                _ => Err(error("expected integer SQL value")),
            }
        };
        let text = || -> Result<String> {
            match (self, parameter) {
                (Self::Text(value), _) | (_, Some(SqlWireValue::Text(value))) => Ok(value.clone()),
                _ => Err(error("expected text SQL value")),
            }
        };
        Ok(match data_type.base_type() {
            DataType::Bool => Value::Bool(match (self, parameter) {
                (Self::Bool(value), _) => *value,
                _ => match integer()? {
                    0 => false,
                    1 => true,
                    _ => return Err(error("boolean integer must be 0 or 1")),
                },
            }),
            DataType::Int => Value::Int(integer()?),
            DataType::Decimal { .. } => Value::Decimal(integer()?),
            DataType::Timestamp => Value::Timestamp(integer()?),
            DataType::Float => {
                let number = match (self, parameter) {
                    (Self::Number(value), _) => value
                        .parse::<f64>()
                        .map_err(|_| error("invalid finite float"))?,
                    (_, Some(SqlWireValue::Real(value))) => *value,
                    (_, Some(SqlWireValue::Integer(_))) => {
                        let exact = integer()?;
                        let float = exact as f64;
                        if float as i128 != exact as i128 {
                            return Err(error("integer cannot be represented exactly as float"));
                        }
                        float
                    }
                    _ => return Err(error("expected finite numeric SQL value")),
                };
                Value::Float(FiniteFloat::new(number)?)
            }
            DataType::Text => Value::Text(text()?),
            DataType::Ref(_) => Value::Ref(Id(
                uuid::Uuid::parse_str(&text()?).map_err(|_| error("expected UUID text"))?
            )),
            DataType::Bytes => match parameter {
                Some(SqlWireValue::Blob(bytes)) => Value::Bytes(bytes.clone()),
                _ => return Err(error("bytes require a bound Blob parameter")),
            },
            DataType::Nullable(_) => return Err(error("invalid nested nullable role")),
        })
    }
    fn id(&self, params: &[SqlWireValue]) -> Result<Id> {
        match self.typed(&DataType::Ref(String::new()), params)? {
            Value::Ref(id) => Ok(id),
            _ => Err(error("expected UUID identity")),
        }
    }
}

enum Kind {
    Insert {
        columns: Vec<String>,
        rows: Vec<Vec<Atom>>,
    },
    Update {
        fields: Vec<(String, Atom)>,
        ids: Vec<Atom>,
    },
    Delete {
        ids: Vec<Atom>,
    },
}
pub(super) struct Plan {
    relation: String,
    kind: Kind,
}
enum BoundWrite {
    Insert(Vec<Fields>),
    Update(Fields, Vec<Id>),
    Delete(Vec<Id>),
}
fn name(name: &ObjectName, field: bool) -> Result<String> {
    if name.0.is_empty() || (field && name.0.len() != 1) || (!field && name.0.len() > 2) {
        return Err(unsupported());
    }
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|id| id.value.clone())
                .ok_or_else(unsupported)
        })
        .collect::<Result<Vec<_>>>()?;
    let name = parts.join(".");
    if field && name == "id" {
        return Err(error(
            "identity is generated on INSERT and immutable on UPDATE",
        ));
    }
    Ok(name)
}
fn table(table: &TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return Err(unsupported());
    }
    match &table.relation {
        TableFactor::Table {
            name: table_name,
            alias: None,
            args: None,
            with_hints,
            version: None,
            with_ordinality: false,
            partitions,
            json_path: None,
            sample: None,
            index_hints,
        } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty() => {
            name(table_name, false)
        }
        _ => Err(unsupported()),
    }
}
fn targets(selection: Option<&Expr>) -> Result<Vec<Atom>> {
    fn is_id(expr: &Expr) -> bool {
        matches!(expr,Expr::Identifier(id) if id.value=="id")
    }
    let expressions = match selection.ok_or_else(unsupported)? {
        Expr::Nested(expr) => return targets(Some(expr)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_id(left) => vec![right.as_ref()],
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if is_id(right) => vec![left.as_ref()],
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_id(expr) => list.iter().collect(),
        _ => return Err(unsupported()),
    };
    if expressions.is_empty() || expressions.len() > 100 {
        return Err(error("SQL write targets must be 1..=100 identities"));
    }
    expressions.into_iter().map(Atom::parse).collect()
}
fn plain_values(query: &Query) -> Result<&[datafusion::sql::sqlparser::ast::Parens<Vec<Expr>>]> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(unsupported());
    }
    match query.body.as_ref() {
        SetExpr::Values(values) => Ok(&values.rows),
        _ => Err(unsupported()),
    }
}
impl Plan {
    pub(super) fn parse(sql: &str, params: &[SqlWireValue]) -> Result<Self> {
        if sql.len() > 1024 * 1024 {
            return Err(error("SQL text budget exceeded"));
        }
        let mut parameter_bytes = 0usize;
        for p in params {
            match p {
                SqlWireValue::Real(v) if !v.is_finite() => {
                    return Err(error("non-finite SQL parameter"));
                }
                SqlWireValue::Integer(v) => {
                    v.parse::<i64>()
                        .map_err(|_| error("invalid signed 64-bit SQL parameter"))?;
                }
                _ => {}
            };
            parameter_bytes = parameter_bytes.saturating_add(match p {
                SqlWireValue::Text(v) | SqlWireValue::Integer(v) => v.len(),
                SqlWireValue::Blob(v) => v.len(),
                _ => 8,
            });
        }
        if parameter_bytes > 1024 * 1024 || params.len() > 1024 {
            return Err(error("SQL parameter budget exceeded"));
        }
        let dialect = GenericDialect {};
        let mut tokens = Tokenizer::new(&dialect, sql)
            .tokenize()
            .map_err(|_| unsupported())?;
        let mut count = 0;
        for token in &mut tokens {
            if let Token::Placeholder(text) = token {
                let number = if text == "?" {
                    count + 1
                } else {
                    text.strip_prefix('?')
                        .or_else(|| text.strip_prefix('$'))
                        .ok_or_else(unsupported)?
                        .parse::<usize>()
                        .map_err(|_| unsupported())?
                };
                if !(1..=1024).contains(&number) {
                    return Err(error("SQL parameter index must be 1..=1024"));
                }
                count = count.max(number);
                *text = format!("${number}");
            }
        }
        if count != params.len() {
            return Err(error("SQL parameter count mismatch"));
        }
        let normalized = tokens
            .into_iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let mut statements = Parser::parse_sql(&dialect, &normalized).map_err(|_| unsupported())?;
        if statements.len() != 1 {
            return Err(error("exactly one SQL write statement required"));
        }
        let statement = statements.pop().unwrap();
        Ok(match statement {
            Statement::Insert(insert) => {
                if !insert.optimizer_hints.is_empty()
                    || insert.or.is_some()
                    || insert.ignore
                    || insert.table_alias.is_some()
                    || insert.overwrite
                    || !insert.assignments.is_empty()
                    || insert.partitioned.is_some()
                    || !insert.after_columns.is_empty()
                    || insert.has_table_keyword
                    || insert.on.is_some()
                    || insert.returning.is_some()
                    || insert.output.is_some()
                    || insert.replace_into
                    || insert.priority.is_some()
                    || insert.insert_alias.is_some()
                    || insert.settings.is_some()
                    || insert.format_clause.is_some()
                    || insert.multi_table_insert_type.is_some()
                    || !insert.multi_table_into_clauses.is_empty()
                    || !insert.multi_table_when_clauses.is_empty()
                    || insert.multi_table_else_clause.is_some()
                {
                    return Err(unsupported());
                }
                let TableObject::TableName(table) = &insert.table else {
                    return Err(unsupported());
                };
                let columns = insert
                    .columns
                    .iter()
                    .map(|v| name(v, true))
                    .collect::<Result<Vec<_>>>()?;
                if columns.is_empty()
                    || columns.len() > 1024
                    || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
                {
                    return Err(error("INSERT requires distinct named role columns"));
                }
                let rows = plain_values(insert.source.as_ref().ok_or_else(unsupported)?)?;
                if rows.is_empty() || rows.len() > 100 {
                    return Err(error("SQL insert must contain 1..=100 rows"));
                }
                let rows = rows
                    .iter()
                    .map(|row| {
                        if row.len() != columns.len() {
                            return Err(error("INSERT column/value count mismatch"));
                        }
                        row.iter().map(Atom::parse).collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self {
                    relation: name(table, false)?,
                    kind: Kind::Insert { columns, rows },
                }
            }
            Statement::Update(update) => {
                if !update.optimizer_hints.is_empty()
                    || update.from.is_some()
                    || update.returning.is_some()
                    || update.output.is_some()
                    || update.or.is_some()
                    || !update.order_by.is_empty()
                    || update.limit.is_some()
                {
                    return Err(unsupported());
                }
                let mut fields = Vec::new();
                let mut names = BTreeSet::new();
                for assignment in update.assignments {
                    let AssignmentTarget::ColumnName(column) = assignment.target else {
                        return Err(unsupported());
                    };
                    let column = name(&column, true)?;
                    if !names.insert(column.clone()) {
                        return Err(error("duplicate UPDATE field"));
                    }
                    fields.push((column, Atom::parse(&assignment.value)?));
                }
                if fields.is_empty() || fields.len() > 1024 {
                    return Err(error("UPDATE requires named role assignments"));
                }
                Self {
                    relation: table(&update.table)?,
                    kind: Kind::Update {
                        fields,
                        ids: targets(update.selection.as_ref())?,
                    },
                }
            }
            Statement::Delete(delete) => {
                if !delete.optimizer_hints.is_empty()
                    || !delete.tables.is_empty()
                    || delete.using.is_some()
                    || delete.returning.is_some()
                    || delete.output.is_some()
                    || !delete.order_by.is_empty()
                    || delete.limit.is_some()
                {
                    return Err(unsupported());
                }
                let from = match &delete.from {
                    FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
                };
                if from.len() != 1 {
                    return Err(unsupported());
                }
                Self {
                    relation: table(&from[0])?,
                    kind: Kind::Delete {
                        ids: targets(delete.selection.as_ref())?,
                    },
                }
            }
            _ => return Err(unsupported()),
        })
    }
    fn bind(&self, schema: &Relation, params: &[SqlWireValue]) -> Result<BoundWrite> {
        // Count repeated bindings before cloning: one 1 MiB parameter repeated
        // across many rows must not expand into an unbounded transaction buffer.
        let bytes = |atom: &Atom| -> usize {
            64usize.saturating_add(match atom {
                Atom::Text(v) | Atom::Number(v) => v.len(),
                Atom::Parameter(i) => match params.get(*i) {
                    Some(SqlWireValue::Text(v) | SqlWireValue::Integer(v)) => v.len(),
                    Some(SqlWireValue::Blob(v)) => v.len(),
                    _ => 16,
                },
                _ => 16,
            })
        };
        let bound_bytes = match &self.kind {
            Kind::Insert { rows, .. } => rows
                .iter()
                .flatten()
                .fold(0usize, |n, v| n.saturating_add(bytes(v))),
            Kind::Update { fields, ids } => fields
                .iter()
                .fold(0usize, |n, (_, v)| n.saturating_add(bytes(v)))
                .saturating_mul(ids.len()),
            Kind::Delete { .. } => 0,
        };
        if bound_bytes > 4 * 1024 * 1024 {
            return Err(error("SQL write materialization budget exceeded"));
        }
        let fields = |values: Vec<(&str, &Atom)>| -> Result<Fields> {
            values
                .into_iter()
                .map(|(name, value)| {
                    let role = schema
                        .roles
                        .iter()
                        .find(|r| r.name == name)
                        .ok_or_else(|| error("unknown SQL write role"))?;
                    Ok((name.to_owned(), value.typed(&role.data_type, params)?))
                })
                .collect()
        };
        let ids = |values: &[Atom]| -> Result<Vec<Id>> {
            let mut seen = BTreeSet::new();
            let mut ids = Vec::new();
            for v in values {
                let id = v.id(params)?;
                if seen.insert(id) {
                    ids.push(id);
                }
            }
            Ok(ids)
        };
        match &self.kind {
            Kind::Insert { columns, rows } => Ok(BoundWrite::Insert(
                rows.iter()
                    .map(|row| {
                        fields(
                            columns
                                .iter()
                                .zip(row)
                                .map(|(n, v)| (n.as_str(), v))
                                .collect(),
                        )
                    })
                    .collect::<Result<_>>()?,
            )),
            Kind::Update {
                fields: values,
                ids: targets,
            } => Ok(BoundWrite::Update(
                fields(values.iter().map(|(n, v)| (n.as_str(), v)).collect())?,
                ids(targets)?,
            )),
            Kind::Delete { ids: targets } => Ok(BoundWrite::Delete(ids(targets)?)),
        }
    }
}

macro_rules! execute_body {
    ($session:expr,$plan:expr,$params:expr) => {{
        $session.transact(RetryPolicy::default(), |tx| {
            let schema = tx.writable_schema(&$plan.relation)?;
            let bound = $plan.bind(&schema, $params)?;
            let mut ids = Vec::new();
            match bound {
                BoundWrite::Insert(rows) => {
                    for fields in rows {
                        ids.push(tx.insert_named(&$plan.relation, fields)?);
                    }
                }
                BoundWrite::Update(fields, targets) => {
                    for id in targets {
                        if tx.contains_writable(&$plan.relation, id)? {
                            tx.update_named(&$plan.relation, id, fields.clone())?;
                            ids.push(id);
                        }
                    }
                }
                BoundWrite::Delete(targets) => {
                    // Select all writable roots before any cascade changes the
                    // tentative state. Counting and Restrict validation are for
                    // the whole SQL statement, independent of input ID order.
                    for id in targets {
                        if tx.contains_writable(&$plan.relation, id)? {
                            ids.push(id);
                        }
                    }
                    tx.delete_many(&$plan.relation, &ids)?;
                }
            }
            Ok(Output::Execute {
                affected_rows: ids.len(),
                ids,
            })
        })
    }};
}
pub(super) fn resident(session: &Session, sql: &str, params: &[SqlWireValue]) -> Result<Output> {
    let plan = Plan::parse(sql, params)?;
    execute_body!(session, plan, params)
}
pub(super) fn paged(
    session: &crate::paged::PagedSession,
    sql: &str,
    params: &[SqlWireValue],
) -> Result<Output> {
    let plan = Plan::parse(sql, params)?;
    execute_body!(session, plan, params)
}
