//! Bounded, snapshot-consistent relationship loading. One SQL statement per
//! nonempty plan node, never one statement/request per parent occurrence.
use crate::{
    service::{Contract, NamedRow, WireValue},
    *,
};
use rusqlite::types::Value as S;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Predicate {
    Compare {
        field: String,
        comparison: Comparison,
        value: WireValue,
    },
    And {
        terms: Vec<Predicate>,
    },
    Or {
        terms: Vec<Predicate>,
    },
    Not {
        term: Box<Predicate>,
    },
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparison {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Order {
    pub field: String,
    #[serde(default)]
    pub descending: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPlan {
    pub contract: Contract,
    #[serde(default)]
    pub filter: Option<Predicate>,
    #[serde(default)]
    pub order: Vec<Order>,
    pub limit: usize,
    #[serde(default)]
    pub include: BTreeMap<String, Include>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Many,
    One,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Include {
    pub direction: Direction,
    pub role: String,
    pub plan: ReadPlan,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReadStats {
    pub sql_statements: usize,
    pub fetched_rows: usize,
    pub returned_nodes: usize,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct NestedRow {
    pub row: NamedRow,
    pub related: BTreeMap<String, NestedValue>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum NestedValue {
    Many(Vec<NestedRow>),
    One(Option<Box<NestedRow>>),
}
#[derive(Debug, Serialize, Deserialize)]
pub struct NestedResult {
    pub revision: String,
    pub rows: Vec<NestedRow>,
    pub stats: ReadStats,
}
const MAX_NODES: usize = 17;
const MAX_DEPTH: usize = 4;
const MAX_ROWS: usize = 5000;
const MAX_BYTES: usize = 2 * 1024 * 1024;
fn error(s: &str) -> Error {
    Error::Query(s.into())
}
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn table(def: &Relation) -> String {
    let (m, r) = def.name.split_once('.').expect("validated name");
    format!("{}.{}", quote(m), quote(r))
}
fn field_type(def: &Relation, field: &str) -> Result<DataType> {
    if field == "id" {
        Ok(DataType::Ref(def.name.clone()))
    } else {
        Ok(def.roles[def.role(field)?].data_type.clone())
    }
}
fn scalar(v: Value) -> S {
    match v {
        Value::Bool(v) => S::Integer(i64::from(v)),
        Value::Int(v) | Value::Decimal(v) | Value::Timestamp(v) => S::Integer(v),
        Value::Ref(v) => S::Text(v.to_string()),
        Value::Text(v) => S::Text(v),
        Value::Bytes(v) => S::Blob(v),
    }
}
fn predicate(
    def: &Relation,
    p: &Predicate,
    params: &mut Vec<S>,
    depth: usize,
    budget: &mut usize,
) -> Result<String> {
    if depth > 16 || *budget == 0 {
        return Err(error("predicate complexity limit exceeded"));
    }
    *budget -= 1;
    Ok(match p {
        Predicate::Compare {
            field,
            comparison,
            value,
        } => {
            let kind = field_type(def, field)?;
            let value = value.value(&BTreeMap::new())?;
            if !kind.accepts(&value) {
                return Err(error("predicate scalar type mismatch"));
            }
            if !matches!(comparison, Comparison::Eq | Comparison::Ne)
                && matches!(kind, DataType::Bool | DataType::Ref(_) | DataType::Bytes)
            {
                return Err(error("ordered comparison requires text or numeric scalar"));
            }
            params.push(scalar(value));
            let op = match comparison {
                Comparison::Eq => "=",
                Comparison::Ne => "!=",
                Comparison::Gt => ">",
                Comparison::Ge => ">=",
                Comparison::Lt => "<",
                Comparison::Le => "<=",
            };
            format!("{} {op} ?", quote(field))
        }
        Predicate::And { terms } | Predicate::Or { terms } => {
            if terms.is_empty() {
                return Err(error("empty predicate group"));
            }
            let join = if matches!(p, Predicate::And { .. }) {
                " AND "
            } else {
                " OR "
            };
            format!(
                "({})",
                terms
                    .iter()
                    .map(|p| predicate(def, p, params, depth + 1, budget))
                    .collect::<Result<Vec<_>>>()?
                    .join(join)
            )
        }
        Predicate::Not { term } => {
            format!("(NOT {})", predicate(def, term, params, depth + 1, budget)?)
        }
    })
}
struct Prepared {
    def: Relation,
    filter: String,
    params: Vec<S>,
    order: String,
    limit: usize,
    children: BTreeMap<String, (Direction, String, Prepared)>,
}
fn prepare(
    snapshot: &Snapshot,
    plan: &ReadPlan,
    depth: usize,
    nodes: &mut usize,
    predicates: &mut usize,
) -> Result<Prepared> {
    if depth > MAX_DEPTH || *nodes >= MAX_NODES {
        return Err(error("nested plan size/depth limit exceeded"));
    }
    *nodes += 1;
    // Authorize every requested branch before executing anything, even for empty roots.
    let def = snapshot.schema(&plan.contract.relation)?;
    if def.id != plan.contract.schema_id
        || snapshot.state.relations[&def.id].fingerprint != plan.contract.fingerprint
    {
        return Err(Error::Schema(format!("contract mismatch for {}", def.name)));
    }
    if !(1..=1000).contains(&plan.limit) || plan.order.len() > 8 {
        return Err(error("invalid read limit/order size"));
    }
    let mut params = vec![];
    let filter = plan
        .filter
        .as_ref()
        .map(|p| predicate(&def, p, &mut params, 0, predicates))
        .transpose()?
        .unwrap_or_else(|| "TRUE".into());
    let mut order = vec![];
    let mut seen = BTreeSet::new();
    for o in &plan.order {
        field_type(&def, &o.field)?;
        if !seen.insert(o.field.as_str()) {
            return Err(error("duplicate order field"));
        }
        order.push(format!(
            "{} {}",
            quote(&o.field),
            if o.descending { "DESC" } else { "ASC" }
        ));
    }
    if !seen.contains("id") {
        order.push("\"id\" ASC".into());
    }
    let mut children = BTreeMap::new();
    for (name, edge) in &plan.include {
        if !crate::model::identifier(name) {
            return Err(error("invalid include name"));
        }
        let child = prepare(snapshot, &edge.plan, depth + 1, nodes, predicates)?;
        let (source, target) = if edge.direction == Direction::Many {
            (&child.def, &def)
        } else {
            (&def, &child.def)
        };
        if edge.role == "id"
            || field_type(source, &edge.role)? != DataType::Ref(target.name.clone())
        {
            return Err(error(
                "relationship role does not reference the selected family",
            ));
        }
        if edge.direction == Direction::One && child.limit != 1 {
            return Err(error("forward reference limit must be one"));
        }
        children.insert(name.clone(), (edge.direction, edge.role.clone(), child));
    }
    Ok(Prepared {
        def,
        filter,
        params,
        order: order.join(","),
        limit: plan.limit,
        children,
    })
}
struct Loaded {
    rows: Vec<Row>,
    by_id: BTreeMap<Id, usize>,
    by_parent: BTreeMap<Id, Vec<usize>>,
    children: BTreeMap<String, (Direction, String, Loaded)>,
}
struct Budget {
    rows: usize,
    bytes: usize,
}
impl Budget {
    fn charge(&mut self, row: &Row) -> Result<()> {
        let size = 16
            + row
                .values
                .iter()
                .map(|v| match v {
                    Value::Text(s) => s.len(),
                    Value::Bytes(b) => b.len(),
                    Value::Ref(_) => 16,
                    _ => 8,
                })
                .sum::<usize>();
        if self.rows == 0 || size > self.bytes {
            return Err(error("nested result budget exceeded"));
        }
        self.rows -= 1;
        self.bytes -= size;
        Ok(())
    }
}
fn decode(def: &Relation, values: Vec<S>) -> Result<Row> {
    let mut values = values.into_iter();
    let id = match values.next() {
        Some(S::Text(s)) => {
            Id(uuid::Uuid::parse_str(&s).map_err(|_| error("invalid SQL identity"))?)
        }
        _ => return Err(error("invalid SQL identity")),
    };
    let mut decoded = Vec::new();
    for (role, value) in def.roles.iter().zip(values.by_ref()) {
        decoded.push(match (&role.data_type, value) {
            (DataType::Bool, S::Integer(v @ 0..=1)) => Value::Bool(v == 1),
            (DataType::Int, S::Integer(v)) => Value::Int(v),
            (DataType::Decimal { .. }, S::Integer(v)) => Value::Decimal(v),
            (DataType::Timestamp, S::Integer(v)) => Value::Timestamp(v),
            (DataType::Text, S::Text(v)) => Value::Text(v),
            (DataType::Bytes, S::Blob(v)) => Value::Bytes(v),
            (DataType::Ref(_), S::Text(v)) => Value::Ref(Id(
                uuid::Uuid::parse_str(&v).map_err(|_| error("invalid SQL reference"))?
            )),
            _ => return Err(error("SQL result scalar mismatch")),
        });
    }
    if decoded.len() != def.roles.len() || values.next().is_some() {
        return Err(error("SQL result arity mismatch"));
    }
    Ok(Row {
        id,
        values: decoded.into_boxed_slice(),
    })
}
fn reference(row: &Row, def: &Relation, role: &str) -> Id {
    let Value::Ref(id) = row.values[def.role(role).expect("validated role")] else {
        unreachable!("validated row")
    };
    id
}
fn load(
    sql: &SqlSnapshot,
    p: &Prepared,
    restriction: Option<(&str, BTreeSet<Id>, bool)>,
    budget: &mut Budget,
    stats: &mut ReadStats,
) -> Result<Loaded> {
    let columns = std::iter::once("id")
        .chain(p.def.roles.iter().map(|r| r.name.as_str()))
        .map(quote)
        .collect::<Vec<_>>()
        .join(",");
    let mut params = vec![];
    let mut filters = vec![];
    let mut partition = None;
    let restricted = restriction.is_some();
    if let Some((field, ids, many)) = restriction {
        if ids.is_empty() {
            return Ok(Loaded {
                rows: vec![],
                by_id: BTreeMap::new(),
                by_parent: BTreeMap::new(),
                children: BTreeMap::new(),
            });
        }
        if ids.len() > MAX_ROWS {
            return Err(error("nested key budget exceeded"));
        }
        filters.push(format!(
            "{} IN ({})",
            quote(field),
            vec!["?"; ids.len()].join(",")
        ));
        params.extend(ids.into_iter().map(|id| S::Text(id.to_string())));
        if many {
            partition = Some(field);
        }
    }
    filters.push(p.filter.clone());
    params.extend(p.params.clone());
    let filters = filters.join(" AND ");
    let query = if let Some(partition) = partition {
        let mut rank = "__liquid_rank".to_string();
        while p
            .def
            .roles
            .iter()
            .any(|r| r.name.eq_ignore_ascii_case(&rank))
        {
            rank.push('_');
        }
        let rank = quote(&rank);
        params.push(S::Integer(p.limit as i64));
        format!(
            "SELECT {columns} FROM (SELECT {columns},ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) AS {rank} FROM {} WHERE {filters}) WHERE {rank}<=? ORDER BY {},{rank}",
            quote(partition),
            p.order,
            table(&p.def),
            quote(partition)
        )
    } else {
        // Forward references fetch all distinct requested IDs, not one global row.
        let limit = if restricted { MAX_ROWS } else { p.limit };
        params.push(S::Integer(limit as i64));
        format!(
            "SELECT {columns} FROM {} WHERE {filters} ORDER BY {} LIMIT ?",
            table(&p.def),
            p.order
        )
    };
    if budget.rows == 0 || budget.bytes == 0 {
        return Err(error("nested result budget exceeded"));
    }
    let output = sql.query_with_limits(
        &query,
        &params,
        QueryLimits {
            max_rows: budget.rows,
            max_bytes: budget.bytes,
            vm_steps: 1_000_000,
            timeout_ms: 5_000,
        },
    )?;
    stats.sql_statements += 1;
    let rows = output
        .rows
        .into_iter()
        .map(|v| decode(&p.def, v))
        .collect::<Result<Vec<_>>>()?;
    for row in &rows {
        budget.charge(row)?;
    }
    stats.fetched_rows += rows.len();
    let mut children = BTreeMap::new();
    if !rows.is_empty() {
        for (name, (direction, role, child)) in &p.children {
            let (field, ids) = if *direction == Direction::Many {
                (role.as_str(), rows.iter().map(|r| r.id).collect())
            } else {
                (
                    "id",
                    rows.iter().map(|r| reference(r, &p.def, role)).collect(),
                )
            };
            children.insert(
                name.clone(),
                (
                    *direction,
                    role.clone(),
                    load(
                        sql,
                        child,
                        Some((field, ids, *direction == Direction::Many)),
                        budget,
                        stats,
                    )?,
                ),
            );
        }
    }
    let by_id = rows.iter().enumerate().map(|(i, r)| (r.id, i)).collect();
    let mut by_parent: BTreeMap<Id, Vec<usize>> = BTreeMap::new();
    if let Some(role) = partition {
        for (i, row) in rows.iter().enumerate() {
            by_parent
                .entry(reference(row, &p.def, role))
                .or_default()
                .push(i);
        }
    }
    Ok(Loaded {
        rows,
        by_id,
        by_parent,
        children,
    })
}
fn assemble(
    p: &Prepared,
    loaded: &Loaded,
    row: &Row,
    budget: &mut Budget,
    stats: &mut ReadStats,
) -> Result<NestedRow> {
    budget.charge(row)?;
    stats.returned_nodes += 1;
    let mut related = BTreeMap::new();
    for (name, (direction, role, child)) in &p.children {
        let Some((_, _, data)) = loaded.children.get(name) else {
            return Err(error("missing loaded branch"));
        };
        let value = if *direction == Direction::Many {
            NestedValue::Many(
                data.by_parent
                    .get(&row.id)
                    .into_iter()
                    .flatten()
                    .map(|i| assemble(child, data, &data.rows[*i], budget, stats))
                    .collect::<Result<Vec<_>>>()?,
            )
        } else {
            let target = reference(row, &p.def, role);
            NestedValue::One(
                data.by_id
                    .get(&target)
                    .map(|i| assemble(child, data, &data.rows[*i], budget, stats).map(Box::new))
                    .transpose()?,
            )
        };
        related.insert(name.clone(), value);
    }
    let fields = p
        .def
        .roles
        .iter()
        .zip(row.values.iter())
        .map(|(role, value)| (role.name.clone(), WireValue::from(value.clone())))
        .collect();
    Ok(NestedRow {
        row: NamedRow { id: row.id, fields },
        related,
    })
}
impl Snapshot {
    /// Validate every branch up front, then execute a bounded batch of SELECTs
    /// over this one immutable data AND authorization snapshot.
    pub fn read_nested(&self, plan: &ReadPlan) -> Result<NestedResult> {
        self.check_live()?;
        let prepared = prepare(self, plan, 0, &mut 0, &mut 256)?;
        fn families(p: &Prepared, ids: &mut BTreeSet<Id>) {
            ids.insert(p.def.id);
            for (_, _, child) in p.children.values() {
                families(child, ids);
            }
        }
        let mut selected = BTreeSet::new();
        families(&prepared, &mut selected);
        let sql = self.sql_for_relations(&selected)?;
        let mut stats = ReadStats::default();
        let loaded = load(
            &sql,
            &prepared,
            None,
            &mut Budget {
                rows: MAX_ROWS,
                bytes: MAX_BYTES,
            },
            &mut stats,
        )?;
        let mut output = Budget {
            rows: MAX_ROWS,
            bytes: MAX_BYTES,
        };
        let rows = loaded
            .rows
            .iter()
            .map(|r| assemble(&prepared, &loaded, r, &mut output, &mut stats))
            .collect::<Result<Vec<_>>>()?;
        self.check_live()?;
        Ok(NestedResult {
            revision: self.revision().to_string(),
            rows,
            stats,
        })
    }
}
