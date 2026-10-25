use crate::*;
use std::collections::{BTreeMap, BTreeSet};

/// Native relational algebra uses bags; distinct is explicit. No native NULL values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}
impl Snapshot {
    pub fn table(&self, relation: &str, alias: &str) -> Result<Table> {
        let def = self.schema(relation)?;
        let columns = std::iter::once(format!("{alias}.id"))
            .chain(def.roles.iter().map(|r| format!("{alias}.{}", r.name)))
            .collect();
        let rows = self
            .scan(relation)?
            .into_iter()
            .map(|r| std::iter::once(Value::Ref(r.id)).chain(r.values).collect())
            .collect();
        Ok(Table { columns, rows })
    }
}
impl Table {
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    pub fn rows(&self) -> &[Vec<Value>] {
        &self.rows
    }
    fn column(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| Error::Query(format!("unknown column {name}")))
    }
    pub fn project(&self, columns: &[&str]) -> Result<Self> {
        let indexes = columns
            .iter()
            .map(|c| self.column(c))
            .collect::<Result<Vec<_>>>()?;
        if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() {
            return Err(Error::Query("duplicate projection name".into()));
        }
        Ok(Self {
            columns: columns.iter().map(|s| (*s).into()).collect(),
            rows: self
                .rows
                .iter()
                .map(|r| indexes.iter().map(|i| r[*i].clone()).collect())
                .collect(),
        })
    }
    pub fn select_eq(&self, column: &str, value: &Value) -> Result<Self> {
        let i = self.column(column)?;
        Ok(Self {
            columns: self.columns.clone(),
            rows: self
                .rows
                .iter()
                .filter(|r| &r[i] == value)
                .cloned()
                .collect(),
        })
    }
    pub fn join(&self, other: &Self, left: &str, right: &str) -> Result<Self> {
        let (l, r) = (self.column(left)?, other.column(right)?);
        if self.columns.iter().any(|c| other.columns.contains(c)) {
            return Err(Error::Query("join aliases must be distinct".into()));
        }
        let mut index: BTreeMap<&Value, Vec<&Vec<Value>>> = BTreeMap::new();
        for row in &other.rows {
            index.entry(&row[r]).or_default().push(row);
        }
        let mut rows = vec![];
        for row in &self.rows {
            for matching in index.get(&row[l]).into_iter().flatten() {
                rows.push(row.iter().chain(matching.iter()).cloned().collect());
            }
        }
        Ok(Self {
            columns: self.columns.iter().chain(&other.columns).cloned().collect(),
            rows,
        })
    }
    pub fn distinct(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            rows: self
                .rows
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }
}
