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
        self.table_columns(relation, alias, None)
    }
    /// Project during authorized row traversal, avoiding copies of unselected
    /// payloads. Names use the same qualified form as `Table::project`, e.g.
    /// `snapshot.projected_table("core.Person", "p", &["p.id", "p.name"])`.
    /// The complete source row and every reference still require read access.
    pub fn projected_table(&self, relation: &str, alias: &str, columns: &[&str]) -> Result<Table> {
        self.table_columns(relation, alias, Some(columns))
    }
    fn table_columns(
        &self,
        relation: &str,
        alias: &str,
        selected: Option<&[&str]>,
    ) -> Result<Table> {
        let def = self.schema(relation)?;
        let all_columns: Vec<String> = std::iter::once(format!("{alias}.id"))
            .chain(def.roles.iter().map(|r| format!("{alias}.{}", r.name)))
            .collect();
        let (columns, indexes) = if let Some(selected) = selected {
            if selected.iter().collect::<BTreeSet<_>>().len() != selected.len() {
                return Err(Error::Query("duplicate projection name".into()));
            }
            let indexes = selected
                .iter()
                .map(|name| {
                    all_columns
                        .iter()
                        .position(|c| c == name)
                        .ok_or_else(|| Error::Query(format!("unknown column {name}")))
                })
                .collect::<Result<Vec<_>>>()?;
            (
                selected.iter().map(|name| (*name).into()).collect(),
                indexes,
            )
        } else {
            let indexes = (0..all_columns.len()).collect();
            (all_columns, indexes)
        };
        let rows = self.map_rows(relation, None, None, |row| {
            indexes
                .iter()
                .map(|index| {
                    if *index == 0 {
                        Value::Ref(row.id)
                    } else {
                        row.values[index - 1].clone()
                    }
                })
                .collect()
        })?;
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
