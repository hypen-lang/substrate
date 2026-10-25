//! Validated declarations are compiled once on install/recovery. These caches are
//! derived state, never a second wire format or a source of authority.
use crate::*;
use std::{collections::BTreeMap, ops::Deref};

#[derive(Clone)]
pub(crate) struct CompiledRelation {
    pub declaration: Relation,
    pub fingerprint: String,
    offsets: BTreeMap<String, usize>,
    unique_slots: Vec<Vec<usize>>,
    checks: Vec<(usize, Check)>,
}
impl CompiledRelation {
    pub fn new(declaration: Relation) -> Self {
        let offsets: BTreeMap<_, _> = declaration
            .roles
            .iter()
            .enumerate()
            .map(|(i, r)| (r.name.clone(), i))
            .collect();
        let unique_slots = declaration
            .unique
            .iter()
            .map(|key| key.iter().map(|r| offsets[r]).collect())
            .collect();
        let checks = declaration
            .checks
            .iter()
            .map(|check| (offsets[check.role()], check.clone()))
            .collect();
        Self {
            checks,
            fingerprint: declaration.fingerprint(),
            declaration,
            offsets,
            unique_slots,
        }
    }
    pub fn check_values(&self, values: &[Value]) -> Result<()> {
        for (slot, check) in &self.checks {
            let accepted = match (check, values.get(*slot)) {
                (Check::IntRange { min, max, .. }, Some(Value::Int(value))) => {
                    value >= min && value <= max
                }
                (Check::ByteLength { min, max, .. }, Some(Value::Text(value))) => {
                    (*min as usize..=*max as usize).contains(&value.len())
                }
                (Check::ByteLength { min, max, .. }, Some(Value::Bytes(value))) => {
                    (*min as usize..=*max as usize).contains(&value.len())
                }
                _ => false,
            };
            if !accepted {
                return Err(Error::Constraint(format!(
                    "check failed for role {}",
                    check.role()
                )));
            }
        }
        Ok(())
    }
    pub fn role(&self, name: &str) -> Result<usize> {
        self.offsets
            .get(name)
            .copied()
            .ok_or_else(|| Error::Query(format!("unknown role {name}")))
    }
    pub fn unique_values(&self, row: &Row, key: usize) -> Vec<Value> {
        self.unique_slots[key]
            .iter()
            .map(|i| row.values[*i].clone())
            .collect()
    }
}
impl Deref for CompiledRelation {
    type Target = Relation;
    fn deref(&self) -> &Relation {
        &self.declaration
    }
}
#[derive(Clone)]
pub(crate) struct CompiledGrant {
    pub declaration: Grant,
    predicate: Option<(usize, Value)>,
}
impl CompiledGrant {
    pub fn new(declaration: Grant, relation: &CompiledRelation) -> Self {
        let predicate = match &declaration.filter {
            RowFilter::All => None,
            RowFilter::Equals { role, value } => Some((
                relation.role(role).expect("validated policy"),
                value.clone(),
            )),
        };
        Self {
            declaration,
            predicate,
        }
    }
    pub fn allows(&self, row: &Row) -> bool {
        self.predicate
            .as_ref()
            .is_none_or(|(slot, value)| row.values.get(*slot) == Some(value))
    }
}
impl Deref for CompiledGrant {
    type Target = Grant;
    fn deref(&self) -> &Grant {
        &self.declaration
    }
}
