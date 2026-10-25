//! Frozen LQ01 metadata envelope. Changing this wire contract requires a new
//! codec version and an explicit old -> new conversion, not a serde default.
//! DataType, Role, RowFilter, and Grant's serialized variants are also part of
//! that v1 contract. Their semantic validation occurs before State::apply.
use crate::{storage::Op, *};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) enum EventV1 {
    Install(ModuleV1),
    Grant(String, Id, Option<Grant>),
    SchemaGrant(String, String),
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleV1 {
    name: String,
    version: u64,
    imports: Vec<(String, u64)>,
    relations: Vec<RelationV1>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationV1 {
    // Required in storage even though authoring syntax can allocate an omitted ID.
    id: Id,
    name: String,
    roles: Vec<Role>,
    unique: Vec<Vec<String>>,
}
impl EventV1 {
    pub fn from_op(op: &Op) -> Result<Self> {
        Ok(match op {
            Op::Install(m) if m.relations.iter().all(|r| r.checks.is_empty()) => {
                Self::Install(ModuleV1 {
                    name: m.name.clone(),
                    version: m.version,
                    imports: m.imports.clone(),
                    relations: m
                        .relations
                        .iter()
                        .map(|r| RelationV1 {
                            id: r.id,
                            name: r.name.clone(),
                            roles: r.roles.clone(),
                            unique: r.unique.clone(),
                        })
                        .collect(),
                })
            }
            Op::Grant(p, t, g) => Self::Grant(p.clone(), *t, g.clone()),
            Op::SchemaGrant(p, n) => Self::SchemaGrant(p.clone(), n.clone()),
            _ => return Err(Error::Storage("not a metadata event".into())),
        })
    }
    pub fn into_op(self) -> Op {
        match self {
            Self::Install(m) => Op::Install(Module::new(
                &m.name,
                m.version,
                m.imports,
                m.relations
                    .into_iter()
                    .map(|r| Relation {
                        id: r.id,
                        name: r.name,
                        roles: r.roles,
                        unique: r.unique,
                        checks: vec![],
                    })
                    .collect(),
            )),
            Self::Grant(p, t, g) => Op::Grant(p, t, g),
            Self::SchemaGrant(p, n) => Op::SchemaGrant(p, n),
        }
    }
}

// Frozen LQ02 metadata contract. Role, DataType, Grant and Check serialized
// variants are part of this version; extending them requires a new wire version.
#[derive(Serialize, Deserialize)]
pub(crate) enum EventV2 {
    Install(ModuleV2),
    Grant(String, Id, Option<Grant>),
    SchemaGrant(String, String),
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleV2 {
    name: String,
    version: u64,
    imports: Vec<(String, u64)>,
    relations: Vec<RelationV2>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationV2 {
    id: Id,
    name: String,
    roles: Vec<Role>,
    unique: Vec<Vec<String>>,
    checks: Vec<Check>,
}
impl EventV2 {
    pub fn from_op(op: &Op) -> Result<Self> {
        Ok(match op {
            Op::Install(m) => Self::Install(ModuleV2 {
                name: m.name.clone(),
                version: m.version,
                imports: m.imports.clone(),
                relations: m
                    .relations
                    .iter()
                    .map(|r| RelationV2 {
                        id: r.id,
                        name: r.name.clone(),
                        roles: r.roles.clone(),
                        unique: r.unique.clone(),
                        checks: r.checks.clone(),
                    })
                    .collect(),
            }),
            Op::Grant(p, t, g) => Self::Grant(p.clone(), *t, g.clone()),
            Op::SchemaGrant(p, n) => Self::SchemaGrant(p.clone(), n.clone()),
            _ => return Err(Error::Storage("not a metadata event".into())),
        })
    }
    pub fn into_op(self) -> Op {
        match self {
            Self::Install(m) => Op::Install(Module::new(
                &m.name,
                m.version,
                m.imports,
                m.relations
                    .into_iter()
                    .map(|r| Relation {
                        id: r.id,
                        name: r.name,
                        roles: r.roles,
                        unique: r.unique,
                        checks: r.checks,
                    })
                    .collect(),
            )),
            Self::Grant(p, t, g) => Op::Grant(p, t, g),
            Self::SchemaGrant(p, n) => Op::SchemaGrant(p, n),
        }
    }
}
