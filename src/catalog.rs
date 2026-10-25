//! Authorized discovery DTOs, deliberately separate from executable schema IR.
use crate::*;
use serde::{Deserialize, Serialize};
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequest {
    pub after: Option<Id>,
    pub expected_revision: Option<String>,
    pub relation: Option<String>,
    /// 1..=100. Defaults to 50 through the wire and Rust Default.
    #[serde(default = "default_limit")]
    pub limit: Option<usize>,
}
fn default_limit() -> Option<usize> {
    Some(50)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogPage {
    pub format: u32,
    pub revision: String,
    pub relations: Vec<CatalogRelation>,
    pub next: Option<Id>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogRelation {
    pub id: Id,
    pub name: String,
    /// Omitted for redacted definitions: a full-contract hash could be used to
    /// guess an undisclosed reference domain from otherwise known schema fields.
    pub fingerprint: Option<String>,
    pub module: CatalogModule,
    pub roles: Vec<CatalogRole>,
    pub unique: Vec<Vec<RoleId>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<Check>,
    pub operations: CatalogOperations,
    pub row_restricted: bool,
    pub lifecycle: crate::schema_control::Lifecycle,
    /// None denotes an existing grant-only schema with no visibility policy.
    pub visibility: Option<Visibility>,
    pub aliases: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogModule {
    pub name: String,
    /// Exact owning revision, not the newest installed revision in the namespace.
    pub version: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogOperations {
    pub read: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub reference: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogRole {
    pub id: RoleId,
    pub name: String,
    pub data_type: CatalogType,
    pub required: bool,
    pub equality_index: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogType {
    Bool,
    Int,
    Text,
    Timestamp,
    Bytes,
    Decimal {
        scale: u8,
    },
    /// None is an opaque reference domain; never substitute a hidden name or ID.
    Ref {
        target: Option<CatalogTarget>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogTarget {
    pub id: Id,
    pub name: String,
}

impl Snapshot {
    pub(crate) fn can_discover(&self, id: Id) -> bool {
        self.check_live().is_ok() && self.state.can_discover(self.principal.as_deref(), id)
    }

    /// Discovery confers no authority: every subsequent data operation checks its
    /// current policy. Authority changes invalidate retained snapshots.
    pub fn catalog(&self, request: &CatalogRequest) -> Result<CatalogPage> {
        self.check_live()?;
        let limit = request.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) || (request.relation.is_some() && request.after.is_some()) {
            return Err(Error::Query("invalid catalog page options".into()));
        }
        if request
            .expected_revision
            .as_ref()
            .is_some_and(|r| r != &self.revision().to_string())
        {
            return Err(Error::Conflict);
        }
        let ids: Vec<Id> = if let Some(name) = &request.relation {
            let resolved = self.resolve_schema_alias(name)?;
            let name = resolved.as_ref().unwrap_or(name);
            // Match missing and denied names even for an empty catalog.
            let id = self
                .state
                .names
                .get(name)
                .copied()
                .filter(|id| self.can_discover(*id))
                .ok_or(Error::Denied)?;
            vec![id]
        } else {
            self.state
                .relations
                .range((request.after.map_or(Unbounded, Excluded), Unbounded))
                .map(|(id, _)| *id)
                .filter(|id| self.can_discover(*id))
                .take(limit + 1)
                .collect()
        };
        let next = (ids.len() > limit).then(|| ids[limit - 1]);
        let mut relations = Vec::new();
        let mut bytes = 0usize;
        for id in ids.into_iter().take(limit) {
            let def = &self.state.relations[&id];
            let grant = self
                .principal
                .as_deref()
                .and_then(|p| self.state.grant(p, id));
            let read = self.state.can_read(self.principal.as_deref(), id);
            let (write, reference) = if self.principal.is_none() {
                (true, true)
            } else if self.principal.as_deref() == Some(crate::visibility::ANONYMOUS) {
                (false, false)
            } else {
                grant.map_or((false, false), |g| (g.write, g.reference))
            };
            let (module, version) = &self.state.origins[&id];
            let roles: Vec<CatalogRole> = def
                .roles
                .iter()
                .enumerate()
                .map(|(slot, role)| {
                    let data_type = match &role.data_type {
                        DataType::Bool => CatalogType::Bool,
                        DataType::Int => CatalogType::Int,
                        DataType::Text => CatalogType::Text,
                        DataType::Timestamp => CatalogType::Timestamp,
                        DataType::Bytes => CatalogType::Bytes,
                        DataType::Decimal { scale } => CatalogType::Decimal { scale: *scale },
                        DataType::Ref(name) => CatalogType::Ref {
                            target: self
                                .state
                                .names
                                .get(name)
                                .filter(|id| self.can_discover(**id))
                                .map(|id| CatalogTarget {
                                    id: *id,
                                    name: name.clone(),
                                }),
                        },
                    };
                    CatalogRole {
                        id: RoleId {
                            relation: id,
                            slot: slot as u32,
                        },
                        name: role.name.clone(),
                        data_type,
                        required: true,
                        equality_index: true,
                    }
                })
                .collect();
            let complete = roles
                .iter()
                .all(|r| !matches!(r.data_type, CatalogType::Ref { target: None }));
            let relation = CatalogRelation {
                checks: def.checks.clone(),
                id,
                name: def.name.clone(),
                fingerprint: complete.then(|| def.fingerprint.clone()),
                module: CatalogModule {
                    name: module.clone(),
                    version: version.to_string(),
                },
                roles,
                unique: def
                    .unique
                    .iter()
                    .map(|key| key.iter().map(|name| def.role_id(name)).collect())
                    .collect::<Result<_>>()?,
                operations: CatalogOperations {
                    read,
                    insert: write,
                    update: write,
                    delete: write,
                    reference,
                },
                row_restricted: !self
                    .principal
                    .as_deref()
                    .is_some_and(|p| self.state.ambient_read(p, id))
                    && grant.is_some_and(|g| !matches!(g.filter, RowFilter::All)),
                visibility: self.state.schema_visibility.get(&id).map(|p| p.visibility),
                lifecycle: self.schema_lifecycle(&def.name)?,
                aliases: self.schema_aliases(&def.name)?,
            };
            // Keep direct API use bounded as well as the host's response frame.
            bytes += serde_json::to_vec(&relation)
                .map_err(|e| Error::Query(e.to_string()))?
                .len();
            if bytes > 1024 * 1024 {
                return Err(Error::Query(
                    "catalog page exceeds 1 MiB; request fewer relations".into(),
                ));
            }
            relations.push(relation);
        }
        self.check_live()?;
        Ok(CatalogPage {
            format: 1,
            revision: self.revision().to_string(),
            relations,
            next,
        })
    }
}
