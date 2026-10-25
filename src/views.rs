//! Durable, read-only invoker views. Definitions are private; empty marker
//! relations reuse ordinary grant/capability identities without exposing SQL.
use crate::{
    storage::{Key, State},
    *,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock},
};

pub(crate) const TYPE_ID: Id = Id::from_u128(0x6c697175696400000000000076696577);
const FAMILY: &str = "liquid_view_catalog.Definition";
const RESULT_VIEW: &str = "__liquid_result";
const MAX_VIEWS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewSource {
    pub id: Id,
    pub name: String,
    pub fingerprint: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewDefinition {
    pub format: u32,
    /// Stable permission-marker relation identity, independent of the view name.
    pub id: Id,
    /// Canonical marker name, e.g. liquid_views.People.
    pub name: String,
    pub select: String,
    /// Explicit source contracts, sorted by canonical name. May be a superset of
    /// query dependencies, but omitted relations cannot be read by the query.
    pub sources: Vec<ViewSource>,
    /// SQL result column labels; SQL expression types are not generated Rust types.
    pub columns: Vec<String>,
}
fn module() -> &'static Module {
    static MODULE: OnceLock<Module> = OnceLock::new();
    MODULE.get_or_init(|| {
        Module::new(
            "liquid_view_catalog",
            1,
            vec![],
            vec![Relation {
                id: TYPE_ID,
                name: FAMILY.into(),
                roles: vec![
                    Role::new("view", DataType::Bytes),
                    Role::new("record", DataType::Text),
                ],
                unique: vec![vec!["view".into()]],
                checks: vec![],
            }],
        )
    })
}
pub(crate) fn compile(state: &State, row: &Row) -> Result<ViewDefinition> {
    if state
        .modules
        .get(&("liquid_view_catalog".into(), 1))
        .map(AsRef::as_ref)
        != Some(module())
    {
        return Err(Error::Denied);
    }
    let [Value::Bytes(id), Value::Text(json)] = row.values.as_ref() else {
        return Err(Error::Denied);
    };
    if json.len() > 131072 {
        return Err(Error::Denied);
    }
    let definition: ViewDefinition = serde_json::from_str(json).map_err(|_| Error::Denied)?;
    if definition.format != 1
        || id.as_slice() != definition.id.0.as_bytes()
        || definition.select.is_empty()
        || definition.select.len() > 65536
        || definition.sources.len() > 64
        || definition.columns.is_empty()
        || definition.columns.len() > 256
        || definition
            .columns
            .iter()
            .any(|c| c.is_empty() || c.len() > 256)
        || definition
            .sources
            .windows(2)
            .any(|s| s[0].name >= s[1].name)
    {
        return Err(Error::Denied);
    }
    let marker = state.relations.get(&definition.id).ok_or(Error::Denied)?;
    if marker.name != definition.name
        || !marker.name.starts_with("liquid_views.")
        || !marker.roles.is_empty()
        || !marker.unique.is_empty()
    {
        return Err(Error::Denied);
    }
    let mut ids = BTreeSet::new();
    for source in &definition.sources {
        let actual = state.relations.get(&source.id).ok_or(Error::Denied)?;
        if source.name.starts_with("liquid_")
            || !ids.insert(source.id)
            || source.name != actual.name
            || source.fingerprint != actual.fingerprint
        {
            return Err(Error::Denied);
        }
    }
    Ok(definition)
}
fn find(state: &State, id: Id) -> Result<Arc<ViewDefinition>> {
    let bucket = (TYPE_ID, 0, Value::Bytes(id.0.as_bytes().to_vec()));
    let rows = state.index.get(&bucket).ok_or(Error::Denied)?;
    if rows.len() != 1 {
        return Err(Error::Denied);
    }
    state
        .views
        .get(rows.iter().next().ok_or(Error::Denied)?)
        .cloned()
        .ok_or(Error::Denied)
}
impl Database {
    /// Host-only additive definition. Existing definitions are immutable; use a
    /// new ID/name for a replacement. No old application records are rewritten.
    pub fn define_view(
        &self,
        id: Id,
        name: &str,
        select: &str,
        sources: &[&str],
    ) -> Result<ViewDefinition> {
        if !crate::model::identifier(name)
            || name.len() > 64
            || select.is_empty()
            || select.len() > 65536
            || sources.len() > 64
        {
            return Err(Error::Schema("invalid view declaration bounds".into()));
        }
        self.admin().transact(RetryPolicy::default(), |tx| {
            tx.track_authority_key(Key::Catalog);
            tx.track_authority_key(Key::Family(TYPE_ID));
            let snapshot = tx.snapshot();
            if snapshot.state.views.len() >= MAX_VIEWS {
                return Err(Error::Constraint("view catalog quota exceeded".into()));
            }
            let mut contracts = sources
                .iter()
                .map(|name| {
                    if name.starts_with("liquid_") {
                        return Err(Error::Denied);
                    }
                    let schema = snapshot.schema(name)?;
                    Ok(ViewSource {
                        id: schema.id,
                        name: schema.name,
                        fingerprint: snapshot.state.relations[&schema.id].fingerprint.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            contracts.sort_by(|a, b| a.name.cmp(&b.name));
            if contracts.windows(2).any(|w| w[0].name == w[1].name) {
                return Err(Error::Schema("duplicate view source".into()));
            }
            let mut sql = snapshot.sql_selected(sources)?;
            // CREATE VIEW rejects parameters; prepare_one and readonly checks in
            // SqlSnapshot::view reject DML, multiple statements and forbidden SQL.
            sql.view(RESULT_VIEW, select)?;
            let columns = sql
                .query("SELECT * FROM temp.__liquid_result LIMIT 0", &[])?
                .columns;
            let version = snapshot
                .state
                .modules
                .keys()
                .filter(|(n, _)| n == "liquid_views")
                .map(|(_, v)| *v)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| Error::Schema("view version exhausted".into()))?;
            let definition = ViewDefinition {
                format: 1,
                id,
                name: format!("liquid_views.{name}"),
                select: select.into(),
                sources: contracts,
                columns,
            };
            tx.install(Module::new(
                "liquid_views",
                version,
                vec![],
                vec![Relation {
                    id,
                    name: definition.name.clone(),
                    roles: vec![],
                    unique: vec![],
                    checks: vec![],
                }],
            ))?;
            if !snapshot
                .state
                .modules
                .contains_key(&("liquid_view_catalog".into(), 1))
            {
                tx.install(module().clone())?;
            }
            let record_id = Id::new();
            let values = vec![
                Value::Bytes(id.0.as_bytes().to_vec()),
                Value::Text(serde_json::to_string(&definition).map_err(|_| Error::Denied)?),
            ];
            compile(
                &tx.snapshot().state,
                &Row {
                    id: record_id,
                    values: values.clone().into_boxed_slice(),
                },
            )?;
            tx.insert_with_id(FAMILY, record_id, values)?;
            Ok(definition)
        })
    }
    /// Host-only permission grant. It grants a view marker, not source data or SQL
    /// definition metadata; inspecting/executing still requires source permissions.
    pub fn grant_view(&self, id: Id, principal: &str) -> Result<()> {
        self.admin().transact(RetryPolicy::default(), |tx| {
            tx.track_authority_key(Key::Family(TYPE_ID));
            let definition = find(&tx.snapshot().state, id)?;
            tx.grant(principal, &definition.name, Grant::read_only())
        })
    }
    pub fn revoke_view(&self, id: Id, principal: &str) -> Result<()> {
        self.admin().transact(RetryPolicy::default(), |tx| {
            tx.track_authority_key(Key::Family(TYPE_ID));
            let definition = find(&tx.snapshot().state, id)?;
            tx.revoke(principal, &definition.name)
        })
    }
}
impl Snapshot {
    /// Inspect only when both the marker and every declared source contract are
    /// readable, including the source's reference-domain metadata.
    pub fn view_definition(&self, id: Id) -> Result<ViewDefinition> {
        self.check_live()?;
        if !self.state.can_read(self.principal.as_deref(), id) {
            return Err(Error::Denied);
        }
        let definition = find(&self.state, id)?;
        for source in &definition.sources {
            let schema = self.schema(&source.name).map_err(|_| Error::Denied)?;
            if schema.id != source.id
                || self.state.relations[&schema.id].fingerprint != source.fingerprint
            {
                return Err(Error::Denied);
            }
        }
        self.check_live()?;
        Ok((*definition).clone())
    }
    pub fn view_definitions(&self) -> Result<Vec<ViewDefinition>> {
        self.check_live()?;
        let mut definitions = self
            .state
            .views
            .values()
            .filter_map(|v| self.view_definition(v.id).ok())
            .collect::<Vec<_>>();
        definitions.sort_by(|a, b| a.name.cmp(&b.name));
        self.check_live()?;
        Ok(definitions)
    }
    /// Revalidates stored SQL in a read-only, explicitly selected invoker catalog.
    /// Source schema aliases are not resolved implicitly.
    pub fn open_view(&self, id: Id) -> Result<DurableView> {
        let definition = self.view_definition(id)?;
        let names = definition
            .sources
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>();
        let mut sql = self.sql_selected(&names)?;
        sql.view(RESULT_VIEW, &definition.select)?;
        let columns = sql
            .query("SELECT * FROM temp.__liquid_result LIMIT 0", &[])?
            .columns;
        if columns != definition.columns {
            return Err(Error::Schema("view result contract changed".into()));
        }
        self.check_live()?;
        Ok(DurableView {
            snapshot: self.clone(),
            definition,
            sql,
        })
    }
    pub fn query_view(&self, id: Id) -> Result<SqlTable> {
        self.open_view(id)?.query()
    }
}
/// Pinned data with the same live permission/revocation gates as native SQL.
pub struct DurableView {
    snapshot: Snapshot,
    definition: ViewDefinition,
    sql: SqlSnapshot,
}
impl DurableView {
    pub fn definition(&self) -> Result<&ViewDefinition> {
        self.snapshot.check_live()?;
        Ok(&self.definition)
    }
    pub fn query(&self) -> Result<SqlTable> {
        self.sql.query_with_limits(
            "SELECT * FROM temp.__liquid_result",
            &[],
            QueryLimits::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{CapabilityScope, RelationCapability};
    #[test]
    fn capability_and_credential_deadlines_fence_retained_views() {
        for credential in [false, true] {
            let db = Database::memory();
            db.clock.set(100);
            let id = Id::new();
            db.define_view(id, "Hello", "SELECT 1 AS hello", &[])
                .unwrap();
            db.grant_view(id, "owner").unwrap();
            let root = db
                .issue_capability_until(
                    Id::new(),
                    "owner",
                    CapabilityScope {
                        relations: vec![RelationCapability {
                            relation: id,
                            grant: Grant::read_only(),
                        }],
                        schema_namespace: None,
                        delegations: 0,
                    },
                    Some(200),
                )
                .unwrap();
            let session = if credential {
                let token = db.issue_credential("owner", root.id, 150).unwrap();
                db.authenticate(&token.token).unwrap()
            } else {
                db.session("owner").with_capability(root.id).unwrap()
            };
            let snapshot = session.snapshot();
            let view = snapshot.open_view(id).unwrap();
            assert_eq!(view.query().unwrap().rows.len(), 1);
            db.clock.set(if credential { 150 } else { 200 });
            assert!(view.query().is_err());
            assert!(view.definition().is_err());
            assert!(snapshot.view_definition(id).is_err());
            assert!(snapshot.view_definitions().is_err());
        }
    }
}
