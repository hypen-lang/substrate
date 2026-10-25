//! Principal-bound, attenuating capabilities. Canonical records are durable typed
//! rows; decoded scopes and role predicates are compiled on publication/recovery.
use crate::{
    compiled::CompiledGrant,
    storage::{Key, State},
    *,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub(crate) const FAMILY: &str = "liquid_security.Capability";
pub(crate) const TYPE_ID: Id = Id(uuid::Uuid::from_u128(0x6c697175696400000000000000736563));

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationCapability {
    pub relation: Id,
    #[serde(with = "grant_wire")]
    pub grant: Grant,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityScope {
    pub relations: Vec<RelationCapability>,
    /// Proposal/read/cancel/apply and schema visibility authority in ONE host-owned namespace. Does not
    /// confer data grants, provisioning, approval, aliases or lifecycle authority.
    pub schema_namespace: Option<String>,
    /// Remaining delegation depth (0 = cannot delegate), at most eight.
    pub delegations: u8,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegatedCapability {
    pub format: u32,
    pub id: Id,
    pub subject: String,
    pub issuer: String,
    pub authority: String,
    pub parent: Option<Id>,
    pub scope: CapabilityScope,
    pub namespace_generation: Option<String>,
    /// Exclusive UTC Unix-second deadline. V1 records have no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}
#[derive(Clone)]
pub(crate) struct CompiledCapability {
    pub record: DelegatedCapability,
    pub grants: BTreeMap<Id, CompiledGrant>,
    /// Ready-to-use scoped policy root. Snapshots clone this persistent map
    /// instead of allocating every grant again on every read.
    pub scoped_grants: im::OrdMap<(String, Id), CompiledGrant>,
    pub revoked: bool,
}
pub(crate) fn module() -> &'static Module {
    static MODULE: OnceLock<Module> = OnceLock::new();
    MODULE.get_or_init(|| {
        Module::new(
            "liquid_security",
            1,
            vec![],
            vec![Relation {
                id: TYPE_ID,
                name: FAMILY.into(),
                roles: vec![
                    Role::new("record", DataType::Text),
                    Role::new("revoked", DataType::Bool),
                ],
                unique: vec![],
                checks: vec![],
            }],
        )
    })
}
fn initialized(state: &State) -> Result<bool> {
    match state.modules.get(&("liquid_security".into(), 1)) {
        None => Ok(false),
        Some(m) if m.as_ref() == module() => Ok(true),
        _ => Err(Error::Storage("invalid capability catalog".into())),
    }
}
fn attenuates(child: &Grant, parent: &Grant) -> bool {
    (!child.read || parent.read)
        && (!child.write || parent.write)
        && (!child.reference || parent.reference)
        && (matches!(parent.filter, RowFilter::All) || child.filter == parent.filter)
}
fn normalize(mut scope: CapabilityScope) -> Result<CapabilityScope> {
    if scope.delegations > 8
        || scope.relations.len() > 64
        || (scope.relations.is_empty() && scope.schema_namespace.is_none())
        || scope
            .schema_namespace
            .as_ref()
            .is_some_and(|n| n.len() > 64 || !crate::model::identifier(n))
    {
        return Err(Error::Denied);
    }
    scope.relations.sort_by_key(|r| r.relation);
    if scope
        .relations
        .windows(2)
        .any(|w| w[0].relation == w[1].relation)
        || scope
            .relations
            .iter()
            .any(|r| !(r.grant.read || r.grant.write || r.grant.reference))
    {
        return Err(Error::Denied);
    }
    Ok(scope)
}
pub(crate) fn compile(state: &State, row: &Row) -> Result<CompiledCapability> {
    if !initialized(state)? {
        return Err(Error::Denied);
    }
    let [Value::Text(json), Value::Bool(revoked)] = row.values.as_ref() else {
        return Err(Error::Denied);
    };
    if json.len() > 65536 {
        return Err(Error::Denied);
    }
    let record: DelegatedCapability = serde_json::from_str(json).map_err(|_| Error::Denied)?;
    if !matches!(record.format, 1 | 2)
        || (record.format == 1 && record.expires_at.is_some())
        || record
            .expires_at
            .is_some_and(|t| !crate::auth::valid_deadline(t))
        || record.id != row.id
        || normalize(record.scope.clone())? != record.scope
        || [&record.subject, &record.issuer, &record.authority]
            .iter()
            .any(|p| p.is_empty() || p.len() > 128)
        || record.scope.schema_namespace.is_some() != record.namespace_generation.is_some()
    {
        return Err(Error::Denied);
    }
    let mut grants = BTreeMap::new();
    for r in &record.scope.relations {
        let def = state.relations.get(&r.relation).ok_or(Error::Denied)?;
        if def.name.starts_with("liquid_security.")
            || def.name.starts_with("liquid_control.")
            || def.name.starts_with("liquid_auth.")
            || def.name.starts_with("liquid_view_catalog.")
            || def.name.starts_with("liquid_visibility.")
        {
            return Err(Error::Denied);
        }
        if let RowFilter::Equals { role, value } = &r.grant.filter {
            let slot = def.role(role).map_err(|_| Error::Denied)?;
            if !def.roles[slot].data_type.accepts(value) {
                return Err(Error::Denied);
            }
        }
        grants.insert(r.relation, CompiledGrant::new(r.grant.clone(), def));
    }
    let scoped_grants = grants
        .iter()
        .map(|(id, grant)| ((record.subject.clone(), *id), grant.clone()))
        .collect();
    Ok(CompiledCapability {
        record,
        grants,
        scoped_grants,
        revoked: *revoked,
    })
}
fn narrower(child: &CapabilityScope, parent: &CapabilityScope) -> bool {
    child.delegations < parent.delegations
        && (child.schema_namespace.is_none() || child.schema_namespace == parent.schema_namespace)
        && child.relations.iter().all(|c| {
            parent
                .relations
                .iter()
                .any(|p| c.relation == p.relation && attenuates(&c.grant, &p.grant))
        })
}
/// Resolve one selected chain. Never union it with ambient principal grants.
pub(crate) fn resolve(
    state: &State,
    subject: &str,
    id: Id,
    now: u64,
) -> Result<(Arc<CompiledCapability>, Vec<Key>)> {
    if !initialized(state)? {
        return Err(Error::Denied);
    }
    let leaf = state.capabilities.get(&id).cloned().ok_or(Error::Denied)?;
    if leaf.record.subject != subject {
        return Err(Error::Denied);
    }
    let mut node = leaf.clone();
    let mut dependencies = vec![];
    for _ in 0..=8 {
        if node.revoked || node.record.expires_at.is_some_and(|end| now >= end) {
            return Err(Error::Denied);
        }
        dependencies.push(Key::Row(TYPE_ID, node.record.id));
        if let Some(parent_id) = node.record.parent {
            let parent = state
                .capabilities
                .get(&parent_id)
                .cloned()
                .ok_or(Error::Denied)?;
            if !crate::auth::deadline_within(node.record.expires_at, parent.record.expires_at)
                || node.record.authority != parent.record.authority
                || node.record.issuer != parent.record.subject
                || (node.record.scope.schema_namespace.is_some()
                    && node.record.namespace_generation != parent.record.namespace_generation)
                || !narrower(&node.record.scope, &parent.record.scope)
            {
                return Err(Error::Denied);
            }
            node = parent;
        } else {
            if node.record.authority != node.record.subject
                || node.record.issuer != node.record.subject
            {
                return Err(Error::Denied);
            }
            for r in &node.record.scope.relations {
                if !state
                    .grant(&node.record.authority, r.relation)
                    .is_some_and(|g| attenuates(&r.grant, g))
                {
                    return Err(Error::Denied);
                }
            }
            if let Some(namespace) = &node.record.scope.schema_namespace {
                let (generation, key) = crate::schema_control::delegation_namespace(
                    state,
                    &node.record.authority,
                    namespace,
                )?;
                if Some(&generation) != node.record.namespace_generation.as_ref() {
                    return Err(Error::Denied);
                }
                dependencies.push(key);
            }
            return Ok((leaf, dependencies));
        }
    }
    Err(Error::Denied)
}
impl Transaction {
    fn store_capability(&mut self, record: DelegatedCapability) -> Result<DelegatedCapability> {
        let snapshot = self.snapshot();
        if !initialized(&snapshot.state)? {
            self.install(module().clone())?;
        }
        if self.count_internal_family(FAMILY)? >= 8192
            || self
                .snapshot()
                .state
                .capabilities
                .values()
                .filter(|c| c.record.issuer == record.issuer)
                .count()
                >= 256
        {
            return Err(Error::Constraint("capability ledger quota exceeded".into()));
        }
        let row = Row {
            id: record.id,
            values: vec![
                Value::Text(serde_json::to_string(&record).map_err(|_| Error::Denied)?),
                Value::Bool(false),
            ]
            .into_boxed_slice(),
        };
        compile(&self.snapshot().state, &row)?;
        self.insert_with_id(FAMILY, row.id, row.values.to_vec())?;
        Ok(record)
    }
    pub(crate) fn invalidate_capability_roots(
        &mut self,
        principal: &str,
        relation: Id,
    ) -> Result<()> {
        self.track_authority_key(Key::Family(TYPE_ID));
        let ids = self
            .snapshot()
            .state
            .capability_roots
            .get(&(principal.into(), relation))
            .cloned()
            .unwrap_or_default();
        for id in ids {
            self.revoke_capability_row(id)?;
        }
        Ok(())
    }
    fn revoke_capability_row(&mut self, id: Id) -> Result<()> {
        let row = self.get(FAMILY, id)?.ok_or(Error::Denied)?;
        let mut values = row.values.to_vec();
        values[1] = Value::Bool(true);
        self.update(FAMILY, id, values)
    }
}
impl Database {
    /// Host-only root issuance. Root scope must fit the owner's current authority.
    pub fn issue_capability(
        &self,
        id: Id,
        owner: &str,
        scope: CapabilityScope,
    ) -> Result<DelegatedCapability> {
        self.issue_capability_until(id, owner, scope, None)
    }
    /// Host issuance with an optional exclusive Unix-second deadline.
    pub fn issue_capability_until(
        &self,
        id: Id,
        owner: &str,
        scope: CapabilityScope,
        expires_at: Option<u64>,
    ) -> Result<DelegatedCapability> {
        crate::auth::check_deadline(expires_at, self.clock.now())?;
        let scope = normalize(scope)?;
        self.admin().transact(RetryPolicy::default(), |tx| {
            tx.expire_at(expires_at);
            let snapshot = tx.schema_control_snapshot(owner)?;
            for r in &scope.relations {
                if !snapshot
                    .state
                    .grant(owner, r.relation)
                    .is_some_and(|g| attenuates(&r.grant, g))
                {
                    return Err(Error::Denied);
                }
            }
            let namespace_generation = scope
                .schema_namespace
                .as_ref()
                .map(|n| {
                    let (generation, key) =
                        crate::schema_control::delegation_namespace(&snapshot.state, owner, n)?;
                    tx.track_authority_key(key);
                    Ok::<_, Error>(generation)
                })
                .transpose()?;
            tx.store_capability(DelegatedCapability {
                format: 2,
                id,
                subject: owner.into(),
                issuer: owner.into(),
                authority: owner.into(),
                parent: None,
                scope: scope.clone(),
                namespace_generation,
                expires_at,
            })
        })
    }
    pub fn revoke_capability(&self, id: Id) -> Result<()> {
        self.admin()
            .transact(RetryPolicy::default(), |tx| tx.revoke_capability_row(id))
    }
}
impl Session {
    pub fn with_capability(&self, id: Id) -> Result<Self> {
        // A restricted session cannot shed its selected authority or switch chains.
        if self.capability.is_some() && self.capability != Some(id) {
            return Err(Error::Denied);
        }
        self.snapshot().check_live()?;
        let subject = self.principal.as_deref().ok_or(Error::Denied)?;
        self.db.check_capability(subject, id)?;
        let mut selected = self.clone();
        selected.select_capability(id);
        Ok(selected)
    }
    pub fn delegate_capability(
        &self,
        parent: Id,
        id: Id,
        subject: &str,
        scope: CapabilityScope,
    ) -> Result<DelegatedCapability> {
        self.delegate_capability_until(parent, id, subject, scope, None)
    }
    /// None inherits the parent's deadline; explicit deadlines may only shorten it.
    pub fn delegate_capability_until(
        &self,
        parent: Id,
        id: Id,
        subject: &str,
        scope: CapabilityScope,
        expires_at: Option<u64>,
    ) -> Result<DelegatedCapability> {
        let actor = self.principal.as_deref().ok_or(Error::Denied)?;
        if self.capability.is_some_and(|selected| selected != parent) {
            return Err(Error::Denied);
        }
        let scope = normalize(scope)?;
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            tx.bind_session(self)?;
            let snapshot = tx.snapshot();
            let (cap, keys) = resolve(&snapshot.state, actor, parent, self.db.clock.now())?;
            let expires_at = expires_at.or(cap.record.expires_at);
            crate::auth::check_deadline(expires_at, self.db.clock.now())?;
            if !crate::auth::deadline_within(expires_at, cap.record.expires_at) {
                return Err(Error::Denied);
            }
            tx.expire_at(expires_at);
            for key in keys {
                tx.track_authority_key(key);
            }
            if !narrower(&scope, &cap.record.scope) {
                return Err(Error::Denied);
            }
            tx.store_capability(DelegatedCapability {
                format: 2,
                id,
                subject: subject.into(),
                issuer: actor.into(),
                authority: cap.record.authority.clone(),
                parent: Some(parent),
                expires_at,
                namespace_generation: scope
                    .schema_namespace
                    .as_ref()
                    .and(cap.record.namespace_generation.clone()),
                scope: scope.clone(),
            })
        })
    }
    pub fn revoke_capability(&self, id: Id) -> Result<()> {
        let actor = self.principal.as_deref().ok_or(Error::Denied)?;
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            tx.bind_session(self)?;
            let snapshot = tx.snapshot();
            let cap = snapshot.state.capabilities.get(&id).ok_or(Error::Denied)?;
            if let Some(selected) = self.capability {
                let (_, keys) = resolve(&snapshot.state, actor, selected, self.db.clock.now())?;
                for key in keys {
                    tx.track_authority_key(key);
                }
                if id != selected && cap.record.parent != Some(selected) {
                    return Err(Error::Denied);
                }
            }
            if actor != cap.record.issuer
                && actor != cap.record.subject
                && (self.capability.is_some() || actor != cap.record.authority)
            {
                return Err(Error::Denied);
            }
            tx.revoke_capability_row(id)
        })
    }
}

// Capability declarations use the application's exact scalar wire codec too.
// In particular, JS must not round an i64 equality policy before it reaches Rust.
mod grant_wire {
    use super::*;
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct WireGrant {
        read: bool,
        write: bool,
        reference: bool,
        filter: Filter,
    }
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    enum Filter {
        All,
        Equals {
            role: String,
            value: crate::service::WireValue,
        },
    }
    pub fn serialize<S: serde::Serializer>(
        g: &Grant,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let filter = match &g.filter {
            RowFilter::All => Filter::All,
            RowFilter::Equals { role, value } => Filter::Equals {
                role: role.clone(),
                value: value.clone().into(),
            },
        };
        WireGrant {
            read: g.read,
            write: g.write,
            reference: g.reference,
            filter,
        }
        .serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Grant, D::Error> {
        let g = WireGrant::deserialize(d)?;
        let filter = match g.filter {
            Filter::All => RowFilter::All,
            Filter::Equals { role, value } => RowFilter::Equals {
                role,
                value: value
                    .value(&BTreeMap::new())
                    .map_err(serde::de::Error::custom)?,
            },
        };
        Ok(Grant {
            read: g.read,
            write: g.write,
            reference: g.reference,
            filter,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn revocation_after_schema_validation_before_commit_prevents_atomic_install() {
        use crate::schema_control::*;
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.grant("owner", "core.Person", Grant::read_write())
            .unwrap();
        tx.commit().unwrap();
        db.configure_namespace(NamespacePolicy {
            namespace: "app".into(),
            owner: "owner".into(),
            enabled: true,
            auto_approve: true,
            quotas: SchemaQuotas::default(),
        })
        .unwrap();
        let scope = CapabilityScope {
            relations: vec![],
            schema_namespace: Some("app".into()),
            delegations: 1,
        };
        let root = db
            .issue_capability(Id::new(), "owner", scope.clone())
            .unwrap();
        let child = db
            .session("owner")
            .delegate_capability(
                root.id,
                Id::new(),
                "builder",
                CapabilityScope {
                    delegations: 0,
                    ..scope
                },
            )
            .unwrap();
        let builder = db.session("builder").with_capability(child.id).unwrap();
        let mut staged = db.admin().transaction();
        staged.bind_schema_session(&builder, Some("app")).unwrap();
        staged
            .install(Module::new(
                "app",
                1,
                vec![],
                vec![Relation::new("app.Note", vec![])],
            ))
            .unwrap();
        staged
            .grant("owner", "app.Note", Grant::read_write())
            .unwrap();
        db.revoke_capability(root.id).unwrap();
        assert!(staged.commit().is_err());
        assert!(db.admin().snapshot().schema("app.Note").is_err());
    }
    #[test]
    fn root_issuance_racing_grant_revocation_cannot_escape_invalidation() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.grant("owner", "core.Person", Grant::read_write())
            .unwrap();
        tx.commit().unwrap();
        let tid = db.admin().snapshot().schema("core.Person").unwrap().id;
        // Start revocation while no capability table exists. New root publication
        // touches the same family dependency, forcing the stale revoker to retry.
        let mut stale = db.admin().transaction();
        stale.revoke("owner", "core.Person").unwrap();
        let root = db
            .issue_capability(
                Id::new(),
                "owner",
                CapabilityScope {
                    relations: vec![RelationCapability {
                        relation: tid,
                        grant: Grant::read_only(),
                    }],
                    schema_namespace: None,
                    delegations: 1,
                },
            )
            .unwrap();
        assert!(matches!(stale.commit(), Err(Error::Conflict)));
        let mut revoke = db.admin().transaction();
        revoke.revoke("owner", "core.Person").unwrap();
        revoke.commit().unwrap();
        let mut grant = db.admin().transaction();
        grant
            .grant("owner", "core.Person", Grant::read_write())
            .unwrap();
        grant.commit().unwrap();
        assert!(db.session("owner").with_capability(root.id).is_err());
    }
}
