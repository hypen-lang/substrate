//! Durable, host-provisioned schema control. Private typed relations keep the
//! existing log/checkpoint ABI. Only authenticated Session methods reach this API.
use crate::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const NS: &str = "liquid_control";
const POLICY: &str = "liquid_control.Namespace";
const PROPOSAL: &str = "liquid_control.Proposal";
const LIFE: &str = "liquid_control.Lifecycle";
const ALIAS: &str = "liquid_control.Alias";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SchemaQuotas {
    pub relations: u32,
    pub roles: u32,
    pub versions: u32,
    pub schema_bytes: u32,
    /// Lifetime ledger entries, including rejected/cancelled proposals.
    pub proposals: u32,
}
impl Default for SchemaQuotas {
    fn default() -> Self {
        Self {
            relations: 64,
            roles: 512,
            versions: 32,
            schema_bytes: 262144,
            proposals: 128,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespacePolicy {
    pub namespace: String,
    pub owner: String,
    pub auto_approve: bool,
    pub enabled: bool,
    pub quotas: SchemaQuotas,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaUsage {
    pub relations: u32,
    pub roles: u32,
    pub versions: u32,
    pub schema_bytes: u32,
    pub proposals: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamespaceInfo {
    pub policy: NamespacePolicy,
    pub generation: String,
    pub latest_version: String,
    pub usage: SchemaUsage,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProposalStatus {
    Pending,
    Approved,
    Installed,
    Rejected,
    Cancelled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FamilyCost {
    pub relation: String,
    pub role_index_entries_per_row: usize,
    pub reference_index_entries_per_row: usize,
    pub unique_index_entries_per_row: usize,
    /// Minimum packed Put event bytes. Excludes Identity, transaction framing,
    /// metadata, tree overhead, checksums and variable text/blob payload bytes.
    pub minimum_packed_put_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceRequirement {
    pub relation: String,
    pub schema_id: Id,
    pub fingerprint: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProposalPlan {
    pub format: u32,
    pub digest: String,
    pub policy_generation: String,
    #[serde(deserialize_with = "explicit_module")]
    pub module: Module,
    /// Format 1 proposals predate visibility and retain grant-only/private semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<BTreeMap<String, Visibility>>,
    pub usage_after: SchemaUsage,
    pub costs: Vec<FamilyCost>,
    pub references: Vec<ReferenceRequirement>,
    pub existing_record_rewrites: usize,
    pub grants_to_owner: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaProposal {
    pub id: Id,
    pub owner: String,
    pub status: ProposalStatus,
    pub plan: ProposalPlan,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    #[default]
    Active,
    Deprecated,
    Retired,
}

fn invalid(s: &str) -> Error {
    Error::Schema(s.into())
}
fn json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| invalid(&e.to_string()))
}
fn text(row: &Row, i: usize) -> &str {
    let Value::Text(s) = &row.values[i] else {
        unreachable!("validated control schema")
    };
    s
}
fn number(row: &Row, i: usize) -> i64 {
    let Value::Int(n) = row.values[i] else {
        unreachable!("validated control schema")
    };
    n
}
fn boolean(row: &Row, i: usize) -> bool {
    let Value::Bool(b) = row.values[i] else {
        unreachable!("validated control schema")
    };
    b
}
pub(crate) fn control_module() -> &'static Module {
    static MODULE: std::sync::OnceLock<Module> = std::sync::OnceLock::new();
    MODULE.get_or_init(|| {
        let specs = [
            (
                POLICY,
                vec![
                    ("namespace", DataType::Text),
                    ("owner", DataType::Text),
                    ("autoApprove", DataType::Bool),
                    ("enabled", DataType::Bool),
                    ("relations", DataType::Int),
                    ("roles", DataType::Int),
                    ("versions", DataType::Int),
                    ("schemaBytes", DataType::Int),
                    ("proposals", DataType::Int),
                    ("generation", DataType::Int),
                ],
                vec![vec!["namespace"]],
            ),
            (
                PROPOSAL,
                vec![
                    ("namespace", DataType::Text),
                    ("owner", DataType::Text),
                    ("plan", DataType::Text),
                    ("status", DataType::Text),
                ],
                vec![],
            ),
            (
                LIFE,
                vec![("relation", DataType::Text), ("state", DataType::Text)],
                vec![vec!["relation"]],
            ),
            (
                ALIAS,
                vec![
                    ("namespace", DataType::Text),
                    ("alias", DataType::Text),
                    ("target", DataType::Text),
                ],
                vec![vec!["alias"]],
            ),
        ];
        Module::new(
            NS,
            1,
            vec![],
            specs
                .into_iter()
                .enumerate()
                .map(|(i, (name, roles, keys))| {
                    let mut r = Relation::new(
                        name,
                        roles.into_iter().map(|(n, t)| Role::new(n, t)).collect(),
                    );
                    r.id = Id::from_u128(0x6c69717569645f636f6e74726f6c0000 + i as u128);
                    r.unique = keys
                        .into_iter()
                        .map(|k| k.into_iter().map(str::to_owned).collect())
                        .collect();
                    r
                })
                .collect(),
        )
    })
}

pub(crate) fn reserved_schema_id(id: Id) -> bool {
    id == crate::views::TYPE_ID
        || id == crate::auth::TYPE_ID
        || id == crate::capability::TYPE_ID
        || control_module().relations.iter().any(|r| r.id == id)
}
fn initialized(snapshot: &Snapshot) -> Result<bool> {
    let module = control_module();
    match snapshot.state.modules.get(&(NS.into(), 1)) {
        None => Ok(false),
        Some(actual) if actual.as_ref() == module => Ok(true),
        _ => Err(Error::Storage("incompatible schema control catalog".into())),
    }
}
pub(crate) fn delegation_namespace(
    state: &crate::storage::State,
    owner: &str,
    namespace: &str,
) -> Result<(String, crate::storage::Key)> {
    if state.modules.get(&(NS.into(), 1)).map(|m| m.as_ref()) != Some(control_module()) {
        return Err(Error::Denied);
    }
    let tid = state.names[POLICY];
    let slot = state.relations[&tid].role("namespace")?;
    let ids = state
        .index
        .get(&(tid, slot, namespace.into()))
        .ok_or(Error::Denied)?;
    let id = ids.iter().next().ok_or(Error::Denied)?;
    let row = state.row(tid, *id).ok_or(Error::Denied)?;
    let info = policy_from(row)?;
    if info.policy.owner != owner || !info.policy.enabled {
        return Err(Error::Denied);
    }
    Ok((info.generation, crate::storage::Key::Row(tid, *id)))
}
fn policy_from(row: &Row) -> Result<NamespaceInfo> {
    let n = |i| u32::try_from(number(row, i)).map_err(|_| invalid("invalid stored quota"));
    Ok(NamespaceInfo {
        policy: NamespacePolicy {
            namespace: text(row, 0).into(),
            owner: text(row, 1).into(),
            auto_approve: boolean(row, 2),
            enabled: boolean(row, 3),
            quotas: SchemaQuotas {
                relations: n(4)?,
                roles: n(5)?,
                versions: n(6)?,
                schema_bytes: n(7)?,
                proposals: n(8)?,
            },
        },
        generation: number(row, 9).to_string(),
        latest_version: "0".into(),
        usage: SchemaUsage {
            relations: 0,
            roles: 0,
            versions: 0,
            schema_bytes: 0,
            proposals: 0,
        },
    })
}
fn policy_row(tx: &mut Transaction, namespace: &str) -> Result<Row> {
    if !initialized(&tx.snapshot())? {
        return Err(Error::Denied);
    }
    tx.lookup(POLICY, "namespace", &namespace.into())?
        .into_iter()
        .next()
        .ok_or(Error::Denied)
}
fn owned_policy(
    tx: &mut Transaction,
    principal: &str,
    namespace: &str,
) -> Result<(Row, NamespaceInfo)> {
    let row = policy_row(tx, namespace)?;
    let policy = policy_from(&row)?;
    if policy.policy.owner != principal {
        return Err(Error::Denied);
    }
    Ok((row, policy))
}
fn usage(tx: &mut Transaction, namespace: &str) -> Result<(SchemaUsage, u64)> {
    let snapshot = tx.schema_control_snapshot("")?;
    let mut u = SchemaUsage {
        relations: 0,
        roles: 0,
        versions: 0,
        schema_bytes: 0,
        proposals: 0,
    };
    let mut latest = 0;
    for module in snapshot
        .state
        .modules
        .values()
        .filter(|m| m.name == namespace)
    {
        u.versions += 1;
        latest = latest.max(module.version);
        u.relations += module.relations.len() as u32;
        u.roles += module
            .relations
            .iter()
            .map(|r| r.roles.len() as u32)
            .sum::<u32>();
        u.schema_bytes += u32::try_from(json(module.as_ref())?.len())
            .map_err(|_| invalid("schema size overflow"))?;
    }
    u.proposals = tx.lookup(PROPOSAL, "namespace", &namespace.into())?.len() as u32;
    Ok((u, latest))
}
fn within(usage: &SchemaUsage, q: &SchemaQuotas) -> bool {
    usage.relations <= q.relations
        && usage.roles <= q.roles
        && usage.versions <= q.versions
        && usage.schema_bytes <= q.schema_bytes
        && usage.proposals <= q.proposals
}
fn canonical(mut module: Module) -> Result<Module> {
    if module.name.len() > 64
        || (module.name.eq_ignore_ascii_case(NS)
            || module.name.eq_ignore_ascii_case("liquid_security")
            || module.name.eq_ignore_ascii_case("liquid_auth")
            || module.name.eq_ignore_ascii_case("liquid_views")
            || module.name.eq_ignore_ascii_case("liquid_view_catalog")
            || module.name.eq_ignore_ascii_case("liquid_visibility"))
        || !crate::model::identifier(&module.name)
        || module.version == 0
        || module.version > i64::MAX as u64
        || module.relations.is_empty()
        || module.relations.len() > 64
        || module.imports.len() > 64
    {
        return Err(invalid("invalid additive module bounds"));
    }
    for def in &module.relations {
        if reserved_schema_id(def.id) {
            return Err(Error::Denied);
        }
        if def.name.len() > 128
            || def.roles.len() > 128
            || def.unique.len() > 16
            || def.roles.iter().any(|r| r.name.len() > 64)
            || def.unique.iter().any(|k| k.len() > 16)
        {
            return Err(invalid("schema declaration bounds exceeded"));
        }
    }
    module.relations.sort_by(|a, b| a.name.cmp(&b.name));
    module.imports.sort();
    if json(&module)?.len() > 65536 {
        return Err(invalid("module exceeds 64 KiB"));
    }
    Ok(module)
}
fn build_plan(
    tx: &mut Transaction,
    principal: &str,
    module: Module,
    policy: &NamespaceInfo,
    visibility: Option<BTreeMap<String, Visibility>>,
) -> Result<ProposalPlan> {
    if !policy.policy.enabled {
        return Err(Error::Denied);
    }
    let module = canonical(module)?;
    let snapshot = tx.schema_control_snapshot(principal)?;
    let aliases = tx.scan(ALIAS)?;
    if module.relations.iter().any(|r| {
        aliases
            .iter()
            .any(|a| text(a, 1).eq_ignore_ascii_case(&r.name))
    }) {
        return Err(invalid("relation collides with an alias"));
    }
    let mut imports: BTreeMap<String, u64> = BTreeMap::new();
    let mut references = BTreeMap::new();
    for def in &module.relations {
        for role in &def.roles {
            if let DataType::Ref(target) = &role.data_type {
                if module.relations.iter().any(|r| &r.name == target) {
                    continue;
                }
                let referenced = snapshot.state.relation(target).map_err(|_| Error::Denied)?;
                if !snapshot
                    .state
                    .grant(principal, referenced.id)
                    .is_some_and(|g| g.reference)
                {
                    return Err(Error::Denied);
                }
                tx.lookup(LIFE, "relation", &target.as_str().into())?;
                if snapshot.schema_lifecycle(target)? == Lifecycle::Retired {
                    return Err(invalid("cannot introduce references to a retired relation"));
                }
                let (owner, version) = &snapshot.state.origins[&referenced.id];
                if owner != &module.name {
                    imports
                        .entry(owner.clone())
                        .and_modify(|v| *v = (*v).max(*version))
                        .or_insert(*version);
                }
                references.insert(
                    target.clone(),
                    ReferenceRequirement {
                        relation: target.clone(),
                        schema_id: referenced.id,
                        fingerprint: referenced
                            .roles
                            .iter()
                            .all(|r| match &r.data_type {
                                DataType::Ref(name) => {
                                    snapshot.state.names.get(name).is_some_and(|id| {
                                        snapshot
                                            .state
                                            .grant(principal, *id)
                                            .is_some_and(|g| g.read || g.write || g.reference)
                                    })
                                }
                                _ => true,
                            })
                            .then(|| referenced.fingerprint.clone()),
                    },
                );
            }
        }
    }
    // Derive an exact minimal import contract solely from authorized targets.
    // Unused/guessed import revisions cannot become a protected-catalog oracle.
    if module.imports != imports.into_iter().collect::<Vec<_>>() {
        return Err(invalid(
            "imports must exactly match referenced families' owning revisions",
        ));
    }
    tx.check_additive_module(&module)?;
    let (mut after, _) = usage(tx, &module.name)?;
    after.relations += module.relations.len() as u32;
    after.roles += module
        .relations
        .iter()
        .map(|r| r.roles.len() as u32)
        .sum::<u32>();
    after.versions += 1;
    after.schema_bytes += json(&module)?.len() as u32;
    if !within(&after, &policy.policy.quotas) {
        return Err(invalid("namespace quota exceeded"));
    }
    let costs = module
        .relations
        .iter()
        .map(|r| FamilyCost {
            relation: r.name.clone(),
            role_index_entries_per_row: r.roles.len(),
            reference_index_entries_per_row: r
                .roles
                .iter()
                .filter(|r| r.data_type.reference_target().is_some())
                .count(),
            unique_index_entries_per_row: r.unique.len(),
            minimum_packed_put_bytes: 33
                + r.roles
                    .iter()
                    .map(|r| match r.data_type {
                        DataType::Nullable(_) => 1,
                        DataType::Bool => 1,
                        DataType::Ref(_) => 16,
                        DataType::Text | DataType::Bytes => 4,
                        _ => 8,
                    })
                    .sum::<usize>(),
        })
        .collect();
    let grants_to_owner = module.relations.iter().map(|r| r.name.clone()).collect();
    let mut hash = Sha256::new();
    hash.update(if visibility.is_some() {
        b"LiquidSchemaProposal/2\0"
    } else {
        b"LiquidSchemaProposal/1\0"
    });
    hash.update(principal.as_bytes());
    hash.update([0]);
    hash.update(policy.generation.as_bytes());
    hash.update([0]);
    hash.update(json(&module)?.as_bytes());
    if let Some(modes) = &visibility {
        hash.update([0]);
        hash.update(json(modes)?.as_bytes());
    }
    Ok(ProposalPlan {
        format: if visibility.is_some() { 2 } else { 1 },
        digest: format!("{:x}", hash.finalize()),
        policy_generation: policy.generation.clone(),
        module,
        visibility,
        usage_after: after,
        costs,
        references: references.into_values().collect(),
        existing_record_rewrites: 0,
        grants_to_owner,
    })
}
fn proposal_from(row: &Row) -> Result<SchemaProposal> {
    let plan: ProposalPlan = serde_json::from_str(text(row, 2))
        .map_err(|_| Error::Storage("invalid stored schema proposal".into()))?;
    if !matches!((plan.format, &plan.visibility), (1, None) | (2, Some(_)))
        || plan.module.name != text(row, 0)
        || plan.visibility.as_ref().is_some_and(|v| {
            crate::visibility::normalize(&plan.module, v.clone())
                .ok()
                .as_ref()
                != Some(v)
        })
    {
        return Err(Error::Storage("unsupported stored schema proposal".into()));
    }
    let status = serde_json::from_str(text(row, 3))
        .map_err(|_| Error::Storage("invalid proposal status".into()))?;
    Ok(SchemaProposal {
        id: row.id,
        owner: text(row, 1).into(),
        status,
        plan,
    })
}
fn proposal_row(tx: &mut Transaction, id: Id) -> Result<Row> {
    if !initialized(&tx.snapshot())? {
        return Err(Error::Denied);
    }
    tx.get(PROPOSAL, id)?.ok_or(Error::Denied)
}
fn set_status(tx: &mut Transaction, row: &Row, status: ProposalStatus) -> Result<SchemaProposal> {
    let mut values = row.values.to_vec();
    values[3] = json(&status)?.as_str().into();
    tx.update(PROPOSAL, row.id, values)?;
    let mut result = proposal_from(row)?;
    result.status = status;
    Ok(result)
}
// Namespace delegation does not disclose reference domains beyond the selected
// data scope, including when viewing another proposal in the owned namespace.
fn scoped_receipt(tx: &mut Transaction, mut proposal: SchemaProposal) -> Result<SchemaProposal> {
    if tx.has_schema_capability() {
        let snapshot = tx.schema_control_snapshot(&proposal.owner)?;
        for reference in &mut proposal.plan.references {
            if !snapshot
                .state
                .grant(&proposal.owner, reference.schema_id)
                .is_some_and(|g| g.reference)
            {
                return Err(Error::Denied);
            }
            let def = snapshot
                .state
                .relations
                .get(&reference.schema_id)
                .ok_or(Error::Denied)?;
            if def.roles.iter().any(|role| matches!(&role.data_type,DataType::Ref(name)
                if !snapshot.state.grant(&proposal.owner,snapshot.state.names[name]).is_some_and(|g|g.read||g.write||g.reference))) {
                reference.fingerprint=None;
            }
        }
    }
    Ok(proposal)
}
fn matching_plan(tx: &mut Transaction, proposal: &SchemaProposal) -> Result<NamespaceInfo> {
    let (_, p) = owned_policy(tx, &proposal.owner, &proposal.plan.module.name)?;
    if p.generation != proposal.plan.policy_generation {
        return Err(Error::Conflict);
    }
    let (_, latest) = usage(tx, &proposal.plan.module.name)?;
    if latest.checked_add(1) != Some(proposal.plan.module.version) {
        return Err(Error::Conflict);
    }
    let current = build_plan(
        tx,
        &proposal.owner,
        proposal.plan.module.clone(),
        &p,
        proposal.plan.visibility.clone(),
    )?;
    let references_match = current.references.len() == proposal.plan.references.len()
        && current
            .references
            .iter()
            .zip(&proposal.plan.references)
            .all(|(now, planned)| {
                now.relation == planned.relation
                    && now.schema_id == planned.schema_id
                    && planned
                        .fingerprint
                        .as_ref()
                        .is_none_or(|hash| now.fingerprint.as_ref() == Some(hash))
            });
    if current.digest != proposal.plan.digest || !references_match {
        return Err(Error::Conflict);
    }
    Ok(p)
}
impl Database {
    /// Host-only provisioning. Ownership is immutable; changing limits/mode/enabled
    /// advances policy generation and invalidates outstanding approvals.
    pub fn configure_namespace(&self, policy: NamespacePolicy) -> Result<NamespaceInfo> {
        if !crate::model::identifier(&policy.namespace)
            || policy.namespace.len() > 64
            || (policy.namespace.eq_ignore_ascii_case(NS)
                || policy.namespace.eq_ignore_ascii_case("liquid_security")
                || policy.namespace.eq_ignore_ascii_case("liquid_auth")
                || policy.namespace.eq_ignore_ascii_case("liquid_views")
                || policy.namespace.eq_ignore_ascii_case("liquid_view_catalog")
                || policy.namespace.eq_ignore_ascii_case("liquid_visibility"))
            || policy.owner.is_empty()
            || policy.owner == crate::visibility::ANONYMOUS
            || policy.owner.len() > 128
        {
            return Err(invalid("invalid namespace policy"));
        }
        let q = &policy.quotas;
        if q.relations == 0
            || q.relations > 4096
            || q.roles == 0
            || q.roles > 32768
            || q.versions == 0
            || q.versions > 1024
            || q.schema_bytes == 0
            || q.schema_bytes > 16 * 1024 * 1024
            || q.proposals == 0
            || q.proposals > 4096
        {
            return Err(invalid("invalid namespace quota bounds"));
        }
        self.admin().transact(RetryPolicy::default(), |tx| {
            if !initialized(&tx.snapshot())? {
                tx.install(control_module().clone())?;
            }
            let old = tx
                .lookup(POLICY, "namespace", &policy.namespace.as_str().into())?
                .into_iter()
                .next();
            if old.as_ref().is_some_and(|r| text(r, 1) != policy.owner) {
                return Err(invalid("namespace ownership cannot be reassigned"));
            }
            // Case-fold ownership so two policies cannot claim aliases of one SQL namespace.
            if tx.scan(POLICY)?.iter().any(|r| {
                text(r, 0) != policy.namespace && text(r, 0).eq_ignore_ascii_case(&policy.namespace)
            }) {
                return Err(invalid("namespace ownership collision"));
            }
            let generation = old.as_ref().map_or(Ok(1), |r| {
                number(r, 9)
                    .checked_add(1)
                    .ok_or_else(|| invalid("policy generation exhausted"))
            })?;
            let values = vec![
                policy.namespace.as_str().into(),
                policy.owner.as_str().into(),
                Value::Bool(policy.auto_approve),
                Value::Bool(policy.enabled),
                Value::Int(q.relations.into()),
                Value::Int(q.roles.into()),
                Value::Int(q.versions.into()),
                Value::Int(q.schema_bytes.into()),
                Value::Int(q.proposals.into()),
                Value::Int(generation),
            ];
            let id = if let Some(row) = old {
                tx.update(POLICY, row.id, values)?;
                row.id
            } else {
                tx.insert(POLICY, values)?
            };
            tx.bind_namespace_visibility_owner(&policy.namespace, &policy.owner)?;
            let mut info = policy_from(&tx.get(POLICY, id)?.expect("staged"))?;
            let (usage, version) = usage(tx, &policy.namespace)?;
            info.usage = usage;
            info.latest_version = version.to_string();
            Ok(info)
        })
    }
    /// Host reviews the exact durable plan/digest while the service is online.
    pub fn review_schema_proposal(&self, id: Id) -> Result<SchemaProposal> {
        let mut tx = self.admin().transaction();
        proposal_from(&proposal_row(&mut tx, id)?)
    }
    pub fn decide_schema_proposal(
        &self,
        id: Id,
        digest: &str,
        approve: bool,
    ) -> Result<SchemaProposal> {
        self.admin().transact(RetryPolicy::default(), |tx| {
            let row = proposal_row(tx, id)?;
            let proposal = proposal_from(&row)?;
            if proposal.plan.digest != digest {
                return Err(Error::Conflict);
            }
            if !matches!(
                proposal.status,
                ProposalStatus::Pending | ProposalStatus::Approved
            ) {
                return Err(invalid("proposal is terminal"));
            }
            if approve {
                matching_plan(tx, &proposal)?;
            }
            set_status(
                tx,
                &row,
                if approve {
                    ProposalStatus::Approved
                } else {
                    ProposalStatus::Rejected
                },
            )
        })
    }
}
impl Session {
    fn schema_principal(&self) -> Result<&str> {
        if self.capability.is_some() {
            return Err(Error::Denied);
        }
        self.principal.as_deref().ok_or(Error::Denied)
    }
    pub fn schema_namespaces(&self) -> Result<Vec<NamespaceInfo>> {
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let principal = tx.bind_schema_session(self, None)?;
            if !initialized(&tx.snapshot())? {
                return Ok(vec![]);
            }
            let mut output = Vec::new();
            for row in tx.lookup(POLICY, "owner", &principal.as_str().into())? {
                let mut info = policy_from(&row)?;
                if tx
                    .schema_namespace_scope()
                    .is_some_and(|n| n != info.policy.namespace)
                {
                    continue;
                }
                let (u, v) = usage(tx, &info.policy.namespace)?;
                info.usage = u;
                info.latest_version = v.to_string();
                output.push(info);
            }
            Ok(output)
        })
    }
    pub fn propose_schema(&self, id: Id, module: Module) -> Result<SchemaProposal> {
        self.propose_schema_with_visibility(id, module, BTreeMap::new())
    }
    pub fn propose_schema_with_visibility(
        &self,
        id: Id,
        module: Module,
        visibility: BTreeMap<String, Visibility>,
    ) -> Result<SchemaProposal> {
        let modes = crate::visibility::normalize(&module, visibility.clone())?;
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let principal = tx.bind_schema_session(self, Some(&module.name))?;
            let (_, policy) = owned_policy(tx, &principal, &module.name)?;
            let canonical = canonical(module.clone())?;
            if let Some(row) = tx.get(PROPOSAL, id)? {
                let proposal = proposal_from(&row)?;
                if proposal.owner != principal {
                    return Err(Error::Denied);
                }
                if proposal.plan.module != canonical
                    || match &proposal.plan.visibility {
                        Some(previous) => previous != &modes,
                        None => !visibility.is_empty(),
                    }
                {
                    return Err(Error::Conflict);
                }
                return scoped_receipt(tx, proposal);
            }
            let mut plan = build_plan(tx, &principal, canonical, &policy, Some(modes.clone()))?;
            plan.usage_after.proposals += 1;
            if !within(&plan.usage_after, &policy.policy.quotas) {
                return Err(invalid("namespace proposal quota exceeded"));
            }
            let status = if policy.policy.auto_approve {
                ProposalStatus::Approved
            } else {
                ProposalStatus::Pending
            };
            tx.insert_with_id(
                PROPOSAL,
                id,
                vec![
                    module.name.as_str().into(),
                    principal.as_str().into(),
                    json(&plan)?.as_str().into(),
                    json(&status)?.as_str().into(),
                ],
            )?;
            Ok(SchemaProposal {
                id,
                owner: principal,
                status,
                plan,
            })
        })
    }
    pub fn schema_proposal(&self, id: Id) -> Result<SchemaProposal> {
        let mut tx = self.db.admin().transaction();
        let proposal = proposal_from(&proposal_row(&mut tx, id)?)?;
        let principal = tx.bind_schema_session(self, Some(&proposal.plan.module.name))?;
        if proposal.owner != principal {
            return Err(Error::Denied);
        }
        let proposal = scoped_receipt(&mut tx, proposal)?;
        tx.commit()?;
        Ok(proposal)
    }
    pub fn cancel_schema_proposal(&self, id: Id) -> Result<SchemaProposal> {
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let row = proposal_row(tx, id)?;
            let p = proposal_from(&row)?;
            let principal = tx.bind_schema_session(self, Some(&p.plan.module.name))?;
            if p.owner != principal {
                return Err(Error::Denied);
            }
            let p = scoped_receipt(tx, p)?;
            if p.status == ProposalStatus::Cancelled {
                return Ok(p);
            }
            if !matches!(p.status, ProposalStatus::Pending | ProposalStatus::Approved) {
                return Err(invalid("proposal is terminal"));
            }
            let result = set_status(tx, &row, ProposalStatus::Cancelled)?;
            scoped_receipt(tx, result)
        })
    }
    pub fn apply_schema_proposal(&self, id: Id, digest: &str) -> Result<SchemaProposal> {
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let row = proposal_row(tx, id)?;
            let proposal = proposal_from(&row)?;
            let principal = tx.bind_schema_session(self, Some(&proposal.plan.module.name))?;
            if proposal.owner != principal {
                return Err(Error::Denied);
            }
            let proposal = scoped_receipt(tx, proposal)?;
            if proposal.plan.digest != digest {
                return Err(Error::Conflict);
            }
            if proposal.status == ProposalStatus::Installed {
                return Ok(proposal);
            }
            if proposal.status != ProposalStatus::Approved {
                return Err(Error::Denied);
            }
            matching_plan(tx, &proposal)?;
            let modes = proposal.plan.visibility.clone().unwrap_or_else(|| {
                proposal
                    .plan
                    .module
                    .relations
                    .iter()
                    .map(|r| (r.name.clone(), Visibility::Private))
                    .collect()
            });
            tx.install_with_visibilities(proposal.plan.module.clone(), modes.clone())?;
            for def in &proposal.plan.module.relations {
                tx.store_visibility(&def.name, &principal, modes[&def.name])?;
                tx.grant(&principal, &def.name, Grant::read_write())?;
            }
            let result = set_status(tx, &row, ProposalStatus::Installed)?;
            scoped_receipt(tx, result)
        })
    }
    /// Namespace authority may change read visibility, but not write/reference grants.
    pub fn set_schema_visibility(&self, relation: &str, visibility: Visibility) -> Result<()> {
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let namespace = relation.split_once('.').ok_or(Error::Denied)?.0;
            let principal = tx.bind_schema_session(self, Some(namespace))?;
            let (_, policy) = owned_policy(tx, &principal, namespace)?;
            if !policy.policy.enabled {
                return Err(Error::Denied);
            }
            tx.store_visibility(relation, &principal, visibility)
        })
    }
    /// Advisory lifecycle. Existing schemas, records, grants and queries remain
    /// valid; retirement forbids NEW proposal references, never deletes data.
    pub fn set_schema_lifecycle(&self, relation: &str, state: Lifecycle) -> Result<()> {
        let principal = self.schema_principal()?;
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let namespace = relation.split_once('.').ok_or(Error::Denied)?.0;
            let (_, policy) = owned_policy(tx, principal, namespace)?;
            if !policy.policy.enabled {
                return Err(Error::Denied);
            }
            let snapshot = tx.schema_control_snapshot(principal)?;
            let def = snapshot
                .state
                .relation(relation)
                .map_err(|_| Error::Denied)?;
            if snapshot.state.origins[&def.id].0 != namespace {
                return Err(Error::Denied);
            }
            let old = tx
                .lookup(LIFE, "relation", &relation.into())?
                .into_iter()
                .next();
            let previous = snapshot.schema_lifecycle(relation)?;
            if matches!(
                (previous, state),
                (
                    Lifecycle::Retired,
                    Lifecycle::Active | Lifecycle::Deprecated
                ) | (Lifecycle::Deprecated, Lifecycle::Active)
            ) {
                return Err(invalid("lifecycle transitions are monotonic"));
            }
            let values = vec![relation.into(), json(&state)?.as_str().into()];
            if let Some(row) = old {
                tx.update(LIFE, row.id, values)?;
            } else {
                tx.insert(LIFE, values)?;
            }
            Ok(())
        })
    }
    /// Immutable namespaced alias. It resolves to a canonical family descriptor;
    /// aliases do not create new SQL tables or alter existing query meanings.
    pub fn create_schema_alias(&self, alias: &str, target: &str) -> Result<()> {
        let principal = self.schema_principal()?;
        self.db.admin().transact(RetryPolicy::default(), |tx| {
            let (namespace, local) = alias.split_once('.').ok_or(Error::Denied)?;
            let (_, policy) = owned_policy(tx, principal, namespace)?;
            if !policy.policy.enabled
                || !crate::model::identifier(local)
                || alias.len() > 128
                || target.len() > 128
            {
                return Err(Error::Denied);
            }
            let snapshot = tx.schema_control_snapshot(principal)?;
            let def = snapshot.state.relation(target).map_err(|_| Error::Denied)?;
            tx.lookup(LIFE, "relation", &target.into())?;
            if snapshot.state.origins[&def.id].0 != namespace
                || snapshot.schema_lifecycle(target)? == Lifecycle::Retired
            {
                return Err(Error::Denied);
            }
            if snapshot
                .state
                .names
                .keys()
                .any(|n| n.eq_ignore_ascii_case(alias))
            {
                return Err(invalid("alias collides with a relation"));
            }
            let aliases = tx.lookup(ALIAS, "namespace", &namespace.into())?;
            if let Some(old) = aliases
                .iter()
                .find(|r| text(r, 1).eq_ignore_ascii_case(alias))
            {
                return if text(old, 1) == alias && text(old, 2) == target {
                    Ok(())
                } else {
                    Err(invalid("alias is immutable"))
                };
            }
            if aliases.len() >= policy.policy.quotas.relations as usize {
                return Err(invalid("alias quota exceeded"));
            }
            tx.insert(ALIAS, vec![namespace.into(), alias.into(), target.into()])?;
            Ok(())
        })
    }
}
impl Snapshot {
    pub(crate) fn schema_namespace_dependency(
        &self,
        namespace: &str,
    ) -> Result<Option<crate::storage::Bucket>> {
        if !initialized(self)? {
            return Ok(None);
        }
        let id = self.state.names[POLICY];
        Ok(Some((
            id,
            self.state.relations[&id].role("namespace")?,
            namespace.into(),
        )))
    }
    pub(crate) fn schema_namespace_managed(&self, namespace: &str) -> Result<bool> {
        Ok(!self
            .control_rows(POLICY, "namespace", namespace)?
            .is_empty())
    }
    pub(crate) fn schema_lifecycle(&self, relation: &str) -> Result<Lifecycle> {
        self.control_rows(LIFE, "relation", relation)?
            .first()
            .map(|r| {
                serde_json::from_str(text(r, 1))
                    .map_err(|_| Error::Storage("invalid lifecycle metadata".into()))
            })
            .transpose()
            .map(|v| v.unwrap_or_default())
    }
    pub(crate) fn schema_aliases(&self, relation: &str) -> Result<Vec<String>> {
        Ok(self
            .control_rows(ALIAS, "target", relation)?
            .iter()
            .map(|r| text(r, 1).into())
            .collect())
    }
    pub(crate) fn resolve_schema_alias(&self, alias: &str) -> Result<Option<String>> {
        Ok(self
            .control_rows(ALIAS, "alias", alias)?
            .first()
            .map(|r| text(r, 2).into()))
    }
    fn control_rows(&self, family: &str, role: &str, value: &str) -> Result<Vec<&Row>> {
        if !initialized(self)? {
            return Ok(vec![]);
        }
        let id = self.state.names[family];
        let def = &self.state.relations[&id];
        let slot = def.role(role)?;
        Ok(self
            .state
            .index
            .get(&(id, slot, value.into()))
            .into_iter()
            .flatten()
            .filter_map(|rid| self.state.row(id, *rid).map(|r| r.as_ref()))
            .collect())
    }
}

/// Wire proposals require explicit IDs. Authoring's optional-ID convenience must
/// not allocate different identities when identical requests are retried.
pub(crate) fn explicit_module<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Module, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ExplicitRelation {
        id: Id,
        name: String,
        roles: Vec<Role>,
        #[serde(default)]
        unique: Vec<Vec<String>>,
        #[serde(default)]
        checks: Vec<Check>,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ExplicitModule {
        name: String,
        version: u64,
        imports: Vec<(String, u64)>,
        relations: Vec<ExplicitRelation>,
    }
    let m = ExplicitModule::deserialize(d)?;
    Ok(Module::new(
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
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Database, SchemaProposal, Id) {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let person = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.grant(
            "agent",
            "core.Person",
            Grant {
                reference: true,
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        db.configure_namespace(NamespacePolicy {
            namespace: "app".into(),
            owner: "agent".into(),
            auto_approve: true,
            enabled: true,
            quotas: SchemaQuotas::default(),
        })
        .unwrap();
        let p = db
            .session("agent")
            .propose_schema(
                Id::new(),
                Module::new(
                    "app",
                    1,
                    vec![("core".into(), 1)],
                    vec![Relation::new(
                        "app.Link",
                        vec![Role::new("owner", DataType::Ref("core.Person".into()))],
                    )],
                ),
            )
            .unwrap();
        (db, p, person)
    }
    #[test]
    fn approved_install_preserves_old_arcs_and_exact_packed_bytes() {
        let (db, p, person) = fixture();
        let before = db.admin().snapshot();
        let tid = before.schema("core.Person").unwrap().id;
        let old = before.state.row(tid, person).unwrap();
        let ops = vec![crate::storage::Op::Put(tid, (**old).clone())];
        let bytes = crate::codec::encode(1, &ops, &before.state).unwrap();
        db.session("agent")
            .apply_schema_proposal(p.id, &p.plan.digest)
            .unwrap();
        let after = db.admin().snapshot();
        assert!(std::sync::Arc::ptr_eq(
            old,
            after.state.row(tid, person).unwrap()
        ));
        assert!(std::sync::Arc::ptr_eq(
            &before.state.relations[&tid],
            &after.state.relations[&tid]
        ));
        assert_eq!(bytes, crate::codec::encode(1, &ops, &after.state).unwrap());
    }
    #[test]
    fn reference_revocation_racing_validated_plan_conflicts_at_commit() {
        let (db, p, _) = fixture();
        let mut apply = db.admin().transaction();
        matching_plan(&mut apply, &p).unwrap();
        apply.install(p.plan.module).unwrap();
        let mut revoke = db.admin().transaction();
        revoke.revoke("agent", "core.Person").unwrap();
        revoke.commit().unwrap();
        assert!(matches!(apply.commit(), Err(Error::Conflict)));
        assert!(db.admin().snapshot().schema("app.Link").is_err());
    }
    #[test]
    fn alias_reservation_and_retirement_are_transaction_dependencies() {
        let (db, p, _) = fixture();
        let mut apply = db.admin().transaction();
        matching_plan(&mut apply, &p).unwrap();
        let mut metadata = db.admin().transaction();
        metadata
            .insert(
                LIFE,
                vec![
                    "core.Person".into(),
                    json(&Lifecycle::Retired).unwrap().as_str().into(),
                ],
            )
            .unwrap();
        metadata.commit().unwrap();
        assert!(matches!(apply.commit(), Err(Error::Conflict)));
        assert!(
            db.session("agent")
                .apply_schema_proposal(p.id, &p.plan.digest)
                .is_err()
        );
    }
}

impl Transaction {
    pub(crate) fn visibility_namespace_owner(&mut self, namespace: &str) -> Result<Option<String>> {
        if !initialized(&self.snapshot())? {
            return Ok(None);
        }
        self.lookup(POLICY, "namespace", &namespace.into())?
            .into_iter()
            .next()
            .map(|row| policy_from(&row).map(|p| p.policy.owner))
            .transpose()
    }
}

#[cfg(test)]
mod legacy_visibility_proposals {
    use super::*;
    #[test]
    fn format_one_pending_proposal_keeps_old_digest_and_private_install() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old-proposal.redb");
        let id = Id::new();
        let module = Module::new(
            "legacy",
            1,
            vec![],
            vec![Relation::new(
                "legacy.Row",
                vec![Role::new("value", DataType::Int)],
            )],
        );
        let digest;
        {
            let db = Database::open(&path).unwrap();
            db.configure_namespace(NamespacePolicy {
                namespace: "legacy".into(),
                owner: "owner".into(),
                auto_approve: false,
                enabled: true,
                quotas: SchemaQuotas::default(),
            })
            .unwrap();
            let mut tx = db.admin().transaction();
            let (_, policy) = owned_policy(&mut tx, "owner", "legacy").unwrap();
            let plan = build_plan(&mut tx, "owner", module.clone(), &policy, None).unwrap();
            assert_eq!(plan.format, 1);
            assert!(!json(&plan).unwrap().contains("visibility"));
            digest = plan.digest.clone();
            tx.insert_with_id(
                PROPOSAL,
                id,
                vec![
                    "legacy".into(),
                    "owner".into(),
                    json(&plan).unwrap().as_str().into(),
                    json(&ProposalStatus::Pending).unwrap().as_str().into(),
                ],
            )
            .unwrap();
            tx.commit().unwrap();
            db.checkpoint().unwrap();
        }
        let db = Database::open(path).unwrap();
        let owner = db.session("owner");
        assert_eq!(
            owner.propose_schema(id, module).unwrap().plan.digest,
            digest
        );
        db.decide_schema_proposal(id, &digest, true).unwrap();
        let applied = owner.apply_schema_proposal(id, &digest).unwrap();
        assert_eq!(applied.plan.format, 1);
        assert!(db.session("other").snapshot().schema("legacy.Row").is_err());
        assert!(owner.snapshot().schema("legacy.Row").is_ok());
    }
}
