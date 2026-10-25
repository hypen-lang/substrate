//! Versioned application protocol. No administrative session is reachable here.
//! Transport is separate from this authenticated, bounded request dispatcher.
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCredential {
    pub token: String,
    pub principal: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub version: u32,
    pub clients: Vec<ClientCredential>,
}
impl ServerConfig {
    pub fn validate(&self) -> Result<()> {
        let mut tokens = std::collections::BTreeSet::new();
        if self.version != 1 || self.clients.is_empty() || self.clients.len() > 256 {
            return Err(Error::Constraint("invalid server configuration".into()));
        }
        for client in &self.clients {
            if !(32..=256).contains(&client.token.len())
                || client.principal.is_empty()
                || client.principal.len() > 128
                || !tokens.insert(&client.token)
            {
                return Err(Error::Constraint("invalid/duplicate credential".into()));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum WireValue {
    Bool(bool),
    Int(String),
    Text(String),
    Ref(String),
    Decimal(String),
    Timestamp(String),
    Bytes(Vec<u8>),
}
impl From<Value> for WireValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Bool(v) => Self::Bool(v),
            Value::Int(v) => Self::Int(v.to_string()),
            Value::Text(v) => Self::Text(v),
            Value::Ref(v) => Self::Ref(v.to_string()),
            Value::Decimal(v) => Self::Decimal(v.to_string()),
            Value::Timestamp(v) => Self::Timestamp(v.to_string()),
            Value::Bytes(v) => Self::Bytes(v),
        }
    }
}
impl WireValue {
    pub(crate) fn value(&self, aliases: &BTreeMap<String, Id>) -> Result<Value> {
        let integer = |s: &str| {
            s.parse::<i64>()
                .map_err(|_| Error::Constraint("expected signed 64-bit decimal string".into()))
        };
        Ok(match self {
            Self::Bool(v) => Value::Bool(*v),
            Self::Int(v) => Value::Int(integer(v)?),
            Self::Text(v) => Value::Text(v.clone()),
            Self::Decimal(v) => Value::Decimal(integer(v)?),
            Self::Timestamp(v) => Value::Timestamp(integer(v)?),
            Self::Bytes(v) => Value::Bytes(v.clone()),
            Self::Ref(v) => Value::Ref(if let Some(alias) = v.strip_prefix('$') {
                *aliases
                    .get(alias)
                    .ok_or_else(|| Error::Constraint("unknown/forward batch reference".into()))?
            } else {
                Id(uuid::Uuid::parse_str(v)
                    .map_err(|_| Error::Constraint("invalid UUID reference".into()))?)
            }),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SqlWireValue {
    Null,
    Integer(String),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
impl SqlWireValue {
    fn sql(&self) -> Result<rusqlite::types::Value> {
        use rusqlite::types::Value as S;
        Ok(match self {
            Self::Null => S::Null,
            Self::Integer(v) => S::Integer(
                v.parse()
                    .map_err(|_| Error::Query("invalid integer parameter".into()))?,
            ),
            Self::Real(v) if v.is_finite() => S::Real(*v),
            Self::Text(v) => S::Text(v.clone()),
            Self::Blob(v) => S::Blob(v.clone()),
            _ => return Err(Error::Query("nonfinite real".into())),
        })
    }
    fn from_sql(v: rusqlite::types::Value) -> Result<Self> {
        use rusqlite::types::Value as S;
        Ok(match v {
            S::Null => Self::Null,
            S::Integer(v) => Self::Integer(v.to_string()),
            S::Real(v) if v.is_finite() => Self::Real(v),
            S::Text(v) => Self::Text(v),
            S::Blob(v) => Self::Blob(v),
            _ => return Err(Error::Query("nonfinite SQL result".into())),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Insert {
        relation: String,
        schema_id: Id,
        fields: BTreeMap<String, WireValue>,
        #[serde(default)]
        alias: Option<String>,
    },
    Update {
        relation: String,
        schema_id: Id,
        id: Id,
        fields: BTreeMap<String, WireValue>,
    },
    Delete {
        relation: String,
        schema_id: Id,
        id: Id,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    UsingCapability {
        capability: Id,
        command: Box<Command>,
    },
    DelegateCapability {
        parent: Id,
        id: Id,
        subject: String,
        scope: crate::capability::CapabilityScope,
    },
    DelegateCapabilityUntil {
        parent: Id,
        id: Id,
        subject: String,
        scope: crate::capability::CapabilityScope,
        expires_at: u64,
    },
    RevokeCapability {
        id: Id,
    },
    SchemaNamespaces,
    SchemaPropose {
        id: Id,
        #[serde(deserialize_with = "crate::schema_control::explicit_module")]
        module: Module,
    },
    SchemaProposeWithVisibility {
        id: Id,
        #[serde(deserialize_with = "crate::schema_control::explicit_module")]
        module: Module,
        visibility: BTreeMap<String, Visibility>,
    },
    SchemaVisibility {
        relation: String,
        visibility: Visibility,
    },
    SchemaProposal {
        id: Id,
    },
    SchemaApply {
        id: Id,
        digest: String,
    },
    SchemaCancel {
        id: Id,
    },
    SchemaLifecycle {
        relation: String,
        state: crate::schema_control::Lifecycle,
    },
    SchemaAlias {
        alias: String,
        target: String,
    },
    Catalog {
        options: crate::catalog::CatalogRequest,
    },
    Read {
        plan: crate::nested::ReadPlan,
    },
    Capabilities,
    Checked {
        contracts: Vec<Contract>,
        command: Box<Command>,
    },
    Batch {
        operations: Vec<Mutation>,
    },
    Query {
        sql: String,
        #[serde(default)]
        params: Vec<SqlWireValue>,
    },
    Page {
        relation: String,
        schema_id: Id,
        #[serde(default)]
        after: Option<Id>,
        limit: usize,
        #[serde(default)]
        expected_revision: Option<String>,
        #[serde(default)]
        filter: Option<(String, WireValue)>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub relation: String,
    pub schema_id: Id,
    pub fingerprint: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub token: String,
    pub command: Command,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct NamedRow {
    pub id: Id,
    pub fields: BTreeMap<String, WireValue>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Capability {
    pub relation: Relation,
    pub read: bool,
    pub write: bool,
    pub reference: bool,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Output {
    DelegatedCapability {
        capability: Box<crate::capability::DelegatedCapability>,
    },
    CapabilityRevoked,
    SchemaNamespaces {
        namespaces: Vec<crate::schema_control::NamespaceInfo>,
    },
    SchemaProposal {
        proposal: Box<crate::schema_control::SchemaProposal>,
    },
    SchemaChanged,
    Catalog {
        page: crate::catalog::CatalogPage,
    },
    Read {
        result: crate::nested::NestedResult,
    },
    Capabilities {
        protocol: u32,
        relations: Vec<Capability>,
        max_batch: usize,
        max_page: usize,
        sql_writes: bool,
    },
    Batch {
        ids: Vec<Id>,
    },
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<SqlWireValue>>,
    },
    Page {
        revision: String,
        rows: Vec<NamedRow>,
        next: Option<Id>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteError {
    pub code: String,
    pub message: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum Response {
    Ok(Output),
    Error(RemoteError),
}
impl Response {
    pub fn error(code: &str, message: &str) -> Self {
        Self::Error(RemoteError {
            code: code.into(),
            message: message.into(),
        })
    }
    fn result(result: Result<Output>) -> Self {
        match result {
            Ok(v) => Self::Ok(v),
            Err(e) => {
                let code = match &e {
                    Error::Denied => "denied",
                    Error::Conflict => "conflict",
                    Error::Aborted => "aborted",
                    Error::Schema(_) => "schema",
                    Error::Constraint(_) => "constraint",
                    Error::NotFound => "not_found",
                    Error::Query(_) | Error::Sql(_) => "query",
                    Error::Storage(_) => "storage",
                };
                // Storage internals and paths are host diagnostics, not client messages.
                let message = if matches!(e, Error::Storage(_)) {
                    "storage outcome uncertain; reconnect and reconcile".into()
                } else {
                    e.to_string()
                };
                Self::Error(RemoteError {
                    code: code.into(),
                    message,
                })
            }
        }
    }
}
#[derive(Clone)]
pub struct Gateway {
    db: Database,
    config: ServerConfig,
}
impl Gateway {
    pub fn new(db: Database, config: ServerConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { db, config })
    }
    pub fn handle(&self, request: Request) -> Response {
        if request.token.is_empty() {
            return self.handle_anonymous(request);
        }
        let mut principal = None;
        for client in &self.config.clients {
            if token_eq(&client.token, &request.token) {
                principal = Some(client.principal.as_str());
            }
        }
        let Some(principal) = principal else {
            return Response::error("denied", "access denied");
        };
        if request.version != 1 {
            return Response::error("protocol", "unsupported protocol version");
        }
        // Even a principal literally named "admin" is a restricted session.
        Response::result(self.dispatch(self.db.session(principal), request.command))
    }
    pub(crate) fn authenticated(db: Database) -> Self {
        Self {
            db,
            config: ServerConfig {
                version: 1,
                clients: vec![],
            },
        }
    }
    pub fn handle_authenticated(&self, request: Request) -> Response {
        self.handle_authenticated_status(request).0
    }
    /// Transport audit distinguishes credential failure from a scoped operation denial.
    pub(crate) fn handle_authenticated_status(&self, request: Request) -> (Response, bool) {
        if request.token.is_empty() {
            // Explicitly anonymous, never a fallback for an invalid credential.
            return (self.handle_anonymous(request), true);
        }
        let session = match self.db.authenticate(&request.token) {
            Ok(session) => session,
            Err(_) => return (Response::error("denied", "access denied"), false),
        };
        (
            Response::result((|| {
                if request.version != 1 {
                    return Err(Error::Query("unsupported protocol version".into()));
                }
                let fence = session.snapshot();
                fence.check_live()?;
                // Each operation has its own result/commit gate. A second check here
                // would falsely report failure after a successful self-revocation.
                self.dispatch(session, request.command)
            })()),
            true,
        )
    }
    fn handle_anonymous(&self, request: Request) -> Response {
        fn readable(command: &Command) -> bool {
            match command {
                Command::Catalog { .. }
                | Command::Read { .. }
                | Command::Capabilities
                | Command::Query { .. }
                | Command::Page { .. } => true,
                Command::Checked { command, .. } => matches!(
                    command.as_ref(),
                    Command::Catalog { .. }
                        | Command::Read { .. }
                        | Command::Capabilities
                        | Command::Query { .. }
                        | Command::Page { .. }
                ),
                _ => false,
            }
        }
        if request.version != 1 {
            return Response::error("protocol", "unsupported protocol version");
        }
        if !readable(&request.command) {
            return Response::error("denied", "access denied");
        }
        Response::result(self.dispatch(self.db.anonymous(), request.command))
    }
    fn dispatch(&self, session: Session, command: Command) -> Result<Output> {
        match command {
            Command::UsingCapability {
                capability,
                command,
            } => {
                if session.capability.is_some_and(|id| id != capability)
                    || matches!(*command, Command::UsingCapability { .. })
                {
                    return Err(Error::Denied);
                }
                self.dispatch(session.with_capability(capability)?, *command)
            }
            Command::DelegateCapability {
                parent,
                id,
                subject,
                scope,
            } => Ok(Output::DelegatedCapability {
                capability: Box::new(session.delegate_capability(parent, id, &subject, scope)?),
            }),
            Command::DelegateCapabilityUntil {
                parent,
                id,
                subject,
                scope,
                expires_at,
            } => Ok(Output::DelegatedCapability {
                capability: Box::new(session.delegate_capability_until(
                    parent,
                    id,
                    &subject,
                    scope,
                    Some(expires_at),
                )?),
            }),
            Command::RevokeCapability { id } => {
                session.revoke_capability(id)?;
                Ok(Output::CapabilityRevoked)
            }
            Command::SchemaNamespaces => Ok(Output::SchemaNamespaces {
                namespaces: session.schema_namespaces()?,
            }),
            Command::SchemaPropose { id, module } => Ok(Output::SchemaProposal {
                proposal: Box::new(session.propose_schema(id, module)?),
            }),
            Command::SchemaProposeWithVisibility {
                id,
                module,
                visibility,
            } => Ok(Output::SchemaProposal {
                proposal: Box::new(session.propose_schema_with_visibility(id, module, visibility)?),
            }),
            Command::SchemaVisibility {
                relation,
                visibility,
            } => {
                session.set_schema_visibility(&relation, visibility)?;
                Ok(Output::SchemaChanged)
            }
            Command::SchemaProposal { id } => Ok(Output::SchemaProposal {
                proposal: Box::new(session.schema_proposal(id)?),
            }),
            Command::SchemaApply { id, digest } => Ok(Output::SchemaProposal {
                proposal: Box::new(session.apply_schema_proposal(id, &digest)?),
            }),
            Command::SchemaCancel { id } => Ok(Output::SchemaProposal {
                proposal: Box::new(session.cancel_schema_proposal(id)?),
            }),
            Command::SchemaLifecycle { relation, state } => {
                session.set_schema_lifecycle(&relation, state)?;
                Ok(Output::SchemaChanged)
            }
            Command::SchemaAlias { alias, target } => {
                session.create_schema_alias(&alias, &target)?;
                Ok(Output::SchemaChanged)
            }
            Command::Catalog { options } => Ok(Output::Catalog {
                page: session.snapshot().catalog(&options)?,
            }),
            Command::Read { plan } => Ok(Output::Read {
                result: session.snapshot().read_nested(&plan)?,
            }),
            Command::Checked { contracts, command } => {
                if contracts.is_empty()
                    || contracts.len() > 100
                    || matches!(*command, Command::Checked { .. })
                {
                    return Err(Error::Schema("invalid contract check".into()));
                }
                let snapshot = session.snapshot();
                snapshot.check_live()?;
                let mut selected = std::collections::BTreeSet::new();
                for contract in contracts {
                    selected.insert(contract.schema_id);
                    let schema = snapshot
                        .state
                        .relation(&contract.relation)
                        .map_err(|_| Error::Denied)?;
                    let principal = snapshot.principal.as_deref().expect("restricted");
                    if !snapshot.state.can_discover(Some(principal), schema.id)
                        || schema.roles.iter().any(|role| {
                            matches!(&role.data_type, DataType::Ref(name)
                                if !snapshot.can_discover(snapshot.state.names[name]))
                        })
                    {
                        // A full contract fingerprint includes reference domains.
                        // Never expose a match/mismatch oracle for domains that
                        // catalog discovery deliberately makes opaque.
                        return Err(Error::Denied);
                    }
                    if schema.id != contract.schema_id
                        || snapshot.state.relations[&schema.id].fingerprint != contract.fingerprint
                    {
                        return Err(Error::Schema(format!(
                            "contract mismatch for {}",
                            contract.relation
                        )));
                    }
                }
                if let Command::Query { sql, params } = *command {
                    query_output(snapshot.sql_for_relations(&selected)?, sql, params)
                } else {
                    self.dispatch(session, *command)
                }
            }
            Command::Capabilities => {
                let snapshot = session.snapshot();
                snapshot.check_live()?;
                let p = snapshot.principal.as_deref().expect("restricted gateway");
                let relations = snapshot
                    .state
                    .relations
                    .values()
                    // The legacy full-declaration shape cannot redact reference
                    // domains. Omit such families; Catalog exposes safe opaque roles.
                    .filter(|def| snapshot.can_discover(def.id) && def.roles.iter().all(|r| {
                        !matches!(&r.data_type, DataType::Ref(name) if !snapshot.can_discover(snapshot.state.names[name]))
                    }))
                    .map(|def| {
                        let grant = snapshot.state.grant(p, def.id);
                        Capability {
                            relation: def.declaration.clone(),
                            read: snapshot.state.can_read(Some(p), def.id),
                            write: p != crate::visibility::ANONYMOUS && grant.is_some_and(|g| g.write),
                            reference: p != crate::visibility::ANONYMOUS && grant.is_some_and(|g| g.reference),
                        }
                    })
                    .collect();
                snapshot.check_live()?;
                Ok(Output::Capabilities {
                    protocol: 1,
                    relations,
                    max_batch: 100,
                    max_page: 1000,
                    sql_writes: false,
                })
            }
            Command::Batch { operations } => {
                if operations.is_empty() || operations.len() > 100 {
                    return Err(Error::Constraint("batch size must be 1..=100".into()));
                }
                let ids = session.transact(RetryPolicy::default(), |tx| {
                    let mut ids = Vec::new();
                    let mut aliases = BTreeMap::new();
                    for op in &operations {
                        let (relation, schema_id) = match op {
                            Mutation::Insert {
                                relation,
                                schema_id,
                                ..
                            }
                            | Mutation::Update {
                                relation,
                                schema_id,
                                ..
                            }
                            | Mutation::Delete {
                                relation,
                                schema_id,
                                ..
                            } => (relation, *schema_id),
                        };
                        tx.check_write_schema(relation, schema_id)?;
                        let convert = |fields: &BTreeMap<String, WireValue>| {
                            fields
                                .iter()
                                .map(|(name, v)| Ok((name.clone(), v.value(&aliases)?)))
                                .collect::<Result<Fields>>()
                        };
                        match op {
                            Mutation::Insert { fields, alias, .. } => {
                                if let Some(alias) = alias
                                    && (!crate::model::identifier(alias)
                                        || aliases.contains_key(alias))
                                {
                                    return Err(Error::Constraint(
                                        "invalid/duplicate batch alias".into(),
                                    ));
                                }
                                let id = tx.insert_named(relation, convert(fields)?)?;
                                ids.push(id);
                                if let Some(alias) = alias {
                                    aliases.insert(alias.clone(), id);
                                }
                            }
                            Mutation::Update { id, fields, .. } => {
                                tx.update_named(relation, *id, convert(fields)?)?
                            }
                            Mutation::Delete { id, .. } => tx.delete(relation, *id)?,
                        }
                    }
                    Ok(ids)
                })?;
                Ok(Output::Batch { ids })
            }
            Command::Query { sql, params } => query_output(session.snapshot().sql()?, sql, params),
            Command::Page {
                relation,
                schema_id,
                after,
                limit,
                expected_revision,
                filter,
            } => {
                let snapshot = session.snapshot();
                if snapshot.schema(&relation)?.id != schema_id {
                    return Err(Error::Schema("generated schema identity mismatch".into()));
                }
                if expected_revision.is_some_and(|r| r != snapshot.revision().to_string()) {
                    return Err(Error::Conflict);
                }
                let filter = filter
                    .map(|(r, v)| v.value(&BTreeMap::new()).map(|v| (r, v)))
                    .transpose()?;
                let page = snapshot.page(
                    &relation,
                    filter.as_ref().map(|(r, v)| (r.as_str(), v)),
                    after,
                    limit,
                )?;
                let rows = page
                    .rows
                    .into_iter()
                    .map(|row| {
                        let id = row.id;
                        Ok(NamedRow {
                            id,
                            fields: snapshot
                                .named(&relation, row)?
                                .into_iter()
                                .map(|(k, v)| (k, v.into()))
                                .collect(),
                        })
                    })
                    .collect::<Result<_>>()?;
                Ok(Output::Page {
                    revision: page.revision.to_string(),
                    rows,
                    next: page.next,
                })
            }
        }
    }
}
fn token_eq(a: &str, b: &str) -> bool {
    let mut difference = a.len() ^ b.len();
    for (i, x) in a.bytes().enumerate() {
        difference |= usize::from(x ^ b.as_bytes().get(i).copied().unwrap_or(0));
    }
    difference == 0
}

fn query_output(
    sql_snapshot: SqlSnapshot,
    sql: String,
    params: Vec<SqlWireValue>,
) -> Result<Output> {
    let params = params
        .iter()
        .map(SqlWireValue::sql)
        .collect::<Result<Vec<_>>>()?;
    let table = sql_snapshot.query_with_limits(&sql, &params, QueryLimits::default())?;
    Ok(Output::Query {
        columns: table.columns,
        rows: table
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(SqlWireValue::from_sql).collect())
            .collect::<Result<_>>()?,
    })
}
