//! The same application protocol over paged storage. No engine fallback or
//! application rows are copied into resident storage.
use super::*;
impl Gateway {
    pub(super) fn dispatch_paged(
        &self,
        session: crate::paged::PagedSession,
        command: Command,
    ) -> Result<Output> {
        match command {
            Command::UsingCapability {
                capability,
                command,
            } => {
                if matches!(*command, Command::UsingCapability { .. }) {
                    return Err(Error::Denied);
                }
                self.dispatch_paged(session.with_capability(capability)?, *command)
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
                page: session.snapshot()?.catalog(&options)?,
            }),
            Command::Read { plan } => Ok(Output::Read {
                result: session.snapshot()?.read_nested(&plan)?,
            }),
            Command::Checked { contracts, command } => {
                if contracts.is_empty()
                    || contracts.len() > 100
                    || matches!(*command, Command::Checked { .. })
                {
                    return Err(Error::Schema("invalid contract check".into()));
                }
                let snapshot = session.snapshot()?;
                let metadata = snapshot.metadata_snapshot()?;
                snapshot.check_live()?;
                let mut selected = std::collections::BTreeSet::new();
                for contract in contracts {
                    selected.insert(contract.schema_id);
                    let schema = metadata
                        .state
                        .relation(&contract.relation)
                        .map_err(|_| Error::Denied)?;
                    let principal = metadata.principal.as_deref().expect("restricted");
                    if !metadata.state.can_discover(Some(principal), schema.id)
                        || schema.roles.iter().any(|role| {
                            matches!(role.data_type.base_type(), DataType::Ref(name)
                                if !metadata.can_discover(metadata.state.names[name]))
                        })
                    {
                        // A full contract fingerprint includes reference domains.
                        // Never expose a match/mismatch oracle for domains that
                        // catalog discovery deliberately makes opaque.
                        return Err(Error::Denied);
                    }
                    if schema.id != contract.schema_id
                        || metadata.state.relations[&schema.id].fingerprint != contract.fingerprint
                    {
                        return Err(Error::Schema(format!(
                            "contract mismatch for {}",
                            contract.relation
                        )));
                    }
                }
                if let Command::Query { sql, params } = *command {
                    paged_query_output(snapshot.sql_for_relations(&selected)?, sql, params)
                } else {
                    self.dispatch_paged(session, *command)
                }
            }
            Command::Capabilities => {
                let snapshot = session.snapshot()?;
                let metadata = snapshot.metadata_snapshot()?;
                snapshot.check_live()?;
                let p = metadata.principal.as_deref().expect("restricted gateway");
                let relations = metadata
                    .state
                    .relations
                    .values()
                    // The legacy full-declaration shape cannot redact reference
                    // domains. Omit such families; Catalog exposes safe opaque roles.
                    .filter(|def| metadata.can_discover(def.id) && def.roles.iter().all(|r| {
                        !matches!(r.data_type.base_type(), DataType::Ref(name) if !metadata.can_discover(metadata.state.names[name]))
                    }))
                    .map(|def| {
                        let grant = metadata.state.grant(p, def.id);
                        Capability {
                            relation: def.declaration.clone(),
                            read: metadata.state.can_read(Some(p), def.id),
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
                    sql_writes: true,
                    sql_write_features: sql_dml::FEATURES.iter().map(|s| (*s).to_owned()).collect(),
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
            Command::Query { sql, params } => {
                paged_query_output(session.snapshot()?.sql()?, sql, params)
            }
            Command::Execute { sql, params } => sql_dml::paged(&session, &sql, &params),
            Command::Page {
                relation,
                schema_id,
                after,
                limit,
                expected_revision,
                filter,
            } => {
                let snapshot = session.snapshot()?;
                if snapshot.schema(&relation)?.id != schema_id {
                    return Err(Error::Schema("generated schema identity mismatch".into()));
                }
                if expected_revision.is_some_and(|r| r != snapshot.revision().to_string()) {
                    return Err(Error::Conflict);
                }
                let filter = filter
                    .map(|(r, v)| v.value(&BTreeMap::new()).map(|v| (r, v)))
                    .transpose()?;
                let page = snapshot.page_native(
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
                    revision: snapshot.revision().to_string(),
                    rows,
                    next: page.next,
                })
            }
        }
    }
}
fn paged_query_output(
    sql_snapshot: crate::paged::PagedSqlSnapshot,
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
