//! Reuses the established SDK contracts, schema-control policy and protocol DTOs.
//! Only bounded metadata is adapted to the existing control engine; rows stay paged.
use super::*;
use crate::schema_control::*;

impl PagedSession {
    /// Retries only optimistic conflicts. External side effects must stay outside the closure.
    pub fn transact<T>(
        &self,
        policy: RetryPolicy,
        mut f: impl FnMut(&mut PagedTransaction) -> Result<T>,
    ) -> Result<T> {
        if policy.max_attempts == 0 {
            return Err(Error::Constraint("max_attempts must be positive".into()));
        }
        for attempt in 0..policy.max_attempts {
            let mut tx = self.transaction()?;
            let result = f(&mut tx).and_then(|value| tx.commit().map(|_| value));
            match result {
                Err(Error::Conflict) if attempt + 1 < policy.max_attempts => {
                    std::thread::sleep(policy.delay)
                }
                result => return result,
            }
        }
        unreachable!("positive attempts")
    }
    pub fn schema_namespaces(&self) -> Result<Vec<NamespaceInfo>> {
        self.control(|s| s.schema_namespaces())
    }
    pub fn propose_schema(&self, id: Id, module: Module) -> Result<SchemaProposal> {
        self.control(|s| s.propose_schema(id, module.clone()))
    }
    pub fn propose_schema_with_visibility(
        &self,
        id: Id,
        module: Module,
        visibility: BTreeMap<String, Visibility>,
    ) -> Result<SchemaProposal> {
        self.control(|s| s.propose_schema_with_visibility(id, module.clone(), visibility.clone()))
    }
    pub fn schema_proposal(&self, id: Id) -> Result<SchemaProposal> {
        self.control(|s| s.schema_proposal(id))
    }
    pub fn apply_schema_proposal(&self, id: Id, digest: &str) -> Result<SchemaProposal> {
        self.control(|s| s.apply_schema_proposal(id, digest))
    }
    pub fn cancel_schema_proposal(&self, id: Id) -> Result<SchemaProposal> {
        self.control(|s| s.cancel_schema_proposal(id))
    }
    pub fn set_schema_visibility(&self, relation: &str, visibility: Visibility) -> Result<()> {
        self.control(|s| s.set_schema_visibility(relation, visibility))
    }
    pub fn set_schema_lifecycle(&self, relation: &str, state: Lifecycle) -> Result<()> {
        self.control(|s| s.set_schema_lifecycle(relation, state))
    }
    pub fn create_schema_alias(&self, alias: &str, target: &str) -> Result<()> {
        self.control(|s| s.create_schema_alias(alias, target))
    }
}
impl PagedDatabase {
    pub fn configure_namespace(&self, policy: NamespacePolicy) -> Result<NamespaceInfo> {
        self.host_control(|db| db.configure_namespace(policy.clone()))
    }
    pub fn review_schema_proposal(&self, id: Id) -> Result<SchemaProposal> {
        self.host_control(|db| db.review_schema_proposal(id))
    }
    pub fn decide_schema_proposal(
        &self,
        id: Id,
        digest: &str,
        approve: bool,
    ) -> Result<SchemaProposal> {
        self.host_control(|db| db.decide_schema_proposal(id, digest, approve))
    }
}
impl PagedSnapshot {
    pub fn describe(&self, name: &str) -> Result<RuntimeDescriptor> {
        self.fence()?;
        let descriptor = self.metadata_snapshot()?.describe(name)?;
        self.fence()?;
        Ok(descriptor)
    }
    pub fn get_dynamic(&self, descriptor: &RuntimeDescriptor, id: Id) -> Result<Option<Fields>> {
        let current = self.describe(&descriptor.relation().name)?;
        if current.relation().id != descriptor.relation().id
            || current.fingerprint() != descriptor.fingerprint()
        {
            return Err(Error::Schema("runtime descriptor contract mismatch".into()));
        }
        self.get(&descriptor.relation().name, id)?
            .map(|row| self.named(&descriptor.relation().name, row))
            .transpose()
    }
    pub fn manifest(&self) -> Result<SchemaManifest> {
        self.fence()?;
        let manifest = self.metadata_snapshot()?.manifest()?;
        self.fence()?;
        Ok(manifest)
    }
    pub fn plan_manifest(&self, manifest: &SchemaManifest) -> Result<SchemaPlan> {
        self.fence()?;
        let plan = self.metadata_snapshot()?.plan_manifest(manifest)?;
        self.fence()?;
        Ok(plan)
    }
    pub fn check_live(&self) -> Result<()> {
        self.fence()
    }
    pub fn schema(&self, name: &str) -> Result<Relation> {
        self.fence()?;
        let result = self.metadata_snapshot()?.schema(name)?;
        self.fence()?;
        Ok(result)
    }
    pub fn catalog(
        &self,
        options: &crate::catalog::CatalogRequest,
    ) -> Result<crate::catalog::CatalogPage> {
        self.fence()?;
        let result = self.metadata_snapshot()?.catalog(options)?;
        self.fence()?;
        Ok(result)
    }
    pub fn named(&self, name: &str, row: Row) -> Result<Fields> {
        self.fence()?;
        let result = self.metadata_snapshot()?.named(name, row)?;
        self.fence()?;
        Ok(result)
    }
    pub fn page_native(
        &self,
        name: &str,
        predicate: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<PagedPage> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Query("page limit must be 1..=1000".into()));
        }
        match predicate {
            Some((role, value)) => self.lookup_page(name, role, value, after, limit),
            None => self.scan_page(name, after, limit),
        }
    }
    pub fn get_record<R: Record>(&self, id: TypedId<R>) -> Result<Option<R>> {
        let metadata = self.metadata_snapshot()?;
        let def = metadata.schema(R::RELATION)?;
        if def.id != R::TYPE_ID
            || R::FINGERPRINT
                .is_some_and(|f| metadata.state.relations[&R::TYPE_ID].fingerprint != f)
        {
            return Err(Error::Schema("generated schema identity mismatch".into()));
        }
        self.get(R::RELATION, id.raw())?.map(R::decode).transpose()
    }
    pub fn read_nested(
        &self,
        plan: &crate::nested::ReadPlan,
    ) -> Result<crate::nested::NestedResult> {
        let metadata = self.metadata_snapshot()?;
        crate::nested::read_nested_with(
            &metadata,
            plan,
            self.revision(),
            |ids| self.sql_for_relations(ids),
            || self.fence(),
        )
    }
}
impl crate::nested::NestedSql for super::sql::PagedSqlSnapshot {
    fn query_with_limits(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
        limits: QueryLimits,
    ) -> Result<SqlTable> {
        super::sql::PagedSqlSnapshot::query_with_limits(self, sql, params, limits)
    }
}
impl PagedTransaction {
    /// Internal SQL write compilation does not require or imply read access.
    pub(crate) fn writable_schema(&mut self, name: &str) -> Result<Relation> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            if !tx
                .snapshot
                .permission_available(def.id, Permission::Write)?
            {
                return Err(Error::Denied);
            }
            tx.track(b"catalog".to_vec())?;
            tx.track(b"auth".to_vec())?;
            Ok(def.declaration.clone())
        })
    }
    /// Test existence under write policy, including an absent-row OCC dependency.
    pub(crate) fn contains_writable(&mut self, name: &str, id: Id) -> Result<bool> {
        self.mutate(|tx| {
            let def = tx.def(name)?;
            if !tx
                .snapshot
                .permission_available(def.id, Permission::Write)?
            {
                return Err(Error::Denied);
            }
            let Some(row) = tx.raw(def.id, id)? else {
                return Ok(false);
            };
            match tx.permission(def.id, &row, Permission::Write) {
                Ok(()) => Ok(true),
                Err(Error::Denied) => Ok(false),
                Err(error) => Err(error),
            }
        })
    }
    pub fn install_with_visibility(
        &mut self,
        module: Module,
        visibility: Visibility,
    ) -> Result<()> {
        self.install_owned(module, HOST, visibility)
    }
    pub fn install_manifest(&mut self, manifest: &SchemaManifest) -> Result<SchemaPlan> {
        self.install_manifest_with_visibility(manifest, Visibility::Authenticated)
    }
    pub fn install_manifest_with_visibility(
        &mut self,
        manifest: &SchemaManifest,
        visibility: Visibility,
    ) -> Result<SchemaPlan> {
        self.mutate(|tx| {
            if tx.snapshot.principal.is_some() {
                return Err(Error::Denied);
            }
            let metadata = Database::from_state(tx.catalog.state.clone(), None);
            let plan = metadata.admin().snapshot().plan_manifest(manifest)?;
            for module in manifest.canonical()?.modules {
                if !tx
                    .catalog
                    .state
                    .modules
                    .contains_key(&(module.name.clone(), module.version))
                {
                    tx.install_with_visibility(module, visibility)?;
                }
            }
            Ok(plan)
        })
    }
    pub fn insert_dynamic(&mut self, descriptor: &RuntimeDescriptor, fields: Fields) -> Result<Id> {
        self.mutate(|tx| {
            tx.check_dynamic_descriptor(descriptor)?;
            tx.insert_named(&descriptor.relation().name, fields)
        })
    }
    pub fn update_dynamic(
        &mut self,
        descriptor: &RuntimeDescriptor,
        id: Id,
        fields: Fields,
    ) -> Result<()> {
        self.mutate(|tx| {
            tx.check_dynamic_descriptor(descriptor)?;
            tx.update_named(&descriptor.relation().name, id, fields)
        })
    }
    pub fn delete_dynamic(&mut self, descriptor: &RuntimeDescriptor, id: Id) -> Result<()> {
        self.mutate(|tx| {
            tx.check_dynamic_descriptor(descriptor)?;
            tx.delete(&descriptor.relation().name, id)
        })
    }
    fn check_dynamic_descriptor(&mut self, descriptor: &RuntimeDescriptor) -> Result<()> {
        self.check_write_schema(&descriptor.relation().name, descriptor.relation().id)?;
        if self.def(&descriptor.relation().name)?.fingerprint != descriptor.fingerprint() {
            return Err(Error::Schema("runtime descriptor contract mismatch".into()));
        }
        Ok(())
    }
    pub fn check_write_schema(&mut self, relation: &str, schema_id: Id) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(relation)?;
            if !tx
                .snapshot
                .permission_available(def.id, Permission::Write)?
            {
                return Err(Error::Denied);
            }
            if def.id != schema_id {
                return Err(Error::Schema("generated schema identity mismatch".into()));
            }
            Ok(())
        })
    }
    fn named_values(
        &self,
        def: &CompiledRelation,
        fields: impl IntoIterator<Item = (impl Into<String>, Value)>,
        base: Option<&Row>,
    ) -> Result<Vec<Value>> {
        let mut supplied = BTreeMap::new();
        for (name, value) in fields {
            let name = name.into();
            if def.role(&name).is_err() {
                return Err(Error::Constraint(format!(
                    "unknown field {}.{name}",
                    def.name
                )));
            }
            if supplied.insert(name.clone(), value).is_some() {
                return Err(Error::Constraint(format!("duplicate field {name}")));
            }
        }
        def.roles
            .iter()
            .enumerate()
            .map(|(i, role)| {
                supplied
                    .remove(&role.name)
                    .or_else(|| base.map(|r| r.values[i].clone()))
                    .ok_or_else(|| {
                        Error::Constraint(format!(
                            "missing required field {}.{}",
                            def.name, role.name
                        ))
                    })
            })
            .collect()
    }
    pub fn insert_named(
        &mut self,
        relation: &str,
        fields: impl IntoIterator<Item = (impl Into<String>, Value)>,
    ) -> Result<Id> {
        self.mutate(|tx| {
            let def = tx.def(relation)?;
            if !tx
                .snapshot
                .permission_available(def.id, Permission::Write)?
            {
                return Err(Error::Denied);
            }
            let values = tx.named_values(&def, fields, None)?;
            tx.insert(relation, values)
        })
    }
    pub fn update_named(
        &mut self,
        relation: &str,
        id: Id,
        fields: impl IntoIterator<Item = (impl Into<String>, Value)>,
    ) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(relation)?;
            let old = tx
                .raw(def.id, id)?
                .ok_or(if tx.snapshot.principal.is_some() {
                    Error::Denied
                } else {
                    Error::NotFound
                })?;
            tx.permission(def.id, &old, Permission::Write)?;
            let values = tx.named_values(&def, fields, Some(&old))?;
            tx.update(relation, id, values)
        })
    }
    pub fn insert_record<R: Record>(&mut self, record: R) -> Result<TypedId<R>> {
        self.mutate(|tx| {
            let def = tx.def(R::RELATION)?;
            if !tx
                .snapshot
                .permission_available(def.id, Permission::Write)?
            {
                return Err(Error::Denied);
            }
            if def.id != R::TYPE_ID || R::FINGERPRINT.is_some_and(|f| def.fingerprint != f) {
                return Err(Error::Schema("generated schema identity mismatch".into()));
            }
            tx.insert_named(R::RELATION, record.fields())
                .map(TypedId::from_raw)
        })
    }
    pub fn replace_record<R: Record>(&mut self, id: TypedId<R>, record: R) -> Result<()> {
        self.mutate(|tx| {
            let def = tx.def(R::RELATION)?;
            if !tx
                .snapshot
                .permission_available(def.id, Permission::Write)?
            {
                return Err(Error::Denied);
            }
            if def.id != R::TYPE_ID || R::FINGERPRINT.is_some_and(|f| def.fingerprint != f) {
                return Err(Error::Schema("generated schema identity mismatch".into()));
            }
            let values = tx.named_values(&def, record.fields(), None)?;
            tx.update(R::RELATION, id.raw(), values)
        })
    }
}
