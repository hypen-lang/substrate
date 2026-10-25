//! Runtime descriptors use the same compiled schemas and mutation checks as
//! generated records. They do not imply compile-time knowledge of new fields.
use crate::*;
#[derive(Clone, Debug)]
pub struct RuntimeDescriptor {
    relation: Relation,
    fingerprint: String,
}
impl RuntimeDescriptor {
    pub fn relation(&self) -> &Relation {
        &self.relation
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    fn check(&self, snapshot: &Snapshot) -> Result<()> {
        snapshot.check_live()?;
        let def = snapshot
            .state
            .relation(&self.relation.name)
            .map_err(|_| Error::Denied)?;
        if !snapshot.can_discover(def.id) {
            return Err(Error::Denied);
        }
        if def.id != self.relation.id || def.fingerprint != self.fingerprint {
            return Err(Error::Schema("runtime descriptor contract mismatch".into()));
        }
        Ok(())
    }
    fn decode(&self, row: Row) -> Fields {
        self.relation
            .roles
            .iter()
            .map(|r| r.name.clone())
            .zip(row.values)
            .collect()
    }
}
impl Snapshot {
    pub fn describe(&self, name: &str) -> Result<RuntimeDescriptor> {
        let page = self.catalog(&catalog::CatalogRequest {
            relation: Some(name.into()),
            ..Default::default()
        })?;
        let def = &page.relations[0];
        let fingerprint = def.fingerprint.clone().ok_or(Error::Denied)?;
        Ok(RuntimeDescriptor {
            relation: self.state.relations[&def.id].declaration.clone(),
            fingerprint,
        })
    }
    pub fn get_dynamic(&self, descriptor: &RuntimeDescriptor, id: Id) -> Result<Option<Fields>> {
        descriptor.check(self)?;
        Ok(self
            .get(&descriptor.relation.name, id)?
            .map(|row| descriptor.decode(row)))
    }
}
impl Transaction {
    pub fn insert_dynamic(&mut self, descriptor: &RuntimeDescriptor, fields: Fields) -> Result<Id> {
        // Validate through a mutation wrapper so mismatches poison the transaction.
        self.check_dynamic_descriptor(descriptor)?;
        self.insert_named(&descriptor.relation.name, fields)
    }
    pub fn update_dynamic(
        &mut self,
        descriptor: &RuntimeDescriptor,
        id: Id,
        fields: Fields,
    ) -> Result<()> {
        self.check_dynamic_descriptor(descriptor)?;
        self.update_named(&descriptor.relation.name, id, fields)
    }
    pub fn delete_dynamic(&mut self, descriptor: &RuntimeDescriptor, id: Id) -> Result<()> {
        self.check_dynamic_descriptor(descriptor)?;
        self.delete(&descriptor.relation.name, id)
    }
    pub(crate) fn validate_dynamic_descriptor(&self, descriptor: &RuntimeDescriptor) -> Result<()> {
        descriptor.check(&self.snapshot())
    }
}
