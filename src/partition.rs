//! Fixed local partition routing. Host provisioning is trusted; application operations
//! remain subject to the supplied sessions' authorization and the single-shard kernel.
use crate::{Error, Id, Result, Row, Session, Snapshot, Transaction, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

/// Immutable format-1 routing declaration. Its ID and slot ordering must be pinned
/// by the host for the lifetime of the data; changing either is not a migration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Declaration {
    format_version: u32,
    id: Id,
    slots: Vec<Id>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingMap(Declaration);
impl RoutingMap {
    pub fn new(id: Id, slots: Vec<Id>) -> Result<Self> {
        Self::validate(Declaration {
            format_version: 1,
            id,
            slots,
        })
    }
    fn validate(value: Declaration) -> Result<Self> {
        let n = value.slots.len();
        if value.format_version != 1 || n == 0 || n > 256 || !n.is_power_of_two() {
            return Err(Error::Schema(
                "routing requires format 1 and 1–256 power-of-two slots".into(),
            ));
        }
        if value
            .slots
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != n
        {
            return Err(Error::Schema("partition slots must be unique".into()));
        }
        Ok(Self(value))
    }
    pub fn from_json(json: &str) -> Result<Self> {
        let value = serde_json::from_str(json)
            .map_err(|e| Error::Schema(format!("routing declaration: {e}")))?;
        Self::validate(value)
    }
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.0).expect("routing declaration serializes")
    }
    /// Canonical declaration digest. Store the expected value in trusted host
    /// configuration and check it before binding; the router does not persist it.
    pub fn fingerprint(&self) -> [u8; 32] {
        Sha256::digest(self.to_json().as_bytes()).into()
    }
    pub fn check_fingerprint(&self, expected: [u8; 32]) -> Result<()> {
        if self.fingerprint() != expected {
            return Err(Error::Schema(
                "routing topology fingerprint mismatch".into(),
            ));
        }
        Ok(())
    }
    pub fn id(&self) -> Id {
        self.0.id
    }
    pub fn partitions(&self) -> &[Id] {
        &self.0.slots
    }
    pub fn partition_for(&self, id: Id) -> Id {
        self.0.slots[usize::from(id.0.as_bytes()[0]) & (self.0.slots.len() - 1)]
    }
    pub fn allocate(&self, partition: Id) -> Result<Id> {
        let slot = self
            .0
            .slots
            .iter()
            .position(|p| *p == partition)
            .ok_or_else(|| Error::Schema("unknown partition".into()))?;
        let mut bytes = *Id::new().0.as_bytes();
        let mask = (self.0.slots.len() - 1) as u8;
        // UUID version and variant occupy bytes 6 and 8, not this free prefix byte.
        bytes[0] = (bytes[0] & !mask) | slot as u8;
        Ok(Id(uuid::Uuid::from_bytes(bytes)))
    }
    fn require(&self, partition: Id, id: Id) -> Result<()> {
        if self.partition_for(id) != partition {
            return Err(Error::Constraint(
                "cross-partition operation is unsupported".into(),
            ));
        }
        Ok(())
    }
    fn values(&self, partition: Id, values: &[Value]) -> Result<()> {
        for value in values {
            if let Value::Ref(id) = value {
                self.require(partition, *id)?;
            }
        }
        Ok(())
    }
    fn row(&self, partition: Id, row: &Row) -> Result<()> {
        self.require(partition, row.id)?;
        self.values(partition, &row.values)
    }
}

/// A trusted host binds pre-authorized sessions, one per physical partition.
/// This does not authenticate principals, provision schemas, or grant privileges.
/// Bind distinct database files and keep raw database/session handles host-only.
#[derive(Clone)]
pub struct PartitionRouter {
    map: Arc<RoutingMap>,
    sessions: BTreeMap<Id, Session>,
}
impl PartitionRouter {
    pub fn bind(map: RoutingMap, bindings: Vec<(Id, Session)>) -> Result<Self> {
        let mut sessions = BTreeMap::new();
        for (id, session) in bindings {
            if !map.partitions().contains(&id) || sessions.insert(id, session).is_some() {
                return Err(Error::Schema(
                    "unknown or duplicate partition binding".into(),
                ));
            }
        }
        if sessions.len() != map.partitions().len() {
            return Err(Error::Schema(
                "every partition requires a session binding".into(),
            ));
        }
        Ok(Self {
            map: Arc::new(map),
            sessions,
        })
    }
    pub fn routing_map(&self) -> &RoutingMap {
        &self.map
    }
    pub fn partition(&self, partition: Id) -> Result<PartitionSession> {
        let session = self
            .sessions
            .get(&partition)
            .ok_or_else(|| Error::Schema("unknown partition".into()))?
            .clone();
        Ok(PartitionSession {
            map: self.map.clone(),
            partition,
            session,
        })
    }
    pub fn for_identity(&self, id: Id) -> PartitionSession {
        self.partition(self.map.partition_for(id))
            .expect("complete validated bindings")
    }
    /// Empty sets have no routing affinity and are rejected. Each returned
    /// transaction has exactly one local atomic outcome and revision.
    pub fn transaction_for(&self, identities: &[Id]) -> Result<PartitionTransaction> {
        let first = identities
            .first()
            .ok_or_else(|| Error::Constraint("routing requires an identity".into()))?;
        let partition = self.map.partition_for(*first);
        for id in identities {
            self.map.require(partition, *id)?;
        }
        Ok(self.partition(partition)?.transaction())
    }
}

#[derive(Clone)]
pub struct PartitionSession {
    map: Arc<RoutingMap>,
    partition: Id,
    session: Session,
}
impl PartitionSession {
    pub fn partition_id(&self) -> Id {
        self.partition
    }
    pub fn allocate_identity(&self) -> Id {
        self.map.allocate(self.partition).expect("bound partition")
    }
    pub fn transaction(&self) -> PartitionTransaction {
        PartitionTransaction {
            map: self.map.clone(),
            partition: self.partition,
            inner: self.session.transaction(),
            failed: false,
        }
    }
    pub fn snapshot(&self) -> PartitionSnapshot {
        PartitionSnapshot {
            map: self.map.clone(),
            partition: self.partition,
            inner: self.session.snapshot(),
        }
    }
}

#[derive(Clone)]
pub struct PartitionSnapshot {
    map: Arc<RoutingMap>,
    partition: Id,
    inner: Snapshot,
}
impl PartitionSnapshot {
    /// This revision is local to the partition, not a global revision.
    pub fn revision(&self) -> u64 {
        self.inner.revision()
    }
    fn rows(&self, rows: Vec<Row>) -> Result<Vec<Row>> {
        for row in &rows {
            self.map.row(self.partition, row)?;
        }
        Ok(rows)
    }
    pub fn scan(&self, relation: &str) -> Result<Vec<Row>> {
        self.rows(self.inner.scan(relation)?)
    }
    pub fn get(&self, relation: &str, id: Id) -> Result<Option<Row>> {
        self.map.require(self.partition, id)?;
        let row = self.inner.get(relation, id)?;
        if let Some(row) = &row {
            self.map.row(self.partition, row)?;
        }
        Ok(row)
    }
    pub fn lookup(&self, relation: &str, role: &str, value: &Value) -> Result<Vec<Row>> {
        self.map
            .values(self.partition, std::slice::from_ref(value))?;
        self.rows(self.inner.lookup(relation, role, value)?)
    }
}

/// Fixed-partition transaction. Any failed wrapper operation poisons the whole
/// transaction, including a routing failure that occurs before engine validation.
/// No SQL/raw-handle escape exists; the first slice supports typed positional rows.
pub struct PartitionTransaction {
    map: Arc<RoutingMap>,
    partition: Id,
    inner: Transaction,
    failed: bool,
}
impl PartitionTransaction {
    fn apply<T>(
        &mut self,
        operation: impl FnOnce(&RoutingMap, Id, &mut Transaction) -> Result<T>,
    ) -> Result<T> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let result = operation(&self.map, self.partition, &mut self.inner);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn insert(&mut self, relation: &str, values: Vec<Value>) -> Result<Id> {
        let id = self.map.allocate(self.partition)?;
        self.insert_with_id(relation, id, values)
    }
    pub fn insert_with_id(&mut self, relation: &str, id: Id, values: Vec<Value>) -> Result<Id> {
        self.apply(|map, partition, tx| {
            map.require(partition, id)?;
            map.values(partition, &values)?;
            tx.insert_with_id(relation, id, values)
        })
    }
    pub fn add_membership(
        &mut self,
        source: &str,
        id: Id,
        target: &str,
        values: Vec<Value>,
    ) -> Result<()> {
        self.apply(|map, partition, tx| {
            map.require(partition, id)?;
            map.values(partition, &values)?;
            tx.add_membership(source, id, target, values)
        })
    }
    pub fn update(&mut self, relation: &str, id: Id, values: Vec<Value>) -> Result<()> {
        self.apply(|map, partition, tx| {
            map.require(partition, id)?;
            map.values(partition, &values)?;
            tx.update(relation, id, values)
        })
    }
    pub fn delete(&mut self, relation: &str, id: Id) -> Result<()> {
        self.apply(|map, partition, tx| {
            map.require(partition, id)?;
            tx.delete(relation, id)
        })
    }
    pub fn get(&mut self, relation: &str, id: Id) -> Result<Option<Row>> {
        self.apply(|map, partition, tx| {
            map.require(partition, id)?;
            let row = tx.get(relation, id)?;
            if let Some(row) = &row {
                map.row(partition, row)?;
            }
            Ok(row)
        })
    }
    pub fn scan(&mut self, relation: &str) -> Result<Vec<Row>> {
        self.apply(|map, partition, tx| {
            let rows = tx.scan(relation)?;
            for row in &rows {
                map.row(partition, row)?;
            }
            Ok(rows)
        })
    }
    pub fn lookup(&mut self, relation: &str, role: &str, value: &Value) -> Result<Vec<Row>> {
        self.apply(|map, partition, tx| {
            map.values(partition, std::slice::from_ref(value))?;
            let rows = tx.lookup(relation, role, value)?;
            for row in &rows {
                map.row(partition, row)?;
            }
            Ok(rows)
        })
    }
    pub fn commit(self) -> Result<u64> {
        if self.failed {
            return Err(Error::Aborted);
        }
        self.inner.commit()
    }
}
