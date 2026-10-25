use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("schema error: {0}")]
    Schema(String),
    #[error("constraint violation: {0}")]
    Constraint(String),
    #[error("access denied")]
    Denied,
    #[error("occurrence not found")]
    NotFound,
    #[error("transaction conflict; retry from a fresh snapshot")]
    Conflict,
    #[error("transaction aborted after an earlier failed operation")]
    Aborted,
    #[error("query error: {0}")]
    Query(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

/// Stable UUID v4. Allocation is independent of physical placement and membership.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Id(pub Uuid);
impl Id {
    pub const fn from_u128(value: u128) -> Self {
        Self(Uuid::from_u128(value))
    }
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}
impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Exact fixed-scale decimals use signed minor units. Scale belongs to the role type.
/// Timestamps are signed UTC microseconds since Unix epoch; leap seconds are excluded.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Text(String),
    Ref(Id),
    Decimal(i64),
    Timestamp(i64),
    Bytes(Vec<u8>),
}
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::Text(s.into())
    }
}
impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Self::Int(n)
    }
}
impl From<Id> for Value {
    fn from(id: Id) -> Self {
        Self::Ref(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DataType {
    Bool,
    Int,
    Text,
    Ref(String),
    Decimal { scale: u8 },
    Timestamp,
    Bytes,
}
impl DataType {
    pub(crate) fn accepts(&self, v: &Value) -> bool {
        matches!(
            (self, v),
            (Self::Bool, Value::Bool(_))
                | (Self::Int, Value::Int(_))
                | (Self::Text, Value::Text(_))
                | (Self::Ref(_), Value::Ref(_))
                | (Self::Decimal { .. }, Value::Decimal(_))
                | (Self::Timestamp, Value::Timestamp(_))
                | (Self::Bytes, Value::Bytes(_))
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    pub name: String,
    pub data_type: DataType,
}
impl Role {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Name-independent role/attribute identity within an immutable relation layout.
/// Persist the relation UUID in schema declarations to reuse identities across builds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RoleId {
    pub relation: Id,
    pub slot: u32,
}

/// Immutable per-row checks. Bounds are inclusive; byte length for Text counts
/// UTF-8 bytes, not Unicode characters. IntRange applies only to Int roles.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Check {
    IntRange {
        role: String,
        #[serde(with = "signed_bound")]
        min: i64,
        #[serde(with = "signed_bound")]
        max: i64,
    },
    ByteLength {
        role: String,
        min: u32,
        max: u32,
    },
}
impl Check {
    pub fn int_range(role: &str, min: i64, max: i64) -> Self {
        Self::IntRange {
            role: role.into(),
            min,
            max,
        }
    }
    pub fn byte_length(role: &str, min: u32, max: u32) -> Self {
        Self::ByteLength {
            role: role.into(),
            min,
            max,
        }
    }
    pub(crate) fn role(&self) -> &str {
        match self {
            Self::IntRange { role, .. } | Self::ByteLength { role, .. } => role,
        }
    }
}
// Canonical strings preserve every i64 bound in JavaScript-authored declarations.
mod signed_bound {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    pub fn serialize<S: Serializer>(value: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        let raw = String::deserialize(d)?;
        let value = raw.parse::<i64>().map_err(D::Error::custom)?;
        if value.to_string() != raw {
            return Err(D::Error::custom("noncanonical signed bound"));
        }
        Ok(value)
    }
}

/// Ordered roles are immutable. Their positions are stable role IDs within a type.
/// Empty roles encode pure membership. Distinct IDs permit duplicate tuples.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    #[serde(default)]
    pub id: Id,
    pub name: String,
    pub roles: Vec<Role>,
    #[serde(default)]
    pub unique: Vec<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<Check>,
}
impl Relation {
    pub fn new(name: &str, roles: Vec<Role>) -> Self {
        Self {
            id: Id::new(),
            name: name.into(),
            roles,
            unique: vec![],
            checks: vec![],
        }
    }
    pub fn check(mut self, check: Check) -> Self {
        self.checks.push(check);
        self
    }
    pub fn role_id(&self, name: &str) -> Result<RoleId> {
        Ok(RoleId {
            relation: self.id,
            slot: u32::try_from(self.role(name)?)
                .map_err(|_| Error::Schema("too many roles".into()))?,
        })
    }
    pub fn unique(mut self, roles: &[&str]) -> Self {
        self.unique
            .push(roles.iter().map(|s| (*s).into()).collect());
        self
    }
    pub(crate) fn role(&self, name: &str) -> Result<usize> {
        self.roles
            .iter()
            .position(|r| r.name == name)
            .ok_or_else(|| Error::Query(format!("unknown role {name}")))
    }
    pub(crate) fn validate(&self, module: &str) -> Result<()> {
        let prefix = format!("{module}.");
        if !self.name.starts_with(&prefix) || !identifier(&self.name[prefix.len()..]) {
            return Err(Error::Schema(
                "relation must belong to module namespace".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for role in &self.roles {
            if !identifier(&role.name)
                || role.name.eq_ignore_ascii_case("id")
                || !seen.insert(role.name.to_ascii_lowercase())
            {
                return Err(Error::Schema("invalid, reserved or duplicate role".into()));
            }
            if matches!(role.data_type, DataType::Decimal { scale } if scale > 18) {
                return Err(Error::Schema("decimal scale must be <= 18".into()));
            }
        }
        if self.checks.len() > 64 {
            return Err(Error::Schema("at most 64 checks per relation".into()));
        }
        let mut seen_checks = BTreeSet::new();
        for check in &self.checks {
            let slot = self
                .role(check.role())
                .map_err(|_| Error::Schema("unknown check role".into()))?;
            let valid = match check {
                Check::IntRange { min, max, .. } => {
                    min <= max && self.roles[slot].data_type == DataType::Int
                }
                Check::ByteLength { min, max, .. } => {
                    min <= max
                        && matches!(self.roles[slot].data_type, DataType::Text | DataType::Bytes)
                }
            };
            if !valid || !seen_checks.insert(check) {
                return Err(Error::Schema(
                    "invalid, incompatible or duplicate check".into(),
                ));
            }
        }
        for key in &self.unique {
            let mut seen = BTreeSet::new();
            if key.is_empty() {
                return Err(Error::Schema("empty unique key".into()));
            }
            for role in key {
                self.role(role)
                    .map_err(|_| Error::Schema("unknown unique role".into()))?;
                if !seen.insert(role) {
                    return Err(Error::Schema("duplicate unique role".into()));
                }
            }
        }
        Ok(())
    }
}
pub(crate) fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Module {
    pub name: String,
    pub version: u64,
    /// Exact installed versions. Version n exports additions from versions 1..=n.
    pub imports: Vec<(String, u64)>,
    /// New names only; old definitions cannot be replaced.
    pub relations: Vec<Relation>,
}
impl Module {
    pub fn new(
        name: &str,
        version: u64,
        imports: Vec<(String, u64)>,
        relations: Vec<Relation>,
    ) -> Self {
        Self {
            name: name.into(),
            version,
            imports,
            relations,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub id: Id,
    pub values: Box<[Value]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RowFilter {
    All,
    Equals { role: String, value: Value },
}
/// Read, reference, and write are distinct capabilities. The same restricted
/// predicate applies to old and new rows; separate policy languages are future work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub read: bool,
    pub write: bool,
    pub reference: bool,
    pub filter: RowFilter,
}
impl Grant {
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            reference: false,
            filter: RowFilter::All,
        }
    }
    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            reference: true,
            filter: RowFilter::All,
        }
    }
    pub fn reference_only() -> Self {
        Self {
            read: false,
            write: false,
            reference: true,
            filter: RowFilter::All,
        }
    }
}
