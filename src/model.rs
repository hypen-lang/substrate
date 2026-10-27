use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt,
    hash::{Hash, Hasher},
};
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

/// A finite IEEE-754 binary64 value with one representation for zero. Rejecting
/// NaN/infinities and canonicalizing signed zero keeps equality, hashing, JSON,
/// ordered indexes and durable replay consistent.
#[derive(Clone, Copy, Debug)]
pub struct FiniteFloat(f64);
impl FiniteFloat {
    pub fn new(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(Error::Constraint("float must be finite".into()));
        }
        Ok(Self(if value == 0.0 { 0.0 } else { value }))
    }
    pub fn get(self) -> f64 {
        self.0
    }
}
impl TryFrom<f64> for FiniteFloat {
    type Error = Error;
    fn try_from(value: f64) -> Result<Self> {
        Self::new(value)
    }
}
impl PartialEq for FiniteFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for FiniteFloat {}
impl PartialOrd for FiniteFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FiniteFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl Hash for FiniteFloat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
impl Serialize for FiniteFloat {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_f64(self.0)
    }
}
impl<'de> Deserialize<'de> for FiniteFloat {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::new(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
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
    Null,
    Float(FiniteFloat),
}
impl TryFrom<f64> for Value {
    type Error = Error;
    fn try_from(value: f64) -> Result<Self> {
        FiniteFloat::new(value).map(Self::Float)
    }
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
    Float,
    Nullable(Box<DataType>),
}
impl DataType {
    pub fn nullable(self) -> Self {
        Self::Nullable(Box::new(self))
    }
    pub fn is_nullable(&self) -> bool {
        matches!(self, Self::Nullable(_))
    }
    pub fn base_type(&self) -> &Self {
        match self {
            Self::Nullable(inner) => inner.base_type(),
            other => other,
        }
    }
    pub fn reference_target(&self) -> Option<&str> {
        match self.base_type() {
            Self::Ref(target) => Some(target),
            _ => None,
        }
    }
    pub(crate) fn accepts(&self, v: &Value) -> bool {
        if let Self::Nullable(inner) = self {
            return matches!(v, Value::Null) || inner.accepts(v);
        }
        matches!(
            (self, v),
            (Self::Bool, Value::Bool(_))
                | (Self::Int, Value::Int(_))
                | (Self::Text, Value::Text(_))
                | (Self::Ref(_), Value::Ref(_))
                | (Self::Decimal { .. }, Value::Decimal(_))
                | (Self::Timestamp, Value::Timestamp(_))
                | (Self::Bytes, Value::Bytes(_))
                | (Self::Float, Value::Float(_))
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    pub name: String,
    pub data_type: DataType,
    #[serde(default, skip_serializing_if = "OnDelete::is_restrict")]
    pub on_delete: OnDelete,
    #[serde(default, skip_serializing_if = "is_false")]
    pub deferrable: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnDelete {
    #[default]
    Restrict,
    Cascade,
}
impl OnDelete {
    fn is_restrict(&self) -> bool {
        *self == Self::Restrict
    }
}
impl Role {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            on_delete: OnDelete::Restrict,
            deferrable: false,
        }
    }
    pub fn on_delete(mut self, action: OnDelete) -> Self {
        self.on_delete = action;
        self
    }
    pub fn deferrable(mut self) -> Self {
        self.deferrable = true;
        self
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
            if (role.on_delete != OnDelete::Restrict || role.deferrable)
                && role.data_type.reference_target().is_none()
            {
                return Err(Error::Schema(
                    "delete actions and deferral apply only to reference roles".into(),
                ));
            }
            if !identifier(&role.name)
                || role.name.eq_ignore_ascii_case("id")
                || !seen.insert(role.name.to_ascii_lowercase())
            {
                return Err(Error::Schema("invalid, reserved or duplicate role".into()));
            }
            if matches!(role.data_type, DataType::Nullable(ref inner) if inner.is_nullable()) {
                return Err(Error::Schema(
                    "nested nullable types are not supported".into(),
                ));
            }
            if matches!(role.data_type.base_type(), DataType::Decimal { scale } if *scale > 18) {
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
                    min <= max && self.roles[slot].data_type.base_type() == &DataType::Int
                }
                Check::ByteLength { min, max, .. } => {
                    min <= max
                        && matches!(
                            self.roles[slot].data_type.base_type(),
                            DataType::Text | DataType::Bytes
                        )
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
