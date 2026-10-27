//! Ordered index keys and versioned schema-directed packed rows. Index encodings
//! preserve `Value::Ord`; packed row payloads do not contain JSON or type names.
use crate::{DataType, Error, FiniteFloat, Id, Result, Row, Value, compiled::CompiledRelation};

const MAGIC: &[u8; 4] = b"PG01";
const EXTENDED_MAGIC: &[u8; 4] = b"PG02";
const HEADER: usize = 20;

fn row_magic(def: &CompiledRelation) -> &'static [u8; 4] {
    if def.extended_encoding {
        EXTENDED_MAGIC
    } else {
        MAGIC
    }
}

#[cold]
#[inline(never)]
fn invalid() -> Error {
    Error::Storage("invalid or unsupported paged row encoding".into())
}

pub(super) fn row_key(tid: Id, id: Id) -> Vec<u8> {
    row_key_array(tid, id).to_vec()
}

pub(super) fn row_key_array(tid: Id, id: Id) -> [u8; 32] {
    let mut out = [0; 32];
    out[..16].copy_from_slice(tid.0.as_bytes());
    out[16..].copy_from_slice(id.0.as_bytes());
    out
}

// Escaping 0 as 00ff and terminating with 0000 preserves byte ordering,
// including embedded zeros and strict prefixes. UTF-8 byte order matches str Ord.
fn ordered_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        out.push(byte);
        if byte == 0 {
            out.push(0xff);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

#[cfg(test)]
pub(super) fn value_key(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    append_value(&mut out, value);
    out
}

fn append_value(out: &mut Vec<u8>, value: &Value) {
    // Tags follow declaration order in Value. Zero is reserved for tuple end.
    match value {
        Value::Bool(value) => out.extend_from_slice(&[1, u8::from(*value)]),
        Value::Int(value) => {
            out.push(2);
            out.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes());
        }
        Value::Text(value) => {
            out.push(3);
            ordered_bytes(out, value.as_bytes());
        }
        Value::Ref(value) => {
            out.push(4);
            out.extend_from_slice(value.0.as_bytes());
        }
        Value::Decimal(value) => {
            out.push(5);
            out.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes());
        }
        Value::Timestamp(value) => {
            out.push(6);
            out.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes());
        }
        Value::Bytes(value) => {
            out.push(7);
            ordered_bytes(out, value);
        }
        Value::Null => out.push(8),
        Value::Float(value) => {
            out.push(9);
            let bits = value.get().to_bits();
            let ordered = if bits >> 63 != 0 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
    }
}

pub(super) fn index_prefix(tid: Id, slot: usize, value: &Value) -> Vec<u8> {
    // Fit the prefix and the largest fixed scalar (a typed reference).
    let mut out = Vec::with_capacity(41);
    out.extend_from_slice(tid.0.as_bytes());
    out.extend_from_slice(&(slot as u64).to_be_bytes());
    append_value(&mut out, value);
    out
}

pub(super) fn index_key(tid: Id, slot: usize, value: &Value, id: Id) -> Vec<u8> {
    // Reserve the occurrence suffix too, avoiding two growth operations for
    // ordinary integer keys. Variable-length values can still grow normally.
    let mut out = Vec::with_capacity(57);
    index_key_into(&mut out, tid, slot, value, id);
    out
}

/// Reuse a scan's bounded key scratch while checking every stored index entry.
pub(super) fn index_key_into(out: &mut Vec<u8>, tid: Id, slot: usize, value: &Value, id: Id) {
    out.clear();
    out.extend_from_slice(tid.0.as_bytes());
    out.extend_from_slice(&(slot as u64).to_be_bytes());
    append_value(out, value);
    out.extend_from_slice(id.0.as_bytes());
}

pub(super) fn unique_key(tid: Id, constraint: usize, values: &[Value]) -> Vec<u8> {
    // The common single reference key needs 24 prefix + 17 value + 1 terminator
    // bytes. Reserving only the prefix made even integer keys reallocate.
    let mut out = Vec::with_capacity(42);
    out.extend_from_slice(tid.0.as_bytes());
    out.extend_from_slice(&(constraint as u64).to_be_bytes());
    for value in values {
        append_value(&mut out, value);
    }
    // Terminates the tuple, preserving even unequal-arity Vec<Value> ordering.
    out.push(0);
    out
}

pub(super) fn unique_key_into(out: &mut Vec<u8>, tid: Id, constraint: usize, values: &[Value]) {
    out.clear();
    out.extend_from_slice(tid.0.as_bytes());
    out.extend_from_slice(&(constraint as u64).to_be_bytes());
    for value in values {
        append_value(out, value);
    }
    out.push(0);
}

/// Common singleton keys fit on the stack. Variable-length values keep the
/// canonical general encoder; the returned prefix has identical byte ordering.
pub(super) fn unique_scalar_key(
    tid: Id,
    constraint: usize,
    value: &Value,
) -> Option<([u8; 42], usize)> {
    let mut out = [0; 42];
    out[..16].copy_from_slice(tid.0.as_bytes());
    out[16..24].copy_from_slice(&(constraint as u64).to_be_bytes());
    let end = match value {
        Value::Bool(value) => {
            out[24] = 1;
            out[25] = u8::from(*value);
            26
        }
        Value::Int(number) | Value::Decimal(number) | Value::Timestamp(number) => {
            out[24] = match value {
                Value::Int(_) => 2,
                Value::Decimal(_) => 5,
                Value::Timestamp(_) => 6,
                _ => unreachable!(),
            };
            out[25..33].copy_from_slice(&((*number as u64) ^ (1 << 63)).to_be_bytes());
            33
        }
        Value::Ref(value) => {
            out[24] = 4;
            out[25..41].copy_from_slice(value.0.as_bytes());
            41
        }
        Value::Null => {
            out[24] = 8;
            25
        }
        Value::Float(value) => {
            out[24] = 9;
            let bits = value.get().to_bits();
            let ordered = if bits >> 63 != 0 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            out[25..33].copy_from_slice(&ordered.to_be_bytes());
            33
        }
        Value::Text(_) | Value::Bytes(_) => return None,
    };
    // The zero-filled last byte is the tuple terminator.
    Some((out, end + 1))
}

pub(super) fn incoming_prefix(target_tid: Id, target_id: Id) -> Vec<u8> {
    row_key(target_tid, target_id)
}

pub(super) fn incoming_key(
    target_tid: Id,
    target_id: Id,
    source_tid: Id,
    source_id: Id,
    slot: usize,
) -> Vec<u8> {
    let mut out = incoming_prefix(target_tid, target_id);
    out.extend_from_slice(source_tid.0.as_bytes());
    out.extend_from_slice(source_id.0.as_bytes());
    out.extend_from_slice(&(slot as u64).to_be_bytes());
    out
}

/// Exclusive end of all byte keys beginning with prefix. None denotes an
/// unbounded upper end, including the empty prefix selecting the whole table.
pub(super) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != u8::MAX {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}

/// Exact packed length without allocating or serializing a temporary row.
pub(super) fn row_encoded_len(def: &CompiledRelation, row: &Row) -> Result<usize> {
    if row.values.len() != def.roles.len() {
        return Err(invalid());
    }
    u32::try_from(def.roles.len()).map_err(|_| invalid())?;
    let mut body_len = 0usize;
    for (role, value) in def.roles.iter().zip(row.values.iter()) {
        if !role.data_type.accepts(value) {
            return Err(invalid());
        }
        let bytes = match value {
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Decimal(_) | Value::Timestamp(_) | Value::Float(_) => 8,
            Value::Null => 0,
            Value::Ref(_) => 16,
            Value::Text(value) => value.len().checked_add(4).ok_or_else(invalid)?,
            Value::Bytes(value) => value.len().checked_add(4).ok_or_else(invalid)?,
        };
        body_len = body_len
            .checked_add(bytes)
            .and_then(|n| n.checked_add(usize::from(role.data_type.is_nullable())))
            .ok_or_else(invalid)?;
    }
    u32::try_from(body_len).map_err(|_| invalid())?;
    HEADER.checked_add(body_len).ok_or_else(invalid)
}

pub(super) fn encode_row(def: &CompiledRelation, row: &Row, revision: u64) -> Result<Vec<u8>> {
    let encoded_len = row_encoded_len(def, row)?;
    let body_len = encoded_len - HEADER;
    let count = u32::try_from(def.roles.len()).map_err(|_| invalid())?;
    let wire_len = u32::try_from(body_len).map_err(|_| invalid())?;
    let mut out = Vec::new();
    out.try_reserve_exact(HEADER.checked_add(body_len).ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    out.extend_from_slice(row_magic(def));
    out.extend_from_slice(&revision.to_be_bytes());
    out.extend_from_slice(&wire_len.to_be_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    for (role, value) in def.roles.iter().zip(row.values.iter()) {
        if role.data_type.is_nullable() {
            out.push(u8::from(!matches!(value, Value::Null)));
        }
        match value {
            Value::Null => {}
            Value::Float(value) => out.extend_from_slice(&value.get().to_bits().to_be_bytes()),
            Value::Bool(value) => out.push(u8::from(*value)),
            Value::Int(value) | Value::Decimal(value) | Value::Timestamp(value) => {
                out.extend_from_slice(&value.to_be_bytes());
            }
            Value::Ref(value) => out.extend_from_slice(value.0.as_bytes()),
            Value::Text(value) => append_variable(&mut out, value.as_bytes())?,
            Value::Bytes(value) => append_variable(&mut out, value)?,
        }
    }
    Ok(out)
}

fn append_variable(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    out.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| invalid())?
            .to_be_bytes(),
    );
    out.extend_from_slice(value);
    Ok(())
}

struct Input<'a> {
    rest: &'a [u8],
}
impl<'a> Input<'a> {
    #[inline(always)]
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let value = self.rest.get(..len).ok_or_else(invalid)?;
        self.rest = &self.rest[len..];
        Ok(value)
    }
    #[inline(always)]
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| invalid())
    }
    #[inline(always)]
    fn variable(&mut self) -> Result<&'a [u8]> {
        let len = u32::from_be_bytes(self.fixed()?) as usize;
        self.take(len)
    }
}

#[inline(always)]
fn header<'a>(bytes: &'a [u8], expected: Option<&[u8; 4]>) -> Result<(u64, u32, Input<'a>)> {
    let mut input = Input { rest: bytes };
    let magic = input.take(MAGIC.len())?;
    if expected.map_or(magic != MAGIC && magic != EXTENDED_MAGIC, |expected| {
        magic != expected
    }) {
        return Err(invalid());
    }
    let revision = u64::from_be_bytes(input.fixed()?);
    let body_len = u32::from_be_bytes(input.fixed()?) as usize;
    let count = u32::from_be_bytes(input.fixed()?);
    if input.rest.len() != body_len || count as usize > body_len || (count == 0 && body_len != 0) {
        return Err(invalid());
    }
    Ok((revision, count, input))
}

/// Checks framing/version, not schema-dependent field validity. Cache miss
/// callers must use decode_row or validate_row to validate the complete typed record.
pub(super) fn row_revision(bytes: &[u8]) -> Result<u64> {
    header(bytes, None).map(|(revision, _, _)| revision)
}

fn typed_input<'a>(def: &CompiledRelation, bytes: &'a [u8]) -> Result<(u64, Input<'a>)> {
    let (revision, count, input) = header(bytes, Some(row_magic(def)))?;
    if count as usize != def.roles.len() {
        return Err(invalid());
    }
    // Every supported role requires at least one byte; reject impossible counts
    // before allocating, even if a caller supplies an excessively large schema.
    if def.roles.len() > input.rest.len() {
        return Err(invalid());
    }
    Ok((revision, input))
}

/// Borrowed values let validation and owned decoding use exactly one parser.
/// The no-op validation visitor never allocates strings, byte buffers or rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum PackedValue<'a> {
    Bool(bool),
    Int(i64),
    Text(&'a str),
    Ref(Id),
    Decimal(i64),
    Timestamp(i64),
    Bytes(&'a [u8]),
    Null,
    Float(FiniteFloat),
}

impl<'a> PackedValue<'a> {
    pub(super) fn from_owned(value: &'a Value) -> Self {
        match value {
            Value::Bool(value) => Self::Bool(*value),
            Value::Int(value) => Self::Int(*value),
            Value::Text(value) => Self::Text(value),
            Value::Ref(value) => Self::Ref(*value),
            Value::Decimal(value) => Self::Decimal(*value),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Bytes(value) => Self::Bytes(value),
            Value::Null => Self::Null,
            Value::Float(value) => Self::Float(*value),
        }
    }

    pub(super) fn cmp_owned(&self, other: &Value) -> std::cmp::Ordering {
        self.cmp(&PackedValue::from_owned(other))
    }

    pub(super) fn into_owned(self) -> Value {
        match self {
            Self::Bool(value) => Value::Bool(value),
            Self::Int(value) => Value::Int(value),
            Self::Decimal(value) => Value::Decimal(value),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Ref(value) => Value::Ref(value),
            Self::Text(value) => Value::Text(value.to_owned()),
            Self::Bytes(value) => Value::Bytes(value.to_vec()),
            Self::Null => Value::Null,
            Self::Float(value) => Value::Float(value),
        }
    }
}

#[derive(Clone, Copy)]
enum FixedKind {
    Bool,
    Int,
    Ref,
    Decimal,
    Timestamp,
}

#[derive(Clone, Copy)]
struct FixedField {
    offset: usize,
    kind: FixedKind,
}

/// A compiled, bounded physical decoder for the existing PG01 encoding. It is
/// not another row format: unsupported schemas keep the general decoder.
pub(super) struct FixedPackedLayout {
    fields: Box<[FixedField]>,
    bool_offsets: Box<[usize]>,
    body_len: usize,
}

#[derive(Clone, Copy)]
enum ValidationKind {
    Bool,
    Text,
    Bytes,
}

struct ValidationStep {
    prefix: usize,
    kind: ValidationKind,
}

/// Schema-directed validation without materializing values. Consecutive fields
/// whose every bit pattern is valid are one bounded skip; bool and UTF-8 fields
/// still receive their complete checks, including unprojected target columns.
/// The plan is bounded by schema width and retains no row or page bytes.
pub(super) struct PackedValidationLayout {
    fields: usize,
    steps: Box<[ValidationStep]>,
    tail: usize,
}

impl PackedValidationLayout {
    pub(super) fn compile(def: &CompiledRelation) -> Option<Self> {
        if def.roles.len() > FixedPackedLayout::MAX_FIELDS {
            return None;
        }
        let mut steps = Vec::new();
        steps.try_reserve_exact(def.roles.len()).ok()?;
        let mut prefix = 0usize;
        for role in &def.roles {
            let kind = match role.data_type {
                DataType::Int | DataType::Decimal { .. } | DataType::Timestamp => {
                    prefix = prefix.checked_add(8)?;
                    continue;
                }
                DataType::Ref(_) => {
                    prefix = prefix.checked_add(16)?;
                    continue;
                }
                DataType::Bool => ValidationKind::Bool,
                DataType::Text => ValidationKind::Text,
                DataType::Bytes => ValidationKind::Bytes,
                DataType::Float | DataType::Nullable(_) => return None,
            };
            steps.push(ValidationStep { prefix, kind });
            prefix = 0;
        }
        Some(Self {
            fields: def.roles.len(),
            steps: steps.into_boxed_slice(),
            tail: prefix,
        })
    }

    /// As with validate_row, the caller checks row-size and revision limits.
    #[inline(always)]
    pub(super) fn validate(&self, bytes: &[u8]) -> Result<u64> {
        let (revision, count, mut input) = header(bytes, Some(MAGIC))?;
        if count as usize != self.fields {
            return Err(invalid());
        }
        for step in &self.steps {
            input.take(step.prefix)?;
            match step.kind {
                ValidationKind::Bool => {
                    if input.take(1)?[0] > 1 {
                        return Err(invalid());
                    }
                }
                ValidationKind::Text => {
                    std::str::from_utf8(input.variable()?).map_err(|_| invalid())?;
                }
                ValidationKind::Bytes => {
                    input.variable()?;
                }
            }
        }
        if input.rest.len() != self.tail {
            return Err(invalid());
        }
        Ok(revision)
    }
}

impl FixedPackedLayout {
    pub(super) const MAX_FIELDS: usize = 64;

    pub(super) fn compile(def: &CompiledRelation) -> Option<Self> {
        if def.roles.len() > Self::MAX_FIELDS {
            return None;
        }
        let mut fields = Vec::new();
        let mut bool_offsets = Vec::new();
        fields.try_reserve_exact(def.roles.len()).ok()?;
        bool_offsets.try_reserve_exact(def.roles.len()).ok()?;
        let mut offset = HEADER;
        for role in &def.roles {
            let (kind, width) = match role.data_type {
                DataType::Bool => {
                    bool_offsets.push(offset);
                    (FixedKind::Bool, 1)
                }
                DataType::Int => (FixedKind::Int, 8),
                DataType::Ref(_) => (FixedKind::Ref, 16),
                DataType::Decimal { .. } => (FixedKind::Decimal, 8),
                DataType::Timestamp => (FixedKind::Timestamp, 8),
                DataType::Text | DataType::Bytes | DataType::Float | DataType::Nullable(_) => {
                    return None;
                }
            };
            fields.push(FixedField { offset, kind });
            offset = offset.checked_add(width)?;
        }
        Some(Self {
            fields: fields.into_boxed_slice(),
            bool_offsets: bool_offsets.into_boxed_slice(),
            body_len: offset - HEADER,
        })
    }

    pub(super) fn encoded_len(&self) -> usize {
        HEADER + self.body_len
    }

    /// Validates every byte's schema-dependent interpretation before exposing
    /// any projected value. All bit patterns are valid integers and identities;
    /// booleans still need explicit checks even when the query never reads them.
    /// The caller must enforce its row-byte limit and snapshot revision bound.
    #[inline(always)]
    pub(super) fn read<'layout, 'bytes>(
        &'layout self,
        bytes: &'bytes [u8],
    ) -> Result<FixedPackedRow<'layout, 'bytes>> {
        let (revision, count, _input) = header(bytes, Some(MAGIC))?;
        if count as usize != self.fields.len() || bytes.len() != self.encoded_len() {
            return Err(invalid());
        }
        for &offset in &self.bool_offsets {
            if bytes[offset] > 1 {
                return Err(invalid());
            }
        }
        Ok(FixedPackedRow {
            layout: self,
            bytes,
            revision,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct FixedPackedRow<'layout, 'bytes> {
    layout: &'layout FixedPackedLayout,
    bytes: &'bytes [u8],
    revision: u64,
}

impl<'bytes> FixedPackedRow<'_, 'bytes> {
    /// Compare a typed binding without constructing temporary tagged values on
    /// the numeric hot path. Mismatched kinds retain the canonical total order.
    #[inline]
    pub(super) fn compare(&self, slot: usize, value: &Value) -> Option<std::cmp::Ordering> {
        let field = self.layout.fields.get(slot)?;
        let number = match (field.kind, value) {
            (FixedKind::Bool, Value::Bool(value)) => Some(i64::from(*value)),
            (FixedKind::Int, Value::Int(value))
            | (FixedKind::Decimal, Value::Decimal(value))
            | (FixedKind::Timestamp, Value::Timestamp(value)) => Some(*value),
            _ => None,
        };
        if let Some(value) = number {
            return Some(self.number(slot)?.cmp(&value));
        }
        Some(self.value(slot)?.cmp_owned(value))
    }

    #[inline]
    pub(super) fn revision(&self) -> u64 {
        self.revision
    }

    /// Read the executor's numeric representation directly. The layout's field
    /// kind is still checked; a reference cannot be reinterpreted as a number.
    #[inline]
    pub(super) fn number(&self, slot: usize) -> Option<i64> {
        let field = self.layout.fields.get(slot)?;
        let bytes = &self.bytes[field.offset..];
        match field.kind {
            FixedKind::Bool => Some(i64::from(bytes[0])),
            FixedKind::Int | FixedKind::Decimal | FixedKind::Timestamp => Some(i64::from_be_bytes(
                bytes[..8]
                    .try_into()
                    .expect("validated fixed numeric value"),
            )),
            FixedKind::Ref => None,
        }
    }

    #[inline(always)]
    pub(super) fn reference(&self, slot: usize) -> Option<Id> {
        let field = self.layout.fields.get(slot)?;
        if !matches!(field.kind, FixedKind::Ref) {
            return None;
        }
        Some(Id(uuid::Uuid::from_bytes(
            self.bytes[field.offset..field.offset + 16]
                .try_into()
                .expect("validated fixed reference"),
        )))
    }

    #[inline]
    pub(super) fn value(&self, slot: usize) -> Option<PackedValue<'bytes>> {
        let field = self.layout.fields.get(slot)?;
        let bytes = &self.bytes[field.offset..];
        Some(match field.kind {
            FixedKind::Bool => PackedValue::Bool(bytes[0] != 0),
            FixedKind::Ref => PackedValue::Ref(Id(uuid::Uuid::from_bytes(
                bytes[..16].try_into().expect("validated fixed reference"),
            ))),
            kind => {
                let value = i64::from_be_bytes(
                    bytes[..8]
                        .try_into()
                        .expect("validated fixed numeric value"),
                );
                match kind {
                    FixedKind::Int => PackedValue::Int(value),
                    FixedKind::Decimal => PackedValue::Decimal(value),
                    FixedKind::Timestamp => PackedValue::Timestamp(value),
                    FixedKind::Bool | FixedKind::Ref => unreachable!(),
                }
            }
        })
    }
}

fn walk_values<'a>(
    def: &CompiledRelation,
    mut input: Input<'a>,
    mut visit: impl FnMut(PackedValue<'a>),
) -> Result<()> {
    for role in &def.roles {
        if role.data_type.is_nullable() {
            match input.take(1)?[0] {
                0 => {
                    visit(PackedValue::Null);
                    continue;
                }
                1 => {}
                _ => return Err(invalid()),
            }
        }
        let value = match role.data_type.base_type() {
            DataType::Bool => match input.take(1)?[0] {
                0 => PackedValue::Bool(false),
                1 => PackedValue::Bool(true),
                _ => return Err(invalid()),
            },
            DataType::Int => PackedValue::Int(i64::from_be_bytes(input.fixed()?)),
            DataType::Decimal { .. } => PackedValue::Decimal(i64::from_be_bytes(input.fixed()?)),
            DataType::Timestamp => PackedValue::Timestamp(i64::from_be_bytes(input.fixed()?)),
            DataType::Ref(_) => PackedValue::Ref(Id(uuid::Uuid::from_bytes(input.fixed()?))),
            DataType::Text => {
                PackedValue::Text(std::str::from_utf8(input.variable()?).map_err(|_| invalid())?)
            }
            DataType::Bytes => PackedValue::Bytes(input.variable()?),
            DataType::Float => {
                let bits = u64::from_be_bytes(input.fixed()?);
                let value = FiniteFloat::new(f64::from_bits(bits)).map_err(|_| invalid())?;
                if value.get().to_bits() != bits {
                    return Err(invalid());
                }
                PackedValue::Float(value)
            }
            DataType::Nullable(_) => return Err(invalid()),
        };
        visit(value);
    }
    if !input.rest.is_empty() {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
fn decode_packed_values<'a>(
    def: &CompiledRelation,
    bytes: &'a [u8],
    values: &mut [PackedValue<'a>],
) -> Result<u64> {
    let (revision, input) = typed_input(def, bytes)?;
    if values.len() != def.roles.len() {
        return Err(invalid());
    }
    let mut slots = values.iter_mut();
    walk_values(def, input, |value| {
        *slots.next().expect("validated borrowed row arity") = value;
    })?;
    Ok(revision)
}

/// Validate every schema-directed field without materializing owned values.
/// Returns the encoded revision, just like decode_row: callers still enforce
/// snapshot revision and configured row-byte bounds.
pub(super) fn validate_row(def: &CompiledRelation, bytes: &[u8]) -> Result<u64> {
    let (revision, input) = typed_input(def, bytes)?;
    walk_values(def, input, |_| {})?;
    Ok(revision)
}

pub(super) fn decode_row(def: &CompiledRelation, id: Id, bytes: &[u8]) -> Result<(u64, Row)> {
    let (revision, input) = typed_input(def, bytes)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(def.roles.len())
        .map_err(|_| invalid())?;
    walk_values(def, input, |value| values.push(value.into_owned()))?;
    Ok((
        revision,
        Row {
            id,
            values: values.into_boxed_slice(),
        },
    ))
}

/// Decode into a scan's single retained row allocation. Variable fields replace
/// their old payload rather than retaining historical capacities: alternating
/// large values in different columns must not grow the scan's memory footprint.
/// On error the scratch row may be partly overwritten and must not be exposed.
pub(super) fn decode_row_into(
    def: &CompiledRelation,
    id: Id,
    bytes: &[u8],
    row: &mut Row,
) -> Result<u64> {
    let (revision, input) = typed_input(def, bytes)?;
    if row.values.len() != def.roles.len() {
        return Err(invalid());
    }
    let mut slots = row.values.iter_mut();
    walk_values(def, input, |value| {
        // walk_values produces exactly one value per compiled role.
        *slots.next().expect("validated scratch row arity") = value.into_owned();
    })?;
    row.id = id;
    Ok(revision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Relation, Role};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn scalar_probe_keys_match_canonical_encoding(number in any::<i64>(), bits in any::<u64>(), id in any::<u128>(), constraint in any::<usize>()) {
            let mut values = vec![Value::Bool(false), Value::Bool(true), Value::Int(number), Value::Decimal(number), Value::Timestamp(number), Value::Ref(Id::from_u128(id)), Value::Null];
            if let Ok(value) = FiniteFloat::new(f64::from_bits(bits)) {
                values.push(Value::Float(value));
            }
            for value in values {
                let (bytes, len) = unique_scalar_key(Id::from_u128(id), constraint, &value).unwrap();
                prop_assert_eq!(&bytes[..len], unique_key(Id::from_u128(id), constraint, &[value]));
            }
            prop_assert!(unique_scalar_key(Id::from_u128(id), constraint, &Value::Text("a\\0b".into())).is_none());
            prop_assert!(unique_scalar_key(Id::from_u128(id), constraint, &Value::Bytes(vec![0,255])).is_none());
        }
    }

    fn arbitrary_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            proptest::collection::vec(any::<char>(), 0..32)
                .prop_map(|chars| Value::Text(chars.into_iter().collect())),
            any::<u128>().prop_map(|id| Value::Ref(Id::from_u128(id))),
            any::<i64>().prop_map(Value::Decimal),
            any::<i64>().prop_map(Value::Timestamp),
            proptest::collection::vec(any::<u8>(), 0..64).prop_map(Value::Bytes),
            Just(Value::Null),
            any::<f64>().prop_filter_map("finite floats", |v| Value::try_from(v).ok()),
        ]
    }

    fn arbitrary_fixed_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<u128>().prop_map(|id| Value::Ref(Id::from_u128(id))),
            any::<i64>().prop_map(Value::Decimal),
            any::<i64>().prop_map(Value::Timestamp),
        ]
    }
    fn definition(values: &[Value]) -> CompiledRelation {
        let roles = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                Role::new(
                    &format!("field{index}"),
                    match value {
                        Value::Bool(_) => DataType::Bool,
                        Value::Int(_) => DataType::Int,
                        Value::Text(_) => DataType::Text,
                        Value::Ref(_) => DataType::Ref("test.Other".into()),
                        Value::Decimal(_) => DataType::Decimal { scale: 2 },
                        Value::Timestamp(_) => DataType::Timestamp,
                        Value::Bytes(_) => DataType::Bytes,
                        Value::Null => DataType::Int.nullable(),
                        Value::Float(_) => DataType::Float,
                    },
                )
            })
            .collect();
        CompiledRelation::new(Relation::new("test.Record", roles))
    }
    proptest! {
        #[test]
        fn compiled_validation_matches_full_decoder(
            values in prop::collection::vec(arbitrary_value().prop_filter("PG01 field", |value| !matches!(value, Value::Null | Value::Float(_))), 0..32),
            revision in any::<u64>(),
        ) {
            let def = definition(&values);
            let layout = PackedValidationLayout::compile(&def).unwrap();
            let row = Row { id: Id::from_u128(1), values: values.into_boxed_slice() };
            let bytes = encode_row(&def, &row, revision).unwrap();
            prop_assert_eq!(layout.validate(&bytes).unwrap(), revision);
            for length in 0..bytes.len() {
                prop_assert_eq!(layout.validate(&bytes[..length]).is_ok(), validate_row(&def, &bytes[..length]).is_ok());
            }
            for index in 0..bytes.len() {
                let mut corrupted = bytes.clone();
                corrupted[index] ^= 0xff;
                prop_assert_eq!(layout.validate(&corrupted).is_ok(), validate_row(&def, &corrupted).is_ok());
            }
        }

        #[test]
        fn borrowed_values_preserve_owned_order(a in arbitrary_value(), b in arbitrary_value()) {
            let packed = PackedValue::from_owned(&a);
            prop_assert_eq!(packed.cmp_owned(&b), a.cmp(&b));
            prop_assert_eq!(packed.into_owned(), a);
        }

        #[test]
        fn fixed_projection_matches_complete_decoder(
            values in proptest::collection::vec(arbitrary_fixed_value(), 0..=64),
            revision in any::<u64>(),
        ) {
            let def = definition(&values);
            let row = Row { id: Id::from_u128(55), values: values.into_boxed_slice() };
            let bytes = encode_row(&def, &row, revision).unwrap();
            let layout = FixedPackedLayout::compile(&def).unwrap();
            prop_assert_eq!(layout.encoded_len(), bytes.len());
            let fixed = layout.read(&bytes).unwrap();
            prop_assert_eq!(fixed.revision(), revision);
            let decoded = decode_row(&def, row.id, &bytes).unwrap().1;
            for (slot, expected) in decoded.values.iter().enumerate() {
                prop_assert_eq!(fixed.value(slot).unwrap().into_owned(), expected.clone());
                let number = match expected {
                    Value::Bool(value) => Some(i64::from(*value)),
                    Value::Int(value) | Value::Decimal(value) | Value::Timestamp(value) => Some(*value),
                    _ => None,
                };
                let reference = match expected { Value::Ref(id) => Some(*id), _ => None };
                prop_assert_eq!(fixed.number(slot), number);
                prop_assert_eq!(fixed.reference(slot), reference);
            }
            prop_assert!(fixed.value(decoded.values.len()).is_none());
            prop_assert!(fixed.value(usize::MAX).is_none());
            prop_assert!(fixed.number(decoded.values.len()).is_none());
            prop_assert!(fixed.number(usize::MAX).is_none());
            prop_assert!(fixed.reference(decoded.values.len()).is_none());
            prop_assert!(fixed.reference(usize::MAX).is_none());
        }

        #[test]
        fn fixed_projection_rejects_exactly_the_same_malformed_records(
            values in proptest::collection::vec(arbitrary_fixed_value(), 0..=64),
            revision in any::<u64>(),
            changes in proptest::collection::vec((any::<usize>(), any::<u8>()), 0..12),
            truncate in any::<usize>(),
        ) {
            let def = definition(&values);
            let row = Row { id: Id::from_u128(55), values: values.into_boxed_slice() };
            let layout = FixedPackedLayout::compile(&def).unwrap();
            let mut bytes = encode_row(&def, &row, revision).unwrap();
            for (index, value) in changes {
                let slot = index % bytes.len();
                bytes[slot] = value;
            }
            bytes.truncate(truncate % (bytes.len() + 1));
            let fixed = layout.read(&bytes).map(|row| row.revision()).map_err(|e| e.to_string());
            let decoded = decode_row(&def, row.id, &bytes).map(|(revision, _)| revision).map_err(|e| e.to_string());
            prop_assert_eq!(fixed, decoded);
        }

        #[test]
        fn borrowed_projection_preserves_variable_validation(
            values in proptest::collection::vec(arbitrary_value(), 0..16),
            revision in any::<u64>(),
            changes in proptest::collection::vec((any::<usize>(), any::<u8>()), 0..12),
            truncate in any::<usize>(),
        ) {
            let def = definition(&values);
            let row = Row { id: Id::from_u128(55), values: values.into_boxed_slice() };
            let mut bytes = encode_row(&def, &row, revision).unwrap();
            for (index, value) in changes {
                let slot = index % bytes.len();
                bytes[slot] = value;
            }
            bytes.truncate(truncate % (bytes.len() + 1));
            let mut values = vec![PackedValue::Bool(false); def.roles.len()];
            let borrowed = decode_packed_values(&def, &bytes, &mut values).map_err(|e| e.to_string());
            let decoded = decode_row(&def, row.id, &bytes).map_err(|e| e.to_string());
            prop_assert_eq!(&borrowed, &decoded.as_ref().map(|(revision,_)| *revision).map_err(Clone::clone));
            if let Ok((_, decoded)) = decoded {
                let actual = values.into_iter().map(PackedValue::into_owned).collect::<Vec<_>>();
                prop_assert_eq!(actual.as_slice(), decoded.values.as_ref());
            }
        }

        #[test]
        fn keys_preserve_value_order(a in arbitrary_value(), b in arbitrary_value()) {
            prop_assert_eq!(a.cmp(&b), value_key(&a).cmp(&value_key(&b)));
        }
        #[test]
        fn unique_keys_preserve_tuple_order(
            a in proptest::collection::vec(arbitrary_value(), 0..8),
            b in proptest::collection::vec(arbitrary_value(), 0..8),
        ) {
            let tid = Id::from_u128(1);
            prop_assert_eq!(a.cmp(&b), unique_key(tid, 0, &a).cmp(&unique_key(tid, 0, &b)));
        }
        #[test]
        fn reusable_index_keys_match_canonical_keys(
            tid in any::<u128>(), id in any::<u128>(), slot in any::<u16>(),
            values in proptest::collection::vec(arbitrary_value(), 0..16),
        ) {
            let tid = Id::from_u128(tid);
            let id = Id::from_u128(id);
            let slot = usize::from(slot);
            let mut scratch = vec![255; 4096];
            unique_key_into(&mut scratch, tid, slot, &values);
            prop_assert_eq!(&scratch, &unique_key(tid, slot, &values));
            let allocation = scratch.as_ptr();
            for value in &values {
                index_key_into(&mut scratch, tid, slot, value, id);
                prop_assert_eq!(&scratch, &index_key(tid, slot, value, id));
                prop_assert_eq!(scratch.as_ptr(), allocation);
            }
            unique_key_into(&mut scratch, tid, slot, &[]);
            prop_assert_eq!(scratch, unique_key(tid, slot, &[]));
        }
        #[test]
        fn packed_roundtrip(values in proptest::collection::vec(arbitrary_value(), 0..16), revision in any::<u64>()) {
            let def = definition(&values);
            let row = Row { id: Id::from_u128(55), values: values.into_boxed_slice() };
            let encoded = encode_row(&def, &row, revision).unwrap();
            prop_assert_eq!(row_encoded_len(&def, &row).unwrap(), encoded.len());
            prop_assert_eq!(row_revision(&encoded).unwrap(), revision);
            prop_assert_eq!(validate_row(&def, &encoded).unwrap(), revision);
            prop_assert_eq!(decode_row(&def, row.id, &encoded).unwrap(), (revision, row.clone()));
            let mut scratch = row.clone();
            let allocation = scratch.values.as_ptr();
            let next_id = Id::from_u128(56);
            prop_assert_eq!(decode_row_into(&def, next_id, &encoded, &mut scratch).unwrap(), revision);
            prop_assert_eq!(scratch.values.as_ptr(), allocation);
            prop_assert_eq!(scratch.id, next_id);
            prop_assert_eq!(scratch.values, row.values);
        }
        #[test]
        fn packed_validation_matches_decode_after_mutations(
            values in proptest::collection::vec(arbitrary_value(), 0..16),
            revision in any::<u64>(),
            changes in proptest::collection::vec((any::<usize>(), any::<u8>()), 0..12),
            truncate in any::<usize>(),
        ) {
            let def = definition(&values);
            let row = Row { id: Id::from_u128(55), values: values.into_boxed_slice() };
            let mut encoded = encode_row(&def, &row, revision).unwrap();
            for (index, value) in changes {
                let slot = index % encoded.len();
                encoded[slot] = value;
            }
            // Include unchanged lengths as well as every possible truncation.
            encoded.truncate(truncate % (encoded.len() + 1));
            let validated = validate_row(&def, &encoded).map_err(|error| error.to_string());
            let decoded = decode_row(&def, row.id, &encoded)
                .map(|(revision, _)| revision).map_err(|error| error.to_string());
            let mut scratch = row;
            let reused = decode_row_into(&def, Id::from_u128(56), &encoded, &mut scratch)
                .map_err(|error| error.to_string());
            prop_assert_eq!(&validated, &decoded);
            prop_assert_eq!(validated, reused);
        }
        #[test]
        fn prefix_bounds_select_exact_prefix(prefix in proptest::collection::vec(any::<u8>(), 0..16), suffix in proptest::collection::vec(any::<u8>(), 0..16)) {
            let mut key = prefix.clone(); key.extend(suffix);
            prop_assert!(key >= prefix);
            if let Some(end) = prefix_end(&prefix) { prop_assert!(key < end); }
        }
    }

    #[test]
    fn compiled_validation_bounds_and_extended_types_fall_back() {
        assert!(PackedValidationLayout::compile(&definition(&vec![Value::Int(0); 65])).is_none());
        for value in [Value::Null, Value::try_from(1.5).unwrap()] {
            assert!(PackedValidationLayout::compile(&definition(&[value])).is_none());
        }
        let values = vec![
            Value::Int(7),
            Value::Timestamp(2),
            Value::Text("é".into()),
            Value::Bool(true),
            Value::Bytes(vec![0xff, 0]),
            Value::Int(9),
        ];
        let def = definition(&values);
        let layout = PackedValidationLayout::compile(&def).unwrap();
        let row = Row {
            id: Id::from_u128(1),
            values: values.into_boxed_slice(),
        };
        let bytes = encode_row(&def, &row, 1).unwrap();
        for index in 0..bytes.len() {
            for value in [0, 1, 2, 0xff] {
                let mut changed = bytes.clone();
                changed[index] = value;
                assert_eq!(
                    layout.validate(&changed).is_ok(),
                    validate_row(&def, &changed).is_ok()
                );
            }
        }
        let mut trailing = bytes;
        trailing.push(0);
        let len = (trailing.len() - HEADER) as u32;
        trailing[12..16].copy_from_slice(&len.to_be_bytes());
        assert!(layout.validate(&trailing).is_err());
    }

    #[test]
    fn fixed_layout_bounds_and_unselected_field_validation() {
        for values in [
            vec![Value::Text("bad if unvalidated".into())],
            vec![Value::Bytes(vec![])],
            vec![Value::Int(0); 65],
        ] {
            assert!(FixedPackedLayout::compile(&definition(&values)).is_none());
        }
        let values = vec![
            Value::Int(17),
            Value::Bool(true),
            Value::Ref(Id::from_u128(42)),
        ];
        let def = definition(&values);
        let row = Row {
            id: Id::from_u128(1),
            values: values.into_boxed_slice(),
        };
        let layout = FixedPackedLayout::compile(&def).unwrap();
        let bytes = encode_row(&def, &row, 7).unwrap();
        assert_eq!(
            layout.read(&bytes).unwrap().value(0),
            Some(PackedValue::Int(17))
        );
        let mut invalid_bool = bytes.clone();
        invalid_bool[HEADER + 8] = 2;
        assert!(layout.read(&invalid_bool).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        let body_len = (trailing.len() - HEADER) as u32;
        trailing[12..16].copy_from_slice(&body_len.to_be_bytes());
        assert!(layout.read(&trailing).is_err());
        let mut wrong_arity = bytes.clone();
        wrong_arity[16..20].copy_from_slice(&2u32.to_be_bytes());
        assert!(layout.read(&wrong_arity).is_err());

        let values = vec![Value::Int(17), Value::Text("ok".into())];
        let def = definition(&values);
        let row = Row {
            id: Id::from_u128(1),
            values: values.into_boxed_slice(),
        };
        let mut bytes = encode_row(&def, &row, 7).unwrap();
        bytes[HEADER + 8 + 4] = 255;
        let mut output = [PackedValue::Bool(false); 2];
        // Even if only numeric slot zero is useful to the query, an invalid
        // later string prevents the borrowed record from becoming observable.
        assert!(decode_packed_values(&def, &bytes, &mut output).is_err());
        assert!(validate_row(&def, &bytes).is_err());
    }
    #[test]
    fn reusable_decode_drops_old_variable_capacities_and_rejects_wrong_arity() {
        let mut row = Row {
            id: Id::from_u128(1),
            values: vec![Value::Text("x".repeat(4096)), Value::Bytes(vec![])].into_boxed_slice(),
        };
        let def = definition(&row.values);
        let allocation = row.values.as_ptr();
        for index in 0..8 {
            let next = Row {
                id: Id::from_u128(index + 2),
                values: if index % 2 == 0 {
                    vec![Value::Text(String::new()), Value::Bytes(vec![255; 4096])]
                } else {
                    vec![Value::Text("é\0".repeat(1024)), Value::Bytes(vec![])]
                }
                .into_boxed_slice(),
            };
            let bytes = encode_row(&def, &next, 2).unwrap();
            assert_eq!(decode_row_into(&def, next.id, &bytes, &mut row).unwrap(), 2);
            assert_eq!(row, next);
            assert_eq!(row.values.as_ptr(), allocation);
            let [Value::Text(text), Value::Bytes(blob)] = row.values.as_ref() else {
                panic!("incorrect reused value types");
            };
            assert_eq!(text.capacity(), text.len());
            assert_eq!(blob.capacity(), blob.len());
        }
        let bytes = encode_row(&def, &row, 3).unwrap();
        row.values = vec![Value::Bool(false)].into_boxed_slice();
        assert!(decode_row_into(&def, row.id, &bytes, &mut row).is_err());
    }

    #[test]
    fn zero_bytes_utf8_integer_limits_and_prefix_order() {
        for values in [
            vec![
                Value::Int(i64::MIN),
                Value::Int(-1),
                Value::Int(0),
                Value::Int(1),
                Value::Int(i64::MAX),
            ],
            ["", "\0", "\0\0", "\0a", "a", "a\0", "aé", "é", "🦀"]
                .into_iter()
                .map(|s| Value::Text(s.into()))
                .collect(),
            [
                vec![],
                vec![0],
                vec![0, 0],
                vec![0, 255],
                vec![1],
                vec![255],
                vec![255, 0],
            ]
            .into_iter()
            .map(Value::Bytes)
            .collect(),
        ] {
            for pair in values.windows(2) {
                assert!(pair[0] < pair[1]);
                assert!(value_key(&pair[0]) < value_key(&pair[1]));
            }
        }
        assert_eq!(prefix_end(&[]), None);
        assert_eq!(prefix_end(&[255, 255]), None);
        assert_eq!(prefix_end(&[3, 255]), Some(vec![4]));
        assert_eq!(prefix_end(&[3, 0]), Some(vec![3, 1]));
    }
    #[test]
    fn composite_keys_separate_roles_relations_occurrences_and_references() {
        let tid = Id::from_u128(1);
        let id = Id::from_u128(2);
        let next = Id::from_u128(3);
        let value = Value::Text("x\0y".into());
        let prefix = index_prefix(tid, 0, &value);
        let key = index_key(tid, 0, &value, id);
        assert!(key.starts_with(&prefix));
        assert!(key < prefix_end(&prefix).unwrap());
        assert!(key < index_key(tid, 0, &value, next));
        assert!(key < index_key(tid, 1, &value, id));
        assert!(key < index_key(next, 0, &value, id));
        assert_eq!(&key[key.len() - 16..], id.0.as_bytes());
        assert!(row_key(tid, id) < row_key(tid, next));
        assert!(
            unique_key(tid, 0, &[Value::Text("a".into()), Value::Text("bc".into())])
                != unique_key(tid, 0, &[Value::Text("ab".into()), Value::Text("c".into())])
        );
        let incoming = incoming_key(tid, id, tid, next, 0);
        assert!(incoming.starts_with(&incoming_prefix(tid, id)));
        assert!(incoming < incoming_key(tid, id, tid, next, 1));
    }
    #[test]
    fn extended_rows_reject_noncanonical_floats_presence_and_version_confusion() {
        let def = CompiledRelation::new(Relation::new(
            "test.Extended",
            vec![
                crate::Role::new("number", DataType::Float.nullable()),
                crate::Role::new("text", DataType::Text.nullable()),
            ],
        ));
        let row = Row {
            id: Id::from_u128(1),
            values: vec![Value::try_from(1.5).unwrap(), Value::Null].into_boxed_slice(),
        };
        let good = encode_row(&def, &row, 1).unwrap();
        assert_eq!(&good[..4], b"PG02");
        assert_eq!(decode_row(&def, row.id, &good).unwrap().1, row);
        for end in 0..good.len() {
            assert!(decode_row(&def, row.id, &good[..end]).is_err());
        }
        for bits in [
            f64::NAN.to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            (-0.0f64).to_bits(),
        ] {
            let mut bad = good.clone();
            bad[HEADER + 1..HEADER + 9].copy_from_slice(&bits.to_be_bytes());
            assert!(validate_row(&def, &bad).is_err());
        }
        for position in [HEADER, HEADER + 9] {
            let mut bad = good.clone();
            bad[position] = 2;
            assert!(validate_row(&def, &bad).is_err());
        }
        let mut bad = good.clone();
        bad[..4].copy_from_slice(b"PG01");
        assert!(validate_row(&def, &bad).is_err());
        let old_def = definition(&[Value::Int(1)]);
        let old = Row {
            id: row.id,
            values: vec![Value::Int(1)].into_boxed_slice(),
        };
        let old_bytes = encode_row(&old_def, &old, 1).unwrap();
        assert_eq!(&old_bytes[..4], b"PG01");
        let mut bad = old_bytes.clone();
        bad[..4].copy_from_slice(b"PG02");
        assert!(validate_row(&old_def, &bad).is_err());
        let mut null_row = row;
        null_row.values[0] = Value::Null;
        assert_eq!(
            decode_row(&def, null_row.id, &encode_row(&def, &null_row, 2).unwrap())
                .unwrap()
                .1,
            null_row
        );
    }

    #[test]
    fn packed_rejects_truncation_trailing_bytes_versions_counts_and_bad_values() {
        let row = Row {
            id: Id::from_u128(2),
            values: vec![
                Value::Bool(true),
                Value::Text("é\0".into()),
                Value::Bytes(vec![0, 255]),
            ]
            .into_boxed_slice(),
        };
        let def = definition(&row.values);
        let bytes = encode_row(&def, &row, 19).unwrap();
        for cut in 0..bytes.len() {
            assert!(decode_row(&def, row.id, &bytes[..cut]).is_err());
            assert!(row_revision(&bytes[..cut]).is_err());
        }
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(decode_row(&def, row.id, &bad).is_err());
        let mut bad = bytes.clone();
        bad[3] = b'9';
        assert!(row_revision(&bad).is_err());
        let mut bad = bytes.clone();
        bad[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_row(&def, row.id, &bad).is_err());
        assert!(row_revision(&bad).is_err());
        let mut bad = bytes.clone();
        bad[20] = 2;
        assert!(decode_row(&def, row.id, &bad).is_err());
        let mut bad = bytes.clone();
        bad[21..25].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_row(&def, row.id, &bad).is_err());
        let mut bad = bytes.clone();
        bad[25] = 255;
        assert!(decode_row(&def, row.id, &bad).is_err());
        let mut wrong = row.clone();
        wrong.values[0] = Value::Int(1);
        assert!(encode_row(&def, &wrong, 1).is_err());
        wrong.values = vec![Value::Bool(true)].into_boxed_slice();
        assert!(encode_row(&def, &wrong, 1).is_err());
        // Consistent framing cannot conceal unconsumed bytes in a typed body.
        let mut bad = bytes.clone();
        bad.push(0);
        let body_len = (bad.len() - HEADER) as u32;
        bad[12..16].copy_from_slice(&body_len.to_be_bytes());
        assert!(decode_row(&def, row.id, &bad).is_err());
    }

    #[test]
    fn packed_validator_checks_all_fields_and_framing_without_owned_rows() {
        let row = Row {
            id: Id::from_u128(7),
            values: vec![
                Value::Bool(true),
                Value::Int(i64::MIN),
                Value::Decimal(i64::MAX),
                Value::Timestamp(-99),
                Value::Ref(Id::from_u128(u128::MAX)),
                Value::Text("é\0🦀".into()),
                Value::Bytes(vec![0, 255, 1]),
            ]
            .into_boxed_slice(),
        };
        let def = definition(&row.values);
        let bytes = encode_row(&def, &row, 19).unwrap();
        let parity = |bytes: &[u8]| {
            assert_eq!(
                validate_row(&def, bytes).map_err(|error| error.to_string()),
                decode_row(&def, row.id, bytes)
                    .map(|(revision, _)| revision)
                    .map_err(|error| error.to_string()),
            );
        };
        assert_eq!(validate_row(&def, &bytes).unwrap(), 19);
        for cut in 0..=bytes.len() {
            parity(&bytes[..cut]);
        }
        // Covers framing/revision/count bytes, invalid Boolean/UTF-8, variable
        // lengths and fixed-width values without assuming mutation must fail.
        for index in 0..bytes.len() {
            for replacement in [0, 1, 2, 255] {
                let mut changed = bytes.clone();
                changed[index] = replacement;
                parity(&changed);
            }
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        parity(&trailing);
        let body_len = (trailing.len() - HEADER) as u32;
        trailing[12..16].copy_from_slice(&body_len.to_be_bytes());
        parity(&trailing);
        assert!(validate_row(&def, &trailing).is_err());
        assert!(validate_row(&definition(&[Value::Int(0)]), &bytes).is_err());
        // Framing permits any revision; snapshot validity belongs to the caller.
        for revision in [0, u64::MAX] {
            assert_eq!(
                validate_row(&def, &encode_row(&def, &row, revision).unwrap()).unwrap(),
                revision
            );
        }
    }
}
