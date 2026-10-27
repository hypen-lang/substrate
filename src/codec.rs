//! Versioned redo encoding (LQ01 legacy; LQ02 constrained schema metadata). Hot rows use schema-directed packed values; rare
//! catalog/policy operations use length-prefixed JSON. All lengths are checked.
use crate::{
    storage::{Op, State},
    *,
};
use std::io::{Cursor, Read};
const MAGIC: &[u8; 4] = b"LQ01";
const MAGIC_V2: &[u8; 4] = b"LQ02";
const MAGIC_V3: &[u8; 4] = b"LQ03";
fn invalid() -> Error {
    Error::Storage("invalid or unsupported Liquid log record".into())
}
fn len(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    out.extend(
        u32::try_from(bytes.len())
            .map_err(|_| invalid())?
            .to_le_bytes(),
    );
    out.extend(bytes);
    Ok(())
}
/// Exact admission size without constructing/copying the packed row payload.
/// Rare metadata is serialized to a counting sink so JSON escaping/versioning
/// obey the same wire contract; no encoded metadata buffer is retained.
pub(crate) fn encoded_len(ops: &[Op], schema: &State) -> Result<usize> {
    u32::try_from(ops.len()).map_err(|_| invalid())?;
    let v3 = uses_v3(ops, schema);
    let v2 = uses_v2(ops);
    let mut total = 16usize;
    let mut add = |n: usize| -> Result<()> {
        total = total.checked_add(n).ok_or_else(invalid)?;
        Ok(())
    };
    for op in ops {
        match op {
            Op::Put(tid, row) => {
                add(row_payload_len(*tid, row, schema)?)?;
            }
            Op::Identity(_) => add(17)?,
            Op::Delete(_, _) => add(33)?,
            _ => {
                let mut counter = JsonSize(0);
                if v3 || v2 {
                    serde_json::to_writer(&mut counter, &crate::wire::EventV2::from_op(op)?)
                } else {
                    serde_json::to_writer(&mut counter, &crate::wire::EventV1::from_op(op)?)
                }
                .map_err(|_| invalid())?;
                add(1)?;
                add(variable_len(counter.0)?)?;
            }
        }
    }
    Ok(total)
}
fn variable_len(length: usize) -> Result<usize> {
    u32::try_from(length).map_err(|_| invalid())?;
    length.checked_add(4).ok_or_else(invalid)
}
struct JsonSize(usize);
impl std::io::Write for JsonSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|size| u32::try_from(*size).is_ok())
            .ok_or_else(|| std::io::Error::other("metadata length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn uses_v2(ops: &[Op]) -> bool {
    ops.iter()
        .any(|op| matches!(op, Op::Install(m) if m.relations.iter().any(|r| !r.checks.is_empty())))
}
fn uses_v3(ops: &[Op], schema: &State) -> bool {
    ops.iter().any(|op| {
        crate::wire::event_v3(op)
            || match op {
                Op::Put(tid, _) => schema
                    .relations
                    .get(tid)
                    .is_some_and(|r| crate::wire::relation_v3(r)),
                Op::Delete(..) => schema
                    .relations
                    .values()
                    .any(|r| crate::wire::relation_v3(r)),
                _ => false,
            }
    })
}
pub(crate) fn encode(revision: u64, ops: &[Op], schema: &State) -> Result<Vec<u8>> {
    let v3 = uses_v3(ops, schema);
    let v2 = uses_v2(ops);
    let mut out = if v3 {
        MAGIC_V3
    } else if v2 {
        MAGIC_V2
    } else {
        MAGIC
    }
    .to_vec();
    out.extend(revision.to_le_bytes());
    out.extend(
        u32::try_from(ops.len())
            .map_err(|_| invalid())?
            .to_le_bytes(),
    );
    for op in ops {
        match op {
            Op::Put(tid, row) => encode_put(&mut out, *tid, row, schema)?,
            Op::Identity(id) => {
                out.push(2);
                out.extend(id.0.as_bytes());
            }
            Op::Delete(tid, id) => {
                out.push(3);
                out.extend(tid.0.as_bytes());
                out.extend(id.0.as_bytes());
            }
            _ => {
                out.push(0);
                len(
                    &mut out,
                    &if v3 || v2 {
                        serde_json::to_vec(&crate::wire::EventV2::from_op(op)?)
                    } else {
                        serde_json::to_vec(&crate::wire::EventV1::from_op(op)?)
                    }
                    .map_err(|_| invalid())?,
                )?;
            }
        }
    }
    Ok(out)
}
/// A checkpoint row uses the same single-Put LQ01 record as before, but borrows
/// its canonical row rather than allocating a temporary boxed payload and Op.
pub(crate) fn encode_checkpoint_row(tid: Id, row: &Row, schema: &State) -> Result<Vec<u8>> {
    let def = schema.relations.get(&tid).ok_or_else(invalid)?;
    let mut out = if crate::wire::relation_v3(def) {
        MAGIC_V3
    } else {
        MAGIC
    }
    .to_vec();
    out.extend(0u64.to_le_bytes());
    out.extend(1u32.to_le_bytes());
    encode_put(&mut out, tid, row, schema)?;
    Ok(out)
}
pub(crate) fn encoded_checkpoint_row_len(tid: Id, row: &Row, schema: &State) -> Result<usize> {
    row_payload_len(tid, row, schema)?
        .checked_add(16)
        .ok_or_else(invalid)
}
fn row_payload_len(tid: Id, row: &Row, schema: &State) -> Result<usize> {
    let def = schema.relations.get(&tid).ok_or_else(invalid)?;
    if row.values.len() != def.roles.len() {
        return Err(invalid());
    }
    def.roles
        .iter()
        .zip(row.values.iter())
        .try_fold(33usize, |total, (role, value)| {
            if !role.data_type.accepts(value) {
                return Err(invalid());
            }
            let size = match value {
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Decimal(_) | Value::Timestamp(_) | Value::Float(_) => 8,
                Value::Null => 0,
                Value::Ref(_) => 16,
                Value::Text(value) => variable_len(value.len())?,
                Value::Bytes(value) => variable_len(value.len())?,
            };
            total
                .checked_add(size + usize::from(role.data_type.is_nullable()))
                .ok_or_else(invalid)
        })
}
fn encode_put(out: &mut Vec<u8>, tid: Id, row: &Row, schema: &State) -> Result<()> {
    out.push(1);
    out.extend(tid.0.as_bytes());
    out.extend(row.id.0.as_bytes());
    let def = schema.relations.get(&tid).ok_or_else(invalid)?;
    if row.values.len() != def.roles.len() {
        return Err(invalid());
    }
    for (role, value) in def.roles.iter().zip(row.values.iter()) {
        if !role.data_type.accepts(value) {
            return Err(invalid());
        }
        if role.data_type.is_nullable() {
            out.push(u8::from(!matches!(value, Value::Null)));
        }
        match value {
            Value::Null => {}
            Value::Float(v) => out.extend(v.get().to_le_bytes()),
            Value::Bool(v) => out.push(u8::from(*v)),
            Value::Int(v) | Value::Decimal(v) | Value::Timestamp(v) => out.extend(v.to_le_bytes()),
            Value::Ref(id) => out.extend(id.0.as_bytes()),
            Value::Text(v) => len(out, v.as_bytes())?,
            Value::Bytes(v) => len(out, v)?,
        }
    }
    Ok(())
}
fn bytes<const N: usize>(input: &mut Cursor<&[u8]>) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    input.read_exact(&mut bytes).map_err(|_| invalid())?;
    Ok(bytes)
}
fn id(input: &mut Cursor<&[u8]>) -> Result<Id> {
    Ok(Id(uuid::Uuid::from_bytes(bytes(input)?)))
}
fn variable(input: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let count = u32::from_le_bytes(bytes(input)?) as usize;
    let start = input.position() as usize;
    let end = start.checked_add(count).ok_or_else(invalid)?;
    let value = input
        .get_ref()
        .get(start..end)
        .ok_or_else(invalid)?
        .to_vec();
    input.set_position(end as u64);
    Ok(value)
}
#[cfg(test)]
pub(crate) fn replay(data: &[u8], revision: u64, state: &mut State) -> Result<()> {
    replay_inner(data, revision, state, false, None, None, &mut |_| {})
}
pub(crate) fn replay_catalog(data: &[u8], revision: u64, state: &mut State) -> Result<()> {
    replay_inner(data, revision, state, true, None, None, &mut |_| {})
}
pub(crate) fn replay_checkpoint_row(
    data: &[u8],
    revision: u64,
    key: &[u8],
    state: &mut State,
) -> Result<()> {
    replay_inner(data, revision, state, false, Some(key), None, &mut |_| {})
}
pub(crate) fn replay_observed(
    data: &[u8],
    revision: u64,
    state: &mut State,
    observe: &mut dyn FnMut(&Op),
) -> Result<()> {
    replay_inner(data, revision, state, false, None, None, observe)
}
/// Metadata v2 entries are revision-independent single operations. Their table key
/// must match the operation, preventing duplicate or misplaced catalog entries.
pub(crate) fn metadata_key(op: &Op) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match op {
        Op::Install(m) => {
            out.push(0);
            len(&mut out, m.name.as_bytes())?;
            out.extend(m.version.to_be_bytes());
        }
        Op::Identity(id) => {
            out.push(1);
            out.extend(id.0.as_bytes());
        }
        Op::Grant(p, tid, _) => {
            out.push(2);
            len(&mut out, p.as_bytes())?;
            out.extend(tid.0.as_bytes());
        }
        Op::SchemaGrant(p, name) => {
            out.push(3);
            len(&mut out, p.as_bytes())?;
            len(&mut out, name.as_bytes())?;
        }
        _ => return Err(invalid()),
    }
    Ok(out)
}
pub(crate) fn replay_metadata_entry(data: &[u8], key: &[u8], state: &mut State) -> Result<()> {
    replay_inner(data, 0, state, true, None, Some(key), &mut |_| {})
}
fn replay_inner(
    data: &[u8],
    revision: u64,
    state: &mut State,
    catalog: bool,
    row_key: Option<&[u8]>,
    catalog_key: Option<&[u8]>,
    observe: &mut dyn FnMut(&Op),
) -> Result<()> {
    let mut input = Cursor::new(data);
    let magic = bytes::<4>(&mut input)?;
    if (&magic != MAGIC && &magic != MAGIC_V2 && &magic != MAGIC_V3)
        || u64::from_le_bytes(bytes(&mut input)?) != revision
    {
        return Err(invalid());
    }
    let count = u32::from_le_bytes(bytes(&mut input)?);
    if (row_key.is_some() || catalog_key.is_some()) && count != 1 {
        return Err(invalid());
    }
    let mut reference_ops = Vec::new();
    for _ in 0..count {
        let tag = bytes::<1>(&mut input)?[0];
        if (catalog && !matches!(tag, 0 | 2)) || (row_key.is_some() && tag != 1) {
            return Err(invalid());
        }
        let op = match tag {
            0 => {
                let json = variable(&mut input)?;
                let op = if &magic == MAGIC_V3 {
                    serde_json::from_slice::<crate::wire::EventV3>(&json)
                        .map_err(|_| invalid())?
                        .into_op()
                } else if &magic == MAGIC_V2 {
                    serde_json::from_slice::<crate::wire::EventV2>(&json)
                        .map_err(|_| invalid())?
                        .into_op()
                } else {
                    serde_json::from_slice::<crate::wire::EventV1>(&json)
                        .map_err(|_| invalid())?
                        .into_op()
                };
                if (&magic != MAGIC_V3 && crate::wire::event_v3(&op))
                    || !matches!(op, Op::Install(_) | Op::Grant(..) | Op::SchemaGrant(..))
                {
                    return Err(invalid());
                }
                op
            }
            1 => {
                let tid = id(&mut input)?;
                let row_id = id(&mut input)?;
                if let Some(key) = row_key
                    && (key.len() != 32
                        || &key[..16] != tid.0.as_bytes()
                        || &key[16..] != row_id.0.as_bytes()
                        || state.row(tid, row_id).is_some())
                {
                    return Err(invalid());
                }
                let def = state.relations.get(&tid).ok_or_else(invalid)?;
                let mut values = Vec::with_capacity(def.roles.len());
                if &magic != MAGIC_V3 && crate::wire::relation_v3(def) {
                    return Err(invalid());
                }
                for role in &def.roles {
                    values.push(decode_value(&role.data_type, &mut input)?);
                }

                if !state.identities.contains(&row_id) {
                    return Err(invalid());
                }
                Op::Put(
                    tid,
                    Row {
                        id: row_id,
                        values: values.into_boxed_slice(),
                    },
                )
            }
            2 => Op::Identity(id(&mut input)?),
            3 => Op::Delete(id(&mut input)?, id(&mut input)?),
            _ => return Err(invalid()),
        };
        if let Some(key) = catalog_key
            && (matches!(op, Op::Grant(_, _, None)) || metadata_key(&op)?.as_slice() != key)
        {
            return Err(invalid());
        }
        if !catalog && row_key.is_none() && !(&magic == MAGIC_V3 && matches!(op, Op::Delete(..))) {
            state.validate_replay_order_versioned(&op, &magic == MAGIC_V3)?;
        }
        state.apply_recovered(&op, revision)?;
        if &magic == MAGIC_V3
            && !catalog
            && row_key.is_none()
            && matches!(op, Op::Put(..) | Op::Delete(..))
        {
            reference_ops.push(op.clone());
        }
        observe(&op);
    }
    if input.position() != data.len() as u64 {
        return Err(invalid());
    }
    if !reference_ops.is_empty() {
        state.validate_final_references(&reference_ops)?;
    }
    Ok(())
}

pub(crate) fn decode_value(kind: &DataType, input: &mut Cursor<&[u8]>) -> Result<Value> {
    Ok(match kind {
        DataType::Nullable(inner) => match bytes::<1>(input)?[0] {
            0 => Value::Null,
            1 => decode_value(inner, input)?,
            _ => return Err(invalid()),
        },
        DataType::Float => {
            let raw = f64::from_le_bytes(bytes(input)?);
            if raw.to_bits() == (-0.0f64).to_bits() {
                return Err(invalid());
            }
            Value::Float(FiniteFloat::new(raw).map_err(|_| invalid())?)
        }
        DataType::Bool => match bytes::<1>(input)?[0] {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(invalid()),
        },
        DataType::Int => Value::Int(i64::from_le_bytes(bytes(input)?)),
        DataType::Decimal { .. } => Value::Decimal(i64::from_le_bytes(bytes(input)?)),
        DataType::Timestamp => Value::Timestamp(i64::from_le_bytes(bytes(input)?)),
        DataType::Text => Value::Text(String::from_utf8(variable(input)?).map_err(|_| invalid())?),
        DataType::Bytes => Value::Bytes(variable(input)?),
        DataType::Ref(_) => Value::Ref(id(input)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packed_roundtrip_and_every_truncation_is_rejected() {
        let relation = Relation::new(
            "test.All",
            vec![
                Role::new("flag", DataType::Bool),
                Role::new("number", DataType::Int),
                Role::new("text", DataType::Text),
                Role::new("cash", DataType::Decimal { scale: 2 }),
                Role::new("time", DataType::Timestamp),
                Role::new("bytes", DataType::Bytes),
            ],
        );
        let tid = relation.id;
        let row_id = Id::new();
        let ops = vec![
            Op::Install(Module::new("test", 1, vec![], vec![relation])),
            Op::Identity(row_id),
            Op::Put(
                tid,
                Row {
                    id: row_id,
                    values: vec![
                        Value::Bool(true),
                        Value::Int(i64::MIN),
                        Value::Text("🦀\0".into()),
                        Value::Decimal(i64::MAX),
                        Value::Timestamp(-1),
                        Value::Bytes(vec![0, 255]),
                    ]
                    .into_boxed_slice(),
                },
            ),
        ];
        let mut expected = State::default();
        for op in &ops {
            expected.apply(op, 1);
        }
        let encoded = encode(1, &ops, &expected).unwrap();
        let mut restored = State::default();
        replay(&encoded, 1, &mut restored).unwrap();
        assert_eq!(expected.families, restored.families);
        for end in 0..encoded.len() {
            assert!(replay(&encoded[..end], 1, &mut State::default()).is_err());
        }
        let mut extra = encoded.clone();
        extra.push(0);
        assert!(replay(&extra, 1, &mut State::default()).is_err());
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    use proptest::prelude::*;
    fn metadata(json: &str) -> Vec<u8> {
        let mut bytes = b"LQ01".to_vec();
        bytes.extend(1u64.to_le_bytes());
        bytes.extend(1u32.to_le_bytes());
        bytes.push(0);
        len(&mut bytes, json.as_bytes()).unwrap();
        bytes
    }
    const LEGACY: &str = r#"{"Install":{"name":"core","version":1,"imports":[],"relations":[{"id":"00000000-0000-4000-8000-000000000001","name":"core.Person","roles":[{"name":"name","data_type":"Text"}],"unique":[]}]}}"#;
    #[test]
    fn constrained_v2_is_explicit_and_recovery_rejects_invalid_values_and_unknown_checks() {
        let relation = Relation::new("app.Age", vec![Role::new("age", DataType::Int)])
            .check(Check::int_range("age", 0, 150));
        let tid = relation.id;
        let install = Op::Install(Module::new("app", 1, vec![], vec![relation]));
        let mut schema = State::default();
        schema.apply_recovered(&install, 1).unwrap();
        assert!(crate::wire::EventV1::from_op(&install).is_err());
        let bytes = encode(1, std::slice::from_ref(&install), &schema).unwrap();
        assert_eq!(&bytes[..4], b"LQ02");
        let mut recovered = State::default();
        replay(&bytes, 1, &mut recovered).unwrap();
        assert_eq!(
            recovered.relations[&tid].checks,
            schema.relations[&tid].checks
        );
        let mut wrong_version = bytes.clone();
        wrong_version[..4].copy_from_slice(b"LQ01");
        assert!(replay(&wrong_version, 1, &mut State::default()).is_err());
        let json = String::from_utf8(bytes[21..].to_vec()).unwrap();
        for bad in [
            json.replace("IntRange", "Evaluate"),
            json.replace("\"min\":\"0\"", "\"min\":\"200\""),
            json.replace("\"min\":\"0\"", "\"min\":0"),
            json.replace("\"min\":\"0\"", "\"min\":\"00\""),
        ] {
            let mut record = metadata(&bad);
            record[..4].copy_from_slice(b"LQ02");
            assert!(replay(&record, 1, &mut State::default()).is_err());
        }
        let id = Id::new();
        let bad = vec![
            Op::Identity(id),
            Op::Put(
                tid,
                Row {
                    id,
                    values: vec![151.into()].into_boxed_slice(),
                },
            ),
        ];
        let bytes = encode(2, &bad, &schema).unwrap();
        assert_eq!(&bytes[..4], b"LQ01"); // Row representation did not change.
        assert!(replay(&bytes, 2, &mut recovered).is_err());
    }
    #[test]
    fn frozen_v1_schema_fixture_has_deterministic_identity_and_bytes() {
        let bytes = metadata(LEGACY);
        let mut a = State::default();
        let mut b = State::default();
        replay(&bytes, 1, &mut a).unwrap();
        replay(&bytes, 1, &mut b).unwrap();
        assert_eq!(a.names, b.names);
        let module = (**a.modules.values().next().unwrap()).clone();
        assert_eq!(encode(1, &[Op::Install(module)], &a).unwrap(), bytes);
    }
    #[test]
    fn durable_schema_cannot_allocate_an_omitted_identity() {
        let bad = LEGACY.replace(r#""id":"00000000-0000-4000-8000-000000000001","#, " ");
        assert!(replay(&metadata(&bad), 1, &mut State::default()).is_err());
    }
    #[test]
    fn invalid_schema_constraints_and_unknown_permissions_fail_closed() {
        for bad in [
            LEGACY.replace(r#""unique":[]"#, r#""unique":[["missing"]]"#),
            LEGACY.replace(r#""name":"name""#, r#""name":"id""#),
            LEGACY.replace(
                r#""data_type":"Text""#,
                r#""data_type":{"Decimal":{"scale":19}}"#,
            ),
        ] {
            assert!(replay(&metadata(&bad), 1, &mut State::default()).is_err());
        }
        let mut state = State::default();
        replay(&metadata(LEGACY), 1, &mut state).unwrap();
        for filter in [
            r#"{"Execute":"allow everything"}"#,
            r#"{"Equals":{"role":"missing","value":{"Text":"x"}}}"#,
            r#"{"Equals":{"role":"name","value":{"Int":1}}}"#,
        ] {
            let event = format!(
                r#"{{"Grant":["app","00000000-0000-4000-8000-000000000001",{{"read":true,"write":false,"reference":false,"filter":{filter}}}]}}"#
            );
            assert!(replay(&metadata(&event), 1, &mut state.clone()).is_err());
        }
    }
    #[test]
    fn packed_person_bytes_are_identical_after_independent_extensions() {
        let mut state = State::default();
        replay(&metadata(LEGACY), 1, &mut state).unwrap();
        let tid = state.names["core.Person"];
        let id = Id::new();
        let op = Op::Put(
            tid,
            Row {
                id,
                values: vec!["Ada".into()].into_boxed_slice(),
            },
        );
        state.apply(&Op::Identity(id), 1);
        state.apply(&op, 1);
        let before = encode(1, std::slice::from_ref(&op), &state).unwrap();
        let role = state.relations[&tid].role_id("name").unwrap();
        for module in [demo::fitness(), demo::crm()] {
            state.apply_recovered(&Op::Install(module), 2).unwrap();
        }
        state
            .apply_recovered(
                &Op::Install(Module::new(
                    "extension",
                    1,
                    vec![],
                    vec![
                        Relation::new("extension.Count", vec![Role::new("count", DataType::Int)])
                            .check(Check::int_range("count", 0, 10)),
                    ],
                )),
                3,
            )
            .unwrap();
        assert_eq!(encode(1, &[op], &state).unwrap(), before);
        assert_eq!(state.relations[&tid].role_id("name").unwrap(), role);
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        #[test]
        fn arbitrary_and_mutated_log_bytes_never_panic(data in prop::collection::vec(any::<u8>(),0..1024),offset in 0usize..500,byte in any::<u8>()) {
            let _=replay(&data,1,&mut State::default());
            let mut valid=metadata(LEGACY);let i=offset%valid.len();valid[i]=byte;
            let _=replay(&valid,1,&mut State::default());
        }
    }
}

#[cfg(test)]
mod sizing_tests {
    use super::*;
    use proptest::prelude::*;

    fn fixture() -> (State, Id, Module) {
        let relation = Relation::new(
            "sizing.Row",
            vec![
                Role::new("text", DataType::Text),
                Role::new("bytes", DataType::Bytes),
                Role::new("flag", DataType::Bool),
                Role::new("count", DataType::Int),
                Role::new("money", DataType::Decimal { scale: 2 }),
                Role::new("time", DataType::Timestamp),
                Role::new("target", DataType::Ref("sizing.Row".into())),
            ],
        );
        let tid = relation.id;
        let module = Module::new("sizing", 1, vec![], vec![relation]);
        let mut state = State::default();
        state.apply(&Op::Install(module.clone()), 1);
        (state, tid, module)
    }
    proptest! {
        #[test]
        fn admission_length_matches_every_wire_variant(
            text in ".{0,512}", payload in prop::collection::vec(any::<u8>(),0..2048),
            count in any::<i64>(), constrained in any::<bool>(),
        ) {
            let (mut state, tid, mut module) = fixture();
            if constrained {
                module.relations[0].checks.push(Check::int_range("count",i64::MIN,i64::MAX));
                state = State::default(); state.apply(&Op::Install(module.clone()),1);
            }
            let id = Id::new();
            let ops = vec![Op::Install(module), Op::Identity(id), Op::Put(tid,Row {
                id, values: vec![Value::Text(text.clone()), Value::Bytes(payload), Value::Bool(true),
                    Value::Int(count), Value::Decimal(count), Value::Timestamp(count), Value::Ref(id)].into_boxed_slice(),
            }), Op::Grant(text.clone(),tid,Some(Grant::read_only())),
                Op::Grant(text,tid,None), Op::SchemaGrant("owner\n\\\"".into(),"sizing".into()), Op::Delete(tid,id)];
            let length = encoded_len(&ops,&state).unwrap();
            prop_assert_eq!(length,encode(0,&ops,&state).unwrap().len());
            prop_assert_eq!(length,encode(u64::MAX,&ops,&state).unwrap().len());
            let Op::Put(tid, row) = &ops[2] else { unreachable!() };
            let borrowed = encode_checkpoint_row(*tid, row, &state).unwrap();
            prop_assert_eq!(encoded_checkpoint_row_len(*tid,row,&state).unwrap(),borrowed.len());
            prop_assert_eq!(&borrowed, &encode(0,std::slice::from_ref(&ops[2]),&state).unwrap());
            prop_assert_eq!(&borrowed[..16], b"LQ01\0\0\0\0\0\0\0\0\x01\0\0\0");
            let mut restored = state.clone();
            restored.apply(&Op::Identity(row.id), 0);
            let mut key = tid.0.as_bytes().to_vec(); key.extend(row.id.0.as_bytes());
            replay_checkpoint_row(&borrowed,0,&key,&mut restored).unwrap();
            prop_assert_eq!(restored.row(*tid,row.id).unwrap().as_ref(),row);
        }
    }
    #[test]
    fn sizing_rejects_invalid_rows_and_overflow_like_encoding() {
        let (state, tid, _) = fixture();
        for op in [
            Op::Put(
                tid,
                Row {
                    id: Id::new(),
                    values: vec![Value::Int(1)].into_boxed_slice(),
                },
            ),
            Op::Put(
                tid,
                Row {
                    id: Id::new(),
                    values: vec![Value::Int(1); 7].into_boxed_slice(),
                },
            ),
            Op::Put(
                Id::new(),
                Row {
                    id: Id::new(),
                    values: Box::new([]),
                },
            ),
        ] {
            assert!(encoded_len(std::slice::from_ref(&op), &state).is_err());
            let Op::Put(tid, row) = &op else {
                unreachable!()
            };
            assert!(encode_checkpoint_row(*tid, row, &state).is_err());
            assert!(encode(0, &[op], &state).is_err());
        }
        assert_eq!(encoded_len(&[], &state).unwrap(), 16);
        if let Some(too_large) = (u32::MAX as usize).checked_add(1) {
            assert!(variable_len(too_large).is_err());
            let mut sink = JsonSize(u32::MAX as usize);
            assert!(std::io::Write::write(&mut sink, &[0]).is_err());
        }
    }
}

#[cfg(test)]
mod nullable_wire_tests {
    use super::*;
    #[test]
    fn new_typed_records_are_versioned_and_reject_noncanonical_scalars() {
        let relation = Relation::new(
            "typed.Row",
            vec![Role::new("value", DataType::Float.nullable())],
        );
        let tid = relation.id;
        let row = Row {
            id: Id::new(),
            values: vec![Value::try_from(1.5).unwrap()].into_boxed_slice(),
        };
        let mut state = State::default();
        state.apply(
            &Op::Install(Module::new("typed", 1, vec![], vec![relation])),
            1,
        );
        state.apply(&Op::Identity(row.id), 1);
        let encoded = encode_checkpoint_row(tid, &row, &state).unwrap();
        assert_eq!(&encoded[..4], b"LQ03");
        assert_eq!(
            encoded_checkpoint_row_len(tid, &row, &state).unwrap(),
            encoded.len()
        );
        let key = [tid.0.as_bytes().as_slice(), row.id.0.as_bytes()].concat();
        replay_checkpoint_row(&encoded, 0, &key, &mut state.clone()).unwrap();
        for end in 0..encoded.len() {
            assert!(replay_checkpoint_row(&encoded[..end], 0, &key, &mut state.clone()).is_err());
        }
        let mut invalid = encoded.clone();
        invalid[49] = 2;
        assert!(replay_checkpoint_row(&invalid, 0, &key, &mut state.clone()).is_err());
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0] {
            let mut invalid = encoded.clone();
            invalid[50..58].copy_from_slice(&value.to_le_bytes());
            assert!(replay_checkpoint_row(&invalid, 0, &key, &mut state.clone()).is_err());
        }
        for magic in [b"LQ01", b"LQ02"] {
            let mut invalid = encoded.clone();
            invalid[..4].copy_from_slice(magic);
            assert!(replay_checkpoint_row(&invalid, 0, &key, &mut state.clone()).is_err());
        }
    }
    #[test]
    fn deferred_redo_still_rejects_a_missing_final_target() {
        let target = Relation::new("deferred.Target", vec![]);
        let link = Relation::new(
            "deferred.Link",
            vec![Role::new("target", DataType::Ref(target.name.clone())).deferrable()],
        );
        let id = Id::new();
        let missing = Id::new();
        let ops = vec![
            Op::Install(Module::new(
                "deferred",
                1,
                vec![],
                vec![target, link.clone()],
            )),
            Op::Identity(id),
            Op::Put(
                link.id,
                Row {
                    id,
                    values: vec![missing.into()].into_boxed_slice(),
                },
            ),
        ];
        let mut staged = State::default();
        for op in &ops {
            staged.apply(op, 1);
        }
        let encoded = encode(1, &ops, &staged).unwrap();
        assert_eq!(encoded_len(&ops, &staged).unwrap(), encoded.len());
        assert!(replay(&encoded, 1, &mut State::default()).is_err());
        let mut downgraded = encoded;
        downgraded[..4].copy_from_slice(b"LQ02");
        assert!(replay(&downgraded, 1, &mut State::default()).is_err());
    }
}
