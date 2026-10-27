//! Compact conflict timestamps. Key domains remain disjoint; historical keys
//! are retained so deletion/reinsertion still conflicts with older transactions.
//! Grouping by relation and role avoids repeating their IDs in every leaf key.
use super::Key;
use crate::{Id, Value};
use im::OrdMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Versions {
    catalog: u64,
    visibility: u64,
    allocated: OrdMap<Id, u64>,
    policies: OrdMap<String, u64>,
    revocations: OrdMap<String, u64>,
    rows: OrdMap<Id, OrdMap<Id, u64>>,
    families: OrdMap<Id, u64>,
    buckets: OrdMap<(Id, usize), OrdMap<Value, u64>>,
    unique: OrdMap<(Id, usize), OrdMap<Vec<Value>, u64>>,
    incoming: OrdMap<Id, OrdMap<Id, u64>>,
}

impl Versions {
    pub(crate) fn get(&self, key: &Key) -> u64 {
        let value = match key {
            Key::Catalog => return self.catalog,
            Key::Visibility => return self.visibility,
            Key::Allocated(id) => self.allocated.get(id),
            Key::Policy(principal) => self.policies.get(principal),
            Key::Revocation(principal) => self.revocations.get(principal),
            Key::Row(tid, id) => self.rows.get(tid).and_then(|rows| rows.get(id)),
            Key::Family(tid) => self.families.get(tid),
            Key::Bucket((tid, slot, value)) => self
                .buckets
                .get(&(*tid, *slot))
                .and_then(|values| values.get(value)),
            Key::Unique((tid, slot, value)) => self
                .unique
                .get(&(*tid, *slot))
                .and_then(|values| values.get(value)),
            Key::Incoming((tid, id)) => self.incoming.get(tid).and_then(|rows| rows.get(id)),
        };
        value.copied().unwrap_or(0)
    }

    pub(crate) fn insert(&mut self, key: Key, revision: u64) {
        match key {
            Key::Catalog => self.catalog = revision,
            Key::Visibility => self.visibility = revision,
            Key::Allocated(id) => {
                self.allocated.insert(id, revision);
            }
            Key::Policy(principal) => {
                self.policies.insert(principal, revision);
            }
            Key::Revocation(principal) => {
                self.revocations.insert(principal, revision);
            }
            Key::Row(tid, id) => insert_nested(&mut self.rows, tid, id, revision),
            Key::Family(tid) => {
                self.families.insert(tid, revision);
            }
            Key::Bucket((tid, slot, value)) => {
                insert_nested(&mut self.buckets, (tid, slot), value, revision)
            }
            Key::Unique((tid, slot, value)) => {
                insert_nested(&mut self.unique, (tid, slot), value, revision)
            }
            Key::Incoming((tid, id)) => insert_nested(&mut self.incoming, tid, id, revision),
        }
    }
}

fn insert_nested<K: Ord + Clone, V: Ord + Clone>(
    map: &mut OrdMap<K, OrdMap<V, u64>>,
    group: K,
    key: V,
    revision: u64,
) {
    if let Some(values) = map.get_mut(&group) {
        values.insert(key, revision);
    } else {
        let mut values = OrdMap::new();
        values.insert(key, revision);
        map.insert(group, values);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn key(kind: u8, group: u8, slot: u8, value: u16) -> Key {
        let tid = Id(uuid::Uuid::from_u128(u128::from(group)));
        let id = Id(uuid::Uuid::from_u128(u128::from(value)));
        let scalar = match value % 4 {
            0 => Value::Int(i64::from(value)),
            1 => Value::Text(value.to_string()),
            2 => Value::Bytes(value.to_le_bytes().to_vec()),
            _ => Value::Ref(id),
        };
        match kind % 10 {
            0 => Key::Catalog,
            1 => Key::Visibility,
            2 => Key::Allocated(id),
            3 => Key::Policy(value.to_string()),
            4 => Key::Revocation(value.to_string()),
            5 => Key::Row(tid, id),
            6 => Key::Family(tid),
            7 => Key::Bucket((tid, slot.into(), scalar)),
            8 => Key::Unique((tid, slot.into(), vec![scalar, Value::Int(i64::from(group))])),
            _ => Key::Incoming((tid, id)),
        }
    }

    proptest! {
        #[test]
        fn timestamps_and_retained_snapshots_match_flat_map(
            changes in prop::collection::vec((0u8..10, 0u8..5, 0u8..4, 0u16..40), 0..300)
        ) {
            let mut compact = Versions::default();
            let mut flat = BTreeMap::new();
            let mut retained = Vec::new();
            for (i, (kind, group, slot, value)) in changes.into_iter().enumerate() {
                let key = key(kind, group, slot, value);
                prop_assert_eq!(compact.get(&key), flat.get(&key).copied().unwrap_or(0));
                compact.insert(key.clone(), i as u64 + 1);
                flat.insert(key, i as u64 + 1);
                if i % 31 == 0 { retained.push((compact.clone(), flat.clone())); }
            }
            retained.push((compact, flat));
            for (compact, flat) in retained {
                for (key, revision) in &flat { prop_assert_eq!(compact.get(key), *revision); }
                for kind in 0..10 {
                    let absent = key(kind, 7, 9, 1000);
                    prop_assert_eq!(compact.get(&absent), flat.get(&absent).copied().unwrap_or(0));
                }
            }
        }
    }

    #[test]
    fn domains_roles_and_relations_do_not_alias() {
        let mut versions = Versions::default();
        let mut keys = Vec::new();
        for kind in 0..10 {
            for group in 0..3 {
                for slot in 0..3 {
                    keys.push(key(kind, group, slot, 1));
                }
            }
        }
        keys.sort();
        keys.dedup();
        for (i, key) in keys.iter().enumerate() {
            versions.insert(key.clone(), i as u64 + 1);
        }
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(versions.get(key), i as u64 + 1);
        }
    }
}
