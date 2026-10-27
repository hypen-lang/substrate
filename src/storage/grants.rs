//! Persistent compiled grants grouped by principal. Borrowed lookup avoids
//! constructing a String for every authorization check; durable entries remain
//! the same ordered (principal, relation, declaration) triples.
use crate::{Id, compiled::CompiledGrant};
use im::OrdMap;

#[derive(Clone, Default)]
pub(crate) struct Grants {
    principals: OrdMap<String, OrdMap<Id, CompiledGrant>>,
    len: usize,
}

impl Grants {
    pub fn get(&self, principal: &str, relation: Id) -> Option<&CompiledGrant> {
        self.principals.get(principal)?.get(&relation)
    }

    pub fn contains_key(&self, key: &(String, Id)) -> bool {
        self.get(&key.0, key.1).is_some()
    }

    pub fn insert(&mut self, (principal, relation): (String, Id), grant: CompiledGrant) {
        let grants = self.principals.entry(principal).or_default();
        if grants.insert(relation, grant).is_none() {
            self.len += 1;
        }
    }

    pub fn remove(&mut self, key: &(String, Id)) {
        let Some(grants) = self.principals.get_mut(key.0.as_str()) else {
            return;
        };
        if grants.remove(&key.1).is_some() {
            self.len -= 1;
        }
        if grants.is_empty() {
            self.principals.remove(key.0.as_str());
        }
    }

    pub fn clear(&mut self) {
        self.principals.clear();
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = ((&String, &Id), &CompiledGrant)> {
        self.principals.iter().flat_map(|(principal, grants)| {
            grants
                .iter()
                .map(move |(relation, grant)| ((principal, relation), grant))
        })
    }
}

impl FromIterator<((String, Id), CompiledGrant)> for Grants {
    fn from_iter<T: IntoIterator<Item = ((String, Id), CompiledGrant)>>(iter: T) -> Self {
        let mut grants = Self::default();
        for (key, grant) in iter {
            grants.insert(key, grant);
        }
        grants
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Grant, compiled::CompiledRelation, demo};

    fn grant(read: bool) -> CompiledGrant {
        let relation = CompiledRelation::new(demo::core().relations[0].clone());
        CompiledGrant::new(
            Grant {
                read,
                ..Grant::read_only()
            },
            &relation,
        )
    }

    #[test]
    fn borrowed_lookup_replacement_removal_and_snapshot_isolation() {
        let first = Id::new();
        let second = Id::new();
        let mut grants = Grants::default();
        grants.insert(("alice".into(), first), grant(true));
        grants.insert(("alice".into(), second), grant(false));
        grants.insert(("bob".into(), first), grant(false));
        let old = grants.clone();
        grants.insert(("alice".into(), first), grant(false));
        assert_eq!(grants.len(), 3);
        assert!(old.get("alice", first).unwrap().read);
        assert!(!grants.get("alice", first).unwrap().read);
        assert!(grants.get("ali", first).is_none());
        assert!(grants.get("alice", Id::new()).is_none());
        grants.remove(&("alice".into(), first));
        grants.remove(&("alice".into(), first));
        grants.remove(&("absent".into(), first));
        assert_eq!(grants.len(), 2);
        assert!(grants.get("alice", second).is_some());
        grants.remove(&("alice".into(), second));
        assert!(!grants.principals.contains_key("alice"));
        assert_eq!(grants.len(), 1);
        assert_eq!(old.len(), 3);
        grants.clear();
        assert_eq!(grants.len(), 0);
        assert_eq!(grants.iter().count(), 0);
        assert!(old.get("bob", first).is_some());
    }

    #[test]
    fn canonical_iteration_matches_flat_tuple_order_and_duplicate_collection() {
        let a = Id(uuid::Uuid::from_u128(1));
        let b = Id(uuid::Uuid::from_u128(2));
        let entries = [
            (("bob".into(), b), grant(true)),
            (("alice".into(), b), grant(false)),
            (("bob".into(), a), grant(false)),
            (("alice".into(), a), grant(true)),
            (("alice".into(), b), grant(true)),
        ];
        let flat: OrdMap<(String, Id), CompiledGrant> = entries.clone().into_iter().collect();
        let nested: Grants = entries.into_iter().collect();
        assert_eq!(nested.len(), flat.len());
        let actual: Vec<_> = nested
            .iter()
            .map(|((p, id), g)| ((p.clone(), *id), g.read))
            .collect();
        let expected: Vec<_> = flat.iter().map(|(key, g)| (key.clone(), g.read)).collect();
        assert_eq!(actual, expected);
    }

    proptest::proptest! {
        #[test]
        fn mixed_grant_updates_match_flat_reference(
            ops in proptest::collection::vec((0u8..8, 0u8..16, proptest::bool::ANY, proptest::bool::ANY), 0..100)
        ) {
            let mut nested = Grants::default();
            let mut flat = std::collections::BTreeMap::new();
            for (principal, relation, read, remove) in ops {
                let key = (format!("app-{principal}"), Id(uuid::Uuid::from_u128(relation as u128)));
                let retained = nested.clone();
                let before = flat.clone();
                if remove {
                    nested.remove(&key);
                    flat.remove(&key);
                } else {
                    nested.insert(key.clone(), grant(read));
                    flat.insert(key.clone(), read);
                }
                proptest::prop_assert_eq!(nested.get(&key.0, key.1).map(|g| g.read), flat.get(&key).copied());
                proptest::prop_assert_eq!(nested.len(), flat.len());
                let actual: Vec<_> = nested.iter().map(|((p, id), g)| ((p.clone(), *id), g.read)).collect();
                let expected: Vec<_> = flat.iter().map(|(key, read)| (key.clone(), *read)).collect();
                proptest::prop_assert_eq!(actual, expected);
                let old: Vec<_> = retained.iter().map(|((p, id), g)| ((p.clone(), *id), g.read)).collect();
                proptest::prop_assert_eq!(old, before.into_iter().collect::<Vec<_>>());
            }
        }
    }
}
