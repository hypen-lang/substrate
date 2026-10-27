//! Compact immutable index postings. Most equality/reference buckets contain
//! only one or two occurrences; allocating a B+tree node for each wastes space.
//! Representation changes are physical only and keep sorted set semantics.
use im::{OrdSet, ordset, shared_ptr::DefaultSharedPtr};
use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

const SMALL_LIMIT: usize = 8;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum Posting<T: Ord + Clone> {
    #[default]
    Empty,
    One(T),
    Small(Arc<[T]>),
    Tree(Arc<OrdSet<T>>),
}

impl<T: Ord + Clone> Posting<T> {
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Small(v) => v.len(),
            Self::Tree(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
    fn small(&self) -> Option<&[T]> {
        match self {
            Self::Empty => Some(&[]),
            Self::One(v) => Some(std::slice::from_ref(v)),
            Self::Small(v) => Some(v),
            Self::Tree(_) => None,
        }
    }
    fn from_small(values: Vec<T>) -> Self {
        match values.len() {
            0 => Self::Empty,
            1 => Self::One(values.into_iter().next().unwrap()),
            2..=SMALL_LIMIT => Self::Small(values.into()),
            _ => Self::Tree(Arc::new(values.into_iter().collect())),
        }
    }
    pub fn insert(&mut self, value: T) -> bool {
        if matches!(self, Self::Empty) {
            *self = Self::One(value);
            return true;
        }
        if let Self::Tree(tree) = self {
            return Arc::make_mut(tree).insert(value).is_none();
        }
        let values = self.small().unwrap();
        let slot = match values.binary_search(&value) {
            Ok(_) => return false,
            Err(slot) => slot,
        };
        let mut next = Vec::with_capacity(values.len() + 1);
        next.extend_from_slice(&values[..slot]);
        next.push(value);
        next.extend_from_slice(&values[slot..]);
        *self = Self::from_small(next);
        true
    }
    pub fn remove(&mut self, value: &T) -> bool {
        if let Self::One(existing) = self {
            if existing != value {
                return false;
            }
            *self = Self::Empty;
            return true;
        }
        if let Self::Tree(tree) = self {
            if Arc::make_mut(tree).remove(value).is_none() {
                return false;
            }
            if tree.len() <= SMALL_LIMIT {
                *self = Self::from_small(tree.iter().cloned().collect());
            }
            return true;
        }
        let values = self.small().unwrap();
        let Ok(slot) = values.binary_search(value) else {
            return false;
        };
        let mut next = Vec::with_capacity(values.len() - 1);
        next.extend_from_slice(&values[..slot]);
        next.extend_from_slice(&values[slot + 1..]);
        *self = Self::from_small(next);
        true
    }
    pub fn iter(&self) -> Iter<'_, T> {
        match self {
            Self::Tree(tree) => Iter::Tree(tree.iter()),
            _ => Iter::Small(self.small().unwrap().iter()),
        }
    }
    pub fn range(&self, bounds: impl RangeBounds<T>) -> RangeIter<'_, T> {
        if let Self::Tree(tree) = self {
            return RangeIter::Tree(tree.range(bounds));
        }
        let values = self.small().unwrap();
        let start = match bounds.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(v) => values.partition_point(|x| x < v),
            Bound::Excluded(v) => values.partition_point(|x| x <= v),
        };
        let end = match bounds.end_bound() {
            Bound::Unbounded => values.len(),
            Bound::Included(v) => values.partition_point(|x| x <= v),
            Bound::Excluded(v) => values.partition_point(|x| x < v),
        };
        RangeIter::Small(values[start..end.max(start)].iter())
    }
    /// Inline singleton values need no separately shared allocation.
    #[cfg(test)]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::One(a), Self::One(b)) => a == b,
            (Self::Small(a), Self::Small(b)) => Arc::ptr_eq(a, b),
            (Self::Tree(a), Self::Tree(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

pub(crate) enum Iter<'a, T> {
    Small(std::slice::Iter<'a, T>),
    Tree(ordset::Iter<'a, T, DefaultSharedPtr>),
}
impl<'a, T: Ord + Clone> IntoIterator for &'a Posting<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a, T: Ord> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next(),
            Self::Tree(i) => i.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Small(i) => i.size_hint(),
            Self::Tree(i) => i.size_hint(),
        }
    }
}
impl<T: Ord> DoubleEndedIterator for Iter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next_back(),
            Self::Tree(i) => i.next_back(),
        }
    }
}
impl<T: Ord> ExactSizeIterator for Iter<'_, T> {}
impl<T: Ord> std::iter::FusedIterator for Iter<'_, T> {}

pub(crate) enum RangeIter<'a, T> {
    Small(std::slice::Iter<'a, T>),
    Tree(ordset::RangedIter<'a, T, DefaultSharedPtr>),
}
impl<'a, T: Ord> Iterator for RangeIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next(),
            Self::Tree(i) => i.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Small(i) => i.size_hint(),
            Self::Tree(i) => i.size_hint(),
        }
    }
}
impl<T: Ord> DoubleEndedIterator for RangeIter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next_back(),
            Self::Tree(i) => i.next_back(),
        }
    }
}
impl<T: Ord> std::iter::FusedIterator for RangeIter<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    #[test]
    fn promotes_demotes_and_preserves_snapshots() {
        let mut posting = Posting::default();
        for i in 0..=SMALL_LIMIT {
            assert!(posting.insert(i));
        }
        assert!(matches!(posting, Posting::Tree(_)));
        let old = posting.clone();
        assert!(posting.remove(&SMALL_LIMIT));
        assert!(matches!(posting, Posting::Small(_)));
        let small = posting.clone();
        assert!(small.ptr_eq(&posting));
        for i in 1..SMALL_LIMIT {
            assert!(posting.remove(&i));
        }
        assert!(matches!(posting, Posting::One(0)));
        assert!(posting.remove(&0));
        assert!(posting.is_empty());
        assert_eq!(
            old.iter().copied().collect::<Vec<_>>(),
            (0..=SMALL_LIMIT).collect::<Vec<_>>()
        );
        assert_eq!(
            small.iter().copied().collect::<Vec<_>>(),
            (0..SMALL_LIMIT).collect::<Vec<_>>()
        );
    }

    proptest! {
        #[test]
        fn matches_sorted_reference_with_retained_snapshots(
            operations in prop::collection::vec((any::<bool>(), 0u8..32), 0..300),
            low in 0u8..32, high in 0u8..32,
        ) {
            let (low, high) = (low.min(high), low.max(high));
            let mut actual = Posting::default();
            let mut expected = BTreeSet::new();
            for (insert, value) in operations {
                let old = actual.clone();
                let old_values = expected.iter().copied().collect::<Vec<_>>();
                if insert { prop_assert_eq!(actual.insert(value), expected.insert(value)); }
                else { prop_assert_eq!(actual.remove(&value), expected.remove(&value)); }
                prop_assert_eq!(old.iter().copied().collect::<Vec<_>>(), old_values);
                prop_assert_eq!(actual.len(), expected.len());
                prop_assert_eq!(actual.iter().copied().collect::<Vec<_>>(), expected.iter().copied().collect::<Vec<_>>());
                prop_assert_eq!(actual.iter().rev().copied().collect::<Vec<_>>(), expected.iter().rev().copied().collect::<Vec<_>>());
                for (a,b) in [(Bound::Included(low),Bound::Included(high)), (Bound::Excluded(low),Bound::Included(high)), (Bound::Unbounded,Bound::Excluded(high))] {
                    let reference = expected.iter().filter(|v| (a,b).contains(v)).copied().collect::<Vec<_>>();
                    prop_assert_eq!(actual.range((a,b)).copied().collect::<Vec<_>>(), reference.clone());
                    prop_assert_eq!(actual.range((a,b)).rev().copied().collect::<Vec<_>>(), reference.into_iter().rev().collect::<Vec<_>>());
                }
            }
        }
    }
}
