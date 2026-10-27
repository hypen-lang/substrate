//! Application-facing types; no JSON interpretation in the write path.
use crate::*;
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc, time::Duration};

pub type Fields = BTreeMap<String, Value>;

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Includes the first attempt. Must be nonzero.
    pub max_attempts: u32,
    pub delay: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: Duration::from_millis(1),
        }
    }
}
impl Session {
    /// Retry only optimistic conflicts, with a fresh transaction each time.
    /// The closure may run repeatedly: keep network calls, notifications, and other
    /// external effects outside it. Storage failures are ambiguous and never retried.
    pub fn transact<T>(
        &self,
        policy: RetryPolicy,
        mut f: impl FnMut(&mut Transaction) -> Result<T>,
    ) -> Result<T> {
        if policy.max_attempts == 0 {
            return Err(Error::Constraint("max_attempts must be positive".into()));
        }
        for attempt in 0..policy.max_attempts {
            let mut tx = self.transaction();
            let result = f(&mut tx).and_then(|value| tx.commit().map(|_| value));
            match result {
                Err(Error::Conflict) if attempt + 1 < policy.max_attempts => {
                    std::thread::sleep(policy.delay)
                }
                result => return result,
            }
        }
        unreachable!("nonzero attempts")
    }
}

/// Phantom typed identity. From_raw is an explicit boundary, not a membership check;
/// referential constraints remain authoritative on every write.
pub struct TypedId<R> {
    raw: Id,
    marker: PhantomData<fn() -> R>,
}
impl<R> TypedId<R> {
    pub fn from_raw(raw: Id) -> Self {
        Self {
            raw,
            marker: PhantomData,
        }
    }
    pub fn raw(self) -> Id {
        self.raw
    }
}
impl<R> Copy for TypedId<R> {}
impl<R> Clone for TypedId<R> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<R> std::fmt::Debug for TypedId<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.raw.fmt(f)
    }
}
impl<R> PartialEq for TypedId<R> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}
impl<R> Eq for TypedId<R> {}
impl<R> From<TypedId<R>> for Value {
    fn from(id: TypedId<R>) -> Self {
        Value::Ref(id.raw)
    }
}

/// Implemented by generated schema records. Definition ID guards against loading a
/// different schema package under the same human-readable name.
pub trait Record: Sized {
    const RELATION: &'static str;
    const TYPE_ID: Id;
    const FINGERPRINT: Option<&'static str> = None;
    fn fields(self) -> Fields;
    fn decode(row: Row) -> Result<Self>;
}
impl Snapshot {
    pub fn get_record<R: Record>(&self, id: TypedId<R>) -> Result<Option<R>> {
        if self.schema(R::RELATION)?.id != R::TYPE_ID
            || R::FINGERPRINT.is_some_and(|f| self.state.relations[&R::TYPE_ID].fingerprint != f)
        {
            return Err(Error::Schema("generated schema identity mismatch".into()));
        }
        self.get(R::RELATION, id.raw())?.map(R::decode).transpose()
    }
    pub fn named(&self, name: &str, row: Row) -> Result<Fields> {
        let def = self.resolve_read(name)?;
        if row.values.len() != def.roles.len() {
            return Err(Error::Constraint("row arity mismatch".into()));
        }
        Ok(def
            .roles
            .iter()
            .map(|r| r.name.clone())
            .zip(row.values.into_vec())
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPage {
    pub revision: u64,
    pub rows: Vec<Row>,
    pub next: Option<Id>,
}
impl Snapshot {
    /// Keyset pagination on one immutable snapshot. Equality predicates use the
    /// role index. At most limit+1 visible rows are materialized, not a full family.
    pub fn page(
        &self,
        relation: &str,
        predicate: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<RowPage> {
        let page = self.page_shared(relation, predicate, after, limit)?;
        Ok(RowPage {
            revision: self.revision(),
            rows: page.rows.into_iter().map(|row| (*row).clone()).collect(),
            next: page.next,
        })
    }
    pub(crate) fn page_shared(
        &self,
        relation: &str,
        predicate: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
    ) -> Result<SharedRowPage> {
        self.page_shared_checked(relation, predicate, after, limit, &mut || Ok(()))
    }
    /// Internal cancellable paging. The callback runs for every candidate,
    /// including denied rows, and after traversal before results are returned.
    /// One candidate's reference-closure authorization remains indivisible.
    pub(crate) fn page_shared_checked(
        &self,
        relation: &str,
        predicate: Option<(&str, &Value)>,
        after: Option<Id>,
        limit: usize,
        check: &mut dyn FnMut() -> Result<()>,
    ) -> Result<SharedRowPage> {
        use std::ops::Bound::{Excluded, Unbounded};
        if !(1..=1000).contains(&limit) {
            return Err(Error::Query("page limit must be 1..=1000".into()));
        }
        let def = self.resolve_read(relation)?;
        let lower = after.map_or(Unbounded, Excluded);
        let ids: Box<dyn Iterator<Item = &Id> + '_> = if let Some((role, value)) = predicate {
            let slot = def.role(role)?;
            if !def.roles[slot].data_type.accepts(value) {
                return Err(Error::Query("predicate type mismatch".into()));
            }
            match self.state.index.get(&(def.id, slot, value.clone())) {
                Some(set) => Box::new(set.range((lower, Unbounded))),
                None => Box::new(std::iter::empty()),
            }
        } else {
            Box::new(
                self.state.families[&def.id]
                    .range((lower, Unbounded))
                    .map(|(id, _)| id),
            )
        };
        let mut rows = Vec::with_capacity(limit + 1);
        let mut visibility = self
            .principal
            .as_ref()
            .map(|_| self.visibility.lock().map_err(|_| Error::Denied))
            .transpose()?;
        for id in ids {
            check()?;
            let Some(row) = self.state.row(def.id, *id) else {
                continue;
            };
            if visibility.as_mut().is_none_or(|memo| {
                memo.visible_row(&self.state, self.principal.as_deref().unwrap(), &def, row)
            }) {
                rows.push(row.clone());
                if rows.len() > limit {
                    break;
                }
            }
        }
        let next = if rows.len() > limit {
            rows.pop();
            rows.last().map(|r| r.id)
        } else {
            None
        };
        check()?;
        self.check_live()?;
        Ok(SharedRowPage { rows, next })
    }
    pub(crate) fn get_shared(&self, relation: &str, id: Id) -> Result<Option<Arc<Row>>> {
        let def = self.resolve_read(relation)?;
        let row = if let Some(row) = self.state.row(def.id, id) {
            let visible = if let Some(principal) = self.principal.as_deref() {
                self.visibility
                    .lock()
                    .map_err(|_| Error::Denied)?
                    .visible_row(&self.state, principal, &def, row)
            } else {
                true
            };
            visible.then(|| row.clone())
        } else {
            None
        };
        self.check_live()?;
        Ok(row)
    }
}
pub(crate) struct SharedRowPage {
    pub rows: Vec<Arc<Row>>,
    pub next: Option<Id>,
}

/// A typed, numeric index range used only by immutable SQL cursors. SQL rechecks
/// every predicate. The continuation is (value, occurrence ID), including ties.
#[derive(Clone)]
pub(crate) struct NumericRange {
    pub slot: usize,
    pub lower: std::ops::Bound<Value>,
    pub upper: std::ops::Bound<Value>,
    pub descending: bool,
}
pub(crate) struct SharedRangePage {
    pub rows: Vec<Arc<Row>>,
    pub next: Option<(Value, Id)>,
}
impl Snapshot {
    pub(crate) fn range_page_shared(
        &self,
        relation: &str,
        range: &NumericRange,
        after: Option<&(Value, Id)>,
        limit: usize,
    ) -> Result<SharedRangePage> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let def = self.resolve_read(relation)?;
        let Some(role) = def.roles.get(range.slot) else {
            return Err(Error::Query("invalid range slot".into()));
        };
        let (minimum, maximum) = match role.data_type {
            DataType::Bool => (Value::Bool(false), Value::Bool(true)),
            DataType::Int => (Value::Int(i64::MIN), Value::Int(i64::MAX)),
            DataType::Decimal { .. } => (Value::Decimal(i64::MIN), Value::Decimal(i64::MAX)),
            DataType::Timestamp => (Value::Timestamp(i64::MIN), Value::Timestamp(i64::MAX)),
            _ => return Err(Error::Query("numeric range required".into())),
        };
        let mut lower = match &range.lower {
            Unbounded => Included(minimum),
            v => v.clone(),
        };
        let mut upper = match &range.upper {
            Unbounded => Included(maximum),
            v => v.clone(),
        };
        if let Some((value, _)) = after {
            if range.descending {
                upper = Included(value.clone());
            } else {
                lower = Included(value.clone());
            }
        }
        fn value(b: &std::ops::Bound<Value>) -> &Value {
            match b {
                Included(v) | Excluded(v) => v,
                Unbounded => unreachable!(),
            }
        }
        if value(&lower) > value(&upper)
            || (value(&lower) == value(&upper)
                && (matches!(lower, Excluded(_)) || matches!(upper, Excluded(_))))
        {
            self.check_live()?;
            return Ok(SharedRangePage {
                rows: vec![],
                next: None,
            });
        }
        let key = |v| (def.id, range.slot, v);
        let lower = lower.map(key);
        let upper = upper.map(key);
        let buckets = self.state.index.range((lower, upper));
        let buckets: Box<dyn Iterator<Item = _> + '_> = if range.descending {
            Box::new(buckets.rev())
        } else {
            Box::new(buckets)
        };
        let mut rows = Vec::with_capacity(limit + 1);
        let mut visibility = self
            .principal
            .as_ref()
            .map(|_| self.visibility.lock().map_err(|_| Error::Denied))
            .transpose()?;
        'buckets: for ((_, _, value), ids) in buckets {
            let ids: Box<dyn Iterator<Item = &Id> + '_> = if range.descending {
                Box::new(ids.iter().rev())
            } else {
                Box::new(ids.iter())
            };
            for id in ids {
                if after.is_some_and(|(v, i)| {
                    v == value && if range.descending { id >= i } else { id <= i }
                }) {
                    continue;
                }
                let Some(row) = self.state.row(def.id, *id) else {
                    continue;
                };
                if visibility.as_mut().is_none_or(|memo| {
                    memo.visible_row(&self.state, self.principal.as_deref().unwrap(), &def, row)
                }) {
                    rows.push(row.clone());
                    if rows.len() > limit {
                        break 'buckets;
                    }
                }
            }
        }
        let next = if rows.len() > limit {
            rows.pop();
            rows.last()
                .map(|row| (row.values[range.slot].clone(), row.id))
        } else {
            None
        };
        self.check_live()?;
        Ok(SharedRangePage { rows, next })
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    #[test]
    fn internal_cursor_pages_share_stored_rows_instead_of_cloning_payloads() {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "test",
            1,
            vec![],
            vec![Relation::new(
                "test.Blob",
                vec![Role::new("bytes", DataType::Bytes)],
            )],
        ))
        .unwrap();
        let id = tx
            .insert("test.Blob", vec![Value::Bytes(vec![42; 65536])])
            .unwrap();
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot();
        let tid = snapshot.schema("test.Blob").unwrap().id;
        let stored = snapshot.state.row(tid, id).unwrap();
        let page = snapshot.page_shared("test.Blob", None, None, 64).unwrap();
        assert!(Arc::ptr_eq(stored, &page.rows[0]));
        assert!(Arc::ptr_eq(
            stored,
            &snapshot.get_shared("test.Blob", id).unwrap().unwrap()
        ));
    }
}
