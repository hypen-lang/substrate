//! Borrowed fixed-width range source. Native/general range traversal stays in
//! `range`; this path uses the same bounds, index checks and visibility budgets.
use super::packed_scan::FixedScanPlan;
use super::*;
use std::ops::{Bound, ControlFlow};

pub(super) trait PackedRangeVisitor {
    fn owned(&mut self, row: &Row) -> Result<()>;
    fn packed(&mut self, id: Id, row: FixedPackedRow<'_, '_>) -> Result<()>;
}

#[derive(Clone, Copy)]
enum IndexPayload {
    Bool,
    Number,
    Reference,
}

/// Validate the complete canonical fixed-width key in place. The prefix is
/// query-local; no row-sized allocation or table-sized validation cache is used.
struct FixedIndexCheck {
    prefix: [u8; 25],
    role: usize,
    payload: IndexPayload,
    payload_end: usize,
    unique: bool,
}
impl FixedIndexCheck {
    fn compile(def: &CompiledRelation, role: usize, unique: Option<usize>) -> Option<Self> {
        let (tag, payload, width) = match def.roles[role].data_type {
            DataType::Bool => (1, IndexPayload::Bool, 1),
            DataType::Int => (2, IndexPayload::Number, 8),
            DataType::Ref(_) => (4, IndexPayload::Reference, 16),
            DataType::Decimal { .. } => (5, IndexPayload::Number, 8),
            DataType::Timestamp => (6, IndexPayload::Number, 8),
            _ => return None,
        };
        let mut prefix = [0; 25];
        prefix[..16].copy_from_slice(def.id.0.as_bytes());
        prefix[16..24].copy_from_slice(&(unique.unwrap_or(role) as u64).to_be_bytes());
        prefix[24] = tag;
        Some(Self {
            prefix,
            role,
            payload,
            payload_end: 25 + width,
            unique: unique.is_some(),
        })
    }

    #[inline]
    fn matches(&self, key: &[u8], id: Id, row: FixedPackedRow<'_, '_>) -> bool {
        if key.len() != self.payload_end + if self.unique { 1 } else { 16 }
            || key[..25] != self.prefix
        {
            return false;
        }
        let payload_matches = match self.payload {
            IndexPayload::Bool => row.number(self.role).is_some_and(|n| key[25] == n as u8),
            IndexPayload::Number => row.number(self.role).is_some_and(|n| {
                key[25..self.payload_end] == ((n as u64) ^ (1 << 63)).to_be_bytes()
            }),
            IndexPayload::Reference => row
                .reference(self.role)
                .is_some_and(|value| &key[25..self.payload_end] == value.0.as_bytes()),
        };
        payload_matches
            && if self.unique {
                key[self.payload_end] == 0
            } else {
                &key[self.payload_end..] == id.0.as_bytes()
            }
    }
}

#[derive(Default)]
struct RootReader {
    direct: Option<dense::RowValueGuard>,
    cursor: dense::ScanCursor,
    previous: Option<Id>,
    direction: Option<std::cmp::Ordering>,
    ordered_steps: usize,
}
impl RootReader {
    fn get(&mut self, snapshot: &PagedSnapshot, tid: Id, id: Id) -> Result<&[u8]> {
        if let Some(previous) = self.previous {
            let direction = id.cmp(&previous);
            if direction != std::cmp::Ordering::Equal {
                if self.direction == Some(direction) {
                    self.ordered_steps = self.ordered_steps.saturating_add(1);
                } else {
                    self.direction = Some(direction);
                    self.ordered_steps = 1;
                    self.cursor = dense::ScanCursor::default();
                }
            }
        }
        self.previous = Some(id);
        if self.ordered_steps >= 8 {
            self.direct = None;
            return self.cursor.get(&snapshot.view.rows, tid, id);
        }
        snapshot.count(metrics::ReadCounter::RowTableGet);
        self.direct = snapshot
            .view
            .rows
            .get(row_key_array(tid, id).as_slice())
            .map_err(err)?;
        self.direct
            .as_ref()
            .map(|guard| guard.value())
            .ok_or_else(|| err("dangling index"))
    }
}

impl PagedSnapshot {
    /// False means no source was consumed: use the general range path. Root
    /// filters, recursive references, nullable/float/variable-width roots and
    /// filtered terminal targets retain that path's complete semantics.
    pub(super) fn visit_fixed_authorized_range(
        &self,
        name: &str,
        role: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
        check: &mut impl FnMut() -> Result<()>,
        visitor: &mut impl PackedRangeVisitor,
    ) -> Result<bool> {
        let Some(FixedScanPlan {
            def,
            layout,
            terminal,
        }) = self.fixed_scan_plan(name)?
        else {
            return Ok(false);
        };
        check()?;
        let slot = def.role(role)?;
        for (value, _) in lower.into_iter().chain(upper) {
            if !def.roles[slot].data_type.accepts(value) {
                return Err(Error::Query("index value type mismatch".into()));
            }
        }
        let unique = def
            .unique
            .iter()
            .position(|key| key.len() == 1 && key[0] == role);
        let index_slot = unique.unwrap_or(slot);
        let fixed_index = FixedIndexCheck::compile(&def, slot, unique);
        let mut prefix = def.id.0.as_bytes().to_vec();
        prefix.extend_from_slice(&(index_slot as u64).to_be_bytes());
        let start = lower.map_or_else(
            || Some(prefix.clone()),
            |(value, inclusive)| {
                let key = index_prefix(def.id, index_slot, value);
                if inclusive {
                    Some(key)
                } else {
                    prefix_end(&key)
                }
            },
        );
        let end = upper.map_or_else(
            || prefix_end(&prefix),
            |(value, inclusive)| {
                let key = index_prefix(def.id, index_slot, value);
                if inclusive {
                    prefix_end(&key)
                } else {
                    Some(key)
                }
            },
        );
        let Some(start) = start else {
            self.fence()?;
            return Ok(true);
        };
        if end.as_ref().is_some_and(|end| start >= *end) {
            check()?;
            self.fence()?;
            return Ok(true);
        }
        let bounds = (
            Bound::Included(start.as_slice()),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
        );
        let options = self.db.inner.options;
        let mut root_reader = RootReader::default();
        let mut target_cursor = dense::ScanCursor::default();
        let mut expected = Vec::new();
        let mut scanned = 0usize;
        let mut examined = 0usize;
        let mut last_accepted_examined = 0usize;
        let mut page_rows = 0usize;
        let mut page_bytes = 0usize;
        let mut consume = |key: &[u8], id: &[u8], singleton: bool| -> Result<()> {
            check()?;
            self.count(metrics::ReadCounter::IndexCandidate);
            if scanned.is_multiple_of(256) {
                self.fence()?;
            }
            scanned = scanned.wrapping_add(1);
            examined += 1;
            if examined > options.max_page_rows.saturating_mul(64) {
                return Err(Error::Query("page visibility work budget exceeded".into()));
            }
            let id = Id(uuid::Uuid::from_slice(id).map_err(err)?);
            self.count(metrics::ReadCounter::RootScanRow);
            // Retain singleton cache/graph behavior and its exact budget counts.
            if singleton {
                let row = self.raw(def.id, id)?.ok_or_else(|| err("dangling index"))?;
                let value = &row.values[slot];
                if let Some(constraint) = unique {
                    unique_key_into(
                        &mut expected,
                        def.id,
                        constraint,
                        std::slice::from_ref(value),
                    );
                } else {
                    index_key_into(&mut expected, def.id, slot, value, id);
                }
                if key != expected {
                    return Err(err("index value mismatch"));
                }
                if self.visible_checked(def.id, id, check)? {
                    if row_encoded_len(&def, &row)? > options.max_page_bytes {
                        return Err(Error::Query("row exceeds page byte bound".into()));
                    }
                    visitor.owned(&row)?;
                }
                return Ok(());
            }
            let bytes = root_reader.get(self, def.id, id)?;
            if bytes.len() > options.max_row_bytes {
                return Err(err("row exceeds configured bound"));
            }
            self.count(metrics::ReadCounter::ValidatedPackedRow);
            let row = layout.read(bytes)?;
            if row.revision() == 0 || row.revision() > self.view.revision {
                return Err(err("row revision is outside snapshot history"));
            }
            let key_matches = if let Some(index) = &fixed_index {
                index.matches(key, id, row)
            } else {
                // Future fixed layouts retain canonical validation until their
                // index representation is explicitly supported here.
                let value = row.value(slot).expect("compiled fixed role").into_owned();
                if let Some(constraint) = unique {
                    unique_key_into(
                        &mut expected,
                        def.id,
                        constraint,
                        std::slice::from_ref(&value),
                    );
                } else {
                    index_key_into(&mut expected, def.id, slot, &value, id);
                }
                key == expected
            };
            if !key_matches {
                return Err(err("index value mismatch"));
            }
            let visible = if let Some(target) = &terminal {
                // Match general indexed traversal: both the root and target
                // consume a visibility check in addition to the candidate.
                check()?;
                if options.max_visibility_rows == 0 {
                    return Err(Error::Query("reference visibility budget exceeded".into()));
                }
                self.count(metrics::ReadCounter::ReferenceCheck);
                check()?;
                if options.max_visibility_rows < 2 {
                    return Err(Error::Query("reference visibility budget exceeded".into()));
                }
                let target_id = row
                    .reference(target.slot)
                    .ok_or_else(|| err("invalid typed reference"))?;
                let packed = target_cursor.get(&self.view.rows, target.def.id, target_id)?;
                // Denied targets are still fully validated, exactly as in the
                // old streaming range path, before suppressing the root row.
                self.validate_terminal_row(target, packed)?;
                target.allowed
            } else {
                true
            };
            if visible {
                let size = bytes.len();
                if size > options.max_page_bytes {
                    return Err(Error::Query("row exceeds page byte bound".into()));
                }
                if page_bytes > options.max_page_bytes - size {
                    page_rows = 0;
                    page_bytes = 0;
                    examined -= last_accepted_examined;
                }
                visitor.packed(id, row)?;
                last_accepted_examined = examined;
                page_rows += 1;
                page_bytes += size;
                if page_rows == 256.min(options.max_page_rows) {
                    page_rows = 0;
                    page_bytes = 0;
                    examined = 0;
                    last_accepted_examined = 0;
                }
            }
            Ok(())
        };
        if unique.is_some() {
            let mut range = self.view.unique.range::<&[u8]>(bounds).map_err(err)?;
            let first = range.next();
            let second = if first.is_some() { range.next() } else { None };
            let singleton = second.is_none();
            for item in first.into_iter().chain(second) {
                let (key, id) = item.map_err(err)?;
                consume(key.value(), id.value(), singleton)?;
            }
            if !singleton
                && let ControlFlow::Break(error) = range
                    .visit_borrowed(|key, id| match consume(key, id, false) {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(error) => ControlFlow::Break(error),
                    })
                    .map_err(err)?
            {
                return Err(error);
            }
        } else {
            let mut range = self.view.index.range::<&[u8]>(bounds).map_err(err)?;
            let first = range.next();
            let second = if first.is_some() { range.next() } else { None };
            let singleton = second.is_none();
            for item in first.into_iter().chain(second) {
                let (key, _) = item.map_err(err)?;
                let key = key.value();
                consume(key, index_identity(key)?, singleton)?;
            }
            if !singleton
                && let ControlFlow::Break(error) = range
                    .visit_borrowed(|key, _| {
                        match index_identity(key).and_then(|id| consume(key, id, false)) {
                            Ok(()) => ControlFlow::Continue(()),
                            Err(error) => ControlFlow::Break(error),
                        }
                    })
                    .map_err(err)?
            {
                return Err(error);
            }
        }
        check()?;
        self.fence()?;
        Ok(true)
    }
}

// Keep malformed-key failure before candidate accounting in both owned
// lookahead and borrowed traversal, matching the general range source.
fn index_identity(key: &[u8]) -> Result<&[u8]> {
    key.get(
        key.len()
            .checked_sub(16)
            .ok_or_else(|| err("short index key"))?..,
    )
    .ok_or_else(|| err("short index key"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Relation, Role};
    use proptest::prelude::*;

    fn verify_index_check(value: Value, id: Id, unique_slot: usize) {
        let data_type = match value {
            Value::Bool(_) => DataType::Bool,
            Value::Int(_) => DataType::Int,
            Value::Ref(_) => DataType::Ref("check.Target".into()),
            Value::Decimal(_) => DataType::Decimal { scale: 2 },
            Value::Timestamp(_) => DataType::Timestamp,
            _ => unreachable!(),
        };
        let def = CompiledRelation::new(Relation::new(
            "check.Record",
            vec![
                Role::new("other", DataType::Bool),
                Role::new("value", data_type),
            ],
        ));
        let row = Row {
            id,
            values: vec![Value::Bool(true), value.clone()].into_boxed_slice(),
        };
        let bytes = encode_row(&def, &row, 1).unwrap();
        let layout = FixedPackedLayout::compile(&def).unwrap();
        let packed = layout.read(&bytes).unwrap();
        for unique in [None, Some(unique_slot)] {
            let check = FixedIndexCheck::compile(&def, 1, unique).unwrap();
            let canonical = if let Some(slot) = unique {
                unique_key(def.id, slot, std::slice::from_ref(&value))
            } else {
                index_key(def.id, 1, &value, id)
            };
            assert!(check.matches(&canonical, id, packed));
            // Every prefix/tag/payload/suffix byte must be checked. This also
            // includes the unique tuple terminator and full occurrence ID.
            for index in 0..canonical.len() {
                let mut changed = canonical.clone();
                changed[index] ^= 0x80;
                assert_eq!(check.matches(&changed, id, packed), changed == canonical);
            }
            for length in 0..canonical.len() {
                assert!(!check.matches(&canonical[..length], id, packed));
            }
            let mut extended = canonical.clone();
            extended.push(0);
            assert!(!check.matches(&extended, id, packed));
            let different_id = Id::from_u128(id.0.as_u128() ^ 1);
            assert_eq!(
                check.matches(&canonical, different_id, packed),
                unique.is_some()
            );
        }
    }

    #[test]
    fn compiled_index_validation_matches_canonical_extrema_and_rejects_malformed_keys() {
        for value in [
            Value::Bool(false),
            Value::Bool(true),
            Value::Ref(Id::from_u128(0)),
            Value::Ref(Id::from_u128(u128::MAX)),
        ] {
            verify_index_check(value, Id::from_u128(0), usize::MAX);
        }
        for number in [i64::MIN, -1, 0, 1, i64::MAX] {
            for value in [
                Value::Int(number),
                Value::Decimal(number),
                Value::Timestamp(number),
            ] {
                verify_index_check(value, Id::from_u128(u128::MAX), 7);
            }
        }
        for data_type in [
            DataType::Text,
            DataType::Bytes,
            DataType::Float,
            DataType::Int.nullable(),
        ] {
            let def = CompiledRelation::new(Relation::new(
                "check.Unsupported",
                vec![Role::new("value", data_type)],
            ));
            assert!(FixedIndexCheck::compile(&def, 0, None).is_none());
        }
    }

    proptest! {
        #[test]
        fn compiled_index_validation_matches_canonical_random_values(
            number in any::<i64>(),
            reference in any::<u128>(),
            occurrence in any::<u128>(),
            constraint in any::<usize>(),
        ) {
            for value in [
                Value::Int(number),
                Value::Decimal(number),
                Value::Timestamp(number),
                Value::Ref(Id::from_u128(reference)),
            ] {
                verify_index_check(value, Id::from_u128(occurrence), constraint);
            }
        }
    }
}
