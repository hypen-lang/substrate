//! Bounded, authorized traversal of a typed role index. Both the legacy role
//! index and the compact single-role unique index preserve typed value order.
use super::*;
use std::ops::Bound;

/// Random index values usually imply random occurrence IDs. Begin with point
/// requests and only retain a streaming cursor after a sustained ordered run.
/// Direction changes discard the cursor immediately; storage is always bounded
/// to one direct guard or one cursor, independently of the relation size.
#[derive(Default)]
struct RangeRootReader {
    direct: Option<dense::RowValueGuard>,
    cursor: TerminalCursor,
    previous: Option<Id>,
    direction: Option<std::cmp::Ordering>,
    ordered_steps: usize,
}
impl RangeRootReader {
    const PROMOTE_AFTER: usize = 8;

    fn get(&mut self, snapshot: &PagedSnapshot, tid: Id, id: Id) -> Result<&[u8]> {
        if let Some(previous) = self.previous {
            let direction = id.cmp(&previous);
            if direction != std::cmp::Ordering::Equal {
                if self.direction == Some(direction) {
                    self.ordered_steps = self.ordered_steps.saturating_add(1);
                } else {
                    self.direction = Some(direction);
                    self.ordered_steps = 1;
                    self.cursor = TerminalCursor::default();
                }
            }
        }
        self.previous = Some(id);
        if self.ordered_steps >= Self::PROMOTE_AFTER {
            self.direct = None;
            // Initialize in the observed direction, including descending runs.
            self.cursor.reverse = self.direction == Some(std::cmp::Ordering::Less);
            return self
                .cursor
                .get_with_missing(&snapshot.view.rows, tid, id, "dangling index");
        }
        let mut key = [0; 32];
        key[..16].copy_from_slice(tid.0.as_bytes());
        key[16..].copy_from_slice(id.0.as_bytes());
        snapshot.count(metrics::ReadCounter::RowTableGet);
        self.direct = snapshot.view.rows.get(key.as_slice()).map_err(err)?;
        self.direct
            .as_ref()
            .map(|row| row.value())
            .ok_or_else(|| err("dangling index"))
    }
}

/// Streaming state is created only after lookahead proves that the range has
/// multiple candidates. Singleton reads use the cache and general visibility
/// traversal, so preparing target policies or cursors for them is wasted work.
struct RangeStreamState {
    policy: ScanReadPolicy,
    terminal: Option<(usize, Arc<CompiledRelation>, ScanReadPolicy)>,
    root_reader: RangeRootReader,
    target_cursor: TerminalCursor,
    root_scratch: Option<Row>,
    target_scratch: Option<Row>,
}
impl RangeStreamState {
    fn new(snapshot: &PagedSnapshot, def: &CompiledRelation) -> Self {
        let terminal = if let [(slot, name)] = def.reference_slots.as_slice()
            && !def.roles[*slot].data_type.is_nullable()
        {
            let tid = snapshot.view.catalog.state.names[name];
            let target = &snapshot.view.catalog.state.relations[&tid];
            target
                .reference_slots
                .is_empty()
                .then(|| (*slot, target.clone(), snapshot.scan_read_policy(tid)))
        } else {
            None
        };
        Self {
            policy: snapshot.scan_read_policy(def.id),
            terminal,
            root_reader: RangeRootReader::default(),
            target_cursor: TerminalCursor::default(),
            root_scratch: None,
            target_scratch: None,
        }
    }
}

// Parse only the exact fixed-width numeric shape. A cost hint must never
// interpret a malformed key or a neighbouring index family's bytes as a value.
fn numeric_index_value(key: &[u8], prefix: &[u8], tag: u8, unique: bool) -> Option<i128> {
    let payload = if tag == 1 { 2 } else { 9 };
    let suffix = if unique { 1 } else { 16 };
    if key.len() != prefix.len() + payload + suffix
        || !key.starts_with(prefix)
        || key[prefix.len()] != tag
    {
        return None;
    }
    if unique && key.last() != Some(&0) {
        return None;
    }
    let bytes = &key[prefix.len() + 1..prefix.len() + payload];
    if tag == 1 {
        match bytes[0] {
            0 | 1 => Some(i128::from(bytes[0])),
            _ => None,
        }
    } else {
        let ordered = u64::from_be_bytes(bytes.try_into().ok()?);
        Some(i128::from((ordered ^ (1 << 63)) as i64))
    }
}

fn numeric_selectivity(
    min: i128,
    max: i128,
    bound: i128,
    lower: bool,
    inclusive: bool,
) -> Option<f64> {
    if min > max {
        return None;
    }
    let span = max.checked_sub(min)?.checked_add(1)?;
    let selected = if lower {
        let first = bound.checked_add(i128::from(!inclusive))?.max(min);
        max.checked_sub(first)?.checked_add(1)?.clamp(0, span)
    } else {
        let last = bound.checked_sub(i128::from(!inclusive))?.min(max);
        last.checked_sub(min)?.checked_add(1)?.clamp(0, span)
    };
    Some(selected as f64 / span as f64)
}

impl PagedSnapshot {
    /// Planner-only estimate from the numeric index's observed endpoints. This
    /// assumes a uniform distribution; it never authorizes or skips execution.
    /// Hidden/filtered root distributions and unsupported encodings give no hint.
    pub(super) fn estimate_one_sided_range(
        &self,
        name: &str,
        role: &str,
        value: &Value,
        lower: bool,
        inclusive: bool,
    ) -> Result<Option<f64>> {
        self.fence()?;
        let def = self.def(name)?;
        if !discover(
            &self.view.catalog,
            self.principal.as_deref(),
            self.scope.as_deref(),
            def.id,
        ) {
            return Err(Error::Denied);
        }
        let slot = def.role(role)?;
        if !def.roles[slot].data_type.accepts(value) {
            return Err(Error::Query("index value type mismatch".into()));
        }
        if !matches!(self.scan_read_policy(def.id), ScanReadPolicy::All) {
            return Ok(None);
        }
        if !def.reference_slots.is_empty() {
            let [(_, target_name)] = def.reference_slots.as_slice() else {
                return Ok(None);
            };
            let tid = self.view.catalog.state.names[target_name];
            let target = &self.view.catalog.state.relations[&tid];
            if !target.reference_slots.is_empty()
                || !matches!(self.scan_read_policy(tid), ScanReadPolicy::All)
            {
                return Ok(None);
            }
        }
        let (tag, bound) = match value {
            Value::Bool(value) => (1, i128::from(*value)),
            Value::Int(value) => (2, i128::from(*value)),
            Value::Decimal(value) => (5, i128::from(*value)),
            Value::Timestamp(value) => (6, i128::from(*value)),
            _ => return Ok(None),
        };
        let unique = def
            .unique
            .iter()
            .position(|key| key.len() == 1 && key[0] == role)
            .filter(|_| !def.roles[slot].data_type.is_nullable());
        let mut prefix = def.id.0.as_bytes().to_vec();
        prefix.extend_from_slice(&(unique.unwrap_or(slot) as u64).to_be_bytes());
        let end = prefix_end(&prefix);
        let bounds = (
            Bound::Included(prefix.as_slice()),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
        );
        let endpoints = if unique.is_some() {
            let mut range = self.view.unique.range::<&[u8]>(bounds).map_err(err)?;
            match range.next().transpose().map_err(err)? {
                None => None,
                Some((key, id)) => {
                    let first = (id.value().len() == 16)
                        .then(|| numeric_index_value(key.value(), &prefix, tag, true))
                        .flatten();
                    let last = match range.next_back().transpose().map_err(err)? {
                        Some((key, id)) => (id.value().len() == 16)
                            .then(|| numeric_index_value(key.value(), &prefix, tag, true))
                            .flatten(),
                        None => first,
                    };
                    first.zip(last)
                }
            }
        } else {
            let mut range = self.view.index.range::<&[u8]>(bounds).map_err(err)?;
            match range.next().transpose().map_err(err)? {
                None => None,
                Some((key, _)) => {
                    let first = numeric_index_value(key.value(), &prefix, tag, false);
                    let last = match range.next_back().transpose().map_err(err)? {
                        Some((key, _)) => numeric_index_value(key.value(), &prefix, tag, false),
                        None => first,
                    };
                    first.zip(last)
                }
            }
        };
        self.fence()?;
        Ok(endpoints.and_then(|(min, max)| numeric_selectivity(min, max, bound, lower, inclusive)))
    }

    /// Bounds apply to typed values, not occurrence IDs. Residual SQL predicates
    /// are the caller's responsibility. No candidate or result list is retained.
    pub(super) fn visit_authorized_range(
        &self,
        name: &str,
        role: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
        check: &mut impl FnMut() -> Result<()>,
        visitor: &mut impl FnMut(&Row) -> Result<()>,
    ) -> Result<()> {
        check()?;
        self.fence()?;
        let def = self.def(name)?;
        if !discover(
            &self.view.catalog,
            self.principal.as_deref(),
            self.scope.as_deref(),
            def.id,
        ) {
            return Err(Error::Denied);
        }
        let slot = def.role(role)?;
        for (value, _) in lower.into_iter().chain(upper) {
            if !def.roles[slot].data_type.accepts(value) {
                return Err(Error::Query("index value type mismatch".into()));
            }
        }
        let unique = def
            .unique
            .iter()
            .position(|key| key.len() == 1 && key[0] == role)
            .filter(|_| !def.roles[slot].data_type.is_nullable());
        // Use the unique table in both file formats; V2 omits its duplicate
        // generic index entirely. Prefix bounds exclude/include every ID of a
        // value, including embedded-zero strings and the largest integer.
        let index_slot = unique.unwrap_or(slot);
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
            return Ok(());
        };
        if end.as_ref().is_some_and(|end| start >= *end) {
            check()?;
            self.fence()?;
            return Ok(());
        }
        let bounds = (
            Bound::Included(start.as_slice()),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
        );
        let options = self.db.inner.options;
        let mut stream = None;
        let mut expected = Vec::new();
        let mut scanned = 0usize;
        let mut examined = 0usize;
        let mut last_accepted_examined = 0usize;
        let mut page_rows = 0usize;
        let mut page_bytes = 0usize;
        let mut consume = |key: &[u8], id: &[u8], use_cache: bool| -> Result<()> {
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
            if use_cache {
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
                    visitor(&row)?;
                }
                return Ok(());
            }
            let state = stream.get_or_insert_with(|| RangeStreamState::new(self, &def));
            // General graph paths retain the bounded decoded cache. Scalar and
            // single-terminal paths stream into their reusable row buffers.
            let streamed = def.reference_slots.is_empty() || state.terminal.is_some();
            let cached = if streamed {
                None
            } else {
                Some(self.raw(def.id, id)?.ok_or_else(|| err("dangling index"))?)
            };
            let packed = if streamed {
                Some(state.root_reader.get(self, def.id, id)?)
            } else {
                None
            };
            let row = match &cached {
                Some(row) => row.as_ref(),
                None => self.decode_scan_row(
                    &def,
                    id,
                    packed.expect("simple range row"),
                    &mut state.root_scratch,
                )?,
            };
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
            let visible = if def.reference_slots.is_empty() {
                state.policy.allows(row)
            } else if let Some((reference_slot, target, target_policy)) = &state.terminal {
                // Match visible_checked's source accounting: the root and its
                // target each consume one check in addition to the candidate.
                check()?;
                if options.max_visibility_rows == 0 {
                    return Err(Error::Query("reference visibility budget exceeded".into()));
                }
                if !state.policy.allows(row) {
                    false
                } else {
                    self.count(metrics::ReadCounter::ReferenceCheck);
                    check()?;
                    if options.max_visibility_rows < 2 {
                        return Err(Error::Query("reference visibility budget exceeded".into()));
                    }
                    let Value::Ref(target_id) = row.values[*reference_slot] else {
                        return Err(err("invalid typed reference"));
                    };
                    let packed = state
                        .target_cursor
                        .get(&self.view.rows, target.id, target_id)?;
                    match target_policy {
                        ScanReadPolicy::Filter(_) => {
                            let target_row = self.decode_scan_row(
                                target,
                                target_id,
                                packed,
                                &mut state.target_scratch,
                            )?;
                            target_policy.allows(target_row)
                        }
                        ScanReadPolicy::All | ScanReadPolicy::Denied => {
                            self.validate_uncached(target, packed)?;
                            matches!(target_policy, ScanReadPolicy::All)
                        }
                    }
                }
            } else {
                self.visible_checked(def.id, id, check)?
            };
            if visible {
                let size = match packed {
                    Some(packed) => packed.len(),
                    None => row_encoded_len(&def, row)?,
                };
                if size > options.max_page_bytes {
                    return Err(Error::Query("row exceeds page byte bound".into()));
                }
                if page_bytes > options.max_page_bytes - size {
                    page_rows = 0;
                    page_bytes = 0;
                    examined -= last_accepted_examined;
                }
                visitor(row)?;
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
            for item in first
                .into_iter()
                .chain(second)
                .chain(range.take(if singleton { 0 } else { usize::MAX }))
            {
                let (key, id) = item.map_err(err)?;
                consume(key.value(), id.value(), singleton)?;
            }
        } else {
            let mut range = self.view.index.range::<&[u8]>(bounds).map_err(err)?;
            let first = range.next();
            let second = if first.is_some() { range.next() } else { None };
            let singleton = second.is_none();
            for item in first
                .into_iter()
                .chain(second)
                .chain(range.take(if singleton { 0 } else { usize::MAX }))
            {
                let (key, _) = item.map_err(err)?;
                let key = key.value();
                let id = key
                    .get(
                        key.len()
                            .checked_sub(16)
                            .ok_or_else(|| err("short index key"))?..,
                    )
                    .ok_or_else(|| err("short index key"))?;
                consume(key, id, singleton)?;
            }
        }
        check()?;
        self.fence()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_root_lookup_matches_random_uuid_index_order_and_counts_point_reads() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = PagedDatabase::open(
                dir.path().join("db"),
                PagedOptions {
                    decoded_cache_bytes: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut relation = Relation::new("r.Item", vec![Role::new("n", DataType::Int)]);
            if unique {
                relation = relation.unique(&["n"]);
            }
            let tid = relation.id;
            let mut tx = db.admin().transaction().unwrap();
            tx.install_owned(
                Module::new("r", 1, vec![], vec![relation]),
                "owner",
                Visibility::Private,
            )
            .unwrap();
            tx.grant("reader", "r.Item", Grant::read_only()).unwrap();
            // A deterministic full-width permutation exercises UUID ordering,
            // independently of value order and without timing/randomness flakes.
            let mut state = 0x243f6a8885a308d313198a2e03707344u128;
            let mut ids = Vec::new();
            for n in 0..256 {
                state ^= state << 23;
                state ^= state >> 17;
                state ^= state << 26;
                let id = Id::from_u128(state);
                ids.push(id);
                tx.insert_with_id("r.Item", id, vec![Value::Int(n)])
                    .unwrap();
            }
            tx.commit().unwrap();
            let (snapshot, metrics) = db
                .with_read_metrics(db.session("reader").unwrap().snapshot().unwrap())
                .unwrap();
            let mut actual = Vec::new();
            snapshot
                .visit_authorized_range("r.Item", "n", None, None, &mut || Ok(()), &mut |row| {
                    actual.push((row.id, row.values[0].clone()));
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                actual,
                ids.iter()
                    .enumerate()
                    .map(|(n, id)| (*id, Value::Int(n as i64)))
                    .collect::<Vec<_>>()
            );
            let counts = metrics.snapshot();
            assert_eq!(counts.index_candidates, 256);
            assert_eq!(counts.root_scan_rows, 256);
            assert_eq!(counts.decoded_cache_hits, 0);
            assert_eq!(counts.decoded_cache_misses, 0);
            // This fixture has no eight-step monotonic run: every root is one
            // point request, with no extra cursor lookahead/seeks.
            assert_eq!(counts.row_table_gets, 256);
            assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);

            let mut sorted = ids;
            sorted.sort_unstable();
            let mut reader = RangeRootReader::default();
            let before = metrics.snapshot();
            for id in sorted.iter().take(24) {
                reader.get(&snapshot, tid, *id).unwrap();
            }
            assert_eq!(metrics.snapshot().delta_since(before).row_table_gets, 8);
            assert!(reader.cursor.current.is_some());
            assert!(reader.direct.is_none());
            // Reversing discards the old cursor and restarts point reads; the
            // next descending run promotes in the opposite direction.
            let before = metrics.snapshot();
            for id in sorted.iter().take(23).rev() {
                reader.get(&snapshot, tid, *id).unwrap();
            }
            assert_eq!(metrics.snapshot().delta_since(before).row_table_gets, 7);
            assert!(reader.cursor.reverse);
            assert!(reader.direct.is_none());
        }
    }

    #[test]
    fn singleton_ranges_use_cache_but_multi_candidate_ranges_stream() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = reference_fixture(&dir.path().join("db"), unique, 65536);
            let (snapshot, metrics) = db
                .with_read_metrics(db.session("owner").unwrap().snapshot().unwrap())
                .unwrap();
            let run = |lower: i64, upper: i64| {
                let mut ids = Vec::new();
                snapshot
                    .visit_authorized_range(
                        "r.Score",
                        "n",
                        Some((&Value::Int(lower), true)),
                        Some((&Value::Int(upper), true)),
                        &mut || Ok(()),
                        &mut |row| {
                            ids.push(row.id);
                            Ok(())
                        },
                    )
                    .unwrap();
                ids
            };
            assert_eq!(run(0, 0).len(), 1);
            let before = metrics.snapshot();
            assert_eq!(run(0, 0).len(), 1);
            let warm = metrics.snapshot().delta_since(before);
            assert!(warm.decoded_cache_hits >= 2);
            assert_eq!(warm.row_table_gets, 0);
            assert_eq!(warm.decoded_rows, 0);
            let before = metrics.snapshot();
            assert_eq!(run(0, 1).len(), 2);
            let multi = metrics.snapshot().delta_since(before);
            assert_eq!(multi.decoded_cache_hits, 0);
            assert_eq!(multi.decoded_cache_misses, 0);
            assert_eq!(multi.index_candidates, 2);
            assert_eq!(multi.decoded_rows, 2);
        }
    }

    #[test]
    fn cached_singleton_preserves_checked_work_errors_and_final_authority_fence() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = reference_fixture(&dir.path().join("db"), unique, 65536);
            let snapshot = db.session("owner").unwrap().snapshot().unwrap();
            let value = Value::Int(0);
            let run = |check: &mut dyn FnMut() -> Result<()>,
                       visit: &mut dyn FnMut(&Row) -> Result<()>| {
                snapshot.visit_authorized_range(
                    "r.Score",
                    "n",
                    Some((&value, true)),
                    Some((&value, true)),
                    &mut || check(),
                    &mut |row| visit(row),
                )
            };
            let mut checks = 0;
            run(
                &mut || {
                    checks += 1;
                    Ok(())
                },
                &mut |_| Ok(()),
            )
            .unwrap();
            assert_eq!(checks, 5, "initial, candidate, root, target and final work");
            let mut returned = false;
            let mut checks = 0;
            let error = run(
                &mut || {
                    checks += 1;
                    if checks == 4 {
                        Err(Error::Query("singleton target budget".into()))
                    } else {
                        Ok(())
                    }
                },
                &mut |_| {
                    returned = true;
                    Ok(())
                },
            );
            assert!(
                matches!(error, Err(Error::Query(message)) if message == "singleton target budget")
            );
            assert!(!returned);
            let error = run(&mut || Ok(()), &mut |_| {
                Err(Error::Query("visitor failed".into()))
            });
            assert!(matches!(error, Err(Error::Query(message)) if message == "visitor failed"));
            let error = run(&mut || Ok(()), &mut |_| {
                let mut tx = db.admin().transaction()?;
                tx.set_visibility("r.Person", Visibility::Public)?;
                tx.commit()?;
                Ok(())
            });
            assert!(matches!(error, Err(Error::Denied)));
        }
    }

    #[test]
    fn numeric_range_estimates_handle_extremes_shapes_and_hidden_references() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = fixture(&dir.path().join("db"), unique);
            let snapshot = db.session("reader").unwrap().snapshot().unwrap();
            assert_eq!(
                snapshot
                    .estimate_one_sided_range("r.Item", "n", &Value::Int(0), true, true)
                    .unwrap(),
                Some(0.5)
            );
            assert_eq!(
                snapshot
                    .estimate_one_sided_range("r.Item", "n", &Value::Int(i64::MAX), true, false)
                    .unwrap(),
                Some(0.0)
            );
            assert_eq!(
                snapshot
                    .estimate_one_sided_range("r.Item", "n", &Value::Int(i64::MIN), false, false)
                    .unwrap(),
                Some(0.0)
            );
            assert!(
                snapshot
                    .estimate_one_sided_range("r.Item", "n", &Value::Int(i64::MAX), true, true)
                    .unwrap()
                    .unwrap()
                    > 0.0
            );
            assert!(matches!(
                db.anonymous().snapshot().unwrap().estimate_one_sided_range(
                    "r.Item",
                    "n",
                    &Value::Int(0),
                    true,
                    true
                ),
                Err(Error::Denied)
            ));
            let mut tx = db.admin().transaction().unwrap();
            tx.grant(
                "reader",
                "r.Item",
                Grant {
                    filter: RowFilter::Equals {
                        role: "n".into(),
                        value: 0.into(),
                    },
                    ..Grant::read_only()
                },
            )
            .unwrap();
            tx.commit().unwrap();
            assert_eq!(
                db.session("reader")
                    .unwrap()
                    .snapshot()
                    .unwrap()
                    .estimate_one_sided_range("r.Item", "n", &Value::Int(0), true, true)
                    .unwrap(),
                None
            );

            let db = reference_fixture(&dir.path().join("refs"), unique, 0);
            assert_eq!(
                db.session("reader")
                    .unwrap()
                    .snapshot()
                    .unwrap()
                    .estimate_one_sided_range("r.Score", "n", &Value::Int(40), true, true)
                    .unwrap(),
                None
            );
            assert_eq!(
                db.session("owner")
                    .unwrap()
                    .snapshot()
                    .unwrap()
                    .estimate_one_sided_range("r.Score", "n", &Value::Int(40), true, true)
                    .unwrap(),
                Some(0.5)
            );
        }
        let tid = Id::from_u128(5);
        let mut prefix = tid.0.as_bytes().to_vec();
        prefix.extend_from_slice(&0u64.to_be_bytes());
        for (value, tag, expected) in [
            (Value::Bool(false), 1, 0),
            (Value::Bool(true), 1, 1),
            (Value::Int(i64::MIN), 2, i128::from(i64::MIN)),
            (Value::Decimal(i64::MAX), 5, i128::from(i64::MAX)),
            (Value::Timestamp(-1), 6, -1),
        ] {
            for unique in [false, true] {
                let mut key = if unique {
                    unique_key(tid, 0, std::slice::from_ref(&value))
                } else {
                    index_key(tid, 0, &value, Id::from_u128(1))
                };
                assert_eq!(
                    numeric_index_value(&key, &prefix, tag, unique),
                    Some(expected)
                );
                key.push(0);
                assert_eq!(numeric_index_value(&key, &prefix, tag, unique), None);
                key.pop();
                key[24] = 99;
                assert_eq!(numeric_index_value(&key, &prefix, tag, unique), None);
            }
        }
        assert_eq!(numeric_selectivity(0, 1, 1, true, true), Some(0.5));
        assert_eq!(numeric_selectivity(7, 7, 7, true, true), Some(1.0));
        assert_eq!(numeric_selectivity(7, 7, 7, true, false), Some(0.0));
        assert_eq!(numeric_selectivity(8, 7, 7, true, true), None);
    }

    fn fixture(path: &Path, unique: bool) -> PagedDatabase {
        let db = PagedDatabase::open(
            path,
            PagedOptions {
                max_page_rows: 2,
                decoded_cache_bytes: 512,
                ..Default::default()
            },
        )
        .unwrap();
        let mut relation = Relation::new("r.Item", vec![Role::new("n", DataType::Int)]);
        if unique {
            relation = relation.unique(&["n"]);
        }
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new("r", 1, vec![], vec![relation]),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        tx.grant("reader", "r.Item", Grant::read_only()).unwrap();
        for (i, n) in [i64::MIN, -2, -1, 0, 1, 2, i64::MAX]
            .into_iter()
            .enumerate()
        {
            tx.insert_with_id("r.Item", Id::from_u128(i as u128 + 1), vec![Value::Int(n)])
                .unwrap();
            if !unique {
                tx.insert_with_id(
                    "r.Item",
                    Id::from_u128(i as u128 + 100),
                    vec![Value::Int(n)],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
        db
    }

    #[test]
    fn range_bounds_match_reference_for_unique_and_duplicate_indexes() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = fixture(&dir.path().join("db"), unique);
            let snapshot = db.session("reader").unwrap().snapshot().unwrap();
            let values = [i64::MIN, -3, -2, -1, 0, 1, 2, 3, i64::MAX];
            for lo in values {
                for hi in values {
                    for li in [false, true] {
                        for ui in [false, true] {
                            let mut actual = Vec::new();
                            snapshot
                                .visit_authorized_range(
                                    "r.Item",
                                    "n",
                                    Some((&Value::Int(lo), li)),
                                    Some((&Value::Int(hi), ui)),
                                    &mut || Ok(()),
                                    &mut |row| {
                                        actual.push(row.values[0].clone());
                                        Ok(())
                                    },
                                )
                                .unwrap();
                            let expected = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX]
                                .into_iter()
                                .filter(|n| {
                                    (if li { *n >= lo } else { *n > lo })
                                        && (if ui { *n <= hi } else { *n < hi })
                                })
                                .flat_map(|n| {
                                    std::iter::repeat_n(Value::Int(n), if unique { 1 } else { 2 })
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(actual, expected, "unique={unique} {lo} {li} {hi} {ui}");
                        }
                    }
                }
            }
            assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
        }
    }

    #[test]
    fn range_policy_and_live_revocation_are_checked_even_for_empty_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let db = fixture(&dir.path().join("db"), false);
        let mut tx = db.admin().transaction().unwrap();
        tx.grant(
            "reader",
            "r.Item",
            Grant {
                filter: RowFilter::Equals {
                    role: "n".into(),
                    value: Value::Int(1),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        let mut actual = Vec::new();
        snapshot
            .visit_authorized_range("r.Item", "n", None, None, &mut || Ok(()), &mut |row| {
                actual.push(row.values[0].clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, vec![Value::Int(1), Value::Int(1)]);
        let mut changed = false;
        let result =
            snapshot.visit_authorized_range("r.Item", "n", None, None, &mut || Ok(()), &mut |_| {
                if !changed {
                    let mut tx = db.admin().transaction()?;
                    tx.grant("reader", "r.Item", Grant::read_only())?;
                    tx.commit()?;
                    changed = true;
                }
                Ok(())
            });
        assert!(matches!(result, Err(Error::Denied)));
        assert!(matches!(
            snapshot.visit_authorized_range(
                "r.Item",
                "n",
                Some((&Value::Int(2), true)),
                Some((&Value::Int(1), true)),
                &mut || Ok(()),
                &mut |_| Ok(())
            ),
            Err(Error::Denied)
        ));
    }

    #[test]
    fn range_does_not_ignore_bad_types_or_inconsistent_index_entries() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let db = fixture(&path, unique);
            let tid = db.inner.head.read().unwrap().catalog.state.names["r.Item"];
            let snapshot = db.admin().snapshot().unwrap();
            assert!(
                snapshot
                    .visit_authorized_range(
                        "r.Item",
                        "n",
                        Some((&Value::Text("bad".into()), true)),
                        None,
                        &mut || Ok(()),
                        &mut |_| Ok(())
                    )
                    .is_err()
            );
            drop(snapshot);
            drop(db);
            let raw = redb::Database::create(&path).unwrap();
            let tx = raw.begin_write().unwrap();
            if unique {
                tx.open_table(UNIQUE)
                    .unwrap()
                    .insert(
                        unique_key(tid, 0, &[Value::Int(99)]).as_slice(),
                        Id::from_u128(1).0.as_bytes().as_slice(),
                    )
                    .unwrap();
            } else {
                tx.open_table(INDEX)
                    .unwrap()
                    .insert(
                        index_key(tid, 0, &Value::Int(99), Id::from_u128(1)).as_slice(),
                        0,
                    )
                    .unwrap();
            }
            tx.commit().unwrap();
            drop(raw);
            let db = PagedDatabase::open(&path, Default::default()).unwrap();
            assert!(
                matches!(db.admin().snapshot().unwrap().visit_authorized_range("r.Item", "n", Some((&Value::Int(99),true)), Some((&Value::Int(99),true)), &mut || Ok(()), &mut |_| Ok(())), Err(Error::Storage(message)) if message.contains("index value mismatch"))
            );
        }
    }

    #[test]
    fn range_text_prefixes_and_embedded_nuls_match_typed_order() {
        for unique in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
            let mut relation = Relation::new("r.Text", vec![Role::new("text", DataType::Text)]);
            if unique {
                relation = relation.unique(&["text"]);
            }
            let values = ["", "\0", "\0a", "a", "a\0", "aa", "é", "🦀"];
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new("r", 1, vec![], vec![relation]))
                .unwrap();
            for (i, text) in values.iter().enumerate() {
                tx.insert_with_id(
                    "r.Text",
                    Id::from_u128(i as u128 + 1),
                    vec![Value::Text((*text).into())],
                )
                .unwrap();
            }
            tx.commit().unwrap();
            let snapshot = db.admin().snapshot().unwrap();
            for lo in values {
                for hi in values {
                    let mut actual = Vec::new();
                    snapshot
                        .visit_authorized_range(
                            "r.Text",
                            "text",
                            Some((&Value::Text(lo.into()), false)),
                            Some((&Value::Text(hi.into()), true)),
                            &mut || Ok(()),
                            &mut |row| {
                                actual.push(row.values[0].clone());
                                Ok(())
                            },
                        )
                        .unwrap();
                    assert_eq!(
                        actual,
                        values
                            .iter()
                            .filter(|text| **text > lo && **text <= hi)
                            .map(|text| Value::Text((*text).into()))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    fn reference_fixture(path: &Path, unique: bool, cache_bytes: usize) -> PagedDatabase {
        let db = PagedDatabase::open(
            path,
            PagedOptions {
                decoded_cache_bytes: cache_bytes,
                ..Default::default()
            },
        )
        .unwrap();
        let mut score = Relation::new(
            "r.Score",
            vec![
                Role::new("person", DataType::Ref("r.Person".into())),
                Role::new("n", DataType::Int),
            ],
        );
        if unique {
            score = score.unique(&["n"]);
        }
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "r",
                1,
                vec![],
                vec![
                    Relation::new(
                        "r.Person",
                        vec![
                            Role::new("n", DataType::Int),
                            Role::new("text", DataType::Text),
                        ],
                    ),
                    score,
                ],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        for n in 1..=80u128 {
            tx.insert_with_id(
                "r.Person",
                Id::from_u128(n),
                vec![
                    Value::Int(n as i64),
                    Value::Text("unprojected payload".repeat(32)),
                ],
            )
            .unwrap();
        }
        // Both occurrence IDs and references change direction repeatedly as
        // the index advances, exercising bounded lookahead and exact seeks.
        for n in 0..80u128 {
            tx.insert_with_id(
                "r.Score",
                Id::from_u128(1000 + (n * 37) % 80),
                vec![
                    Value::Ref(Id::from_u128((n * 53) % 80 + 1)),
                    Value::Int(n as i64),
                ],
            )
            .unwrap();
        }
        tx.grant("reader", "r.Score", Grant::read_only()).unwrap();
        tx.grant(
            "reader",
            "r.Person",
            Grant {
                filter: RowFilter::Equals {
                    role: "n".into(),
                    value: Value::Int(2),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        db
    }

    #[test]
    fn streaming_range_cursors_preserve_reference_filters_without_decoded_cache_churn() {
        for unique in [false, true] {
            for cache_bytes in [0, 512] {
                let dir = tempfile::tempdir().unwrap();
                let db = reference_fixture(&dir.path().join("db"), unique, cache_bytes);
                for principal in ["owner", "reader"] {
                    let snapshot = db.session(principal).unwrap().snapshot().unwrap();
                    let mut actual = Vec::new();
                    let mut checks = 0usize;
                    snapshot
                        .visit_authorized_range(
                            "r.Score",
                            "n",
                            None,
                            None,
                            &mut || {
                                checks += 1;
                                Ok(())
                            },
                            &mut |row| {
                                actual.push(row.clone());
                                Ok(())
                            },
                        )
                        .unwrap();
                    let expected = (0..80u128)
                        .filter(|n| principal == "owner" || (n * 53) % 80 + 1 == 2)
                        .map(|n| Row {
                            id: Id::from_u128(1000 + (n * 37) % 80),
                            values: vec![
                                Value::Ref(Id::from_u128((n * 53) % 80 + 1)),
                                Value::Int(n as i64),
                            ]
                            .into_boxed_slice(),
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected);
                    assert_eq!(
                        checks,
                        2 + 3 * 80,
                        "candidate, root and target checks must be retained"
                    );
                    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
                }
            }
        }
    }

    #[test]
    fn streaming_range_scopes_attenuate_public_owner_reads_and_revocation_stays_live() {
        use crate::capability::{CapabilityScope, RelationCapability};
        let dir = tempfile::tempdir().unwrap();
        let db = reference_fixture(&dir.path().join("db"), true, 0);
        let mut tx = db.admin().transaction().unwrap();
        tx.set_visibility("r.Person", Visibility::Public).unwrap();
        tx.set_visibility("r.Score", Visibility::Public).unwrap();
        tx.commit().unwrap();
        let admin = db.admin().snapshot().unwrap();
        let capability = db
            .issue_capability(
                Id::new(),
                "owner",
                CapabilityScope {
                    relations: vec![
                        RelationCapability {
                            relation: admin.def("r.Score").unwrap().id,
                            grant: Grant::read_only(),
                        },
                        RelationCapability {
                            relation: admin.def("r.Person").unwrap().id,
                            grant: Grant {
                                filter: RowFilter::Equals {
                                    role: "n".into(),
                                    value: Value::Int(2),
                                },
                                ..Grant::read_only()
                            },
                        },
                    ],
                    schema_namespace: None,
                    delegations: 0,
                },
            )
            .unwrap();
        let snapshot = db
            .session("owner")
            .unwrap()
            .with_capability(capability.id)
            .unwrap()
            .snapshot()
            .unwrap();
        let mut values = Vec::new();
        snapshot
            .visit_authorized_range("r.Score", "n", None, None, &mut || Ok(()), &mut |row| {
                values.push(row.values[1].clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, vec![Value::Int(77)]);
        let result = snapshot.visit_authorized_range(
            "r.Score",
            "n",
            None,
            None,
            &mut || Ok(()),
            &mut |_| db.revoke_capability(capability.id),
        );
        assert!(matches!(result, Err(Error::Denied)));
    }

    #[test]
    fn streaming_range_preserves_reference_page_and_source_work_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(reference_fixture(&path, true, 0));
        for options in [
            PagedOptions {
                max_visibility_rows: 1,
                ..Default::default()
            },
            PagedOptions {
                max_page_bytes: 43,
                ..Default::default()
            },
        ] {
            let db = PagedDatabase::open(&path, options).unwrap();
            let snapshot = db.session("owner").unwrap().snapshot().unwrap();
            let mut returned = 0;
            assert!(matches!(
                snapshot.visit_authorized_range(
                    "r.Score",
                    "n",
                    Some((&Value::Int(0), true)),
                    Some((&Value::Int(0), true)),
                    &mut || Ok(()),
                    &mut |_| {
                        returned += 1;
                        Ok(())
                    }
                ),
                Err(Error::Query(_))
            ));
            assert_eq!(returned, 0);
        }
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let snapshot = db.session("owner").unwrap().snapshot().unwrap();
        let mut checks = 0;
        let mut returned = 0;
        let result = snapshot.visit_authorized_range(
            "r.Score",
            "n",
            None,
            None,
            &mut || {
                checks += 1;
                if checks > 4 {
                    Err(Error::Query("test source budget".into()))
                } else {
                    Ok(())
                }
            },
            &mut |_| {
                returned += 1;
                Ok(())
            },
        );
        assert!(matches!(result, Err(Error::Query(message)) if message == "test source budget"));
        assert_eq!(returned, 1);
    }

    #[test]
    fn streaming_range_rejects_missing_or_corrupt_roots_and_referenced_rows() {
        for unique in [false, true] {
            for (root, missing) in [(false, false), (false, true), (true, false), (true, true)] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("db");
                // Deliberate per-row corruption requires the legacy physical layout.
                drop(super::super::tests::empty_format(&path, PagedFormat::V2));
                let db = reference_fixture(&path, unique, 0);
                let snapshot = db.admin().snapshot().unwrap();
                let name = if root { "r.Score" } else { "r.Person" };
                let tid = snapshot.def(name).unwrap().id;
                drop(snapshot);
                drop(db);
                let raw = redb::Database::create(&path).unwrap();
                let tx = raw.begin_write().unwrap();
                {
                    let mut rows = tx.open_table(ROWS).unwrap();
                    let key = row_key(tid, Id::from_u128(if root { 1000 } else { 1 }));
                    if missing {
                        rows.remove(key.as_slice()).unwrap();
                    } else {
                        rows.insert(key.as_slice(), b"bad".as_slice()).unwrap();
                    }
                }
                tx.commit().unwrap();
                drop(raw);
                let db = PagedDatabase::open(&path, Default::default()).unwrap();
                // Reader cannot read Person1, but full validation still precedes
                // its compiled target predicate, exactly as the graph walker.
                for principal in ["owner", "reader"] {
                    let snapshot = db.session(principal).unwrap().snapshot().unwrap();
                    let mut returned = 0;
                    let result = snapshot.visit_authorized_range(
                        "r.Score",
                        "n",
                        Some((&Value::Int(0), true)),
                        Some((&Value::Int(0), true)),
                        &mut || Ok(()),
                        &mut |_| {
                            returned += 1;
                            Ok(())
                        },
                    );
                    assert!(
                        matches!(result, Err(Error::Storage(_))),
                        "root={root} missing={missing}"
                    );
                    assert_eq!(returned, 0);
                }
            }
        }
    }
}
