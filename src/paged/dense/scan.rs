//! Borrowed scan access. A callback cannot retain a row past its physical page,
//! so sequential consumers need neither an Arc per row nor copied payloads.
use super::*;
use crate::Id;

impl RowStore {
    pub(in crate::paged) fn visit_family(
        &self,
        family: &[u8; 16],
        visitor: &mut impl FnMut(Result<(&[u8], &[u8])>) -> Result<()>,
    ) -> Result<()> {
        let end = super::super::prefix_end(family);
        let range = self
            .physical()
            .range::<&[u8]>((
                Bound::Included(family.as_slice()),
                end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            ))
            .map_err(err)?;
        let mut previous: Option<[u8; 32]> = None;
        for entry in range {
            let (key, bytes) = match entry.map_err(err) {
                Ok(entry) => entry,
                Err(error) => {
                    visitor(Err(error))?;
                    return Ok(());
                }
            };
            if matches!(self, Self::Legacy(_)) {
                visitor(Ok((key.value(), bytes.value())))?;
                continue;
            }
            let block = match logical_key(key.value()).and_then(|upper| Block::parse(upper, bytes))
            {
                Ok(block) => block,
                Err(error) => {
                    visitor(Err(error))?;
                    return Ok(());
                }
            };
            if previous.is_some_and(|upper| upper >= block.key(0)) {
                visitor(Err(err("overlapping dense blocks")))?;
                return Ok(());
            }
            for index in 0..block.count {
                let key = block.key(index);
                visitor(Ok((&key, block.row(index))))?;
            }
            previous = Some(block.upper);
        }
        Ok(())
    }
}

/// Exactly one target block plus its physical iterator. References can repeat,
/// run in either direction, or jump arbitrarily without accumulating a cache.
#[derive(Default)]
pub(in crate::paged) struct ScanCursor {
    legacy: super::super::TerminalCursor,
    block: Option<Block>,
    range: Option<PhysicalRange>,
    reverse: bool,
    position: usize,
}
impl ScanCursor {
    #[inline(always)]
    pub(in crate::paged) fn get(&mut self, store: &RowStore, family: Id, id: Id) -> Result<&[u8]> {
        if !matches!(store, RowStore::Dense(_)) {
            return self.legacy.get(store, family, id);
        }
        // An exact match in the retained, validated block already proves its
        // key is inside the block bounds. Ordered/repeated references avoid
        // constructing and comparing full logical boundary keys on this path.
        if let Some(index) = self.adjacent_index(family, id) {
            self.position = index;
            return Ok(self
                .block
                .as_ref()
                .expect("matched retained block")
                .row(index));
        }
        self.get_slow(store, family, id)
    }

    // Keep page seeking, directory search and corruption checks outside the
    // repeated/adjacent hit path. This is not #[cold]: random probes use it.
    #[inline(never)]
    fn get_slow(&mut self, store: &RowStore, family: Id, id: Id) -> Result<&[u8]> {
        let RowStore::Dense(table) = store else {
            return self.legacy.get(store, family, id);
        };
        let mut key = [0; 32];
        key[..16].copy_from_slice(family.0.as_bytes());
        key[16..].copy_from_slice(id.0.as_bytes());
        if !self.contains(&key) {
            let reverse = self.block.as_ref().is_some_and(|block| key < block.key(0));
            let same_family = self
                .block
                .as_ref()
                .is_some_and(|block| block.upper[..16] == key[..16]);
            if same_family
                && reverse == self.reverse
                && let Some(range) = self.range.as_mut()
            {
                // Nearby references advance one block. Large jumps reseek,
                // rather than walking a target family proportional to its size.
                let entry = if reverse {
                    range.next_back()
                } else {
                    range.next()
                }
                .transpose()
                .map_err(err)?;
                if let Some((upper, bytes)) = entry {
                    let block = Block::parse(logical_key(upper.value())?, bytes)?;
                    let old = self.block.as_ref().unwrap();
                    if (!reverse && old.upper >= block.key(0))
                        || (reverse && block.upper >= old.key(0))
                    {
                        return Err(err("overlapping dense blocks"));
                    }
                    self.block = Some(block);
                    self.position = 0;
                }
            }
            if !self.contains(&key) {
                self.seek(table, &key, reverse)?;
            }
        }
        let block = self
            .block
            .as_ref()
            .ok_or_else(|| err("dangling typed reference"))?;
        let directory = block.directory();
        let index = [
            Some(self.position),
            self.position.checked_add(1),
            self.position.checked_sub(1),
        ]
        .into_iter()
        .flatten()
        .find(|index| *index < block.count && directory.id(*index) == &key[16..])
        .or_else(|| directory.find(&key[16..]))
        .ok_or_else(|| err("dangling typed reference"))?;
        self.position = index;
        Ok(block.row(index))
    }

    #[inline]
    fn adjacent_index(&self, family: Id, id: Id) -> Option<usize> {
        let block = self.block.as_ref()?;
        if &block.upper[..16] != family.0.as_bytes() {
            return None;
        }
        let directory = block.directory();
        [
            Some(self.position),
            self.position.checked_add(1),
            self.position.checked_sub(1),
        ]
        .into_iter()
        .flatten()
        .find(|index| *index < block.count && directory.id(*index) == id.0.as_bytes())
    }

    fn contains(&self, key: &[u8; 32]) -> bool {
        self.block.as_ref().is_some_and(|block| {
            block.upper[..16] == key[..16] && block.key(0) <= *key && *key <= block.upper
        })
    }

    fn seek(&mut self, table: &Physical, key: &[u8; 32], reverse: bool) -> Result<()> {
        let end = super::super::prefix_end(&key[..16]);
        let mut forward = table
            .range::<&[u8]>((
                Bound::Included(key.as_slice()),
                end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            ))
            .map_err(err)?;
        let (upper, bytes) = forward
            .next()
            .transpose()
            .map_err(err)?
            .ok_or_else(|| err("dangling typed reference"))?;
        let block = Block::parse(logical_key(upper.value())?, bytes)?;
        // Like the logical range cursor, validate the boundary before exposing
        // a selected block. This is separate from per-row typed validation.
        let mut previous = table
            .range::<&[u8]>((
                Bound::Included(&key[..16]),
                Bound::Excluded(block.upper.as_slice()),
            ))
            .map_err(err)?;
        if let Some((upper, _)) = previous.next_back().transpose().map_err(err)?
            && logical_key(upper.value())? >= block.key(0)
        {
            return Err(err("overlapping dense blocks"));
        }
        self.range = if reverse {
            // PhysicalRange itself is double-ended; reverse stepping is done
            // by `next_back` below without retaining extra block guards.
            Some(
                table
                    .range::<&[u8]>((
                        Bound::Included(&key[..16]),
                        Bound::Excluded(block.upper.as_slice()),
                    ))
                    .map_err(err)?,
            )
        } else {
            Some(forward)
        };
        self.block = Some(block);
        self.position = 0;
        self.reverse = reverse;
        if !self.contains(key) {
            return Err(err("dangling typed reference"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableDatabase;

    fn key(id: u128) -> [u8; 32] {
        let mut key = [7; 32];
        key[16..].copy_from_slice(&id.to_be_bytes());
        key
    }

    // Deliberately construct the physical envelopes independently of the
    // writer so a test can introduce a locally valid, overlapping block.
    fn envelope(ids: &[u128]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&[7; 16]);
        bytes.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for (index, id) in ids.iter().enumerate() {
            bytes.extend_from_slice(&id.to_be_bytes());
            let offset = HEADER + ids.len() * DESCRIPTOR + index * 16;
            bytes.extend_from_slice(&(offset as u32).to_le_bytes());
        }
        for id in ids {
            bytes.extend_from_slice(&id.to_be_bytes());
        }
        bytes
    }

    fn physical_fixture(middle: &[u8]) -> (tempfile::TempDir, redb::Database) {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("db")).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx.open_table(ROWS).unwrap();
            rows.insert(key(20).as_slice(), envelope(&[10, 20]).as_slice())
                .unwrap();
            rows.insert(key(40).as_slice(), middle).unwrap();
            rows.insert(key(60).as_slice(), envelope(&[50, 60]).as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
        (dir, db)
    }

    #[test]
    fn borrowed_scan_rejects_malformed_envelopes_and_overlaps_before_exposing_rows() {
        let valid = envelope(&[30, 40]);
        let mut malformed = valid.clone();
        malformed[0] ^= 255;
        let mut bad_directory = valid.clone();
        bad_directory[HEADER + 16..HEADER + DESCRIPTOR].copy_from_slice(&0u32.to_le_bytes());
        for middle in [malformed, bad_directory, envelope(&[15, 40])] {
            let (_dir, db) = physical_fixture(&middle);
            let read = db.begin_read().unwrap();
            let rows = RowStore::open(&read, true).unwrap();
            let mut exposed = Vec::new();
            let error = rows
                .visit_family(&[7; 16], &mut |entry| {
                    let (key, bytes) = entry?;
                    exposed.push((key.to_vec(), bytes.to_vec()));
                    Ok(())
                })
                .unwrap_err();
            assert!(matches!(error, crate::Error::Storage(_)));
            assert_eq!(exposed.len(), 2);
            assert_eq!(exposed[0].0, key(10));
            assert_eq!(exposed[1].0, key(20));
        }
    }

    #[test]
    fn borrowed_scan_preserves_callback_and_source_budget_error_precedence() {
        let (_dir, db) = physical_fixture(b"invalid later dense block");
        let read = db.begin_read().unwrap();
        let rows = RowStore::open(&read, true).unwrap();
        for fail_at in [1, 2, 3] {
            let mut callbacks = 0;
            let mut exposed = 0;
            let error = rows
                .visit_family(&[7; 16], &mut |entry| {
                    // Production visitors charge before propagating a source
                    // error. Even the callback carrying the malformed block
                    // must allow this check to fail first.
                    callbacks += 1;
                    if callbacks == fail_at {
                        return Err(crate::Error::Query("source budget sentinel".into()));
                    }
                    entry?;
                    exposed += 1;
                    Ok(())
                })
                .unwrap_err();
            assert!(matches!(error, crate::Error::Query(message)
                if message == "source budget sentinel"));
            assert_eq!(callbacks, fail_at);
            assert_eq!(exposed, fail_at - 1);
        }
    }

    #[test]
    fn borrowed_cursor_rejects_selected_corrupt_blocks_in_both_directions() {
        let mut malformed = envelope(&[30, 40]);
        malformed[0] ^= 255;
        let mut bad_directory = envelope(&[30, 40]);
        bad_directory[HEADER + DESCRIPTOR..HEADER + DESCRIPTOR + 16]
            .copy_from_slice(&29u128.to_be_bytes());
        for middle in [malformed, bad_directory, envelope(&[15, 40])] {
            let (_dir, db) = physical_fixture(&middle);
            let read = db.begin_read().unwrap();
            let rows = RowStore::open(&read, true).unwrap();
            let family = Id(uuid::Uuid::from_bytes([7; 16]));
            // Fresh seeks, forward advancement, reverse seeks and alternating
            // directions must all fail before returning the selected bytes.
            for prefix in [
                vec![],
                vec![10],
                vec![60],
                vec![20, 10, 20],
                vec![60, 50, 60, 50],
            ] {
                let mut cursor = ScanCursor::default();
                for n in prefix {
                    assert_eq!(
                        cursor.get(&rows, family, Id::from_u128(n)).unwrap(),
                        n.to_be_bytes()
                    );
                }
                let error = cursor.get(&rows, family, Id::from_u128(40)).unwrap_err();
                assert!(matches!(error, crate::Error::Storage(_)));
            }
        }
    }

    #[test]
    fn adjacent_cursor_hits_never_cross_family_boundaries() {
        let (_dir, db) = physical_fixture(&envelope(&[30, 40]));
        let tx = db.begin_write().unwrap();
        {
            let mut writer = DenseWriter::new(tx.open_table(ROWS).unwrap(), 4096);
            // The same IDs have different payloads in another relation.
            for n in [10, 20] {
                let mut other_key = key(n);
                other_key[..16].fill(8);
                writer.insert(&other_key, b"other family").unwrap();
            }
            writer.finish().unwrap();
        }
        tx.commit().unwrap();
        let rows = RowStore::open(&db.begin_read().unwrap(), true).unwrap();
        let mut cursor = ScanCursor::default();
        for (family, n) in [(7, 10), (7, 20), (8, 20), (8, 10), (7, 10), (7, 20)] {
            let bytes = cursor
                .get(
                    &rows,
                    Id(uuid::Uuid::from_bytes([family; 16])),
                    Id::from_u128(n),
                )
                .unwrap();
            if family == 7 {
                assert_eq!(bytes, n.to_be_bytes());
            } else {
                assert_eq!(bytes, b"other family");
            }
        }
        assert!(
            cursor
                .get(
                    &rows,
                    Id(uuid::Uuid::from_bytes([9; 16])),
                    Id::from_u128(20)
                )
                .is_err()
        );
    }

    #[test]
    fn borrowed_scan_and_cursor_match_ranges_for_arbitrary_and_reversed_references() {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("db")).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let table = tx.open_table(ROWS).unwrap();
            let mut writer = DenseWriter::new(table, 4096);
            for n in 1..=500u128 {
                writer.insert(&key(n * 17), &n.to_be_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }
        tx.commit().unwrap();
        let read = db.begin_read().unwrap();
        let rows = RowStore::open(&read, true).unwrap();
        let expected: Vec<_> = rows
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect();
        let mut actual = Vec::new();
        rows.visit_family(&[7; 16], &mut |entry| {
            let (key, value) = entry?;
            actual.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();
        assert_eq!(actual, expected);
        let mut cursor = ScanCursor::default();
        let family = Id(uuid::Uuid::from_bytes([7; 16]));
        for n in (1..=500u128)
            .chain((1..=500u128).rev())
            .chain((0..500u128).map(|n| (n * 137) % 500 + 1))
        {
            assert_eq!(
                cursor.get(&rows, family, Id::from_u128(n * 17)).unwrap(),
                n.to_be_bytes()
            );
        }
        for n in [0, 18, 8501] {
            assert!(cursor.get(&rows, family, Id::from_u128(n)).is_err());
        }
    }
}
