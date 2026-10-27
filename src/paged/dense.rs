//! Physical, family-local dense blocks. Logical row keys and OCC dependencies
//! remain `(relation, occurrence)`; block boundaries are never logical locks.
use super::{ROWS, Result, err};
use redb::ReadableTable;
#[cfg(test)]
use redb::ReadableTableMetadata;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

mod scan;
pub(super) use scan::ScanCursor;

// Leave room for redb's leaf/key framing inside a 4 KiB page. A row larger
// than this target occupies its own block and retains the engine's row limit.
const TARGET_BYTES: usize = 3500;
const MAX_ROWS: usize = 256;
const HEADER: usize = 24;
const DESCRIPTOR: usize = 20;
const MAGIC: &[u8; 4] = b"LDB1";
type Physical = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;
type PhysicalRange = redb::Range<'static, &'static [u8], &'static [u8]>;
type Guard = redb::AccessGuard<'static, &'static [u8]>;

pub(super) enum RowStore {
    Legacy(Physical),
    Dense(Physical),
}
impl RowStore {
    pub(super) fn open(read: &redb::ReadTransaction, dense: bool) -> Result<Self> {
        let table = read.open_table(ROWS).map_err(err)?;
        Ok(if dense {
            Self::Dense(table)
        } else {
            Self::Legacy(table)
        })
    }
    pub(super) fn physical(&self) -> &Physical {
        match self {
            Self::Legacy(t) | Self::Dense(t) => t,
        }
    }
    pub(super) fn get(&self, key: &[u8]) -> Result<Option<RowValueGuard>> {
        match self {
            Self::Legacy(t) => Ok(t.get(key).map_err(err)?.map(RowValueGuard::Legacy)),
            Self::Dense(t) => {
                let key = logical_key(key)?;
                let Some((upper, bytes)) = t.first_at_or_after(key.as_slice()).map_err(err)? else {
                    return Ok(None);
                };
                if upper.value().get(..16) != Some(&key[..16]) {
                    return Ok(None);
                }
                let upper = logical_key(upper.value())?;
                let directory = validate(&upper, bytes.value())?;
                // Point reads validate the complete selected block. Global
                // block ordering is maintained by writes and checked by scans
                // and the integrity audit, rather than probing another leaf
                // for every point read. Fast open is not a full-file audit.
                let Some(index) = directory.find(&key[16..]) else {
                    return Ok(None);
                };
                let start = directory.offset(index);
                let end = directory.offset(index + 1);
                Ok(Some(RowValueGuard::Point { bytes, start, end }))
            }
        }
    }
    pub(super) fn range<K: AsRef<[u8]>>(&self, bounds: impl RangeBounds<K>) -> Result<RowRange> {
        let lower = own_bound(bounds.start_bound());
        let upper = own_bound(bounds.end_bound());
        match self {
            Self::Legacy(t) => Ok(RowRange::Legacy(
                t.range::<&[u8]>((borrow_bound(&lower), borrow_bound(&upper)))
                    .map_err(err)?,
            )),
            Self::Dense(t) => {
                // A logical upper bound can lie inside a block whose physical
                // upper-bound key is larger. Include that block, then filter
                // individual logical keys. At most one extra block is visited.
                let physical_upper = match &upper {
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(k) | Bound::Excluded(k) if k.len() <= 16 => upper.clone(),
                    Bound::Included(k) | Bound::Excluded(k) => {
                        let mut tail = t.range::<&[u8]>(k.as_slice()..).map_err(err)?;
                        tail.next().transpose().map_err(err)?.map_or_else(
                            || upper.clone(),
                            |(candidate, _)| {
                                if candidate.value().get(..16) == k.get(..16) {
                                    Bound::Included(candidate.value().to_vec())
                                } else {
                                    upper.clone()
                                }
                            },
                        )
                    }
                };
                let physical_lower = match &lower {
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(k) | Bound::Excluded(k) => Bound::Included(k.as_slice()),
                };
                let front_upper = match &lower {
                    Bound::Unbounded => None,
                    Bound::Included(k) | Bound::Excluded(k) => {
                        let mut preceding = t.range::<&[u8]>(..k.as_slice()).map_err(err)?;
                        preceding
                            .next_back()
                            .transpose()
                            .map_err(err)?
                            .filter(|(key, _)| {
                                k.len() >= 16 && key.value().get(..16) == k.get(..16)
                            })
                            .map(|(key, _)| logical_key(key.value()))
                            .transpose()?
                    }
                };
                let range = t
                    .range::<&[u8]>((physical_lower, borrow_bound(&physical_upper)))
                    .map_err(err)?;
                Ok(RowRange::Dense(DenseRange {
                    range,
                    lower,
                    upper,
                    front: None,
                    back: None,
                    front_upper,
                    back_lower: None,
                    done: false,
                }))
            }
        }
    }
    pub(super) fn iter(&self) -> Result<RowRange> {
        self.range::<&[u8]>(..)
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> Result<u64> {
        if let Self::Legacy(t) = self {
            return t.len().map_err(err);
        }
        self.iter()?.try_fold(0u64, |n, entry| {
            entry?;
            n.checked_add(1)
                .ok_or_else(|| err("dense row count overflow"))
        })
    }
}

fn own_bound<K: AsRef<[u8]>>(bound: Bound<&K>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k.as_ref().to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.as_ref().to_vec()),
    }
}
fn borrow_bound(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
    }
}
fn logical_key(key: &[u8]) -> Result<[u8; 32]> {
    key.try_into().map_err(|_| err("invalid dense logical key"))
}

pub(super) enum RowKeyGuard {
    Legacy(Guard),
    Dense([u8; 32]),
}
impl RowKeyGuard {
    pub(super) fn value(&self) -> &[u8] {
        match self {
            Self::Legacy(g) => g.value(),
            Self::Dense(k) => k,
        }
    }
}
pub(super) enum RowValueGuard {
    Legacy(Guard),
    Dense {
        block: Arc<Block>,
        index: usize,
    },
    Point {
        bytes: Guard,
        start: usize,
        end: usize,
    },
}
impl RowValueGuard {
    pub(super) fn value(&self) -> &[u8] {
        match self {
            Self::Legacy(g) => g.value(),
            Self::Dense { block, index } => block.row(*index),
            Self::Point { bytes, start, end } => &bytes.value()[*start..*end],
        }
    }
}

pub(super) struct Block {
    upper: [u8; 32],
    bytes: Guard,
    count: usize,
}
impl Block {
    fn parse(upper: [u8; 32], bytes: Guard) -> Result<Self> {
        let count = validate(&upper, bytes.value())?.count;
        Ok(Self {
            upper,
            bytes,
            count,
        })
    }
    fn row(&self, index: usize) -> &[u8] {
        self.directory().row(index)
    }
    fn key(&self, index: usize) -> [u8; 32] {
        let mut key = self.upper;
        key[16..].copy_from_slice(self.directory().id(index));
        key
    }
    fn directory(&self) -> Directory<'_> {
        Directory {
            bytes: self.bytes.value(),
            count: self.count,
        }
    }
}

/// A view into a fully validated directory. Constructed only by validation or
/// from a Block that retains the same immutable redb guard. No row lookup needs
/// a heap allocation or an owned copy of the directory.
struct Directory<'a> {
    bytes: &'a [u8],
    count: usize,
}
impl<'a> Directory<'a> {
    fn id(&self, index: usize) -> &'a [u8] {
        let start = HEADER + index * DESCRIPTOR;
        &self.bytes[start..start + 16]
    }
    fn offset(&self, index: usize) -> usize {
        if index == self.count {
            return self.bytes.len();
        }
        let start = HEADER + index * DESCRIPTOR + 16;
        u32::from_le_bytes(self.bytes[start..start + 4].try_into().unwrap()) as usize
    }
    fn row(&self, index: usize) -> &'a [u8] {
        &self.bytes[self.offset(index)..self.offset(index + 1)]
    }
    fn find(&self, id: &[u8]) -> Option<usize> {
        let mut low = 0;
        let mut high = self.count;
        while low < high {
            let middle = low + (high - low) / 2;
            match self.id(middle).cmp(id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Some(middle),
            }
        }
        None
    }
}

// Validate the complete directory before exposing any row. Limits precede all
// allocations, offsets are monotonic, and family/upper keys bind the envelope.
fn validate<'a>(upper: &[u8; 32], bytes: &'a [u8]) -> Result<Directory<'a>> {
    if bytes.len() < HEADER || &bytes[..4] != MAGIC || bytes[4..20] != upper[..16] {
        return Err(err("invalid dense block header"));
    }
    let count = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
    if count == 0 || count > MAX_ROWS || (count > 1 && bytes.len() > TARGET_BYTES) {
        return Err(err("invalid dense block size"));
    }
    let payload = HEADER + count * DESCRIPTOR;
    if payload >= bytes.len() {
        return Err(err("truncated dense block directory"));
    }
    let directory = Directory { bytes, count };
    let first = &bytes[HEADER..HEADER + DESCRIPTOR];
    let mut previous_id = u128::from_be_bytes(first[..16].try_into().unwrap());
    let mut previous_offset = u32::from_le_bytes(first[16..].try_into().unwrap()) as usize;
    let mut invalid = previous_offset != payload;
    // Fixed-width descriptors avoid repeated slice comparisons and directory
    // indexing on every point lookup. Every descriptor is still checked, even
    // when the requested row occurs near the beginning of the block.
    for descriptor in bytes[HEADER + DESCRIPTOR..payload].chunks_exact(DESCRIPTOR) {
        let id = u128::from_be_bytes(descriptor[..16].try_into().unwrap());
        let offset = u32::from_le_bytes(descriptor[16..].try_into().unwrap()) as usize;
        invalid |= previous_id >= id;
        invalid |= previous_offset >= offset;
        previous_id = id;
        previous_offset = offset;
    }
    // Strict monotonicity makes the final offset the largest. Checking it
    // against the payload length also bounds every preceding offset.
    if invalid || previous_offset >= bytes.len() {
        return Err(err("invalid dense block directory"));
    }
    if directory.id(count - 1) != &upper[16..] {
        return Err(err("dense upper key mismatch"));
    }
    Ok(directory)
}

struct BlockCursor {
    block: Arc<Block>,
    front: usize,
    back: usize,
}
impl BlockCursor {
    fn new(block: Block) -> Self {
        let back = block.count;
        Self {
            block: Arc::new(block),
            front: 0,
            back,
        }
    }
    fn pop(&mut self, reverse: bool) -> Option<(RowKeyGuard, RowValueGuard)> {
        if self.front == self.back {
            return None;
        }
        let index = if reverse {
            self.back -= 1;
            self.back
        } else {
            let i = self.front;
            self.front += 1;
            i
        };
        Some((
            RowKeyGuard::Dense(self.block.key(index)),
            RowValueGuard::Dense {
                block: self.block.clone(),
                index,
            },
        ))
    }
}

pub(super) enum RowRange {
    Legacy(PhysicalRange),
    Dense(DenseRange),
}
pub(super) struct DenseRange {
    range: PhysicalRange,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    front: Option<BlockCursor>,
    back: Option<BlockCursor>,
    front_upper: Option<[u8; 32]>,
    back_lower: Option<[u8; 32]>,
    done: bool,
}
impl DenseRange {
    fn step(&mut self, reverse: bool) -> Option<Result<(RowKeyGuard, RowValueGuard)>> {
        if self.done {
            return None;
        }
        loop {
            let cursor = if reverse {
                &mut self.back
            } else {
                &mut self.front
            };
            let mut row = cursor.as_mut().and_then(|c| c.pop(reverse));
            if row.is_none() {
                *cursor = None;
                let entry = if reverse {
                    self.range.next_back()
                } else {
                    self.range.next()
                };
                match entry {
                    Some(entry) => {
                        let block = entry
                            .map_err(err)
                            .and_then(|(k, bytes)| Block::parse(logical_key(k.value())?, bytes));
                        let block = match block {
                            Ok(block) => block,
                            Err(e) => {
                                self.done = true;
                                return Some(Err(e));
                            }
                        };
                        let first = block.key(0);
                        let upper = block.upper;
                        if self.front_upper.is_some_and(|key| key >= first)
                            || self.back_lower.is_some_and(|key| key <= upper)
                        {
                            self.done = true;
                            return Some(Err(err("overlapping dense blocks")));
                        }
                        if reverse {
                            self.back_lower = Some(first);
                            self.back = Some(BlockCursor::new(block));
                        } else {
                            self.front_upper = Some(upper);
                            self.front = Some(BlockCursor::new(block));
                        }
                        continue;
                    }
                    None => {
                        // Both ends share the final retained block rather than
                        // yielding it twice when next/next_back are interleaved.
                        let other = if reverse {
                            &mut self.front
                        } else {
                            &mut self.back
                        };
                        row = other.as_mut().and_then(|c| c.pop(reverse));
                    }
                }
            }
            let Some((key, value)) = row else {
                self.done = true;
                return None;
            };
            let below = match &self.lower {
                Bound::Unbounded => false,
                Bound::Included(k) => key.value() < k.as_slice(),
                Bound::Excluded(k) => key.value() <= k.as_slice(),
            };
            let above = match &self.upper {
                Bound::Unbounded => false,
                Bound::Included(k) => key.value() > k.as_slice(),
                Bound::Excluded(k) => key.value() >= k.as_slice(),
            };
            if (!reverse && above) || (reverse && below) {
                self.done = true;
                return None;
            }
            if below || above {
                continue;
            }
            return Some(Ok((key, value)));
        }
    }
}
impl Iterator for RowRange {
    type Item = Result<(RowKeyGuard, RowValueGuard)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Legacy(r) => r.next().map(|r| {
                r.map(|(k, v)| (RowKeyGuard::Legacy(k), RowValueGuard::Legacy(v)))
                    .map_err(err)
            }),
            Self::Dense(r) => r.step(false),
        }
    }
}
impl DoubleEndedIterator for RowRange {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Legacy(r) => r.next_back().map(|r| {
                r.map(|(k, v)| (RowKeyGuard::Legacy(k), RowValueGuard::Legacy(v)))
                    .map_err(err)
            }),
            Self::Dense(r) => r.step(true),
        }
    }
}

pub(super) struct DenseWriter<'a> {
    table: redb::Table<'a, &'static [u8], &'static [u8]>,
    changes: BTreeMap<[u8; 32], Option<Vec<u8>>>,
    max_row_bytes: usize,
}
impl<'a> DenseWriter<'a> {
    pub(super) fn new(
        table: redb::Table<'a, &'static [u8], &'static [u8]>,
        max_row_bytes: usize,
    ) -> Self {
        Self {
            table,
            changes: BTreeMap::new(),
            max_row_bytes,
        }
    }
    #[cfg(test)]
    pub(super) fn insert(&mut self, key: &[u8], bytes: &[u8]) -> Result<()> {
        self.insert_owned(key, bytes.to_vec())
    }
    pub(super) fn insert_owned(&mut self, key: &[u8], bytes: Vec<u8>) -> Result<()> {
        if bytes.is_empty()
            || bytes.len() > self.max_row_bytes
            || bytes.len() > u32::MAX as usize - HEADER - DESCRIPTOR
        {
            return Err(err("invalid dense row length"));
        }
        self.changes.insert(logical_key(key)?, Some(bytes));
        Ok(())
    }
    pub(super) fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.changes.insert(logical_key(key)?, None);
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<()> {
        // Only one existing block and one output block are materialized. In
        // particular, appending a large transaction never duplicates its entire
        // family into a second map of individually allocated row payloads.
        let mut pending = PendingBlock::default();
        while let Some((&first, _)) = self.changes.first_key_value() {
            let existing = {
                let mut range = self.table.range::<&[u8]>(first.as_slice()..).map_err(err)?;
                let following = range.next().transpose().map_err(err)?;
                match following {
                    Some((key, bytes)) if key.value().get(..16) == Some(&first[..16]) => {
                        let key = logical_key(key.value())?;
                        let count = validate_write(&key, bytes.value(), self.max_row_bytes)?;
                        Some((key, bytes.value().to_vec(), false, count))
                    }
                    _ => {
                        let mut previous =
                            self.table.range::<&[u8]>(..first.as_slice()).map_err(err)?;
                        previous
                            .next_back()
                            .transpose()
                            .map_err(err)?
                            .filter(|(key, _)| key.value().get(..16) == Some(&first[..16]))
                            .map(|(key, bytes)| {
                                let key = logical_key(key.value())?;
                                let count =
                                    validate_write(&key, bytes.value(), self.max_row_bytes)?;
                                Ok::<_, super::Error>((key, bytes.value().to_vec(), true, count))
                            })
                            .transpose()?
                    }
                }
            };
            let (boundary, append) = existing
                .as_ref()
                .map_or((first, true), |(k, _, a, _)| (*k, *a));
            let directory = if let Some((key, bytes, _, count)) = &existing {
                // The copied bytes are exactly the fully validated envelope.
                // Retain its validated count instead of walking the directory
                // again after moving it out of the table guard.
                let directory = Directory {
                    bytes,
                    count: *count,
                };
                let mut preceding = self.table.range::<&[u8]>(..key.as_slice()).map_err(err)?;
                if let Some((previous, _)) = preceding.next_back().transpose().map_err(err)?
                    && previous.value().get(..16) == Some(&key[..16])
                {
                    let mut first_key = *key;
                    first_key[16..].copy_from_slice(directory.id(0));
                    if logical_key(previous.value())? >= first_key {
                        return Err(err("overlapping dense blocks"));
                    }
                }
                Some(directory)
            } else {
                None
            };
            if let Some((key, _, _, _)) = &existing {
                self.table.remove(key.as_slice()).map_err(err)?;
            }
            let mut position = 0;
            loop {
                let old_id = directory.as_ref().and_then(|d| {
                    (position < d.count).then(|| <[u8; 16]>::try_from(d.id(position)).unwrap())
                });
                let change_key = self.changes.first_key_value().and_then(|(&key, _)| {
                    (key[..16] == first[..16] && (append || key <= boundary)).then_some(key)
                });
                // Merge two sorted streams. Replacement/deletion consumes the
                // matching old row; unchanged rows are borrowed from its block.
                let take_change = match (&old_id, &change_key) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some(old), Some(change)) => change[16..] <= old[..],
                };
                if take_change {
                    let (key, change) = self.changes.pop_first().unwrap();
                    let id: [u8; 16] = key[16..].try_into().unwrap();
                    if old_id == Some(id) {
                        position += 1;
                    }
                    if let Some(bytes) = change {
                        pending.push(&mut self.table, &first[..16], id, &bytes)?;
                    }
                } else if let Some(id) = old_id {
                    pending.push(
                        &mut self.table,
                        &first[..16],
                        id,
                        directory.as_ref().unwrap().row(position),
                    )?;
                    position += 1;
                } else {
                    break;
                }
            }
            pending.flush(&mut self.table, &first[..16])?;
        }
        Ok(())
    }
}

/// Reusable bounded block encoder. Row bytes are copied directly into one
/// contiguous payload rather than allocating one Vec for every unchanged row.
#[derive(Default)]
struct PendingBlock {
    entries: Vec<([u8; 16], usize)>,
    payload: Vec<u8>,
    encoded: Vec<u8>,
}
impl PendingBlock {
    fn push(
        &mut self,
        table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
        family: &[u8],
        id: [u8; 16],
        bytes: &[u8],
    ) -> Result<()> {
        let size = HEADER + self.entries.len() * DESCRIPTOR + self.payload.len();
        if !self.entries.is_empty()
            && (size.saturating_add(DESCRIPTOR).saturating_add(bytes.len()) > TARGET_BYTES
                || self.entries.len() == MAX_ROWS)
        {
            self.flush(table, family)?;
        }
        self.entries.push((id, self.payload.len()));
        self.payload.extend_from_slice(bytes);
        Ok(())
    }
    fn flush(
        &mut self,
        table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
        family: &[u8],
    ) -> Result<()> {
        let Some((last, _)) = self.entries.last() else {
            return Ok(());
        };
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(family);
        key[16..].copy_from_slice(last);
        let payload_start = HEADER + self.entries.len() * DESCRIPTOR;
        let size = payload_start
            .checked_add(self.payload.len())
            .ok_or_else(|| err("dense block size overflow"))?;
        self.encoded.clear();
        self.encoded.reserve(size);
        self.encoded.extend_from_slice(MAGIC);
        self.encoded.extend_from_slice(family);
        self.encoded
            .extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (id, offset) in &self.entries {
            self.encoded.extend_from_slice(id);
            let offset = u32::try_from(payload_start + offset).map_err(err)?;
            self.encoded.extend_from_slice(&offset.to_le_bytes());
        }
        self.encoded.extend_from_slice(&self.payload);
        table
            .insert(key.as_slice(), self.encoded.as_slice())
            .map_err(err)?;
        self.entries.clear();
        self.payload.clear();
        Ok(())
    }
}

fn validate_write(key: &[u8; 32], bytes: &[u8], max_row_bytes: usize) -> Result<usize> {
    let directory = validate(key, bytes)?;
    if (0..directory.count).any(|i| directory.row(i).len() > max_row_bytes) {
        return Err(err("dense row exceeds configured byte limit"));
    }
    Ok(directory.count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableDatabase;

    fn key(family: u8, id: u128) -> [u8; 32] {
        let mut key = [family; 32];
        key[16..].copy_from_slice(&id.to_be_bytes());
        key
    }
    fn database() -> (tempfile::TempDir, redb::Database) {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("dense.redb")).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(ROWS).unwrap();
        tx.commit().unwrap();
        (dir, db)
    }
    fn apply(db: &redb::Database, edits: impl IntoIterator<Item = ([u8; 32], Option<Vec<u8>>)>) {
        let tx = db.begin_write().unwrap();
        let mut writer = DenseWriter::new(tx.open_table(ROWS).unwrap(), 16384);
        for (key, bytes) in edits {
            match bytes {
                Some(bytes) => writer.insert(&key, &bytes).unwrap(),
                None => writer.remove(&key).unwrap(),
            }
        }
        writer.finish().unwrap();
        tx.commit().unwrap();
    }
    fn store(db: &redb::Database) -> RowStore {
        RowStore::open(&db.begin_read().unwrap(), true).unwrap()
    }
    fn collect(range: RowRange) -> Vec<([u8; 32], Vec<u8>)> {
        range
            .map(|row| {
                let (key, bytes) = row.unwrap();
                (key.value().try_into().unwrap(), bytes.value().to_vec())
            })
            .collect()
    }

    #[test]
    fn arbitrary_ids_mutations_oversized_rows_and_pinned_guards() {
        let (_dir, db) = database();
        let mut model = BTreeMap::new();
        for i in 0..300u128 {
            let key = key(
                (i % 3) as u8,
                i.wrapping_mul(0x9e3779b97f4a7c15000000000000000b),
            );
            let bytes = vec![(i % 255) as u8; 1 + (i as usize % 81)];
            model.insert(key, bytes);
        }
        apply(&db, model.iter().map(|(k, v)| (*k, Some(v.clone()))));
        let old = store(&db);
        assert_eq!(
            collect(old.iter().unwrap()),
            model.clone().into_iter().collect::<Vec<_>>()
        );
        let first = *model.keys().next().unwrap();
        let retained = old.get(&first).unwrap().unwrap();
        assert!(matches!(&retained, RowValueGuard::Point { .. }));
        let old_bytes = retained.value().to_vec();
        let mut edits = Vec::new();
        for (i, (&key, bytes)) in model.iter_mut().enumerate() {
            if i % 5 == 0 {
                edits.push((key, None));
            } else if i % 7 == 0 {
                *bytes = vec![99; 8192];
                edits.push((key, Some(bytes.clone())));
            }
        }
        for (key, bytes) in &edits {
            if bytes.is_none() {
                model.remove(key);
            }
        }
        apply(&db, edits);
        let now = store(&db);
        assert_eq!(
            collect(now.iter().unwrap()),
            model.clone().into_iter().collect::<Vec<_>>()
        );
        assert_eq!(retained.value(), old_bytes);
        assert_eq!(old.len().unwrap(), 300);
        for (&key, bytes) in &model {
            assert_eq!(now.get(&key).unwrap().unwrap().value(), bytes);
        }
        apply(&db, model.keys().map(|k| (*k, None)));
        assert_eq!(store(&db).len().unwrap(), 0);
        assert_eq!(now.len().unwrap(), model.len() as u64);
    }

    #[test]
    fn mixed_direction_ranges_preserve_all_bounds_and_max_family() {
        let (_dir, db) = database();
        apply(
            &db,
            (0..240u128).map(|i| (key(255, i), Some(vec![i as u8; 70]))),
        );
        let rows = store(&db);
        for (start, end) in [
            (0, 0),
            (0, 1),
            (3, 19),
            (27, 92),
            (0, 239),
            (230, 240),
            (240, 241),
        ] {
            for start_included in [false, true] {
                for end_included in [false, true] {
                    let start_key = key(255, start);
                    let end_key = key(255, end);
                    let low = if start_included {
                        Bound::Included(start_key.as_slice())
                    } else {
                        Bound::Excluded(start_key.as_slice())
                    };
                    let high = if end_included {
                        Bound::Included(end_key.as_slice())
                    } else {
                        Bound::Excluded(end_key.as_slice())
                    };
                    let expected: Vec<_> = (0..240u128)
                        .filter(|&i| {
                            (i > start || (start_included && i == start))
                                && (i < end || (end_included && i == end))
                        })
                        .map(|i| key(255, i))
                        .collect();
                    let mut iter = rows.range::<&[u8]>((low, high)).unwrap();
                    let mut forward = Vec::new();
                    let mut backward = Vec::new();
                    loop {
                        let Some(row) = iter.next() else { break };
                        forward.push(logical_key(row.unwrap().0.value()).unwrap());
                        let Some(row) = iter.next_back() else { break };
                        backward.push(logical_key(row.unwrap().0.value()).unwrap());
                    }
                    forward.extend(backward.into_iter().rev());
                    assert_eq!(
                        forward, expected,
                        "{start} {end} {start_included} {end_included}"
                    );
                    assert!(iter.next().is_none());
                    assert!(iter.next_back().is_none());
                }
            }
        }
        let prefix = [255; 16];
        assert_eq!(
            collect(rows.range::<&[u8]>(prefix.as_slice()..).unwrap()).len(),
            240
        );
    }

    #[test]
    fn neighboring_corrupt_family_does_not_break_scoped_range() {
        let (_dir, db) = database();
        apply(&db, (0..100).map(|i| (key(3, i), Some(vec![3; 80]))));
        let tx = db.begin_write().unwrap();
        tx.open_table(ROWS)
            .unwrap()
            .insert(
                [vec![4; 16], vec![0x7f]].concat().as_slice(),
                b"corrupt".as_slice(),
            )
            .unwrap();
        tx.open_table(ROWS)
            .unwrap()
            .insert([2; 17].as_slice(), b"bad prior key".as_slice())
            .unwrap();
        tx.commit().unwrap();
        let rows = store(&db);
        let lower = [3; 16];
        let upper = [4; 16];
        assert!(rows.get(&key(3, 0)).unwrap().is_some());
        assert!(rows.get(&key(3, 1000)).unwrap().is_none());
        assert_eq!(
            collect(
                rows.range::<&[u8]>(lower.as_slice()..upper.as_slice())
                    .unwrap()
            )
            .len(),
            100
        );
        assert_eq!(
            collect(
                rows.range::<&[u8]>(key(3, 7).as_slice()..key(3, 1000).as_slice())
                    .unwrap()
            )
            .len(),
            93
        );
        assert!(rows.get(&key(4, 10)).is_err());
    }

    #[test]
    fn malformed_envelopes_and_overlaps_fail_closed() {
        let (_dir, db) = database();
        apply(&db, (0..2).map(|i| (key(1, i), Some(vec![1; 8]))));
        let rows = store(&db);
        let raw = rows
            .physical()
            .get(key(1, 1).as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        for offset in [0, 4, 20, 24, 40, 44, 60] {
            let mut bad = raw.clone();
            bad[offset] ^= 255;
            assert!(validate(&key(1, 1), &bad).is_err(), "offset {offset}");
        }
        for length in 0..raw.len() {
            // Payload is opaque; only framing/directory truncation is detectable here.
            if length <= HEADER + 2 * DESCRIPTOR {
                assert!(validate(&key(1, 1), &raw[..length]).is_err());
            }
        }
        let mut other = raw.clone();
        other[44..60].copy_from_slice(&2u128.to_be_bytes());
        let tx = db.begin_write().unwrap();
        tx.open_table(ROWS)
            .unwrap()
            .insert(key(1, 2).as_slice(), other.as_slice())
            .unwrap();
        tx.commit().unwrap();
        let rows = store(&db);
        // Each envelope is locally valid; a point read is not a global audit.
        // The scan and bounded scan must still reject the overlap.
        assert_eq!(rows.get(&key(1, 2)).unwrap().unwrap().value(), &[1; 8]);
        assert!(rows.iter().unwrap().any(|row| row.is_err()));
        assert!(
            rows.range::<&[u8]>(key(1, 2).as_slice()..)
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );
        let tx = db.begin_write().unwrap();
        let mut writer = DenseWriter::new(tx.open_table(ROWS).unwrap(), 1024);
        writer.insert(&key(1, 2), b"replacement").unwrap();
        assert!(writer.finish().is_err());
    }

    #[test]
    fn point_and_scan_validate_complete_directory_equally() {
        let (_dir, db) = database();
        apply(
            &db,
            (0..3).map(|i| (key(1, i), Some(vec![i as u8; i as usize * 11 + 1]))),
        );
        let rows = store(&db);
        let raw = rows
            .physical()
            .get(key(1, 2).as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        let expected = collect(rows.iter().unwrap());
        for (key, bytes) in &expected {
            let value = rows.get(key).unwrap().unwrap();
            assert!(matches!(&value, RowValueGuard::Point { .. }));
            assert_eq!(value.value(), bytes);
        }
        // Include faults after the requested first row: finding a matching ID
        // must not short-circuit complete envelope validation.
        for offset in [0, 4, 20, 24, 40, 44, 60, 64, 80] {
            let mut bad = raw.clone();
            bad[offset] ^= 255;
            let tx = db.begin_write().unwrap();
            tx.open_table(ROWS)
                .unwrap()
                .insert(key(1, 2).as_slice(), bad.as_slice())
                .unwrap();
            tx.commit().unwrap();
            let corrupt = store(&db);
            assert!(corrupt.get(&key(1, 0)).is_err(), "point offset {offset}");
            assert!(
                corrupt.iter().unwrap().next().unwrap().is_err(),
                "scan offset {offset}"
            );
        }
    }

    #[test]
    fn directory_validation_matches_scalar_reference_for_mutations() {
        fn reference(upper: &[u8; 32], bytes: &[u8]) -> bool {
            if bytes.len() < HEADER || &bytes[..4] != MAGIC || bytes[4..20] != upper[..16] {
                return false;
            }
            let count = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
            if count == 0 || count > MAX_ROWS || (count > 1 && bytes.len() > TARGET_BYTES) {
                return false;
            }
            let payload = HEADER + count * DESCRIPTOR;
            if payload >= bytes.len() {
                return false;
            }
            let mut previous_id: Option<&[u8]> = None;
            let mut previous_offset = 0;
            for index in 0..count {
                let start = HEADER + index * DESCRIPTOR;
                let id = &bytes[start..start + 16];
                let offset =
                    u32::from_le_bytes(bytes[start + 16..start + DESCRIPTOR].try_into().unwrap())
                        as usize;
                if (index == 0 && offset != payload)
                    || offset >= bytes.len()
                    || previous_id.is_some_and(|previous| previous >= id)
                    || (index > 0 && previous_offset >= offset)
                {
                    return false;
                }
                previous_id = Some(id);
                previous_offset = offset;
            }
            previous_id == Some(&upper[16..])
        }
        for count in [1usize, 2, 16, 48, 128] {
            let upper = key(7, u128::MAX);
            let mut bytes = Vec::from(*MAGIC);
            bytes.extend_from_slice(&upper[..16]);
            bytes.extend_from_slice(&(count as u32).to_le_bytes());
            for index in 0..count {
                let id = u128::MAX - (count - 1 - index) as u128;
                bytes.extend_from_slice(&id.to_be_bytes());
                bytes.extend_from_slice(
                    &((HEADER + count * DESCRIPTOR + index) as u32).to_le_bytes(),
                );
            }
            bytes.resize(HEADER + count * (DESCRIPTOR + 1), 1);
            assert!(validate(&upper, &bytes).is_ok());
            for index in 0..bytes.len() {
                let mut mutated = bytes.clone();
                mutated[index] ^= 255;
                assert_eq!(
                    validate(&upper, &mutated).is_ok(),
                    reference(&upper, &mutated),
                    "count {count}, byte {index}"
                );
                assert_eq!(
                    validate(&upper, &bytes[..index]).is_ok(),
                    reference(&upper, &bytes[..index]),
                    "count {count}, length {index}"
                );
            }
        }
    }

    #[test]
    fn stored_oversized_singleton_rejected_before_writer_copy() {
        let (_dir, db) = database();
        apply(&db, [(key(1, 1), Some(vec![1; 8192]))]);
        let tx = db.begin_write().unwrap();
        let mut writer = DenseWriter::new(tx.open_table(ROWS).unwrap(), 1024);
        writer.insert(&key(1, 2), b"ok").unwrap();
        assert!(writer.finish().is_err());
    }

    #[test]
    fn variable_rows_stay_inside_byte_target_or_singleton() {
        let (_dir, db) = database();
        apply(
            &db,
            (0..500).map(|i| {
                (
                    key(1, i),
                    Some(vec![
                        1;
                        if i % 17 == 0 {
                            4096
                        } else {
                            i as usize % 100 + 1
                        }
                    ]),
                )
            }),
        );
        let rows = store(&db);
        for entry in rows.physical().iter().unwrap() {
            let (key, value) = entry.unwrap();
            let directory = validate(&logical_key(key.value()).unwrap(), value.value()).unwrap();
            assert!(directory.count == 1 || value.value().len() <= TARGET_BYTES);
        }
        assert_eq!(rows.len().unwrap(), 500);
    }

    #[test]
    fn owned_insert_moves_payload_and_streamed_merges_match_model() {
        let (_dir, db) = database();
        let tx = db.begin_write().unwrap();
        let mut writer = DenseWriter::new(tx.open_table(ROWS).unwrap(), 16384);
        let payload = vec![3; 8192];
        let original_allocation = payload.as_ptr();
        writer.insert_owned(&key(1, 0), payload).unwrap();
        assert_eq!(
            writer.changes[&key(1, 0)].as_ref().unwrap().as_ptr(),
            original_allocation
        );
        writer.finish().unwrap();
        tx.commit().unwrap();

        let mut model = BTreeMap::from([(key(1, 0), vec![3; 8192])]);
        // Alternating new IDs, replacements, absent deletions, block-boundary
        // deletions and large singletons exercise both sorted merge inputs.
        let mut random = 0x7f4a7c159e3779b9u64;
        for round in 0..16 {
            let mut edits = Vec::new();
            for i in 0..40 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let k = key((random % 3) as u8, (random / 3 % 160) as u128);
                let value = if (i + round) % 4 == 0 {
                    model.remove(&k);
                    None
                } else {
                    let bytes = vec![i as u8; if i % 13 == 0 { 4096 } else { i * 7 + 1 }];
                    model.insert(k, bytes.clone());
                    Some(bytes)
                };
                edits.push((k, value));
            }
            let before = store(&db);
            let retained = collect(before.iter().unwrap());
            apply(&db, edits);
            let after = store(&db);
            assert_eq!(
                collect(after.iter().unwrap()),
                model.clone().into_iter().collect::<Vec<_>>()
            );
            assert_eq!(collect(before.iter().unwrap()), retained);
            for (k, value) in &model {
                assert_eq!(after.get(k).unwrap().unwrap().value(), value);
            }
        }
    }
}
