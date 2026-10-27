//! Experimental physical-layout comparison, not a Substrate SQL/API backend.
//! Run `scan_layout_bench rows|blocks PEOPLE ITERATIONS` in separate processes.
//! Identical PG01-like rows/IDs, 256-pair transactions, 8 MiB redb cache, Immediate.
//! Ordered one-to-one references only; no production authorization implementation.
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageBackend,
    TableDefinition,
};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("fixture");
const HEAD: TableDefinition<u8, u64> = TableDefinition::new("head");
const BLOCK_ROWS: usize = 256;
const CACHE: usize = 8 * 1024 * 1024;
const PERSON: u8 = 1;
const SCORE: u8 = 2;
const MAX_ROW: usize = 128;
const MAX_BLOCK: usize = 20 + BLOCK_ROWS * (20 + MAX_ROW) + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Rows,
    Blocks,
    Structural,
    Structural56,
}
impl Layout {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "rows" => Ok(Self::Rows),
            "blocks" => Ok(Self::Blocks),
            "blocks-structural" => Ok(Self::Structural),
            "blocks-structural56" => Ok(Self::Structural56),
            _ => Err("expected rows|blocks|blocks-structural|blocks-structural56".into()),
        }
    }
    fn span(self) -> usize {
        if self == Self::Structural56 {
            56
        } else {
            BLOCK_ROWS
        }
    }
    fn checksum(self) -> bool {
        self == Self::Blocks
    }
    fn magic(self) -> &'static [u8; 4] {
        if self.checksum() { b"DB01" } else { b"DB02" }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::Blocks => "blocks",
            Self::Structural => "blocks-structural",
            Self::Structural56 => "blocks-structural56",
        }
    }
}
#[derive(Debug, Default)]
struct Io {
    reads: AtomicU64,
    read_bytes: AtomicU64,
    writes: AtomicU64,
    write_bytes: AtomicU64,
    syncs: AtomicU64,
}
#[derive(Clone, Copy, Serialize)]
struct Counts {
    read_calls: u64,
    read_requested_bytes: u64,
    write_calls: u64,
    write_requested_bytes: u64,
    sync_calls: u64,
}
impl Io {
    fn snapshot(&self) -> Counts {
        Counts {
            read_calls: self.reads.load(Ordering::Relaxed),
            read_requested_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_calls: self.writes.load(Ordering::Relaxed),
            write_requested_bytes: self.write_bytes.load(Ordering::Relaxed),
            sync_calls: self.syncs.load(Ordering::Relaxed),
        }
    }
}
impl Counts {
    fn since(self, before: Self) -> Self {
        Self {
            read_calls: self.read_calls - before.read_calls,
            read_requested_bytes: self.read_requested_bytes - before.read_requested_bytes,
            write_calls: self.write_calls - before.write_calls,
            write_requested_bytes: self.write_requested_bytes - before.write_requested_bytes,
            sync_calls: self.sync_calls - before.sync_calls,
        }
    }
}
#[derive(Debug)]
struct Measured {
    file: redb::backends::FileBackend,
    io: Arc<Io>,
}
impl StorageBackend for Measured {
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.io.reads.fetch_add(1, Ordering::Relaxed);
        self.io
            .read_bytes
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        self.file.read(offset, out)
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.io.writes.fetch_add(1, Ordering::Relaxed);
        self.io
            .write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.file.write(offset, data)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.io.syncs.fetch_add(1, Ordering::Relaxed);
        self.file.sync_data()
    }
    fn close(&self) -> std::io::Result<()> {
        self.file.close()
    }
}
fn open(path: &Path) -> Result<(Database, Arc<Io>)> {
    let io = Arc::new(Io::default());
    let file = redb::backends::FileBackend::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?,
    )?;
    let db = Database::builder()
        .set_cache_size(CACHE)
        .create_with_backend(Measured {
            file,
            io: io.clone(),
        })?;
    Ok((db, io))
}
fn key(family: u8, id: u128) -> [u8; 32] {
    let mut out = [0; 32];
    out[0] = family;
    out[16..].copy_from_slice(&id.to_be_bytes());
    out
}
fn block_key(family: u8, block: u64) -> [u8; 24] {
    let mut out = [0; 24];
    out[0] = family;
    out[16..].copy_from_slice(&block.to_be_bytes());
    out
}
fn parse_key(bytes: &[u8], family: u8) -> Result<u128> {
    if bytes.len() != 32 || bytes[0] != family || bytes[1..16].iter().any(|b| *b != 0) {
        return Err("invalid row identity key".into());
    }
    Ok(u128::from_be_bytes(bytes[16..].try_into()?))
}
fn parse_block_key(bytes: &[u8], family: u8) -> Result<u64> {
    if bytes.len() != 24 || bytes[0] != family || bytes[1..16].iter().any(|b| *b != 0) {
        return Err("invalid block key".into());
    }
    Ok(u64::from_be_bytes(bytes[16..].try_into()?))
}
fn packed(family: u8, n: u64, revision: u64, score: i64) -> Vec<u8> {
    let mut body = Vec::new();
    if family == PERSON {
        let name = format!("Person-{n:09}");
        body.extend_from_slice(&(name.len() as u32).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&(n as i64).to_be_bytes());
    } else {
        body.extend_from_slice(&(n as u128).to_be_bytes());
        body.extend_from_slice(&score.to_be_bytes());
        body.extend_from_slice(&((n % 32) as i64).to_be_bytes());
    }
    let mut out = b"PG01".to_vec();
    out.extend_from_slice(&revision.to_be_bytes());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&(if family == PERSON { 2u32 } else { 3 }).to_be_bytes());
    out.extend(body);
    out
}
#[derive(Clone, Copy, Default, Debug)]
struct Record {
    id: u128,
    reference: u128,
    number: i64,
    bucket: i64,
}
fn decode(family: u8, id: u128, bytes: &[u8], head: u64) -> Result<Record> {
    if bytes.len() < 20 || bytes.len() > MAX_ROW || &bytes[..4] != b"PG01" {
        return Err("invalid packed header".into());
    }
    let revision = u64::from_be_bytes(bytes[4..12].try_into()?);
    let len = u32::from_be_bytes(bytes[12..16].try_into()?) as usize;
    let arity = u32::from_be_bytes(bytes[16..20].try_into()?);
    if revision == 0
        || revision > head
        || len != bytes.len() - 20
        || arity != if family == PERSON { 2 } else { 3 }
        || id == 0
    {
        return Err("invalid packed revision/shape".into());
    }
    let body = &bytes[20..];
    if family == PERSON {
        let name_len =
            u32::from_be_bytes(body.get(..4).ok_or("short name length")?.try_into()?) as usize;
        if name_len > MAX_ROW || body.len() != 4 + name_len + 8 {
            return Err("invalid person shape".into());
        }
        std::str::from_utf8(&body[4..4 + name_len])?;
        let number = i64::from_be_bytes(body[4 + name_len..].try_into()?);
        if u128::try_from(number)? != id {
            return Err("person ID/value mismatch".into());
        }
        Ok(Record {
            id,
            number,
            ..Record::default()
        })
    } else {
        if body.len() != 32 {
            return Err("invalid score shape".into());
        }
        let reference = u128::from_be_bytes(body[..16].try_into()?);
        if reference == 0 {
            return Err("zero reference".into());
        }
        Ok(Record {
            id,
            reference,
            number: i64::from_be_bytes(body[16..24].try_into()?),
            bucket: i64::from_be_bytes(body[24..].try_into()?),
        })
    }
}
fn encode_block(layout: Layout, rows: &[(u128, Vec<u8>)], revision: u64) -> Result<Vec<u8>> {
    if layout == Layout::Rows || rows.len() > layout.span() || revision == 0 {
        return Err("invalid block count/revision".into());
    }
    let mut out = layout.magic().to_vec();
    out.extend_from_slice(&revision.to_be_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    let mut previous = 0;
    for (id, row) in rows {
        if *id <= previous || row.len() > MAX_ROW {
            return Err("unordered or oversized block record".into());
        }
        previous = *id;
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&(row.len() as u32).to_be_bytes());
        out.extend(row);
    }
    let body_len = out.len() - 20;
    out[16..20].copy_from_slice(&(body_len as u32).to_be_bytes());
    if layout.checksum() {
        let hash = Sha256::digest(&out);
        out.extend_from_slice(&hash);
    }
    Ok(out)
}
fn block_rows(layout: Layout, bytes: &[u8], head: u64) -> Result<Vec<(u128, &[u8])>> {
    let suffix = if layout.checksum() { 32 } else { 0 };
    if layout == Layout::Rows
        || !(20 + suffix..=MAX_BLOCK).contains(&bytes.len())
        || &bytes[..4] != layout.magic()
    {
        return Err("invalid block header".into());
    }
    let revision = u64::from_be_bytes(bytes[4..12].try_into()?);
    let count = u32::from_be_bytes(bytes[12..16].try_into()?) as usize;
    let len = u32::from_be_bytes(bytes[16..20].try_into()?) as usize;
    if revision == 0 || revision > head || count > layout.span() || len != bytes.len() - 20 - suffix
    {
        return Err("invalid block shape/revision".into());
    }
    let end = bytes.len() - suffix;
    if layout.checksum() && Sha256::digest(&bytes[..end])[..] != bytes[end..] {
        return Err("block checksum mismatch".into());
    }
    let mut rest = &bytes[20..end];
    let mut rows = Vec::with_capacity(count);
    let mut previous = 0;
    for _ in 0..count {
        if rest.len() < 20 {
            return Err("truncated block record".into());
        }
        let id = u128::from_be_bytes(rest[..16].try_into()?);
        let len = u32::from_be_bytes(rest[16..20].try_into()?) as usize;
        if id <= previous || len > MAX_ROW || rest.len() < 20 + len {
            return Err("invalid block record".into());
        }
        rows.push((id, &rest[20..20 + len]));
        rest = &rest[20 + len..];
        previous = id;
    }
    if !rest.is_empty() {
        return Err("trailing block bytes".into());
    }
    Ok(rows)
}
fn decode_block(
    layout: Layout,
    family: u8,
    block: u64,
    bytes: &[u8],
    head: u64,
    out: &mut [Record; BLOCK_ROWS],
) -> Result<usize> {
    let rows = block_rows(layout, bytes, head)?;
    for (slot, (id, packed)) in rows.iter().enumerate() {
        if (*id - 1) / layout.span() as u128 != block as u128 {
            return Err("identity outside block".into());
        }
        out[slot] = decode(family, *id, packed, head)?;
    }
    Ok(rows.len())
}
fn load(db: &Database, layout: Layout, people: u64) -> Result<()> {
    for first in (1..=people).step_by(BLOCK_ROWS) {
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(TABLE)?;
            for family in [PERSON, SCORE] {
                let rows = (first..=people.min(first + BLOCK_ROWS as u64 - 1))
                    .map(|n| (n as u128, packed(family, n, 1, (n as i64) * 10)))
                    .collect::<Vec<_>>();
                match layout {
                    Layout::Rows => {
                        for (id, row) in rows {
                            table.insert(key(family, id).as_slice(), row.as_slice())?;
                        }
                    }
                    Layout::Blocks | Layout::Structural | Layout::Structural56 => {
                        if layout.span() == BLOCK_ROWS {
                            let blob = encode_block(layout, &rows, 1)?;
                            table.insert(
                                block_key(family, (first - 1) / BLOCK_ROWS as u64).as_slice(),
                                blob.as_slice(),
                            )?;
                        } else {
                            // Keep 256 pairs per durable transaction in every mode.
                            // A 56-row block can straddle that transaction boundary;
                            // only the bounded partial block is read and extended.
                            let mut first = 0;
                            while first < rows.len() {
                                let block = (rows[first].0 - 1) / layout.span() as u128;
                                let end = first
                                    + rows[first..].partition_point(|(id, _)| {
                                        (*id - 1) / layout.span() as u128 == block
                                    });
                                let k = block_key(family, u64::try_from(block)?);
                                let existing =
                                    table.get(k.as_slice())?.map(|value| value.value().to_vec());
                                let mut combined = match existing.as_deref() {
                                    Some(value) => block_rows(layout, value, 1)?
                                        .into_iter()
                                        .map(|(id, row)| (id, row.to_vec()))
                                        .collect(),
                                    None => Vec::new(),
                                };
                                combined.extend_from_slice(&rows[first..end]);
                                let blob = encode_block(layout, &combined, 1)?;
                                table.insert(k.as_slice(), blob.as_slice())?;
                                first = end;
                            }
                        }
                    }
                }
            }
        }
        tx.open_table(HEAD)?.insert(0, 1)?;
        tx.commit()?;
    }
    Ok(())
}
#[derive(Debug, PartialEq, Eq, Serialize)]
struct Aggregate {
    count: u64,
    sum: i128,
    groups: [(u64, i128); 32],
}
impl Aggregate {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0,
            groups: [(0, 0); 32],
        }
    }
    fn add(&mut self, person: Record, score: Record, lower: i64) -> Result<()> {
        if person.id != score.reference || score.id != score.reference {
            return Err("dangling/mismatched fixture reference".into());
        }
        if score.bucket < 0 || score.bucket >= 32 {
            return Err("invalid bucket".into());
        }
        if score.number >= lower {
            self.count = self.count.checked_add(1).ok_or("count overflow")?;
            self.sum = self
                .sum
                .checked_add(score.number as i128)
                .ok_or("sum overflow")?;
            let g = &mut self.groups[score.bucket as usize];
            g.0 = g.0.checked_add(1).ok_or("group count overflow")?;
            g.1 =
                g.1.checked_add(score.number as i128)
                    .ok_or("group sum overflow")?;
        }
        Ok(())
    }
}
fn scan(
    table: &redb::ReadOnlyTable<&[u8], &[u8]>,
    layout: Layout,
    head: u64,
    lower: i64,
) -> Result<Aggregate> {
    let mut result = Aggregate::new();
    let lo = [PERSON];
    let middle = [SCORE];
    let hi = [SCORE + 1];
    let mut persons = table.range(lo.as_slice()..middle.as_slice())?;
    let mut scores = table.range(middle.as_slice()..hi.as_slice())?;
    match layout {
        Layout::Rows => loop {
            match (persons.next(), scores.next()) {
                (None, None) => break,
                (Some(p), Some(s)) => {
                    let (pk, pv) = p?;
                    let (sk, sv) = s?;
                    let person = decode(PERSON, parse_key(pk.value(), PERSON)?, pv.value(), head)?;
                    let score = decode(SCORE, parse_key(sk.value(), SCORE)?, sv.value(), head)?;
                    result.add(person, score, lower)?;
                }
                _ => return Err("unpaired fixture family".into()),
            }
        },
        Layout::Blocks | Layout::Structural | Layout::Structural56 => {
            let mut pbuf = [Record::default(); BLOCK_ROWS];
            let mut sbuf = [Record::default(); BLOCK_ROWS];
            loop {
                match (persons.next(), scores.next()) {
                    (None, None) => break,
                    (Some(p), Some(s)) => {
                        let (pk, pv) = p?;
                        let (sk, sv) = s?;
                        let pb = parse_block_key(pk.value(), PERSON)?;
                        let sb = parse_block_key(sk.value(), SCORE)?;
                        if pb != sb {
                            return Err("mismatched family blocks".into());
                        }
                        let pn = decode_block(layout, PERSON, pb, pv.value(), head, &mut pbuf)?;
                        let sn = decode_block(layout, SCORE, sb, sv.value(), head, &mut sbuf)?;
                        if pn != sn {
                            return Err("unpaired block rows".into());
                        }
                        for i in 0..pn {
                            result.add(pbuf[i], sbuf[i], lower)?;
                        }
                    }
                    _ => return Err("unpaired fixture blocks".into()),
                }
            }
        }
    }
    Ok(result)
}
// Atomically update a score or remove both members of this one-to-one fixture.
// Dense mutation rewrites only its affected blocks; other blocks remain unchanged.
fn mutate(
    db: &Database,
    layout: Layout,
    id: u64,
    new_score: Option<i64>,
    revision: u64,
    commit: bool,
) -> Result<()> {
    let mut tx = db.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    let previous = tx
        .open_table(HEAD)?
        .get(0)?
        .ok_or("missing durable head")?
        .value();
    if revision <= previous {
        return Err("non-monotonic mutation revision".into());
    }
    {
        let mut table = tx.open_table(TABLE)?;
        for family in [PERSON, SCORE] {
            if family == PERSON && new_score.is_some() {
                continue;
            }
            match layout {
                Layout::Rows => {
                    let k = key(family, id as u128);
                    if table.get(k.as_slice())?.is_none() {
                        return Err("missing mutation row".into());
                    }
                    if let Some(score) = new_score {
                        let row = packed(family, id, revision, score);
                        table.insert(k.as_slice(), row.as_slice())?;
                    } else {
                        table.remove(k.as_slice())?;
                    }
                }
                Layout::Blocks | Layout::Structural | Layout::Structural56 => {
                    let k = block_key(family, (id - 1) / layout.span() as u64);
                    let old = table
                        .get(k.as_slice())?
                        .ok_or("missing mutation block")?
                        .value()
                        .to_vec();
                    let mut rows = block_rows(layout, &old, revision)?
                        .into_iter()
                        .map(|(id, row)| (id, row.to_vec()))
                        .collect::<Vec<_>>();
                    let pos = rows
                        .iter()
                        .position(|(n, _)| *n == id as u128)
                        .ok_or("missing mutation ID")?;
                    if let Some(score) = new_score {
                        rows[pos].1 = packed(family, id, revision, score);
                    } else {
                        rows.remove(pos);
                    }
                    let blob = encode_block(layout, &rows, revision)?;
                    table.insert(k.as_slice(), blob.as_slice())?;
                }
            }
        }
    }
    tx.open_table(HEAD)?.insert(0, revision)?;
    if commit {
        tx.commit()?;
    } // Drop is the explicit aborted-transaction control.
    Ok(())
}
fn head(read: &redb::ReadTransaction) -> Result<u64> {
    Ok(read
        .open_table(HEAD)?
        .get(0)?
        .ok_or("missing durable head")?
        .value())
}
fn reference(people: u64, lower: i64) -> Aggregate {
    let mut out = Aggregate::new();
    for n in 1..=people {
        let score = (n as i64) * 10;
        if score >= lower {
            out.count += 1;
            out.sum += score as i128;
            out.groups[(n % 32) as usize].0 += 1;
            out.groups[(n % 32) as usize].1 += score as i128;
        }
    }
    out
}
fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let layout = Layout::parse(args.first().ok_or("missing layout")?)?;
    let people: u64 = args.get(1).ok_or("missing PEOPLE")?.parse()?;
    let iterations: u32 = args.get(2).ok_or("missing ITERATIONS")?.parse()?;
    if !(256..=1_000_000).contains(&people) || !(1..=100).contains(&iterations) || args.len() != 3 {
        return Err("PEOPLE 256..1000000 and ITERATIONS 1..100 required".into());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("db");
    let (db, io) = open(&path)?;
    let before = io.snapshot();
    let start = Instant::now();
    load(&db, layout, people)?;
    let load_seconds = start.elapsed().as_secs_f64();
    let load_io = io.snapshot().since(before);
    let read = db.begin_read()?;
    let table = read.open_table(TABLE)?;
    let mut cases = Vec::new();
    for (name, lower) in [
        ("all", 0),
        ("broad_90_percent", (people / 10 * 10) as i64),
        ("filtered_50_percent", (people / 2 * 10) as i64),
    ] {
        let expected = reference(people, lower);
        assert_eq!(scan(&table, layout, 1, lower)?, expected);
        let before = io.snapshot();
        let start = Instant::now();
        for _ in 0..iterations {
            assert_eq!(
                std::hint::black_box(scan(&table, layout, 1, lower)?),
                expected
            );
        }
        let elapsed = start.elapsed();
        let delta = io.snapshot().since(before);
        cases.push(json!({"case":name,"query_us":elapsed.as_secs_f64()*1e6/iterations as f64,"iterations":iterations,"backend_requests_total":delta}));
    }
    let stats = table.stats()?;
    let stored_bytes = stats.stored_bytes();
    let tree_metadata = stats.metadata_bytes();
    let fragmented = stats.fragmented_bytes();
    drop(table);
    drop(read);
    let before = io.snapshot();
    let start = Instant::now();
    for n in 1..=100 {
        mutate(&db, layout, n, Some(n as i64 * 10 + 1), n + 1, true)?;
    }
    let update_seconds = start.elapsed().as_secs_f64();
    let update_io = io.snapshot().since(before);
    drop(db);
    let (db, _) = open(&path)?;
    let read = db.begin_read()?;
    assert_eq!(head(&read)?, 101);
    let table = read.open_table(TABLE)?;
    let actual = scan(&table, layout, head(&read)?, 0)?;
    let mut expected = reference(people, 0);
    expected.sum += 100;
    for n in 1..=100 {
        expected.groups[n % 32].1 += 1;
    }
    assert_eq!(actual, expected);
    let metadata = std::fs::metadata(&path)?;
    #[cfg(unix)]
    let allocated_bytes = {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks() * 512
    };
    #[cfg(not(unix))]
    let allocated_bytes: Option<u64> = None;
    let packed_payload_bytes =
        people * (packed(PERSON, 1, 1, 10).len() + packed(SCORE, 1, 1, 10).len()) as u64;
    let output = json!({
        "experiment": "dense-family-block-v1",
        "layout": layout.name(),
        "people": people,
        "total_rows": people*2,
        "cache_bytes": CACHE,
        "batch_pairs": BLOCK_ROWS,
        "max_block_bytes": 20+layout.span()*(20+MAX_ROW)+if layout.checksum(){32}else{0},
        "block_rows": if layout == Layout::Rows {None} else {Some(layout.span())},
        "durability": "Immediate",
        "timing_class": "instrumented-layout-experiment",
        "production_backend": false,
        "authorization": "not implemented: fixed ordered one-to-one fixture only; no production security/performance claim",
        "operations": "every scan computes count, sum and 32-group summaries together",
        "checksum": if layout.checksum() {"stock redb integrity plus SHA256 envelope"} else {"stock redb integrity; no additional application checksum"},
        "row_validation": "complete packed shape, revision, identity and ordered reference validation in every mode",
        "load_seconds": load_seconds,
        "load_backend_requests": load_io,
        "canonical_packed_payload_bytes": packed_payload_bytes,
        "load_backend_write_amplification_vs_packed_payload": load_io.write_requested_bytes as f64 / packed_payload_bytes as f64,
        "queries": cases,
        "initial_table_stored_bytes": stored_bytes,
        "initial_table_metadata_bytes": tree_metadata,
        "initial_table_unused_page_bytes": fragmented,
        "table_stats_scope": "fixture table only; excludes durable head table and file allocator/free/history",
        "update_count": 100,
        "update_seconds": update_seconds,
        "update_backend_requests": update_io,
        "update_workload": "100 distinct scores, each in a separate durable transaction; first256-row block or firsttwo56-row blocks",
        "file_logical_bytes_after_updates": metadata.len(),
        "file_allocated_bytes_after_updates": allocated_bytes,
        "verified": true,
        "backend_note": "requested bytes at stock FileBackend; may hit OS cache; not physical I/O"
    });
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn layouts_match_updates_deletes_abort_snapshots_and_reopen() -> Result<()> {
        for layout in [
            Layout::Rows,
            Layout::Blocks,
            Layout::Structural,
            Layout::Structural56,
        ] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("db");
            let (db, _) = open(&path)?;
            load(&db, layout, 600)?;
            let old = db.begin_read()?;
            assert_eq!(head(&old)?, 1);
            let old_table = old.open_table(TABLE)?;
            assert_eq!(scan(&old_table, layout, 1, 0)?, reference(600, 0));
            mutate(&db, layout, 300, Some(999), 2, false)?;
            assert_eq!(
                scan(&db.begin_read()?.open_table(TABLE)?, layout, 1, 0)?,
                reference(600, 0)
            );
            mutate(&db, layout, 300, Some(999), 2, true)?;
            mutate(&db, layout, 1, None, 3, true)?;
            assert_eq!(scan(&old_table, layout, 1, 0)?, reference(600, 0));
            let mut expected = reference(600, 0);
            expected.sum += 999 - 3000 - 10;
            expected.count -= 1;
            expected.groups[300 % 32].1 += 999 - 3000;
            expected.groups[1].0 -= 1;
            expected.groups[1].1 -= 10;
            assert_eq!(
                scan(&db.begin_read()?.open_table(TABLE)?, layout, 3, 0)?,
                expected
            );
            drop(old_table);
            drop(old);
            drop(db);
            let (db, _) = open(&path)?;
            let recovered = db.begin_read()?;
            assert_eq!(head(&recovered)?, 3);
            assert_eq!(
                scan(&recovered.open_table(TABLE)?, layout, head(&recovered)?, 0)?,
                expected
            );
        }
        Ok(())
    }
    #[test]
    fn block_validation_rejects_framing_identity_revision_and_checksum_corruption() -> Result<()> {
        let row = packed(SCORE, 1, 1, 10);
        let good = encode_block(Layout::Blocks, &[(1, row.clone())], 1)?;
        let mut scratch = [Record::default(); BLOCK_ROWS];
        assert_eq!(
            decode_block(Layout::Blocks, SCORE, 0, &good, 1, &mut scratch)?,
            1
        );
        for offset in [0, 4, 12, 16, 20, 36, 45, good.len() - 1] {
            let mut bad = good.clone();
            bad[offset] ^= 1;
            assert!(decode_block(Layout::Blocks, SCORE, 0, &bad, 1, &mut scratch).is_err());
        }
        assert!(decode_block(Layout::Blocks, SCORE, 1, &good, 1, &mut scratch).is_err());
        assert!(decode_block(Layout::Blocks, SCORE, 0, &good, 0, &mut scratch).is_err());
        assert!(encode_block(Layout::Blocks, &[(1, row.clone()), (1, row)], 1).is_err());
        let mut bad_row = packed(PERSON, 1, 1, 10);
        bad_row[24] = 255;
        let bad = encode_block(Layout::Blocks, &[(1, bad_row)], 1)?;
        assert!(decode_block(Layout::Blocks, PERSON, 0, &bad, 1, &mut scratch).is_err());
        Ok(())
    }
    #[test]
    fn structural_blocks_validate_shapes_and_keep_redb_integrity_contract() -> Result<()> {
        for layout in [Layout::Structural, Layout::Structural56] {
            let mut scratch = [Record::default(); BLOCK_ROWS];
            let good = encode_block(layout, &[(1, packed(SCORE, 1, 1, 10))], 1)?;
            assert_eq!(decode_block(layout, SCORE, 0, &good, 1, &mut scratch)?, 1);
            // A valid numeric edit is not detected by a structural parser.
            // Stored bytes still receive stock redb's checksums, like row mode.
            let mut numeric = good.clone();
            numeric[76] ^= 1;
            assert_eq!(
                decode_block(layout, SCORE, 0, &numeric, 1, &mut scratch)?,
                1
            );
            let sha = encode_block(Layout::Blocks, &[(1, packed(SCORE, 1, 1, 10))], 1)?;
            let mut bad_sha = sha.clone();
            bad_sha[76] ^= 1;
            assert!(decode_block(Layout::Blocks, SCORE, 0, &bad_sha, 1, &mut scratch).is_err());
            for offset in [0, 4, 12, 16, 20, 36, 40, 44, 52, 56] {
                let mut malformed = good.clone();
                malformed[offset] ^= 1;
                assert!(
                    decode_block(layout, SCORE, 0, &malformed, 1, &mut scratch).is_err(),
                    "offset {offset}"
                );
            }
            let mut bad_person = packed(PERSON, 1, 1, 10);
            bad_person[24] = 255;
            let bad = encode_block(layout, &[(1, bad_person)], 1)?;
            assert!(decode_block(layout, PERSON, 0, &bad, 1, &mut scratch).is_err());
            assert!(decode_block(Layout::Blocks, SCORE, 0, &good, 1, &mut scratch).is_err());
            assert!(decode_block(layout, SCORE, 0, &sha, 1, &mut scratch).is_err());
            let too_many = (1..=layout.span() + 1)
                .map(|id| (id as u128, packed(SCORE, id as u64, 1, 10)))
                .collect::<Vec<_>>();
            assert!(encode_block(layout, &too_many, 1).is_err());
        }
        Ok(())
    }
    #[test]
    fn layout_crash_worker() -> Result<()> {
        let Some(path) = std::env::var_os("SUBSTRATE_LAYOUT_CRASH_TEST_PATH") else {
            return Ok(());
        };
        let layout = Layout::parse(&std::env::var("SUBSTRATE_LAYOUT_CRASH_TEST_MODE")?)?;
        let (db, _) = open(Path::new(&path))?;
        mutate(&db, layout, 300, Some(999), 2, true)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(TABLE)?;
            match layout {
                Layout::Rows => {
                    table.remove(key(SCORE, 1).as_slice())?;
                }
                Layout::Blocks | Layout::Structural | Layout::Structural56 => {
                    table.remove(block_key(SCORE, 0).as_slice())?;
                }
            }
            tx.open_table(HEAD)?.insert(0, 3)?;
        }
        // Kill without running transaction/database destructors. The prior
        // committed score and head must survive; this uncommitted deletion must not.
        std::process::exit(77)
    }
    #[test]
    fn process_crash_preserves_committed_blocks_and_discards_uncommitted_changes() -> Result<()> {
        for layout in [
            Layout::Rows,
            Layout::Blocks,
            Layout::Structural,
            Layout::Structural56,
        ] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("db");
            let (db, _) = open(&path)?;
            load(&db, layout, 600)?;
            drop(db);
            let status = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "tests::layout_crash_worker"])
                .env("SUBSTRATE_LAYOUT_CRASH_TEST_PATH", &path)
                .env("SUBSTRATE_LAYOUT_CRASH_TEST_MODE", layout.name())
                .status()?;
            assert_eq!(status.code(), Some(77));
            let (db, _) = open(&path)?;
            let read = db.begin_read()?;
            assert_eq!(head(&read)?, 2);
            let mut expected = reference(600, 0);
            expected.sum += 999 - 3000;
            expected.groups[300 % 32].1 += 999 - 3000;
            assert_eq!(
                scan(&read.open_table(TABLE)?, layout, head(&read)?, 0)?,
                expected
            );
        }
        Ok(())
    }
    #[test]
    fn both_layouts_reject_dangling_reference_and_future_row() -> Result<()> {
        for layout in [
            Layout::Rows,
            Layout::Blocks,
            Layout::Structural,
            Layout::Structural56,
        ] {
            let dir = tempfile::tempdir()?;
            let (db, _) = open(&dir.path().join("db"))?;
            load(&db, layout, 256)?;
            mutate(&db, layout, 1, Some(12), 2, true)?;
            assert!(scan(&db.begin_read()?.open_table(TABLE)?, layout, 1, 0).is_err());
            let p = Record {
                id: 1,
                ..Record::default()
            };
            let s = Record {
                id: 2,
                reference: 2,
                number: 1,
                bucket: 0,
            };
            assert!(Aggregate::new().add(p, s, 0).is_err());
        }
        Ok(())
    }
}
