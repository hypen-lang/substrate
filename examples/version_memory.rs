//! Allocator-accounted comparison of conflict-version representations.
//!
//! Run: cargo run --release --example version_memory -- 100000
//! Measures requested heap bytes, not RSS, allocator overhead, or engine capacity.
//! Both variants execute identical generated schema/row/index-version updates.
use im::OrdMap;
use sha2::{Digest, Sha256};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
pub use substrate_liquid::{Id, Value};

type Bucket = (Id, usize, Value);
type UniqueKey = (Id, usize, Vec<Value>);
type RowKey = (Id, Id);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Catalog,
    Visibility,
    Allocated(Id),
    Policy(String),
    Revocation(String),
    Row(Id, Id),
    Family(Id),
    Bucket(Bucket),
    Unique(UniqueKey),
    Incoming(RowKey),
}
#[path = "../src/storage/version.rs"]
mod version;
use version::Versions;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
fn allocated(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}
// Every allocation is delegated unchanged to System. Layout bytes exclude
// allocator metadata, size-class rounding, mapped pages, and file/page caches.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, bytes) };
        if !next.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(bytes);
        }
        next
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

trait VersionMap: Clone + Default {
    fn set(&mut self, key: Key, revision: u64);
    fn read(&self, key: &Key) -> u64;
    fn key_count(&self) -> Option<usize> {
        None
    }
}
impl VersionMap for OrdMap<Key, u64> {
    fn key_count(&self) -> Option<usize> {
        Some(self.len())
    }
    fn set(&mut self, key: Key, revision: u64) {
        self.insert(key, revision);
    }
    fn read(&self, key: &Key) -> u64 {
        self.get(key).copied().unwrap_or(0)
    }
}
impl VersionMap for Versions {
    fn set(&mut self, key: Key, revision: u64) {
        self.insert(key, revision);
    }
    fn read(&self, key: &Key) -> u64 {
        self.get(key)
    }
}

#[derive(Clone, Copy)]
enum Fixture {
    CrossApp,
    UniqueScalar,
}
impl Fixture {
    fn name(self) -> &'static str {
        match self {
            Self::CrossApp => "five_family_cross_app",
            Self::UniqueScalar => "unique_text_scalar",
        }
    }
    fn mutable_rows(self, rows: usize) -> usize {
        match self {
            Self::CrossApp => (rows / 9) * 2,
            Self::UniqueScalar => rows,
        }
    }
}
fn family(number: usize) -> Id {
    Id::from_u128(number as u128 + 1)
}
fn identity(number: usize, row: usize) -> Id {
    Id::from_u128(((number as u128 + 1) << 64) | (row as u128 + 1))
}
fn metadata(mut visit: impl FnMut(Key, u64)) {
    visit(Key::Catalog, 1);
    visit(Key::Visibility, 1);
    for name in ["core", "fitness", "crm"] {
        visit(Key::Policy(name.into()), 1);
        visit(Key::Revocation(name.into()), 1);
    }
}
fn record(
    number: usize,
    index: usize,
    values: &[Value],
    unique: bool,
    revision: u64,
    visit: &mut impl FnMut(Key, u64),
) {
    let tid = family(number);
    let id = identity(number, index);
    visit(Key::Allocated(id), revision);
    visit(Key::Row(tid, id), revision);
    visit(Key::Family(tid), revision);
    for (slot, value) in values.iter().enumerate() {
        visit(Key::Bucket((tid, slot, value.clone())), revision);
        if let Value::Ref(target) = value {
            let target_family = match (number, slot) {
                (2 | 4, 0) => 0,
                (2, 1) => 1,
                (4, 1) => 3,
                _ => unreachable!("fixture reference role"),
            };
            visit(Key::Incoming((family(target_family), *target)), revision);
        }
    }
    if unique {
        visit(Key::Unique((tid, 0, values.to_vec())), revision);
    }
}
fn initial(fixture: Fixture, rows: usize, mut visit: impl FnMut(Key, u64)) {
    metadata(&mut visit);
    match fixture {
        Fixture::UniqueScalar => {
            for i in 0..rows {
                record(
                    0,
                    i,
                    &[Value::Text(format!("Person{i:06}"))],
                    true,
                    i as u64 / 1000 + 1,
                    &mut visit,
                );
            }
        }
        Fixture::CrossApp => {
            let people = rows / 9;
            for i in 0..people {
                let rev = i as u64 / 1000 + 1;
                record(
                    0,
                    i,
                    &[Value::Text(format!("Person{i:06}"))],
                    false,
                    rev,
                    &mut visit,
                );
                for j in 0..2 {
                    let index = i * 2 + j;
                    record(
                        1,
                        index,
                        &[Value::Int(((i * 7 + j * 19) % 120) as i64)],
                        false,
                        rev,
                        &mut visit,
                    );
                    record(
                        2,
                        index,
                        &[Value::Ref(identity(0, i)), Value::Ref(identity(1, index))],
                        false,
                        rev,
                        &mut visit,
                    );
                    record(
                        3,
                        index,
                        &[Value::Decimal((i as i64 + 1) * 100 + j as i64 * 17)],
                        false,
                        rev,
                        &mut visit,
                    );
                    record(
                        4,
                        index,
                        &[Value::Ref(identity(0, i)), Value::Ref(identity(3, index))],
                        false,
                        rev,
                        &mut visit,
                    );
                }
            }
            // Standalone Person rows make arbitrary requested counts exact.
            for i in people..people + rows % 9 {
                record(
                    0,
                    i,
                    &[Value::Text(format!("Person{i:06}"))],
                    false,
                    i as u64 / 1000 + 1,
                    &mut visit,
                );
            }
        }
    }
}
fn updates(fixture: Fixture, rows: usize, count: usize, mut visit: impl FnMut(Key, u64)) {
    for i in 0..count {
        let rev = rows as u64 + i as u64 + 2;
        let (number, old, new) = match fixture {
            Fixture::CrossApp => (
                1,
                Value::Int((((i / 2) * 7 + (i % 2) * 19) % 120) as i64),
                Value::Int(1000 + i as i64),
            ),
            Fixture::UniqueScalar => (
                0,
                Value::Text(format!("Person{i:06}")),
                Value::Text(format!("Updated{i:06}")),
            ),
        };
        let tid = family(number);
        visit(Key::Row(tid, identity(number, i)), rev);
        visit(Key::Family(tid), rev);
        visit(Key::Bucket((tid, 0, old.clone())), rev);
        visit(Key::Bucket((tid, 0, new.clone())), rev);
        if matches!(fixture, Fixture::UniqueScalar) {
            visit(Key::Unique((tid, 0, vec![old])), rev);
            visit(Key::Unique((tid, 0, vec![new])), rev);
        }
    }
}
fn digest<M: VersionMap>(
    map: &M,
    fixture: Fixture,
    rows: usize,
    count: usize,
) -> ([u8; 32], usize) {
    let mut hash = Sha256::new();
    let mut lookups = 0;
    let mut visit = |key, _| {
        hash.update(map.read(&key).to_le_bytes());
        lookups += 1;
    };
    initial(fixture, rows, &mut visit);
    updates(fixture, rows, count, &mut visit);
    for key in [
        Key::Row(family(999), identity(999, 0)),
        Key::Bucket((family(999), 0, Value::Int(99))),
        Key::Incoming((family(999), identity(999, 0))),
        Key::Unique((family(999), 0, vec![Value::Int(99)])),
    ] {
        assert_eq!(map.read(&key), 0);
        hash.update(map.read(&key).to_le_bytes());
        lookups += 1;
    }
    (hash.finalize().into(), lookups)
}
#[derive(Debug, PartialEq, Eq)]
struct Semantics {
    before: [u8; 32],
    after: [u8; 32],
    lookups: usize,
    initial_keys: usize,
    updated_keys: usize,
}
fn run<M: VersionMap>(
    implementation: &str,
    fixture: Fixture,
    rows: usize,
    expected: Option<&Semantics>,
) -> Semantics {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let start = Instant::now();
    let mut map = M::default();
    initial(fixture, rows, |key, revision| map.set(key, revision));
    let build_seconds = start.elapsed().as_secs_f64();
    let live_bytes = LIVE.load(Ordering::Relaxed) - baseline;
    let build_peak_bytes = PEAK.load(Ordering::Relaxed) - baseline;
    let initial_keys = map
        .key_count()
        .or_else(|| expected.map(|s| s.initial_keys))
        .expect("legacy count supplied");
    let clone_iterations = 100_000;
    let start = Instant::now();
    for _ in 0..clone_iterations {
        drop(black_box(map.clone()));
    }
    let clone_drop_seconds = start.elapsed().as_secs_f64();
    let count = 10_000.min(fixture.mutable_rows(rows));
    let (before, lookups) = digest(&map, fixture, rows, count);
    let retained = map.clone();
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let start = Instant::now();
    updates(fixture, rows, count, |key, revision| map.set(key, revision));
    let update_seconds = start.elapsed().as_secs_f64();
    let retained_update_bytes = LIVE.load(Ordering::Relaxed) - baseline;
    let update_peak_bytes = PEAK.load(Ordering::Relaxed) - baseline;
    let updated_keys = map
        .key_count()
        .or_else(|| expected.map(|s| s.updated_keys))
        .expect("legacy count supplied");
    assert_eq!(digest(&retained, fixture, rows, count), (before, lookups));
    let (after, final_lookups) = digest(&map, fixture, rows, count);
    assert_eq!(lookups, final_lookups);
    let start = Instant::now();
    // Identical key generation cost is included for both representations. Hashing
    // is outside this timed loop; black_box keeps every lookup observable.
    initial(fixture, rows, |key, _| {
        black_box(map.read(black_box(&key)));
    });
    updates(fixture, rows, count, |key, _| {
        black_box(map.read(black_box(&key)));
    });
    let lookup_seconds = start.elapsed().as_secs_f64();
    let start = Instant::now();
    drop(retained);
    let release_old_seconds = start.elapsed().as_secs_f64();
    let current_only_bytes = LIVE.load(Ordering::Relaxed) - baseline;
    drop(map);
    assert_eq!(
        LIVE.load(Ordering::Relaxed),
        baseline,
        "all version-map allocations reclaimed"
    );
    println!(
        "{}",
        serde_json::json!({
            "implementation":implementation,"fixture":fixture.name(),"rows":rows,
            "distinct_initial_keys":initial_keys,"distinct_updated_keys":updated_keys,
            "requested_bytes_per_initial_key": live_bytes as f64 / initial_keys as f64,
            "current_bytes_per_updated_key": current_only_bytes as f64 / updated_keys as f64,
            "map_inline_bytes":std::mem::size_of::<M>(),
            "clone_drop_iterations":clone_iterations,"clone_drop_seconds":clone_drop_seconds,
            "clone_drop_ns":clone_drop_seconds*1e9/clone_iterations as f64,
            "live_requested_bytes":live_bytes,"requested_bytes_per_record":live_bytes as f64 / rows as f64,
            "build_peak_requested_bytes":build_peak_bytes,"build_seconds":build_seconds,
            "updated_records":count,"update_seconds":update_seconds,
            "live_bytes_with_retained_snapshot":retained_update_bytes,"update_peak_requested_bytes":update_peak_bytes,
            "current_only_requested_bytes":current_only_bytes,"release_old_snapshot_seconds":release_old_seconds,
            "timed_lookup_count":lookups-4,"lookup_seconds_including_key_generation":lookup_seconds,
            "initial_digest":before.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "updated_digest":after.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "retained_snapshot_verified":true,"all_allocations_reclaimed":true,
            "memory_note":"requested layouts only; excludes allocator rounding/metadata, RSS and row/index storage outside version maps",
            "update_note":"old bucket/unique versions remain tombstones; this measures compact storage, not history reclamation",
        })
    );
    Semantics {
        before,
        after,
        lookups,
        initial_keys,
        updated_keys,
    }
}
fn main() {
    let rows = std::env::args()
        .nth(1)
        .map(|s| s.parse::<usize>().expect("rows must be a positive integer"))
        .unwrap_or(100_000);
    assert!(
        (1..=10_000_000).contains(&rows),
        "rows must be 1..=10000000"
    );
    for fixture in [Fixture::CrossApp, Fixture::UniqueScalar] {
        let old = run::<OrdMap<Key, u64>>("monolithic_key_tree", fixture, rows, None);
        let new = run::<Versions>("typed_version_families", fixture, rows, Some(&old));
        assert_eq!(
            old, new,
            "all lookup outputs must match across representations"
        );
    }
}
