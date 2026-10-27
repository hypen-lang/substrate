//! Allocator-accounted comparison of old and compact posting representations.
//! This isolates index structures, not whole-engine RSS or a serving benchmark.
#[allow(dead_code)]
#[path = "../src/storage/posting.rs"]
mod posting;

use im::{OrdMap, OrdSet};
use posting::Posting;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
use substrate_liquid::Id;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
fn allocated(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}
// Delegates every allocation unchanged to System; accounting uses its exact
// requested layouts and does not claim to include allocator rounding/metadata.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, size) };
        if !next.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(size);
        }
        next
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

trait Set: Clone + Default {
    fn insert(&mut self, value: Id);
    fn replace_first(&mut self, value: Id);
    fn first(&self) -> Id;
}
impl Set for OrdSet<Id> {
    fn insert(&mut self, value: Id) {
        self.insert(value);
    }
    fn replace_first(&mut self, value: Id) {
        self.remove(&self.first());
        self.insert(value);
    }
    fn first(&self) -> Id {
        *self.iter().next().unwrap()
    }
}
impl Set for Posting<Id> {
    fn insert(&mut self, value: Id) {
        self.insert(value);
    }
    fn replace_first(&mut self, value: Id) {
        self.remove(&self.first());
        self.insert(value);
    }
    fn first(&self) -> Id {
        *self.iter().next().unwrap()
    }
}

fn identity(bucket: usize, slot: usize) -> Id {
    Id(uuid::Uuid::from_u128(
        ((bucket as u128) << 64) | slot as u128,
    ))
}

fn run<S: Set>(implementation: &str, entries: usize, cardinality: usize) {
    let buckets = entries / cardinality;
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let start = Instant::now();
    let mut index = OrdMap::new();
    for bucket in 0..buckets {
        let mut set = S::default();
        for i in 0..cardinality {
            set.insert(identity(bucket, i));
        }
        index.insert((7u64, 0u64, bucket as u64), set);
    }
    let build_seconds = start.elapsed().as_secs_f64();
    let live_bytes = LIVE.load(Ordering::Relaxed) - baseline;
    let build_peak_bytes = PEAK.load(Ordering::Relaxed) - baseline;
    let retained = index.clone();
    let start = Instant::now();
    let updates = 10_000.min(buckets);
    for bucket in 0..updates {
        index
            .get_mut(&(7, 0, bucket as u64))
            .unwrap()
            .replace_first(identity(bucket, cardinality));
    }
    let update_seconds = start.elapsed().as_secs_f64();
    let retained_update_bytes = LIVE.load(Ordering::Relaxed) - baseline;
    for (key, set) in &retained {
        assert_eq!(set.first(), identity(key.2 as usize, 0));
    }
    drop(index);
    drop(retained);
    assert_eq!(
        LIVE.load(Ordering::Relaxed),
        baseline,
        "all measured index allocations reclaimed"
    );
    println!(
        "{}",
        serde_json::json!({
            "implementation":implementation,"entries":buckets*cardinality,"buckets":buckets,
            "bucket_cardinality":cardinality,"live_requested_bytes":live_bytes,
            "build_peak_requested_bytes":build_peak_bytes,"build_seconds":build_seconds,
            "updates":updates,"update_seconds":update_seconds,
            "live_bytes_with_old_snapshot_after_updates":retained_update_bytes,
            "old_snapshot_verified":true,"all_allocations_reclaimed":true,
        })
    );
}

fn main() {
    for cardinality in [1, 2, 4, 8, 16, 128] {
        run::<OrdSet<Id>>("imbl_tree", 250_000, cardinality);
        run::<Posting<Id>>("compact_posting", 250_000, cardinality);
    }
}
