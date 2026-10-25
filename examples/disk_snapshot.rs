//! cargo run --release --offline --example disk_snapshot -- 256 65536
//! Counts live Rust allocator bytes (not RSS or OS file-cache memory).
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
use substrate_liquid::{disk_snapshot::*, *};
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(p, layout, size) };
        if !next.is_null() {
            if size >= layout.size() {
                LIVE.fetch_add(size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        next
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
enum ReadLease {
    Pinned(Snapshot),
    Disk(DiskSnapshot),
}
impl ReadLease {
    fn get(&self, id: Id) -> Result<Option<Row>> {
        match self {
            Self::Pinned(s) => s.get("app.Payload", id),
            Self::Disk(s) => s.get("app.Payload", id),
        }
    }
}
fn run(spilled: bool, count: usize, payload: usize) -> Result<serde_json::Value> {
    let base = LIVE.load(Ordering::Relaxed);
    let directory = tempfile::tempdir().unwrap();
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "app",
        1,
        vec![],
        vec![Relation::new(
            "app.Payload",
            vec![
                Role::new("bucket", DataType::Int),
                Role::new("data", DataType::Bytes),
            ],
        )],
    ))?;
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        ids.push(tx.insert(
            "app.Payload",
            vec![(i as i64 % 4).into(), Value::Bytes(vec![i as u8; payload])],
        )?);
    }
    tx.commit()?;
    let before = LIVE.load(Ordering::Relaxed).saturating_sub(base);
    let started = Instant::now();
    let lease = if spilled {
        ReadLease::Disk(db.admin().snapshot().spill(
            directory.path().join("snapshot.redb"),
            &["app.Payload"],
            SpillOptions {
                cache_bytes: 1024 * 1024,
                max_row_bytes: payload + 1024,
                max_page_bytes: (payload + 1024).max(1024 * 1024),
            },
        )?)
    } else {
        ReadLease::Pinned(db.admin().snapshot())
    };
    let build_ms = started.elapsed().as_secs_f64() * 1000.;
    // Current engine releases the rows; only the historical read lease needs them.
    let mut tx = db.admin().transaction();
    for id in &ids {
        tx.delete("app.Payload", *id)?;
    }
    tx.commit()?;
    assert!(db.admin().snapshot().scan("app.Payload")?.is_empty());
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(base);
    let mut latency = Vec::new();
    for id in ids.iter().cycle().take(200) {
        let begin = Instant::now();
        let row = lease.get(*id)?.unwrap();
        latency.push(begin.elapsed().as_secs_f64() * 1e6);
        assert_eq!(
            row.values[1],
            Value::Bytes(vec![
                ids.iter().position(|v| v == id).unwrap() as u8;
                payload
            ])
        );
    }
    latency.sort_by(f64::total_cmp);
    if let ReadLease::Disk(disk) = &lease {
        disk.verify()?;
    }
    let before_lease_drop = LIVE.load(Ordering::Relaxed);
    drop(lease);
    let after_lease_drop = LIVE.load(Ordering::Relaxed);
    let lease_exclusive = before_lease_drop.saturating_sub(after_lease_drop);
    let file_bytes = if spilled {
        std::fs::metadata(directory.path().join("snapshot.redb"))
            .unwrap()
            .len()
    } else {
        0
    };
    Ok(
        serde_json::json!({"mode": if spilled {"disk_lease"} else {"pinned_memory"}, "rows":count,"payload_bytes_per_row":payload,"source_live_allocator_bytes":before,"retained_live_allocator_bytes_after_source_deletion":retained,"lease_exclusive_live_allocator_bytes":lease_exclusive,"build_ms":build_ms,"point_p50_us":latency[100],"point_p99_us":latency[198],"spill_file_bytes":file_bytes,"redb_cache_bytes":if spilled {1024*1024} else {0},"memory_metric":"live Rust allocator requested bytes; excludes RSS and OS file cache"}),
    )
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let count = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(256);
    let payload = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(65536);
    assert!(count > 0 && count <= 10000 && payload <= 1024 * 1024);
    println!("{}", run(false, count, payload)?);
    println!("{}", run(true, count, payload)?);
    Ok(())
}
