//! Matched native overhead probe. Setup/checks/reporting are outside timings.
//! Use the same harness against baseline and current builds; see the report.
use serde_json::{Value as Json, json};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use substrate_liquid::{capability::*, *};
struct Counting;
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        result
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
        if !result.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        result
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(p, layout, size) };
        if !result.is_null() {
            ALLOCATED.fetch_add(size, Ordering::Relaxed);
        }
        result
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
fn measure(mut operation: impl FnMut() -> Result<()>, count: usize) -> Result<Json> {
    for _ in 0..3 {
        operation()?;
    }
    let mut samples = Vec::with_capacity(count);
    let mut allocated = Vec::with_capacity(count);
    for _ in 0..count {
        let bytes = ALLOCATED.load(Ordering::Relaxed);
        let start = Instant::now();
        operation()?;
        let elapsed = start.elapsed().as_secs_f64() * 1e6;
        let bytes = ALLOCATED.load(Ordering::Relaxed) - bytes;
        samples.push(elapsed);
        allocated.push(bytes);
    }
    let mut sorted = samples.clone();
    sorted.sort_by(f64::total_cmp);
    allocated.sort();
    Ok(
        json!({"samples_us":samples,"p50_us":sorted[(count*50).div_ceil(100)-1],
        "p99_us":sorted[(count*99).div_ceil(100)-1],"allocated_bytes_median":allocated[count/2]}),
    )
}
fn writes(durable: bool, width: usize, payload: usize, all: bool, retained: bool) -> Result<Json> {
    let dir = tempfile::tempdir().unwrap();
    let db = if durable {
        Database::open(dir.path().join("db.redb"))?
    } else {
        Database::memory()
    };
    let roles = (0..width)
        .map(|i| Role::new(&format!("r{i}"), DataType::Int))
        .chain(std::iter::once(Role::new("payload", DataType::Bytes)))
        .collect();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "bench",
            1,
            vec![],
            vec![Relation::new("bench.Row", roles).unique(&["r0"])],
        ),
        Visibility::Private,
    )?;
    let values = (0..width)
        .map(|i| Value::Int(i as i64))
        .chain(std::iter::once(Value::Bytes(vec![42; payload])))
        .collect::<Vec<_>>();
    let mut ids = Vec::new();
    for i in 0..64 {
        let mut row = values.clone();
        row[0] = (i as i64).into();
        ids.push(tx.insert("bench.Row", row)?);
    }
    tx.commit()?;
    let old = retained.then(|| db.admin().snapshot());
    let mut step = 0i64;
    let result = measure(
        || {
            step += 1;
            let mut tx = db.admin().transaction();
            let mut row = values.clone();
            row[0] = 0.into();
            row[1] = step.into();
            if all {
                for (i, v) in row.iter_mut().enumerate().take(width).skip(1) {
                    *v = (step + i as i64).into();
                }
            }
            tx.update("bench.Row", ids[0], row)?;
            black_box(tx.commit()?);
            Ok(())
        },
        if durable { 24 } else { 128 },
    )?;
    db.admin().snapshot().verify_indexes()?;
    let final_row = db.admin().snapshot().get("bench.Row", ids[0])?.unwrap();
    assert_eq!(final_row.values[1], Value::Int(step + i64::from(all)));
    if let Some(old) = old {
        assert_eq!(
            old.get("bench.Row", ids[0])?.unwrap().values[1],
            Value::Int(1)
        );
    }
    if durable {
        drop(db);
        let recovered = Database::open(dir.path().join("db.redb"))?;
        recovered.admin().snapshot().verify_indexes()?;
        assert_eq!(
            recovered
                .admin()
                .snapshot()
                .get("bench.Row", ids[0])?
                .unwrap(),
            final_row
        );
    }
    Ok(
        json!({"durable":durable,"roles":width+1,"payload_bytes":payload,"all_fields_changed":all,"retained_snapshot":retained,"result":result}),
    )
}
fn projection() -> Result<Json> {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "projection",
            1,
            vec![],
            vec![Relation::new(
                "projection.Row",
                vec![
                    Role::new("n", DataType::Int),
                    Role::new("blob", DataType::Bytes),
                ],
            )],
        ),
        Visibility::Private,
    )?;
    for i in 0..256 {
        tx.insert(
            "projection.Row",
            vec![i.into(), Value::Bytes(vec![42; 65536])],
        )?;
    }
    tx.grant("reader", "projection.Row", Grant::read_only())?;
    tx.commit()?;
    let snapshot = db.session("reader").snapshot();
    let expected = snapshot
        .table("projection.Row", "r")?
        .project(&["r.id", "r.n"])?;
    assert_eq!(expected.rows().len(), 256);
    let legacy = measure(
        || {
            let table = snapshot
                .table("projection.Row", "r")?
                .project(&["r.id", "r.n"])?;
            black_box(table);
            Ok(())
        },
        24,
    )?;
    let projected = measure(
        || {
            let table = projected(&snapshot)?;
            black_box(table);
            Ok(())
        },
        24,
    )?;
    assert_eq!(expected, self::projected(&snapshot)?);
    let scan = measure(
        || {
            black_box(snapshot.scan("projection.Row")?);
            Ok(())
        },
        24,
    )?;
    Ok(
        json!({"rows":256,"payload_bytes":65536,"legacy_table_project":legacy,"projected_table":projected,"owned_scan":scan,"verified":true}),
    )
}
fn projected(snapshot: &Snapshot) -> Result<Table> {
    snapshot.projected_table("projection.Row", "r", &["r.id", "r.n"])
}
fn contention() -> Result<Json> {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    let relations = (0..32)
        .map(|i| Relation::new(&format!("scopes.R{i}"), vec![Role::new("n", DataType::Int)]))
        .collect::<Vec<_>>();
    let scope = CapabilityScope {
        relations: relations
            .iter()
            .map(|r| RelationCapability {
                relation: r.id,
                grant: Grant::read_only(),
            })
            .collect(),
        schema_namespace: None,
        delegations: 0,
    };
    tx.install_with_visibility(
        Module::new("scopes", 1, vec![], relations),
        Visibility::Private,
    )?;
    for i in 0..32 {
        tx.grant("app", &format!("scopes.R{i}"), Grant::read_only())?;
    }
    let id = tx.insert("scopes.R0", vec![0.into()])?;
    tx.commit()?;
    let cap = db.issue_capability(Id::new(), "app", scope)?;
    let session = db.session("app").with_capability(cap.id)?;
    let snapshots = measure(
        || {
            black_box(session.snapshot());
            Ok(())
        },
        128,
    )?;
    let start = Arc::new(Barrier::new(5));
    let mut writer_samples = Vec::with_capacity(256);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let start = start.clone();
            let session = session.clone();
            scope.spawn(move || {
                start.wait();
                for _ in 0..256 {
                    let snapshot = session.snapshot();
                    assert_eq!(snapshot.scan("scopes.R0").unwrap().len(), 1);
                }
            });
        }
        start.wait();
        for n in 0..256 {
            let now = Instant::now();
            let mut tx = db.admin().transaction();
            tx.update("scopes.R0", id, vec![n.into()]).unwrap();
            tx.commit().unwrap();
            writer_samples.push(now.elapsed().as_secs_f64() * 1e6);
        }
    });
    writer_samples.sort_by(f64::total_cmp);
    Ok(
        json!({"scope_relations":32,"readers":4,"writer_samples_us":writer_samples,
        "writer_p50_us":writer_samples[127],"writer_p99_us":writer_samples[253],"snapshot":snapshots}),
    )
}
fn main() -> Result<()> {
    let mut results = vec![];
    for width in [2, 8, 32] {
        for retained in [false, true] {
            results.push(writes(false, width, 4096, false, retained)?);
        }
    }
    results.push(writes(false, 32, 4096, true, true)?);
    results.push(writes(false, 8, 65536, false, true)?);
    results.push(writes(true, 8, 65536, false, true)?);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"writes":results,"projection":projection()?,"contention":contention()?})
        )
        .unwrap()
    );
    Ok(())
}
