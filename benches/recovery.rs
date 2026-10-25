//! Same-file, same-state recovery comparison; no speed threshold assertions.
use std::{path::Path, time::Instant};
use substrate_liquid::*;
const ROWS: usize = 1000;
const UPDATES: usize = 100;
const SAMPLES: usize = 7;
fn manual() -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: None,
    }
}
fn update(db: &Database, ids: &[Id], commit: usize) {
    let mut tx = db.admin().transaction();
    for j in 0..UPDATES {
        let i = (commit * UPDATES + j) % ids.len();
        tx.update(
            "bench.Item",
            ids[i],
            vec![(commit as i64).into(), Value::Text(format!("item-{i}"))],
        )
        .unwrap();
    }
    tx.commit().unwrap();
}
fn measure(path: &Path, expected: &[Row], label: &str) -> f64 {
    let mut times = Vec::new();
    let mut info = None;
    for iteration in 0..=SAMPLES {
        let start = Instant::now();
        let db = Database::open_with_options(path, manual()).unwrap();
        let elapsed = start.elapsed().as_secs_f64() * 1000.;
        assert_eq!(db.admin().snapshot().scan("bench.Item").unwrap(), expected);
        db.admin().snapshot().verify_indexes().unwrap();
        info = Some(db.recovery_info().unwrap());
        if iteration > 0 {
            times.push(elapsed);
        }
    }
    times.sort_by(f64::total_cmp);
    let info = info.unwrap();
    println!(
        "{label},median_ms={:.3},min_ms={:.3},max_ms={:.3},checkpoint_rows={},replayed_commits={},replayed_bytes={}",
        times[SAMPLES / 2],
        times[0],
        times[SAMPLES - 1],
        info.checkpoint_rows,
        info.replayed_commits,
        info.replayed_bytes
    );
    times[SAMPLES / 2]
}
fn main() {
    println!(
        "rows={ROWS},updates_per_commit={UPDATES},samples={SAMPLES},warm_cache=true,profile=release,automatic_checkpointing=disabled_for_baseline"
    );
    for commits in [100, 500] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Database::open_with_options(&path, manual()).unwrap();
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "bench",
            1,
            vec![],
            vec![
                Relation::new(
                    "bench.Item",
                    vec![
                        Role::new("value", DataType::Int),
                        Role::new("label", DataType::Text),
                    ],
                )
                .unique(&["label"]),
            ],
        ))
        .unwrap();
        tx.commit().unwrap();
        let mut tx = db.admin().transaction();
        let ids = (0..ROWS)
            .map(|i| {
                tx.insert(
                    "bench.Item",
                    vec![0.into(), Value::Text(format!("item-{i}"))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        tx.commit().unwrap();
        for i in 0..commits {
            update(&db, &ids, i);
        }
        let expected = db.admin().snapshot().scan("bench.Item").unwrap();
        drop(db);
        let baseline = measure(&path, &expected, &format!("history_{commits}_full_replay"));
        let db = Database::open_with_options(&path, manual()).unwrap();
        let start = Instant::now();
        let cp = db.checkpoint().unwrap();
        let cost = start.elapsed().as_secs_f64() * 1000.;
        drop(db);
        let checkpoint = measure(&path, &expected, &format!("history_{commits}_checkpoint"));
        println!(
            "history={commits},speedup={:.2}x,checkpoint_ms={cost:.3},checkpoint_bytes={},pruned_commits={},pruned_bytes={},file_bytes_after_checkpoint={}",
            baseline / checkpoint,
            cp.bytes,
            cp.pruned_commits,
            cp.pruned_bytes,
            std::fs::metadata(&path).unwrap().len()
        );
        let db = Database::open_with_options(&path, manual()).unwrap();
        for i in commits..commits + 10 {
            update(&db, &ids, i);
        }
        let expected = db.admin().snapshot().scan("bench.Item").unwrap();
        drop(db);
        measure(
            &path,
            &expected,
            &format!("history_{commits}_checkpoint_plus_10"),
        );
    }
    println!(
        "Measures opening and rebuilding complete current state/indexes. Excludes verification and closing. Checkpoint is synchronous; pruning permits reuse, not guaranteed file shrink. OS cache is not flushed; this is not power-loss testing."
    );
}
