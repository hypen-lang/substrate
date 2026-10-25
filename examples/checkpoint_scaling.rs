//! Reproducible incremental checkpoint probe; no timing assertions.
use std::time::Instant;
use substrate_liquid::*;
fn main() {
    for n in [512usize, 1024, 2048] {
        for sample in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let db = Database::open_with_options(
                dir.path().join("db"),
                DatabaseOptions {
                    checkpoint_policy: None,
                },
            )
            .unwrap();
            let mut tx = db.admin().transaction();
            tx.install(demo::core()).unwrap();
            tx.commit().unwrap();
            let start = Instant::now();
            let mut checkpoint_ms = 0.;
            let mut rows_written = 0;
            for i in 1..=n {
                let mut tx = db.admin().transaction();
                tx.insert("core.Person", vec![Value::Text(format!("person-{i}"))])
                    .unwrap();
                tx.commit().unwrap();
                if i % 128 == 0 {
                    let start = Instant::now();
                    let info = db.checkpoint().unwrap();
                    checkpoint_ms += start.elapsed().as_secs_f64() * 1000.;
                    rows_written += info.written_rows;
                }
            }
            let elapsed = start.elapsed().as_secs_f64() * 1000.;
            drop(db);
            let reopened = Database::open(dir.path().join("db")).unwrap();
            assert_eq!(
                reopened
                    .admin()
                    .snapshot()
                    .scan("core.Person")
                    .unwrap()
                    .len(),
                n
            );
            assert_eq!(reopened.recovery_info().unwrap().replayed_commits, 0);
            reopened.admin().snapshot().verify_indexes().unwrap();
            println!(
                "{{\"rows\":{n},\"sample\":{sample},\"total_ms\":{elapsed:.3},\"checkpoint_ms\":{checkpoint_ms:.3},\"checkpoint_row_writes\":{rows_written},\"recovery_verified\":true}}"
            );
        }
    }
}
