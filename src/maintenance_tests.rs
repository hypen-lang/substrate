use super::*;
use std::sync::atomic::AtomicBool;

fn options() -> CheckpointerOptions {
    CheckpointerOptions {
        interval: Duration::from_millis(2),
        early_policy: CheckpointPolicy {
            max_log_commits: u64::MAX,
            max_log_bytes: u64::MAX,
        },
    }
}
fn manual() -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: None,
    }
}
fn setup(db: &Database) -> Id {
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    let id = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    id
}
#[test]
fn coalesced_requests_cover_current_head_and_noop_without_rewriting() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    setup(&db);
    let checkpointer = db.start_checkpointer(options()).unwrap();
    assert!(db.start_checkpointer(options()).is_err());
    let writer = db.coordinator.writer.lock().unwrap();
    let mut last = 0;
    for _ in 0..1024 {
        last = checkpointer.request().unwrap();
    }
    assert_eq!(checkpointer.stats().completed_ticket, 0);
    assert_eq!(checkpointer.stats().coalesced_requests, 1023);
    assert!(checkpointer.wait(last, Duration::from_millis(5)).is_err());
    drop(writer);
    let receipt = checkpointer.wait(last, Duration::from_secs(5)).unwrap();
    assert_eq!(receipt.revision, 1);
    assert_eq!(receipt.completed_ticket, 1024);
    assert_eq!(checkpointer.stats().checkpoints, 1);
    assert_eq!(db.storage_info().unwrap().log_commits, 0);
    let again = checkpointer.request().unwrap();
    assert_eq!(
        checkpointer
            .wait(again, Duration::from_secs(5))
            .unwrap()
            .revision,
        1
    );
    assert_eq!(checkpointer.stats().checkpoints, 1);
    assert!(checkpointer.wait(0, Duration::ZERO).is_err());
    assert!(checkpointer.wait(again + 1, Duration::ZERO).is_err());
    assert!(checkpointer.stop().stopped);
    let replacement = db.start_checkpointer(options()).unwrap();
    replacement.stop();
}
#[test]
fn idle_handle_does_not_retain_database_file_and_options_are_validated() {
    assert!(Database::memory().start_checkpointer(options()).is_err());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Database::open_with_options(&path, manual()).unwrap();
    setup(&db);
    assert!(
        db.start_checkpointer(CheckpointerOptions {
            interval: Duration::ZERO,
            ..options()
        })
        .is_err()
    );
    let checkpointer = db.start_checkpointer(options()).unwrap();
    drop(db);
    let end = Instant::now() + Duration::from_secs(5);
    while !checkpointer.stats().stopped {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(2));
    }
    assert!(checkpointer.request().is_err());
    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        1
    );
    // The original maintenance handle is deliberately still alive here.
    checkpointer.stop();
}
#[test]
fn background_and_manual_checkpoints_preserve_concurrent_commits_and_hard_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Database::open_with_options(
        &path,
        DatabaseOptions {
            checkpoint_policy: Some(CheckpointPolicy {
                max_log_commits: 7,
                max_log_bytes: 8 * 1024 * 1024,
            }),
        },
    )
    .unwrap();
    setup(&db);
    let pinned = db.session("reader").snapshot();
    let checkpointer = db
        .start_checkpointer(CheckpointerOptions {
            early_policy: CheckpointPolicy {
                max_log_commits: 3,
                max_log_bytes: 4 * 1024 * 1024,
            },
            ..options()
        })
        .unwrap();
    thread::scope(|scope| {
        for _ in 0..4 {
            let db = &db;
            scope.spawn(move || {
                for _ in 0..20 {
                    let mut tx = db.admin().transaction();
                    tx.insert("core.Person", vec!["new".into()]).unwrap();
                    tx.commit().unwrap();
                    assert!(db.storage_info().unwrap().log_commits < 7);
                }
            });
        }
        scope.spawn(|| {
            for _ in 0..10 {
                db.checkpoint().unwrap();
            }
        });
    });
    assert_eq!(pinned.scan("core.Person").unwrap().len(), 1);
    assert_eq!(
        db.session("reader")
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        81
    );
    let ticket = checkpointer.request().unwrap();
    assert_eq!(
        checkpointer
            .wait(ticket, Duration::from_secs(5))
            .unwrap()
            .revision,
        81
    );
    checkpointer.stop();
    drop(pinned);
    drop(db);
    let recovered = Database::open(&path).unwrap();
    assert_eq!(recovered.recovery_info().unwrap().replayed_commits, 0);
    assert_eq!(
        recovered
            .admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        81
    );
    recovered.admin().snapshot().verify_indexes().unwrap();
}

#[derive(Debug)]
struct GatedBackend {
    inner: redb::backends::FileBackend,
    armed: Arc<AtomicBool>,
    gate: Arc<(Mutex<(bool, bool)>, Condvar)>,
    fail: bool,
}
impl redb::StorageBackend for GatedBackend {
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.inner.read(offset, out)
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, data)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            let (lock, changed) = &*self.gate;
            let mut gate = lock.lock().unwrap();
            gate.0 = true;
            changed.notify_all();
            while !gate.1 {
                gate = changed.wait(gate).unwrap();
            }
            if self.fail {
                return Err(std::io::Error::other("injected checkpoint sync failure"));
            }
        }
        self.inner.sync_data()
    }
    fn close(&self) -> std::io::Result<()> {
        self.inner.close()
    }
}
fn gated_checkpoint(fail: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let armed = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let (durable, state) = Durable::recover(
        redb::Database::builder()
            .create_with_backend(GatedBackend {
                inner: redb::backends::FileBackend::new(file).unwrap(),
                armed: armed.clone(),
                gate: gate.clone(),
                fail,
            })
            .unwrap(),
        manual(),
    )
    .unwrap();
    let db = Database::from_state(state, Some(durable));
    let id = setup(&db);
    let reader = db.session("reader").snapshot();
    let checkpointer = db.start_checkpointer(options()).unwrap();
    armed.store(true, Ordering::SeqCst);
    let ticket = checkpointer.request().unwrap();
    {
        let (lock, changed) = &*gate;
        let (guard, timeout) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |g| !g.0)
            .unwrap();
        assert!(!timeout.timed_out() && guard.0);
    }
    // These would deadlock if maintenance held the published-state mutex at IO.
    assert_eq!(
        reader.get("core.Person", id).unwrap().unwrap().values[0],
        "Ada".into()
    );
    assert_eq!(db.session("reader").snapshot().revision(), 1);
    assert!(checkpointer.wait(ticket, Duration::from_millis(5)).is_err());
    {
        let (lock, changed) = &*gate;
        lock.lock().unwrap().1 = true;
        changed.notify_all();
    }
    let receipt = checkpointer.wait(ticket, Duration::from_secs(5));
    if fail {
        assert!(receipt.is_err());
        assert!(reader.get("core.Person", id).is_err());
        assert!(db.admin().transaction().commit().is_err());
        assert!(checkpointer.request().is_err());
    } else {
        assert_eq!(receipt.unwrap().revision, 1);
    }
    checkpointer.stop();
    drop(reader);
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn blocked_background_sync_keeps_authorized_readers_running_and_receipt_waiting() {
    gated_checkpoint(false);
}
#[test]
fn failed_background_sync_stops_writes_until_recovery() {
    gated_checkpoint(true);
}

#[test]
fn background_crash_child() {
    let Ok(path) = std::env::var("SUBSTRATE_BACKGROUND_CRASH_PATH") else {
        return;
    };
    let db = Database::open_with_options(path, manual()).unwrap();
    let checkpointer = db.start_checkpointer(options()).unwrap();
    let ticket = checkpointer.request().unwrap();
    checkpointer.wait(ticket, Duration::from_secs(5)).unwrap();
}
#[test]
fn process_crashes_during_background_checkpoint_recover_same_durable_head() {
    for point in [
        "checkpoint_before_write",
        "checkpoint_after_prune",
        "checkpoint_before_commit",
        "checkpoint_after_commit",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = Database::open_with_options(&path, manual()).unwrap();
            setup(&db);
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::maintenance::tests::background_crash_child",
            ])
            .env("SUBSTRATE_BACKGROUND_CRASH_PATH", &path)
            .env("SUBSTRATE_TEST_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(91),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let db = Database::open_with_options(path, manual()).unwrap();
        assert_eq!(db.admin().snapshot().revision(), 1);
        assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 1);
        assert_eq!(
            db.recovery_info().unwrap().replayed_commits,
            u64::from(point != "checkpoint_after_commit")
        );
        db.admin().snapshot().verify_indexes().unwrap();
    }
}

#[test]
fn periodic_early_threshold_runs_without_an_explicit_request() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    setup(&db);
    let checkpointer = db
        .start_checkpointer(CheckpointerOptions {
            early_policy: CheckpointPolicy {
                max_log_commits: 1,
                max_log_bytes: u64::MAX,
            },
            ..options()
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while checkpointer.stats().checkpoints == 0 {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(checkpointer.stats().requests, 0);
    assert_eq!(db.storage_info().unwrap().log_commits, 0);
    assert_eq!(db.admin().snapshot().revision(), 1);
    checkpointer.stop();
}

#[test]
fn stop_cancels_unstarted_requests_without_waiting_on_busy_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    setup(&db);
    let checkpointer = db.start_checkpointer(options()).unwrap();
    let writer = db.coordinator.writer.lock().unwrap();
    checkpointer.request().unwrap();
    // Stop cannot acquire or wait for this held lock; it joins the try-lock worker.
    let stats = checkpointer.stop();
    assert!(stats.stopped);
    assert_eq!(stats.completed_ticket, 0);
    drop(writer);
    assert_eq!(db.storage_info().unwrap().log_commits, 1);
    assert_eq!(db.admin().transaction().commit().unwrap(), 1);
}
