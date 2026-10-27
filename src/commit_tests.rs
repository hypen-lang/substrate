use super::*;
use std::sync::{
    Barrier,
    atomic::{AtomicBool, Ordering},
};

fn request(tx: Transaction) -> Arc<Request> {
    assert!(!tx.failed);
    let bytes = crate::codec::encode(0, &tx.ops, &tx.work).unwrap().len();
    Request::new(tx.base, tx.reads, tx.ops, tx.live, bytes)
}
fn enqueue(db: &Database, tx: Transaction) -> Arc<Request> {
    let request = request(tx);
    let mut queue = db.coordinator.queue.lock().unwrap();
    queue.bytes += request.bytes;
    queue.pending.push_back(request.clone());
    request
}
fn result(request: &Request) -> Result<u64> {
    request.result.lock().unwrap().take().unwrap()
}
fn setup(db: &Database) -> Id {
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    let id = tx.insert("core.Person", vec!["initial".into()]).unwrap();
    tx.grant("writer", "core.Person", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    id
}
fn manual() -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: None,
    }
}

#[test]
fn ordered_group_keeps_individual_revisions_conflicts_and_read_fences() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    let id = setup(&db);
    let mut first = db.admin().transaction();
    first
        .update("core.Person", id, vec!["first".into()])
        .unwrap();
    let mut conflict = db.admin().transaction();
    conflict
        .update("core.Person", id, vec!["conflict".into()])
        .unwrap();
    let mut reader = db.admin().transaction();
    reader.get("core.Person", id).unwrap();
    let empty = db.admin().transaction();
    let mut independent = db.admin().transaction();
    independent
        .insert("core.Person", vec!["independent".into()])
        .unwrap();
    let first = enqueue(&db, first);
    let conflict = enqueue(&db, conflict);
    let reader = enqueue(&db, reader);
    let empty = enqueue(&db, empty);
    assert_eq!(independent.commit().unwrap(), 3);
    assert_eq!(result(&first).unwrap(), 2);
    assert!(matches!(result(&conflict), Err(Error::Conflict)));
    assert!(matches!(result(&reader), Err(Error::Conflict)));
    assert_eq!(result(&empty).unwrap(), 2);
    assert_eq!(db.group_commit_stats().durable_groups, 2);
    assert_eq!(db.group_commit_stats().committed_writes, 3);
    assert_eq!(db.storage_info().unwrap().log_commits, 3);
    drop(db);
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 3);
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 2);
    db.admin().snapshot().verify_indexes().unwrap();
}

#[test]
fn earlier_revocation_rejects_later_write_and_read_only_transaction() {
    let db = Database::memory();
    let id = setup(&db);
    let mut revoke = db.admin().transaction();
    revoke.revoke("writer", "core.Person").unwrap();
    let mut write = db.session("writer").transaction();
    write
        .update("core.Person", id, vec!["unauthorized".into()])
        .unwrap();
    let read_only = db.session("writer").transaction();
    let revoke = enqueue(&db, revoke);
    let write = enqueue(&db, write);
    assert!(matches!(
        read_only.commit(),
        Err(Error::Denied) | Err(Error::Conflict)
    ));
    assert_eq!(result(&revoke).unwrap(), 2);
    assert!(matches!(
        result(&write),
        Err(Error::Denied) | Err(Error::Conflict)
    ));
    assert_eq!(
        db.admin()
            .snapshot()
            .get("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        "initial".into()
    );
}

#[test]
fn checkpoint_boundary_covers_every_accepted_revision_in_one_group() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        dir.path().join("db"),
        DatabaseOptions {
            checkpoint_policy: Some(CheckpointPolicy {
                max_log_commits: 3,
                max_log_bytes: u64::MAX,
            }),
        },
    )
    .unwrap();
    setup(&db);
    let mut requests = Vec::new();
    for _ in 0..7 {
        let mut tx = db.admin().transaction();
        tx.insert("core.Person", vec!["group".into()]).unwrap();
        requests.push(enqueue(&db, tx));
    }
    assert_eq!(db.admin().transaction().commit().unwrap(), 8);
    for (i, request) in requests.iter().enumerate() {
        assert_eq!(result(request).unwrap(), i as u64 + 2);
    }
    assert_eq!(db.storage_info().unwrap().checkpoint_revision, 8);
    assert_eq!(db.storage_info().unwrap().log_commits, 0);
    assert_eq!(db.group_commit_stats().durable_groups, 2);
    drop(db);
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 8);
}

#[test]
fn queue_and_group_bounds_are_enforced_without_consuming_revisions() {
    let db = Database::memory_with_commit_options(GroupCommitOptions {
        max_queued_transactions: 2,
        max_group_transactions: 1,
        ..Default::default()
    })
    .unwrap();
    setup(&db);
    let first = enqueue(&db, db.admin().transaction());
    let second = enqueue(&db, db.admin().transaction());
    assert!(matches!(
        db.admin().transaction().commit(),
        Err(Error::Constraint(_))
    ));
    // Release one queue slot, then let the submitter drain the bounded groups.
    {
        let mut q = db.coordinator.queue.lock().unwrap();
        let r = q.pending.pop_back().unwrap();
        q.bytes -= r.bytes;
    }
    assert_eq!(db.admin().transaction().commit().unwrap(), 1);
    assert_eq!(result(&first).unwrap(), 1);
    assert!(second.result.lock().unwrap().is_none());
    let db = Database::memory_with_commit_options(GroupCommitOptions {
        max_group_bytes: 15,
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        db.admin().transaction().commit(),
        Err(Error::Constraint(_))
    ));
    assert_eq!(db.admin().snapshot().revision(), 0);
    assert!(
        Database::memory_with_commit_options(GroupCommitOptions {
            max_group_transactions: 0,
            ..Default::default()
        })
        .is_err()
    );
}

#[test]
fn concurrent_submitters_and_checkpoints_complete_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Database::open_with_commit_options(
        &path,
        manual(),
        GroupCommitOptions {
            collection_window: Duration::from_micros(500),
            max_group_transactions: 8,
            ..Default::default()
        },
    )
    .unwrap();
    setup(&db);
    let barrier = Barrier::new(9);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..20 {
                    let mut tx = db.admin().transaction();
                    tx.insert("core.Person", vec!["concurrent".into()]).unwrap();
                    tx.commit().unwrap();
                }
            });
        }
        barrier.wait();
        for _ in 0..10 {
            db.checkpoint().unwrap();
        }
    });
    let stats = db.group_commit_stats();
    assert_eq!(stats.committed_writes, 161);
    assert!(stats.durable_groups < stats.committed_writes);
    assert!(stats.largest_group <= 8);
    let retained = db.admin().snapshot();
    drop(db);
    let db = Database::open_with_options(path, manual()).unwrap();
    assert_eq!(
        db.admin().snapshot().scan("core.Person").unwrap(),
        retained.scan("core.Person").unwrap()
    );
    db.admin().snapshot().verify_indexes().unwrap();
}

#[test]
fn group_crash_child() {
    let Ok(path) = std::env::var("SUBSTRATE_GROUP_CRASH_PATH") else {
        return;
    };
    let automatic = std::env::var("SUBSTRATE_GROUP_CHECKPOINT").is_ok();
    let options = if automatic {
        DatabaseOptions {
            checkpoint_policy: Some(CheckpointPolicy {
                max_log_commits: 3,
                max_log_bytes: u64::MAX,
            }),
        }
    } else {
        manual()
    };
    let db = Database::open_with_options(path, options).unwrap();
    for _ in 0..4 {
        let mut tx = db.admin().transaction();
        tx.insert("core.Person", vec!["crash-group".into()])
            .unwrap();
        enqueue(&db, tx);
    }
    db.admin().transaction().commit().unwrap();
}

#[test]
fn process_crashes_recover_whole_groups_across_persistence_and_publication() {
    for checkpoint in [false, true] {
        let points = if checkpoint {
            [
                "checkpoint_before_commit",
                "checkpoint_after_commit",
                "group_before_publication",
                "group_after_publication",
            ]
        } else {
            [
                "append_before_commit",
                "append_after_commit",
                "group_before_publication",
                "group_after_publication",
            ]
        };
        for (i, point) in points.into_iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            {
                let db = Database::open_with_options(&path, manual()).unwrap();
                setup(&db);
            }
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .args(["--exact", "engine::commit::tests::group_crash_child"])
                .env("SUBSTRATE_GROUP_CRASH_PATH", &path)
                .env("SUBSTRATE_TEST_CRASH_POINT", point);
            if checkpoint {
                child.env("SUBSTRATE_GROUP_CHECKPOINT", "1");
            }
            let output = child.output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(91),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let db = Database::open_with_options(path, manual()).unwrap();
            assert_eq!(
                db.admin().snapshot().revision(),
                if i == 0 { 1 } else { 5 },
                "{point}"
            );
            assert_eq!(
                db.admin().snapshot().scan("core.Person").unwrap().len(),
                if i == 0 { 1 } else { 5 },
                "{point}"
            );
            db.admin().snapshot().verify_indexes().unwrap();
        }
    }
}

#[derive(Debug)]
struct PausedBackend {
    inner: redb::backends::FileBackend,
    armed: Arc<AtomicBool>,
    gate: Arc<(Mutex<(bool, bool)>, Condvar)>,
    fail: bool,
}
impl redb::StorageBackend for PausedBackend {
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
                return Err(std::io::Error::other("injected group sync failure"));
            }
        }
        self.inner.sync_data()
    }
    fn close(&self) -> std::io::Result<()> {
        self.inner.close()
    }
}

#[test]
fn blocked_group_sync_keeps_readers_running_and_no_acknowledgements_escape() {
    exercise_paused_sync(false, false);
}
#[test]
fn failed_group_sync_fails_all_accepted_requests_and_stops_future_writes() {
    exercise_paused_sync(true, false);
}
#[test]
fn checkpoint_group_sync_keeps_authorized_readers_running_and_fences_acknowledgements() {
    exercise_paused_sync(false, true);
}
fn exercise_paused_sync(fail: bool, checkpoint: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let backend = PausedBackend {
        inner: redb::backends::FileBackend::new(file).unwrap(),
        armed: armed.clone(),
        gate: gate.clone(),
        fail,
    };
    let (durable, state) = Durable::recover(
        redb::Database::builder()
            .create_with_backend(backend)
            .unwrap(),
        if checkpoint {
            DatabaseOptions {
                checkpoint_policy: Some(CheckpointPolicy {
                    max_log_commits: 3,
                    max_log_bytes: u64::MAX,
                }),
            }
        } else {
            manual()
        },
    )
    .unwrap();
    let db = Database::from_state(state, Some(durable));
    setup(&db);
    db.clock.set(100);
    let mut requests = Vec::new();
    for _ in 0..8 {
        let mut tx = db.session("writer").transaction();
        tx.expire_at(Some(200));
        tx.insert("core.Person", vec!["group".into()]).unwrap();
        requests.push(enqueue(&db, tx));
    }
    armed.store(true, Ordering::SeqCst);
    let (sent, received) = std::sync::mpsc::channel();
    let worker_db = db.clone();
    let worker = std::thread::spawn(move || {
        sent.send(worker_db.admin().transaction().commit()).unwrap();
    });
    {
        let (lock, changed) = &*gate;
        let guard = lock.lock().unwrap();
        let (guard, timeout) = changed
            .wait_timeout_while(guard, Duration::from_secs(5), |g| !g.0)
            .unwrap();
        assert!(!timeout.timed_out(), "writer did not reach sync");
        drop(guard);
    }
    // Acceptance precedes the sync: expiry during IO cannot retract a durable commit.
    db.clock.set(200);
    let (read_sent, read_received) = std::sync::mpsc::channel();
    let reader_db = db.clone();
    let reader = std::thread::spawn(move || {
        read_sent
            .send(
                reader_db
                    .session("writer")
                    .snapshot()
                    .scan("core.Person")
                    .unwrap()
                    .len(),
            )
            .unwrap();
    });
    // Time bound is a deadlock detector, not a microbenchmark threshold.
    let read_result = read_received.recv_timeout(Duration::from_secs(2));
    let premature_ack = received.try_recv();
    assert!(requests.iter().all(|r| r.result.lock().unwrap().is_none()));
    {
        let mut state = gate.0.lock().unwrap();
        state.1 = true;
        gate.1.notify_all();
    }
    worker.join().unwrap();
    reader.join().unwrap();
    assert_eq!(read_result.unwrap(), 1);
    assert!(matches!(
        premature_ack,
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    let acknowledgement = received.recv().unwrap();
    if fail {
        assert!(matches!(acknowledgement, Err(Error::Storage(_))));
        assert!(
            requests
                .iter()
                .all(|r| matches!(result(r), Err(Error::Storage(_))))
        );
        assert_eq!(db.admin().snapshot().revision(), 1);
        assert!(matches!(
            db.admin().transaction().commit(),
            Err(Error::Storage(_))
        ));
        assert!(db.checkpoint().is_err());
    } else {
        assert_eq!(acknowledgement.unwrap(), 9);
        for (i, request) in requests.iter().enumerate() {
            assert_eq!(result(request).unwrap(), i as u64 + 2);
        }
        assert_eq!(db.group_commit_stats().durable_groups, 2);
    }
    drop(db);
    let reopened = Database::open_with_options(path, manual()).unwrap();
    let rows = reopened
        .admin()
        .snapshot()
        .scan("core.Person")
        .unwrap()
        .len();
    assert!(rows == 1 || rows == 9);
    if !fail {
        assert_eq!(rows, 9);
    }
    reopened.admin().snapshot().verify_indexes().unwrap();
}

#[test]
fn expired_queued_authority_cannot_commit_or_install_schema() {
    let db = Database::memory();
    setup(&db);
    db.clock.set(100);
    let mut grant = db.admin().transaction();
    grant.grant_schema("writer", "app").unwrap();
    grant.commit().unwrap();
    let mut schema = db.session("writer").transaction();
    schema.expire_at(Some(200));
    schema
        .install(Module::new(
            "app",
            1,
            vec![],
            vec![Relation::new("app.Note", vec![])],
        ))
        .unwrap();
    let request = enqueue(&db, schema);
    db.clock.set(200);
    assert_eq!(db.admin().transaction().commit().unwrap(), 2);
    assert!(matches!(result(&request), Err(Error::Denied)));
    assert!(db.admin().snapshot().schema("app.Note").is_err());
}

#[test]
fn byte_caps_split_groups_and_reject_queue_overload() {
    let mut db = Database::memory();
    setup(&db);
    let make = || {
        let mut tx = db.admin().transaction();
        tx.insert("core.Person", vec!["same-size".into()]).unwrap();
        tx
    };
    let a = request(make());
    let b = request(make());
    let c = make();
    // Drop Transaction's Database clone before mutating test-only options.
    let c = request(c);
    let coordinator = Arc::get_mut(&mut db.coordinator).unwrap();
    coordinator.options.max_group_bytes = a.bytes;
    coordinator.options.max_queued_bytes = a.bytes * 2;
    {
        let mut queue = coordinator.queue.lock().unwrap();
        queue.bytes = a.bytes + b.bytes;
        queue.pending.extend([a.clone(), b.clone()]);
    }
    assert!(matches!(
        db.coordinator.submit(&db.head, c.clone()),
        Err(Error::Constraint(_))
    ));
    {
        let mut queue = db.coordinator.queue.lock().unwrap();
        let removed = queue.pending.pop_back().unwrap();
        queue.bytes -= removed.bytes;
    }
    assert_eq!(db.coordinator.submit(&db.head, c).unwrap(), 3);
    assert_eq!(result(&a).unwrap(), 2);
    assert_eq!(db.group_commit_stats().largest_group, 1);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn grouped_updates_match_ordered_reference_model(targets in proptest::collection::vec(0usize..8, 1..40)) {
        let db = Database::memory();
        let first = setup(&db);
        let mut seed = db.admin().transaction();
        let mut ids = vec![first];
        for _ in 1..8 { ids.push(seed.insert("core.Person", vec!["initial".into()]).unwrap()); }
        seed.commit().unwrap();
        let mut expected = vec![Value::from("initial"); 8];
        let mut seen = BTreeSet::new();
        let mut requests = Vec::new();
        for (i, target) in targets.iter().enumerate() {
            let mut tx = db.admin().transaction();
            let value = Value::Text(format!("value-{i}"));
            tx.update("core.Person", ids[*target], vec![value.clone()]).unwrap();
            let accepted = seen.insert(*target);
            if accepted { expected[*target] = value; }
            requests.push((enqueue(&db, tx), accepted));
        }
        proptest::prop_assert_eq!(db.admin().transaction().commit().unwrap(), 2 + seen.len() as u64);
        let mut revision = 2;
        for (request, accepted) in requests {
            if accepted { revision += 1; proptest::prop_assert_eq!(result(&request).unwrap(), revision); }
            else { proptest::prop_assert!(matches!(result(&request), Err(Error::Conflict))); }
        }
        let snapshot = db.admin().snapshot();
        for (id, value) in ids.iter().zip(expected) {
            proptest::prop_assert_eq!(snapshot.get("core.Person", *id).unwrap().unwrap().values[0].clone(), value.clone());
            proptest::prop_assert!(snapshot.lookup("core.Person", "name", &value).unwrap().iter().any(|row| row.id == *id));
        }
        snapshot.verify_indexes().unwrap();
    }
}

#[test]
fn collection_window_does_not_delay_an_isolated_writer() {
    let db = Database::memory_with_commit_options(GroupCommitOptions {
        collection_window: Duration::from_millis(100),
        ..GroupCommitOptions::default()
    })
    .unwrap();
    let id = setup(&db);
    for i in 0..5 {
        let mut tx = db.admin().transaction();
        tx.update("core.Person", id, vec![Value::Text(format!("v{i}"))])
            .unwrap();
        tx.commit().unwrap();
    }
    // Verify the decision directly, not a scheduler-sensitive elapsed threshold.
    assert_eq!(db.group_commit_stats().collection_windows, 0);
    assert_eq!(db.group_commit_stats().largest_group, 1);
}

fn wait_for_pending(db: &Database, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        {
            let queue = db.coordinator.queue.lock().unwrap();
            if queue.active && queue.pending.len() == count {
                return;
            }
        }
        assert!(Instant::now() < deadline, "submitter did not reach queue");
        std::thread::yield_now();
    }
}

#[test]
fn collection_wakes_at_count_or_byte_fill_without_per_arrival_broadcasts() {
    // Check exact count fill, exact byte fill, and crossing the byte limit.
    for mode in 0..3 {
        let mut db = Database::memory();
        setup(&db);
        let requests: Vec<_> = (0..3)
            .map(|_| {
                let mut tx = db.admin().transaction();
                tx.insert("core.Person", vec!["equal-size".into()]).unwrap();
                request(tx)
            })
            .collect();
        let bytes = requests[0].bytes;
        let coordinator = Arc::get_mut(&mut db.coordinator).unwrap();
        coordinator.options.collection_window = Duration::from_secs(1);
        coordinator.options.max_group_transactions = if mode == 0 { 3 } else { 100 };
        coordinator.options.max_group_bytes = match mode {
            1 => bytes * 3,
            2 => bytes * 3 - 1,
            _ => usize::MAX,
        };
        coordinator.queue.get_mut().unwrap().previous_group_size = 2;
        let (done, received) = std::sync::mpsc::channel();
        let mut threads = Vec::new();
        for (i, request) in requests.into_iter().enumerate() {
            let db_clone = db.clone();
            let done = done.clone();
            threads.push(std::thread::spawn(move || {
                let result = db_clone.coordinator.submit(&db_clone.head, request);
                done.send((i, result)).unwrap();
            }));
            if i < 2 {
                wait_for_pending(&db, i + 1);
                assert_eq!(
                    db.coordinator.queue.lock().unwrap().enqueue_notifications,
                    0
                );
            }
        }
        // Filling the queue must wake the collector before its one-second
        // deadline. With an overflowing last request only the first two fit.
        let expected_immediate = if mode == 2 { 2 } else { 3 };
        let mut results = Vec::new();
        for _ in 0..expected_immediate {
            results.push(received.recv_timeout(Duration::from_millis(500)).unwrap());
        }
        while results.len() < 3 {
            results.push(received.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        for thread in threads {
            thread.join().unwrap();
        }
        results.sort_by_key(|(i, _)| *i);
        assert_eq!(
            results
                .into_iter()
                .map(|(_, r)| r.unwrap())
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            db.coordinator.queue.lock().unwrap().enqueue_notifications,
            1
        );
        db.admin().snapshot().verify_indexes().unwrap();
    }
}

#[test]
fn partial_collection_completes_on_deadline_without_an_enqueue_notification() {
    let mut db = Database::memory();
    let id = setup(&db);
    let coordinator = Arc::get_mut(&mut db.coordinator).unwrap();
    coordinator.options.collection_window = Duration::from_millis(20);
    coordinator.queue.get_mut().unwrap().previous_group_size = 2;
    let (done, received) = std::sync::mpsc::channel();
    let worker = db.clone();
    let thread = std::thread::spawn(move || {
        let mut tx = worker.admin().transaction();
        tx.update("core.Person", id, vec!["after-deadline".into()])
            .unwrap();
        done.send(tx.commit()).unwrap();
    });
    assert_eq!(
        received
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap(),
        2
    );
    thread.join().unwrap();
    let queue = db.coordinator.queue.lock().unwrap();
    assert_eq!(queue.enqueue_notifications, 0);
    assert_eq!(queue.stats.collection_windows, 1);
}
