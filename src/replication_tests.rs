use super::*;
use crate::capability::{CapabilityScope, RelationCapability};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
fn setup(path: &Path) -> Database {
    let db = Database::open_with_options(
        path,
        DatabaseOptions {
            checkpoint_policy: None,
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    db
}
fn write(db: &Database, name: &str) {
    let mut tx = db.admin().transaction();
    tx.insert("core.Person", vec![name.into()]).unwrap();
    tx.commit().unwrap();
}
#[test]
fn seed_incremental_queries_ids_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let path = dir.path().join("follower");
    let follower = Replica::create(&path, source.seed()).unwrap();
    assert_eq!(
        follower
            .snapshot_eventual("reader")
            .scan("core.Person")
            .unwrap(),
        db.session("reader").snapshot().scan("core.Person").unwrap()
    );
    assert!(
        follower
            .snapshot_eventual("stranger")
            .scan("core.Person")
            .is_err()
    );
    write(&db, "Grace");
    let bytes = source.next_packet().unwrap().unwrap();
    assert!(
        follower
            .snapshot_at_cursor("reader", source.cursor())
            .is_err()
    );
    assert_eq!(
        follower.apply(&bytes).unwrap(),
        Apply::Applied(source.cursor())
    );
    let rows = follower
        .snapshot_at_cursor("reader", source.cursor())
        .unwrap()
        .sql()
        .unwrap()
        .query("SELECT name FROM core.Person ORDER BY name", &[])
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert!(source.next_packet().unwrap().is_none());
    drop(follower);
    let reopened = Replica::open(&path).unwrap();
    assert_eq!(reopened.cursor().unwrap(), source.cursor());
    assert_eq!(
        reopened
            .snapshot_eventual("reader")
            .scan("core.Person")
            .unwrap()
            .len(),
        2
    );
    assert!(Replica::create(&path, source.seed()).is_err());
}
#[test]
fn duplicates_gaps_divergence_and_wrong_source_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let follower = Replica::create(dir.path().join("follower"), source.seed()).unwrap();
    write(&db, "one");
    let first = source.next_packet().unwrap().unwrap();
    write(&db, "two");
    let second = source.next_packet().unwrap().unwrap();
    assert!(follower.apply(&second).is_err());
    follower.apply(&first).unwrap();
    assert!(matches!(
        follower.apply(&first).unwrap(),
        Apply::Duplicate(_)
    ));
    let parsed = decode_packet(&second).unwrap();
    let mut divergent = parsed.base;
    divergent.digest[0] ^= 1;
    let records = parsed
        .records
        .iter()
        .map(|(r, b)| (*r, b.to_vec()))
        .collect::<Vec<_>>();
    assert!(
        follower
            .apply(&packet(divergent, &records).unwrap().0)
            .is_err()
    );
    let mut wrong = parsed.base;
    wrong.source = Id::new();
    assert!(follower.apply(&packet(wrong, &records).unwrap().0).is_err());
    follower.apply(&second).unwrap();
    assert!(follower.apply(&first).is_err());
}
#[test]
fn packet_validation_is_atomic_even_after_valid_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let follower = Replica::create(dir.path().join("follower"), source.seed()).unwrap();
    let before = follower.cursor().unwrap();
    write(&db, "one");
    write(&db, "two");
    let good = source.next_packet().unwrap().unwrap();
    let parsed = decode_packet(&good).unwrap();
    let mut records = parsed
        .records
        .iter()
        .map(|(r, b)| (*r, b.to_vec()))
        .collect::<Vec<_>>();
    records[1].1[0] ^= 1;
    let invalid = packet(before, &records).unwrap().0;
    assert!(follower.apply(&invalid).is_err());
    assert_eq!(follower.cursor().unwrap(), before);
    assert_eq!(
        follower
            .snapshot_eventual("reader")
            .scan("core.Person")
            .unwrap()
            .len(),
        1
    );
    for offset in [0, 4, 28, 60, 116, good.len() - 1] {
        let mut bad = good.clone();
        bad[offset] ^= 1;
        assert!(follower.apply(&bad).is_err());
    }
    let mut extra = good.clone();
    extra.push(0);
    assert!(follower.apply(&extra).is_err());
    follower.apply(&good).unwrap();
}
#[test]
fn seed_versions_checksums_and_truncation_rejected_before_file_creation() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let source = Source::new(&db).unwrap();
    for len in [0, 1, 20, 67, source.seed().len() - 1] {
        assert!(Replica::create(dir.path().join("bad"), &source.seed()[..len]).is_err());
    }
    let mut bad = source.seed().to_vec();
    bad[3] = b'2';
    let n = bad.len() - 32;
    let hash = digest(&bad[..n]);
    bad[n..].copy_from_slice(&hash);
    assert!(Replica::create(dir.path().join("bad"), &bad).is_err());
    assert!(!dir.path().join("bad").exists());
}
#[test]
fn checkpoint_bounds_recovery_and_missing_source_history() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let path = dir.path().join("follower");
    let follower = Replica::create(&path, source.seed()).unwrap();
    for _ in 0..MAX_PACKET_RECORDS {
        write(&db, "row");
    }
    let packet = source.next_packet().unwrap().unwrap();
    follower.apply(&packet).unwrap();
    assert_eq!(follower.inner.lock().unwrap().tail_records, 0);
    drop(follower);
    let follower = Replica::open(&path).unwrap();
    assert_eq!(follower.cursor().unwrap(), source.cursor());
    assert!(matches!(
        follower.apply(&packet).unwrap(),
        Apply::Duplicate(_)
    ));
    write(&db, "pruned");
    db.checkpoint().unwrap();
    assert!(source.next_packet().is_err());
    let fresh = Source::new(&db).unwrap();
    assert_ne!(fresh.cursor().source, source.cursor().source);
    let replacement = Replica::create(dir.path().join("replacement"), fresh.seed()).unwrap();
    assert_eq!(
        replacement
            .snapshot_eventual("reader")
            .scan("core.Person")
            .unwrap()
            .len(),
        MAX_PACKET_RECORDS + 2
    );
}
#[test]
fn applied_revocation_invalidates_retained_reads_but_lag_is_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let follower = Replica::create(dir.path().join("follower"), source.seed()).unwrap();
    let old = follower.snapshot_eventual("reader");
    let sql = old.sql().unwrap();
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(old.scan("core.Person").is_ok()); // Eventual follower has not applied revocation.
    follower
        .apply(&source.next_packet().unwrap().unwrap())
        .unwrap();
    assert!(old.scan("core.Person").is_err());
    assert!(sql.query("SELECT * FROM core.Person", &[]).is_err());
    assert!(
        follower
            .snapshot_eventual("reader")
            .scan("core.Person")
            .is_err()
    );
}
#[test]
fn replicated_credentials_scope_rotation_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let tid = db.admin().snapshot().schema("core.Person").unwrap().id;
    let cap = Id::new();
    db.issue_capability(
        cap,
        "reader",
        CapabilityScope {
            relations: vec![RelationCapability {
                relation: tid,
                grant: Grant::read_only(),
            }],
            schema_namespace: None,
            delegations: 0,
        },
    )
    .unwrap();
    let expiry = 253402300799;
    let old = db.issue_credential("reader", cap, expiry).unwrap();
    let mut source = Source::new(&db).unwrap();
    let path = dir.path().join("follower");
    let follower = Replica::create(&path, source.seed()).unwrap();
    let retained = follower.authenticate_eventual(&old.token).unwrap();
    assert_eq!(retained.scan("core.Person").unwrap().len(), 1);
    let new = db.rotate_credential(old.id, expiry).unwrap();
    follower
        .apply(&source.next_packet().unwrap().unwrap())
        .unwrap();
    assert!(retained.scan("core.Person").is_err());
    assert!(follower.authenticate_eventual(&old.token).is_err());
    assert!(follower.authenticate_eventual(&new.token).is_ok());
    drop(follower);
    let follower = Replica::open(&path).unwrap();
    assert!(follower.authenticate_eventual(&new.token).is_ok());
    assert!(follower.authenticate_eventual(&old.token).is_err());
}
#[test]
fn crash_child() {
    let Ok(path) = std::env::var("SUBSTRATE_REPLICA_CRASH_PATH") else {
        return;
    };
    let replica = Replica::open(Path::new(&path).join("follower")).unwrap();
    replica
        .apply(&std::fs::read(Path::new(&path).join("packet")).unwrap())
        .unwrap();
}
#[test]
fn process_crash_recovers_atomic_packet_and_cursor_including_checkpoint() {
    for count in [1, MAX_PACKET_RECORDS] {
        for after in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = setup(&dir.path().join("leader"));
            let mut source = Source::new(&db).unwrap();
            let follower = Replica::create(dir.path().join("follower"), source.seed()).unwrap();
            let before = follower.cursor().unwrap();
            drop(follower);
            for _ in 0..count {
                write(&db, "crash");
            }
            let bytes = source.next_packet().unwrap().unwrap();
            std::fs::write(dir.path().join("packet"), bytes).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "replication::tests::crash_child"])
                .env("SUBSTRATE_REPLICA_CRASH_PATH", dir.path())
                .env(
                    "SUBSTRATE_TEST_CRASH_POINT",
                    if after {
                        "replica_after_commit"
                    } else {
                        "replica_before_commit"
                    },
                )
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(91),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let follower = Replica::open(dir.path().join("follower")).unwrap();
            assert_eq!(
                follower.cursor().unwrap(),
                if after { source.cursor() } else { before }
            );
            assert_eq!(
                follower
                    .snapshot_eventual("reader")
                    .scan("core.Person")
                    .unwrap()
                    .len(),
                if after { count + 1 } else { 1 }
            );
        }
    }
}
#[derive(Debug)]
struct Fault {
    inner: redb::backends::FileBackend,
    armed: Arc<AtomicBool>,
}
impl redb::StorageBackend for Fault {
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
            Err(std::io::Error::other("injected follower sync failure"))
        } else {
            self.inner.sync_data()
        }
    }
    fn close(&self) -> std::io::Result<()> {
        self.inner.close()
    }
}
#[test]
fn uncertain_storage_failure_stops_apply_and_denies_retained_reads() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let (state, cursor) = decode_seed(source.seed()).unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("follower"))
        .unwrap();
    let disk = redb::Database::builder()
        .create_with_backend(Fault {
            inner: redb::backends::FileBackend::new(file).unwrap(),
            armed: armed.clone(),
        })
        .unwrap();
    let tx = disk.begin_write().unwrap();
    checkpoint(&tx, source.seed(), cursor).unwrap();
    tx.open_table(CONTROL)
        .unwrap()
        .insert("format", b"LRF1".as_slice())
        .unwrap();
    tx.open_table(LOG).unwrap();
    tx.commit().unwrap();
    let follower = Replica {
        engine: Database::from_state(state.clone(), None),
        inner: Mutex::new(Inner {
            disk,
            state,
            cursor,
            tail_records: 0,
            tail_bytes: 0,
            failed: false,
        }),
    };
    let retained = follower.snapshot_eventual("reader");
    write(&db, "fails");
    let bytes = source.next_packet().unwrap().unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(follower.apply(&bytes).is_err());
    assert!(follower.apply(&bytes).is_err());
    assert!(follower.cursor().is_err());
    assert!(retained.scan("core.Person").is_err());
}

#[test]
fn typed_checks_v2_install_seed_and_invalid_delta_validation() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup(&dir.path().join("leader"));
    let mut source = Source::new(&db).unwrap();
    let follower = Replica::create(dir.path().join("follower"), source.seed()).unwrap();
    let relation = Relation::new("score.Value", vec![Role::new("score", DataType::Int)])
        .check(Check::int_range("score", 0, 10));
    let tid = relation.id;
    let mut tx = db.admin().transaction();
    tx.install(Module::new("score", 1, vec![], vec![relation]))
        .unwrap();
    let id = tx.insert("score.Value", vec![5.into()]).unwrap();
    tx.grant("reader", "score.Value", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let bytes = source.next_packet().unwrap().unwrap();
    assert_eq!(&decode_packet(&bytes).unwrap().records[0].1[..4], b"LQ02");
    follower.apply(&bytes).unwrap();
    assert_eq!(
        follower
            .snapshot_eventual("reader")
            .get("score.Value", id)
            .unwrap()
            .unwrap()
            .values[0],
        Value::Int(5)
    );
    let inner = follower.inner.lock().unwrap();
    let bad = codec::encode(
        inner.cursor.revision + 1,
        &[Op::Put(
            tid,
            Row {
                id,
                values: vec![Value::Int(11)].into_boxed_slice(),
            },
        )],
        &inner.state,
    )
    .unwrap();
    let forged = packet(inner.cursor, &[(inner.cursor.revision + 1, bad)])
        .unwrap()
        .0;
    drop(inner);
    assert!(follower.apply(&forged).is_err());
    let fresh = Source::new(&db).unwrap();
    let seeded = Replica::create(dir.path().join("seeded"), fresh.seed()).unwrap();
    assert_eq!(
        seeded
            .snapshot_eventual("reader")
            .schema("score.Value")
            .unwrap()
            .checks
            .len(),
        1
    );
}
#[test]
fn barrier_requires_exact_digest_and_revision_and_empty_seed_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        dir.path().join("leader"),
        DatabaseOptions {
            checkpoint_policy: None,
        },
    )
    .unwrap();
    let mut source = Source::new(&db).unwrap();
    assert_eq!(source.seed().len(), 68);
    let follower = Replica::create(dir.path().join("follower"), source.seed()).unwrap();
    let mut older = source.cursor();
    older.digest[0] ^= 1;
    assert!(follower.snapshot_at_cursor("reader", older).is_err()); // Equal revision checks digest.
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    follower
        .apply(&source.next_packet().unwrap().unwrap())
        .unwrap();
    // Older barriers cannot prove lineage without retained ancestry.
    assert!(follower.snapshot_at_cursor("reader", older).is_err());
    assert!(
        follower
            .snapshot_at_cursor("reader", source.cursor())
            .is_ok()
    );
    let mut unknown = source.cursor();
    unknown.source = Id::new();
    assert!(follower.snapshot_at_cursor("reader", unknown).is_err());
    assert!(decode_packet(&vec![0; MAX_PACKET_BYTES + 1]).is_err());
    assert!(decode_seed(&vec![0; MAX_SEED_BYTES + 1]).is_err());
}
