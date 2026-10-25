use super::*;
use redb::StorageBackend;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

fn manual() -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: None,
    }
}
fn fixture(path: &Path, checkpoint: bool) {
    let db = Database::open_with_options(path, manual()).unwrap();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction();
    for _ in 0..50 {
        tx.insert("core.Person", vec!["initial".into()]).unwrap();
    }
    tx.commit().unwrap();
    if checkpoint {
        db.checkpoint().unwrap();
    }
    let mut tx = db.admin().transaction();
    for row in db.admin().snapshot().scan("core.Person").unwrap() {
        tx.update("core.Person", row.id, vec!["before".into()])
            .unwrap();
    }
    tx.commit().unwrap();
}

#[derive(Debug)]
struct FaultBackend {
    inner: redb::backends::FileBackend,
    armed: Arc<AtomicBool>,
    calls: AtomicUsize,
    kind: String,
    nth: usize,
}
impl FaultBackend {
    fn hit(&self, kind: &str) -> bool {
        self.armed.load(Ordering::SeqCst)
            && self.kind == kind
            && self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.nth
    }
}
impl StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()?;
        if self.hit("sync") {
            std::process::exit(92);
        }
        if self.hit("sync_error") {
            return Err(io::Error::other("injected sync failure"));
        }
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.hit("torn_write") {
            self.inner.write(offset, &data[..data.len() / 2])?;
            self.inner.sync_data()?;
            std::process::exit(92);
        }
        if self.hit("write_error") {
            return Err(io::Error::other("injected write failure"));
        }
        self.inner.write(offset, data)?;
        if self.hit("write") {
            std::process::exit(92);
        }
        Ok(())
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}
#[test]
fn crash_child() {
    let Ok(path) = std::env::var("SUBSTRATE_TEST_CRASH_PATH") else {
        return;
    };
    let mode = std::env::var("SUBSTRATE_TEST_CRASH_MODE").unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (mut durable, state) = if let Ok(kind) = std::env::var("SUBSTRATE_TEST_IO_FAULT") {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let backend = FaultBackend {
            inner: redb::backends::FileBackend::new(file).unwrap(),
            armed: armed.clone(),
            calls: AtomicUsize::new(0),
            kind,
            nth: std::env::var("SUBSTRATE_TEST_IO_NTH")
                .unwrap()
                .parse()
                .unwrap(),
        };
        Durable::recover(
            redb::Database::builder()
                .create_with_backend(backend)
                .unwrap(),
            manual(),
        )
        .unwrap()
    } else {
        Durable::open(Path::new(&path), manual()).unwrap()
    };
    if mode == "manual" {
        durable.checkpoint(&state).unwrap();
        return;
    }
    let mut ops = Vec::new();
    let tid = state.names["core.Person"];
    for row in state.families[&tid].values() {
        ops.push(Op::Put(
            tid,
            Row {
                id: row.id,
                values: vec!["after".into()].into_boxed_slice(),
            },
        ));
    }
    let def = Relation::new("extra.Flag", vec![]);
    let new_tid = def.id;
    let id = Id::new();
    ops.push(Op::Install(Module::new("extra", 1, vec![], vec![def])));
    ops.push(Op::Identity(id));
    ops.push(Op::Put(
        new_tid,
        Row {
            id,
            values: Box::new([]),
        },
    ));
    ops.push(Op::Grant("reader".into(), tid, Some(Grant::read_only())));
    let mut next = state.clone();
    next.revision += 1;
    for op in &ops {
        next.apply(op, next.revision);
    }
    if mode == "auto" {
        durable.options.checkpoint_policy = Some(CheckpointPolicy {
            max_log_commits: 1,
            max_log_bytes: u64::MAX,
        });
    }
    armed.store(true, Ordering::SeqCst);
    let result = durable.append(next.revision, &ops, &next);
    if result.is_ok() {
        println!("ACKNOWLEDGED_REVISION_4");
    }
    if std::env::var("SUBSTRATE_TEST_IO_FAULT").is_ok_and(|s| s.ends_with("_error")) {
        assert!(result.is_err(), "fault must be reached");
        std::process::exit(93);
    }
    result.unwrap();
}
fn child(
    path: &Path,
    mode: &str,
    point: Option<&str>,
    io: Option<(&str, usize)>,
) -> std::process::Output {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "durable::tests::crash_child", "--nocapture"])
        .env("SUBSTRATE_TEST_CRASH_PATH", path)
        .env("SUBSTRATE_TEST_CRASH_MODE", mode);
    if let Some(point) = point {
        command.env("SUBSTRATE_TEST_CRASH_POINT", point);
    }
    if let Some((kind, nth)) = io {
        command
            .env("SUBSTRATE_TEST_IO_FAULT", kind)
            .env("SUBSTRATE_TEST_IO_NTH", nth.to_string());
    }
    command.output().unwrap()
}
fn assert_atomic(path: &Path, expected: Option<bool>) {
    let db = Database::open_with_options(path, manual()).unwrap();
    let snapshot = db.admin().snapshot();
    let revision = snapshot.revision();
    assert!(
        revision == 3 || revision == 4,
        "unexpected revision {revision}"
    );
    let after = revision == 4;
    if let Some(expected) = expected {
        assert_eq!(after, expected);
    }
    for row in snapshot.scan("core.Person").unwrap() {
        assert_eq!(
            row.values[0],
            Value::from(if after { "after" } else { "before" })
        );
    }
    assert_eq!(snapshot.schema("extra.Flag").is_ok(), after);
    assert_eq!(
        db.session("reader").snapshot().scan("core.Person").is_ok(),
        after
    );
    if after {
        assert_eq!(snapshot.scan("extra.Flag").unwrap().len(), 1);
    }
    snapshot.verify_indexes().unwrap();
}
fn checkpoint_crash(point: &str) {
    // Each logical boundary is exercised for first checkpoint, replacement, and
    // automatic checkpoint including a not-yet-acknowledged new data commit.
    for (has_checkpoint, mode) in [(false, "manual"), (true, "manual"), (true, "auto")] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        fixture(&path, has_checkpoint);
        let output = child(&path, mode, Some(point), None);
        assert_eq!(
            output.status.code(),
            Some(91),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let published = point == "checkpoint_after_commit";
        assert_atomic(&path, Some(mode == "auto" && published));
        let db = Database::open_with_options(&path, manual()).unwrap();
        let cp = db.recovery_info().unwrap().checkpoint_revision;
        assert_eq!(
            cp,
            if published {
                if mode == "auto" { 4 } else { 3 }
            } else if has_checkpoint {
                2
            } else {
                0
            }
        );
    }
}
macro_rules! crash_tests {($($name:ident => $point:literal),* $(,)?)=>{$(#[test] fn $name(){checkpoint_crash($point);})*};}
crash_tests! {
    crash_before_checkpoint => "checkpoint_before_write",
    crash_after_checkpoint_clear => "checkpoint_after_clear",
    crash_after_checkpoint_catalog => "checkpoint_after_catalog",
    crash_during_checkpoint_rows => "checkpoint_during_rows",
    crash_after_checkpoint_manifest => "checkpoint_after_manifest",
    crash_after_checkpoint_prune => "checkpoint_after_prune",
    crash_before_checkpoint_commit => "checkpoint_before_commit",
    crash_after_checkpoint_commit => "checkpoint_after_commit",
}
#[test]
fn redo_append_publication_is_atomic() {
    for (point, published) in [
        ("append_before_commit", false),
        ("append_after_commit", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        fixture(&path, true);
        assert_eq!(
            child(&path, "append", Some(point), None).status.code(),
            Some(91)
        );
        assert_atomic(&path, Some(published));
    }
}
fn io_crash(kind: &str) {
    // Only the first sync is on the commit path; later syncs can be cleanup.
    for nth in 1..=if kind == "sync_error" { 1 } else { 2 } {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        fixture(&path, true);
        let output = child(&path, "auto", None, Some((kind, nth)));
        let expected = if kind.ends_with("_error") { 93 } else { 92 };
        assert_eq!(
            output.status.code(),
            Some(expected),
            "kind={kind} nth={nth}\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        // A failure inside redb's commit can leave either complete revision, never
        // a mixed state. A successful acknowledgement was never returned.
        let acknowledged =
            String::from_utf8_lossy(&output.stdout).contains("ACKNOWLEDGED_REVISION_4");
        assert_atomic(&path, acknowledged.then_some(true));
    }
}
#[test]
fn crash_inside_redb_sync() {
    io_crash("sync");
}
#[test]
fn crash_inside_redb_write() {
    io_crash("write");
}
#[test]
fn torn_redb_write_recovers_atomic_state() {
    io_crash("torn_write");
}
#[test]
fn injected_sync_errors_preserve_atomic_state() {
    io_crash("sync_error");
}
#[test]
fn injected_write_errors_preserve_atomic_state() {
    io_crash("write_error");
}

fn corrupt(f: impl FnOnce(&redb::WriteTransaction)) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    fixture(&path, true);
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        f(&tx);
        tx.commit().unwrap();
    }
    assert!(Database::open_with_options(&path, manual()).is_err());
}
#[test]
fn missing_manifest_is_rejected() {
    corrupt(|tx| {
        tx.open_table(CONTROL).unwrap().remove(MANIFEST).unwrap();
    });
}
#[test]
fn missing_catalog_is_rejected() {
    corrupt(|tx| {
        tx.open_table(CONTROL).unwrap().remove(CATALOG).unwrap();
    });
}
#[test]
fn missing_head_is_rejected() {
    corrupt(|tx| {
        tx.open_table(CONTROL).unwrap().remove(HEAD).unwrap();
    });
}
#[test]
fn unsupported_storage_format_is_rejected() {
    corrupt(|tx| {
        tx.open_table(CONTROL)
            .unwrap()
            .insert(FORMAT, b"LDB9".as_slice())
            .unwrap();
    });
}
#[test]
fn truncated_manifest_is_rejected() {
    corrupt(|tx| {
        tx.open_table(CONTROL)
            .unwrap()
            .insert(MANIFEST, b"LCP1".as_slice())
            .unwrap();
    });
}
#[test]
fn wrong_manifest_counts_are_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(CONTROL).unwrap();
        let mut m = Manifest::decode(table.get(MANIFEST).unwrap().unwrap().value()).unwrap();
        m.rows += 1;
        table.insert(MANIFEST, m.encode().as_slice()).unwrap();
    });
}
#[test]
fn missing_checkpoint_row_is_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(ROWS).unwrap();
        let key = table
            .iter()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0
            .value()
            .to_vec();
        table.remove(key.as_slice()).unwrap();
    });
}
#[test]
fn wrong_checkpoint_row_key_is_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(ROWS).unwrap();
        let (key, value) = {
            let (k, v) = table.iter().unwrap().next().unwrap().unwrap();
            (k.value().to_vec(), v.value().to_vec())
        };
        table.remove(key.as_slice()).unwrap();
        table
            .insert([0u8; 32].as_slice(), value.as_slice())
            .unwrap();
    });
}
#[test]
fn truncated_checkpoint_row_is_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(ROWS).unwrap();
        let (key, mut value) = {
            let (k, v) = table.iter().unwrap().next().unwrap().unwrap();
            (k.value().to_vec(), v.value().to_vec())
        };
        value.pop();
        table.insert(key.as_slice(), value.as_slice()).unwrap();
    });
}
#[test]
fn missing_final_redo_is_detected_by_head() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    fixture(&path, true);
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(LOG).unwrap().remove(3).unwrap();
        tx.commit().unwrap();
    }
    assert!(Database::open_with_options(&path, manual()).is_err());
}
#[test]
fn legacy_log_only_database_is_upgraded_without_identity_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    fixture(&path, false);
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(CONTROL)
            .unwrap()
            .retain(|_, _| false)
            .unwrap();
        tx.commit().unwrap();
    }
    let db = Database::open_with_options(&path, manual()).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 3);
    db.checkpoint().unwrap();
    let expected = db.admin().snapshot().scan("core.Person").unwrap();
    drop(db);
    let db = Database::open_with_options(&path, manual()).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap(), expected);
}

#[test]
fn storage_errors_fail_stop_the_live_engine_until_reopen() {
    for mode in ["manual", "auto", "append"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        fixture(&path, true);
        let armed = Arc::new(AtomicBool::new(false));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let backend = FaultBackend {
            inner: redb::backends::FileBackend::new(file).unwrap(),
            armed: armed.clone(),
            calls: AtomicUsize::new(0),
            kind: "write_error".into(),
            nth: 1,
        };
        let (mut durable, state) = Durable::recover(
            redb::Database::builder()
                .create_with_backend(backend)
                .unwrap(),
            manual(),
        )
        .unwrap();
        if mode == "auto" {
            durable.options.checkpoint_policy = Some(CheckpointPolicy {
                max_log_commits: 1,
                max_log_bytes: u64::MAX,
            });
        }
        let db = Database::from_state(state, Some(durable));
        let old = db.admin().snapshot();
        let id = old.scan("core.Person").unwrap()[0].id;
        armed.store(true, Ordering::SeqCst);
        let result = if mode == "manual" {
            db.checkpoint().map(|_| ())
        } else {
            let mut tx = db.admin().transaction();
            tx.update("core.Person", id, vec!["uncertain".into()])
                .unwrap();
            tx.commit().map(|_| ())
        };
        assert!(matches!(result, Err(Error::Storage(_))));
        assert_eq!(
            db.admin().snapshot().scan("core.Person").unwrap(),
            old.scan("core.Person").unwrap()
        );
        assert!(db.storage_info().is_err());
        assert!(db.checkpoint().is_err());
        assert!(matches!(
            db.admin().transaction().commit(),
            Err(Error::Storage(_))
        ));
        drop(db);
        let reopened = Database::open_with_options(&path, manual()).unwrap();
        reopened.admin().snapshot().verify_indexes().unwrap();
    }
}

#[test]
fn a_gap_in_the_redo_tail_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    fixture(&path, true);
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let mut tx = db.admin().transaction();
        tx.insert("core.Person", vec!["four".into()]).unwrap();
        assert_eq!(tx.commit().unwrap(), 4);
    }
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(LOG).unwrap().remove(3).unwrap();
        tx.commit().unwrap();
    }
    assert!(
        matches!(Database::open_with_options(&path,manual()),Err(Error::Storage(s)) if s.contains("sequence"))
    );
}

type CheckpointEntries = BTreeMap<Vec<u8>, Vec<u8>>;
fn raw_checkpoint(path: &Path) -> (Vec<u8>, CheckpointEntries, CheckpointEntries) {
    let db = redb::Database::create(path).unwrap();
    let tx = db.begin_read().unwrap();
    let manifest = tx
        .open_table(CONTROL)
        .unwrap()
        .get(MANIFEST)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let rows = tx
        .open_table(ROWS)
        .unwrap()
        .iter()
        .unwrap()
        .map(|entry| {
            let (k, v) = entry.unwrap();
            (k.value().to_vec(), v.value().to_vec())
        })
        .collect();
    let metadata = tx
        .open_table(METADATA)
        .unwrap()
        .iter()
        .unwrap()
        .map(|entry| {
            let (k, v) = entry.unwrap();
            (k.value().to_vec(), v.value().to_vec())
        })
        .collect();
    (manifest, rows, metadata)
}
#[test]
fn frozen_v1_checkpoint_upgrades_once_without_changing_rows_or_identities() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tid = Id(uuid::Uuid::from_u128(0x40008000000000000001));
    let rid = Id(uuid::Uuid::from_u128(0x40008000000000000002));
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(LOG).unwrap();
        let mut c = tx.open_table(CONTROL).unwrap();
        c.insert(FORMAT, b"LDB1".as_slice()).unwrap();
        c.insert(HEAD, 2u64.to_le_bytes().as_slice()).unwrap();
        c.insert(
            CATALOG,
            include_bytes!("../tests/fixtures/checkpoint-v1/catalog.bin").as_slice(),
        )
        .unwrap();
        c.insert(
            MANIFEST,
            include_bytes!("../tests/fixtures/checkpoint-v1/manifest.bin").as_slice(),
        )
        .unwrap();
        drop(c);
        tx.open_table(ROWS)
            .unwrap()
            .insert(
                row_key(tid, rid).as_slice(),
                include_bytes!("../tests/fixtures/checkpoint-v1/row.bin").as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
    }
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        assert_eq!(
            db.admin()
                .snapshot()
                .get("core.Person", rid)
                .unwrap()
                .unwrap()
                .values[0],
            Value::from("Ada")
        );
        assert_eq!(db.admin().snapshot().schema("core.Person").unwrap().id, tid);
        let cp = db.checkpoint().unwrap();
        assert_eq!(cp.written_rows, 1);
        assert_eq!(cp.written_metadata, 2);
        let again = db.checkpoint().unwrap();
        assert_eq!(again.written_rows, 0);
        assert_eq!(again.written_metadata, 0);
    }
    let (manifest, _, _) = raw_checkpoint(&path);
    assert_eq!(&manifest[..4], b"LCP2");
    let db = Database::open(&path).unwrap();
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
    assert_eq!(
        db.admin().snapshot().scan("core.Person").unwrap()[0].id,
        rid
    );
}
#[test]
fn incremental_checkpoint_only_writes_changed_rows_and_metadata_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let ids = {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), Visibility::Private)
            .unwrap();
        let ids = (0..100)
            .map(|i| {
                tx.insert("core.Person", vec![Value::Text(format!("p{i}"))])
                    .unwrap()
            })
            .collect::<Vec<_>>();
        tx.grant("reader", "core.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let cp = db.checkpoint().unwrap();
        // One visibility ledger row, its identity, and its internal module are
        // part of the atomic schema install and must be checkpointed too.
        assert_eq!(cp.written_rows, 101);
        assert_eq!(cp.written_metadata, 104);
        ids
    };
    let (_, before, _) = raw_checkpoint(&path);
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let mut tx = db.admin().transaction();
        tx.update("core.Person", ids[0], vec!["changed".into()])
            .unwrap();
        tx.delete("core.Person", ids[1]).unwrap();
        tx.insert("core.Person", vec!["added".into()]).unwrap();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
    }
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let cp = db.checkpoint().unwrap();
        assert_eq!(cp.written_rows, 2);
        assert_eq!(cp.deleted_rows, 1);
        assert_eq!(cp.written_metadata, 1);
        assert_eq!(cp.deleted_metadata, 1);
        assert_eq!(cp.rows, 101);
    }
    let (_, after, _) = raw_checkpoint(&path);
    for id in &ids[2..] {
        let key = before.keys().find(|k| k[16..] == *id.0.as_bytes()).unwrap();
        assert_eq!(before[key], after[key]);
    }
    let db = Database::open(&path).unwrap();
    assert!(db.session("reader").snapshot().scan("core.Person").is_err());
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn repeated_row_updates_coalesce_and_deleted_new_identity_remains_allocated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Database::open_with_options(&path, manual()).unwrap();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)
        .unwrap();
    let id = tx.insert("core.Person", vec!["old".into()]).unwrap();
    tx.commit().unwrap();
    db.checkpoint().unwrap();
    for _ in 0..8 {
        let mut tx = db.admin().transaction();
        tx.update("core.Person", id, vec!["new".into()]).unwrap();
        tx.commit().unwrap();
    }
    let mut tx = db.admin().transaction();
    let gone = tx.insert("core.Person", vec!["gone".into()]).unwrap();
    tx.delete("core.Person", gone).unwrap();
    tx.commit().unwrap();
    let cp = db.checkpoint().unwrap();
    assert_eq!(cp.written_rows, 1);
    assert_eq!(cp.written_metadata, 1);
    assert_eq!(cp.deleted_rows, 0);
    drop(db);
    let db = Database::open(&path).unwrap();
    assert!(
        db.admin()
            .transaction()
            .insert_with_id("core.Person", gone, vec!["reuse".into()])
            .is_err()
    );
}
#[test]
fn v2_noop_checkpoint_has_deterministic_manifest_and_zero_entry_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    fixture(&path, true);
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        db.checkpoint().unwrap();
    }
    let before = raw_checkpoint(&path);
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let cp = db.checkpoint().unwrap();
        assert_eq!(
            cp.written_rows + cp.written_metadata + cp.deleted_rows + cp.deleted_metadata,
            0
        );
        assert_eq!(cp.written_bytes, 0);
    }
    assert_eq!(before, raw_checkpoint(&path));
}
#[test]
fn missing_v2_metadata_is_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(METADATA).unwrap();
        let key = table
            .iter()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0
            .value()
            .to_vec();
        table.remove(key.as_slice()).unwrap();
    });
}
#[test]
fn wrong_v2_metadata_key_is_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(METADATA).unwrap();
        let (key, value) = {
            let (k, v) = table.iter().unwrap().next().unwrap().unwrap();
            (k.value().to_vec(), v.value().to_vec())
        };
        table.remove(key.as_slice()).unwrap();
        table.insert(b"wrong".as_slice(), value.as_slice()).unwrap();
    });
}
#[test]
fn truncated_v2_metadata_is_rejected() {
    corrupt(|tx| {
        let mut table = tx.open_table(METADATA).unwrap();
        let (key, mut value) = {
            let (k, v) = table.iter().unwrap().next().unwrap().unwrap();
            (k.value().to_vec(), v.value().to_vec())
        };
        value.pop();
        table.insert(key.as_slice(), value.as_slice()).unwrap();
    });
}
