mod common;
use common::seed;
use substrate_liquid::*;

fn manual() -> DatabaseOptions {
    DatabaseOptions {
        checkpoint_policy: None,
    }
}

#[test]
fn disk_snapshot_survives_repeated_source_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(dir.path().join("db"), manual()).unwrap();
    let person = seed(&db);
    let snapshot = db.admin().snapshot();
    let expected = snapshot.get("core.Person", person).unwrap();
    let disk = snapshot
        .spill(
            dir.path().join("spill"),
            &["core.Person"],
            disk_snapshot::SpillOptions::default(),
        )
        .unwrap();
    drop(snapshot);
    for name in ["one", "two", "three"] {
        let mut tx = db.admin().transaction();
        tx.update("core.Person", person, vec![name.into()]).unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
        assert_eq!(disk.get("core.Person", person).unwrap(), expected);
        disk.verify().unwrap();
    }
}

#[test]
fn variable_sized_replacements_preserve_manifest_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (person, original_bytes) = {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let person = seed(&db);
        let checkpoint = db.checkpoint().unwrap();
        (person, checkpoint.bytes)
    };
    // Exercise shrinking, empty payloads, and replacements spanning many pages.
    for length in [0, 8192, 1, 262_144, 3, 5] {
        let name = "x".repeat(length);
        let checkpoint = {
            let db = Database::open_with_options(&path, manual()).unwrap();
            let mut tx = db.admin().transaction();
            tx.update("core.Person", person, vec![name.as_str().into()])
                .unwrap();
            tx.commit().unwrap();
            let checkpoint = db.checkpoint().unwrap();
            assert_eq!(checkpoint.written_rows, 1);
            assert_eq!(checkpoint.deleted_rows, 0);
            assert_eq!(checkpoint.written_metadata, 0);
            // Binary text length prefixes have a fixed width; only payload bytes change.
            assert_eq!(
                checkpoint.bytes,
                original_bytes - "Alice".len() as u64 + length as u64
            );
            checkpoint
        };
        let reopened = Database::open_with_options(&path, manual()).unwrap();
        assert_eq!(reopened.recovery_info().unwrap().replayed_commits, 0);
        assert_eq!(
            reopened.storage_info().unwrap().checkpoint_bytes,
            checkpoint.bytes
        );
        assert_eq!(
            reopened
                .admin()
                .snapshot()
                .get("core.Person", person)
                .unwrap()
                .unwrap()
                .values[0],
            Value::Text(name)
        );
        reopened.admin().snapshot().verify_indexes().unwrap();
    }
}

#[test]
fn grant_replacements_and_absent_deletions_keep_exact_checkpoint_counts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let person = {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let person = seed(&db);
        db.checkpoint().unwrap();
        person
    };
    for length in [1, 16_384, 0, 2] {
        {
            let db = Database::open_with_options(&path, manual()).unwrap();
            let mut tx = db.admin().transaction();
            tx.grant(
                "reader",
                "core.Person",
                Grant {
                    filter: RowFilter::Equals {
                        role: "name".into(),
                        value: Value::Text("q".repeat(length)),
                    },
                    ..Grant::read_only()
                },
            )
            .unwrap();
            tx.commit().unwrap();
            let checkpoint = db.checkpoint().unwrap();
            assert_eq!(checkpoint.written_metadata, 1);
            assert_eq!(checkpoint.deleted_metadata, 0);
            assert_eq!(checkpoint.written_rows, 0);
        }
        let reopened = Database::open_with_options(&path, manual()).unwrap();
        assert_eq!(reopened.recovery_info().unwrap().replayed_commits, 0);
        assert!(
            reopened
                .session("reader")
                .snapshot()
                .scan("core.Person")
                .unwrap()
                .is_empty()
        );
    }
    let bytes_after_revoke;
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
        let checkpoint = db.checkpoint().unwrap();
        assert_eq!(checkpoint.deleted_metadata, 1);
        assert_eq!(checkpoint.written_metadata, 0);
        bytes_after_revoke = checkpoint.bytes;
    }
    for _ in 0..2 {
        let db = Database::open_with_options(&path, manual()).unwrap();
        assert!(matches!(
            db.session("reader").snapshot().scan("core.Person"),
            Err(Error::Denied)
        ));
        let mut tx = db.admin().transaction();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
        let checkpoint = db.checkpoint().unwrap();
        assert_eq!(checkpoint.deleted_metadata, 0);
        assert_eq!(checkpoint.written_metadata, 0);
        assert_eq!(checkpoint.bytes, bytes_after_revoke);
    }
    let final_bytes;
    {
        let db = Database::open_with_options(&path, manual()).unwrap();
        // This row never existed in the checkpoint. Its identity is retained,
        // but deleting its absent row key must not subtract any row bytes.
        let mut tx = db.admin().transaction();
        let transient = tx.insert("core.Person", vec!["temporary".into()]).unwrap();
        tx.delete("core.Person", transient).unwrap();
        tx.commit().unwrap();
        let checkpoint = db.checkpoint().unwrap();
        assert_eq!(checkpoint.written_rows, 0);
        assert_eq!(checkpoint.deleted_rows, 0);
        assert_eq!(checkpoint.written_metadata, 1);
        assert_eq!(
            checkpoint.rows,
            db.admin().snapshot().stats().unwrap().rows as u64
        );
        let mut tx = db.admin().transaction();
        tx.delete("core.Person", person).unwrap();
        tx.commit().unwrap();
        let checkpoint = db.checkpoint().unwrap();
        assert_eq!(checkpoint.deleted_rows, 1);
        final_bytes = checkpoint.bytes;
        let mut repeated = db.admin().transaction();
        assert!(matches!(
            repeated.delete("core.Person", person),
            Err(Error::NotFound)
        ));
        assert!(repeated.commit().is_err());
        let unchanged = db.checkpoint().unwrap();
        assert_eq!(unchanged.bytes, final_bytes);
        assert_eq!(unchanged.deleted_rows, 0);
        assert_eq!(unchanged.deleted_metadata, 0);
        assert_eq!(unchanged.written_bytes, 0);
    }
    let db = Database::open_with_options(&path, manual()).unwrap();
    assert_eq!(db.storage_info().unwrap().checkpoint_bytes, final_bytes);
    assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
    assert!(
        db.admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .is_empty()
    );
    db.admin().snapshot().verify_indexes().unwrap();
}
