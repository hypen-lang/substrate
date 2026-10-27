use super::*;

fn setup(count: usize) -> (tempfile::TempDir, Durable, State, Id, Vec<Id>) {
    let dir = tempfile::tempdir().unwrap();
    let (mut durable, mut state) = Durable::open(&dir.path().join("db"), manual()).unwrap();
    let module = demo::core();
    let tid = module.relations[0].id;
    let ids: Vec<_> = (0..count)
        .map(|i| Id(uuid::Uuid::from_u128(i as u128 + 1)))
        .collect();
    let mut ops = vec![Op::Install(module)];
    for id in &ids {
        ops.push(Op::Identity(*id));
        ops.push(put(tid, *id, "before"));
    }
    apply(&mut durable, &mut state, ops);
    durable.checkpoint(&state).unwrap();
    (dir, durable, state, tid, ids)
}
fn put(tid: Id, id: Id, text: &str) -> Op {
    Op::Put(
        tid,
        Row {
            id,
            values: vec![text.into()].into_boxed_slice(),
        },
    )
}
fn apply(durable: &mut Durable, state: &mut State, ops: Vec<Op>) {
    state.revision += 1;
    for op in &ops {
        state.apply(op, state.revision);
    }
    durable.append(state.revision, &ops, state).unwrap();
}
fn reopen(dir: &tempfile::TempDir, durable: Durable, expected: &State) -> (Durable, State) {
    drop(durable);
    let (durable, recovered) = Durable::open(&dir.path().join("db"), manual()).unwrap();
    assert_eq!(recovered.families, expected.families);
    assert_eq!(recovered.identities, expected.identities);
    assert_eq!(recovered.revision, expected.revision);
    (durable, recovered)
}

#[test]
fn key_cap_compacts_atomically_and_preserves_pinned_base_and_overlay() {
    let (dir, mut durable, mut state, tid, ids) = setup(MAX_OVERLAY_KEYS as usize + 2);
    apply(
        &mut durable,
        &mut state,
        ids[..MAX_OVERLAY_KEYS as usize]
            .iter()
            .map(|id| put(tid, *id, "overlay"))
            .collect(),
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, MAX_OVERLAY_KEYS);
    assert_eq!(durable.info.persistence.checkpoint_compactions, 0);
    apply(
        &mut durable,
        &mut state,
        ids[..MAX_OVERLAY_KEYS as usize]
            .iter()
            .map(|id| put(tid, *id, "overlay-new"))
            .collect(),
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, MAX_OVERLAY_KEYS);
    assert_eq!(durable.info.persistence.checkpoint_compactions, 0);
    let pinned = durable.db.begin_read().unwrap();
    let old_base = pinned.open_table(ROWS).unwrap();
    let old_overlay = pinned.open_table(OVERLAY).unwrap();
    // Pick the first sorted override so this exact pinned row is compacted.
    let key = old_overlay
        .iter()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .0
        .value()
        .to_vec();
    let old_base_value = old_base
        .get(key.as_slice())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let old_overlay_value = old_overlay
        .get(key.as_slice())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    assert_ne!(old_base_value, old_overlay_value);
    apply(
        &mut durable,
        &mut state,
        vec![put(tid, ids[MAX_OVERLAY_KEYS as usize], "trigger")],
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.persistence.checkpoint_compactions, 1);
    assert_eq!(
        durable.info.overlay_keys,
        MAX_OVERLAY_KEYS - MIN_COMPACTION_ROWS + 1
    );
    assert_eq!(
        durable.info.persistence.checkpoint_compacted_rows,
        MIN_COMPACTION_ROWS
    );
    assert!(durable.info.overlay_bytes < MAX_OVERLAY_BYTES);
    {
        let current = durable.db.begin_read().unwrap();
        assert!(
            current
                .open_table(OVERLAY)
                .unwrap()
                .get(key.as_slice())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            current
                .open_table(ROWS)
                .unwrap()
                .get(key.as_slice())
                .unwrap()
                .unwrap()
                .value(),
            old_overlay_value
        );
    }
    assert_eq!(
        old_base.get(key.as_slice()).unwrap().unwrap().value(),
        old_base_value
    );
    assert_eq!(
        old_overlay.get(key.as_slice()).unwrap().unwrap().value(),
        old_overlay_value
    );
    drop(old_base);
    drop(old_overlay);
    drop(pinned);
    reopen(&dir, durable, &state);
}

#[test]
fn newest_image_delete_reinsert_and_absent_delete_have_exact_accounting() {
    let (dir, mut durable, mut state, tid, ids) = setup(2);
    for text in ["a", "longer value", "", "latest"] {
        apply(&mut durable, &mut state, vec![put(tid, ids[0], text)]);
        durable.checkpoint(&state).unwrap();
        assert_eq!(durable.info.overlay_keys, 1);
        assert_eq!(
            durable.info.overlay_bytes,
            32 + codec::encode_checkpoint_row(tid, state.row(tid, ids[0]).unwrap(), &state)
                .unwrap()
                .len() as u64
        );
        (durable, state) = reopen(&dir, durable, &state);
    }
    apply(&mut durable, &mut state, vec![Op::Delete(tid, ids[0])]);
    assert_eq!(durable.checkpoint(&state).unwrap().deleted_rows, 1);
    assert_eq!(durable.info.overlay_keys, 1);
    assert_eq!(durable.info.overlay_bytes, 32); // tombstone still consumes its key
    (durable, state) = reopen(&dir, durable, &state);
    apply(&mut durable, &mut state, vec![put(tid, ids[0], "restored")]);
    durable.checkpoint(&state).unwrap();
    (durable, state) = reopen(&dir, durable, &state);
    let added = Id(uuid::Uuid::from_u128(99999));
    apply(
        &mut durable,
        &mut state,
        vec![Op::Identity(added), put(tid, added, "new")],
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, 2);
    apply(&mut durable, &mut state, vec![Op::Delete(tid, added)]);
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, 1); // no base image: remove override entirely
    let bytes = durable.info.checkpoint_bytes;
    apply(&mut durable, &mut state, vec![Op::Delete(tid, added)]);
    assert_eq!(durable.checkpoint(&state).unwrap().deleted_rows, 0);
    assert_eq!(durable.info.checkpoint_bytes, bytes);
    reopen(&dir, durable, &state);
}

#[test]
fn partial_merge_preserves_pending_delete_reinsert_and_new_keys() {
    let (dir, mut durable, mut state, tid, ids) = setup(MAX_OVERLAY_KEYS as usize + 3);
    let added = Id(uuid::Uuid::from_u128(999999));
    let mut ops = vec![
        Op::Delete(tid, ids[0]),
        Op::Identity(added),
        put(tid, added, "new-only"),
    ];
    ops.extend(
        ids[1..MAX_OVERLAY_KEYS as usize - 1]
            .iter()
            .map(|id| put(tid, *id, "overlay")),
    );
    apply(&mut durable, &mut state, ops);
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, MAX_OVERLAY_KEYS);
    apply(
        &mut durable,
        &mut state,
        vec![
            Op::Delete(tid, added),
            put(tid, ids[0], "restored"),
            put(tid, ids[4095], "one"),
            put(tid, ids[4096], "two"),
            put(tid, ids[4097], "three"),
        ],
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(
        durable.info.persistence.checkpoint_compacted_rows,
        MIN_COMPACTION_ROWS
    );
    assert_eq!(
        durable.info.overlay_keys,
        MAX_OVERLAY_KEYS - MIN_COMPACTION_ROWS + 2
    );
    let read = durable.db.begin_read().unwrap();
    let key = row_key(tid, added);
    assert!(
        read.open_table(ROWS)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .is_none()
    );
    assert!(
        read.open_table(OVERLAY)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .is_none()
    );
    drop(read);
    reopen(&dir, durable, &state);
}

#[test]
fn byte_cap_and_single_oversized_image_are_compacted_before_acknowledgement() {
    let (dir, mut durable, mut state, tid, ids) = setup(5);
    let text = "x".repeat(2 * 1024 * 1024);
    apply(
        &mut durable,
        &mut state,
        ids[..3].iter().map(|id| put(tid, *id, &text)).collect(),
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, 3);
    // A fourth image crosses the byte cap while far below the key cap. Flush
    // the existing non-pending images before admitting it to the overlay.
    apply(&mut durable, &mut state, vec![put(tid, ids[3], &text)]);
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.persistence.checkpoint_compactions, 1);
    assert_eq!(durable.info.overlay_keys, 1);
    assert!(durable.info.overlay_bytes <= MAX_OVERLAY_BYTES);
    (durable, state) = reopen(&dir, durable, &state);
    apply(
        &mut durable,
        &mut state,
        ids.iter().map(|id| put(tid, *id, &text)).collect(),
    );
    durable.checkpoint(&state).unwrap();
    // This pending batch cannot fit even an empty overlay, so it goes directly
    // to the base without a redundant fill/compaction cycle.
    assert_eq!(durable.info.overlay_keys, 0);
    assert!(durable.info.overlay_bytes <= MAX_OVERLAY_BYTES);
    (durable, state) = reopen(&dir, durable, &state);
    apply(
        &mut durable,
        &mut state,
        vec![put(
            tid,
            ids[0],
            &"y".repeat(MAX_OVERLAY_BYTES as usize + 1),
        )],
    );
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_bytes, 0);
    reopen(&dir, durable, &state);
}

#[test]
fn v2_to_v3_upgrade_reuses_unchanged_base_bytes() {
    let (dir, mut durable, mut state, tid, ids) = setup(3);
    // Test-only direct writer constructs a genuine v2 checkpoint, including its
    // original magic and 60-byte manifest; production has no downgrade setting.
    durable.force_v2_checkpoint = true;
    durable.checkpoint(&state).unwrap();
    drop(durable);
    let (before, base, _) = raw_checkpoint(&dir.path().join("db"));
    assert_eq!(&before[..4], b"LCP2");
    let (mut durable, _) = Durable::open(&dir.path().join("db"), manual()).unwrap();
    apply(&mut durable, &mut state, vec![put(tid, ids[0], "v3")]);
    durable.checkpoint(&state).unwrap();
    assert_eq!(durable.info.overlay_keys, 1);
    drop(durable);
    let (after, actual_base, _) = raw_checkpoint(&dir.path().join("db"));
    assert_eq!(&after[..4], b"LCP3");
    assert_eq!(actual_base, base);
    let (_, recovered) = Durable::open(&dir.path().join("db"), manual()).unwrap();
    assert_eq!(recovered.families, state.families);
}

#[test]
fn compaction_crash_boundaries_keep_rows_schema_and_grants_atomic() {
    for point in [
        "checkpoint_before_compaction",
        "checkpoint_during_compaction",
        "checkpoint_after_compaction",
        "checkpoint_after_manifest",
        "checkpoint_after_prune",
        "checkpoint_before_commit",
        "checkpoint_after_commit",
    ] {
        let (dir, mut durable, mut state, tid, ids) = setup(MAX_OVERLAY_KEYS as usize + 2);
        apply(
            &mut durable,
            &mut state,
            ids[..MAX_OVERLAY_KEYS as usize]
                .iter()
                .map(|id| put(tid, *id, "before"))
                .collect(),
        );
        durable.checkpoint(&state).unwrap();
        drop(durable);
        let path = dir.path().join("db");
        let output = child(&path, "auto", Some(point), None);
        assert_eq!(
            output.status.code(),
            Some(91),
            "{}: {}",
            point,
            String::from_utf8_lossy(&output.stderr)
        );
        let db = Database::open_with_options(&path, manual()).unwrap();
        let after = point == "checkpoint_after_commit";
        assert_eq!(db.admin().snapshot().revision(), if after { 3 } else { 2 });
        for row in db.admin().snapshot().scan("core.Person").unwrap() {
            assert_eq!(
                row.values[0],
                Value::from(if after { "after" } else { "before" })
            );
        }
        assert_eq!(db.admin().snapshot().schema("extra.Flag").is_ok(), after);
        assert_eq!(
            db.session("reader").snapshot().scan("core.Person").is_ok(),
            after
        );
        assert_eq!(db.recovery_info().unwrap().replayed_commits, 0);
        db.admin().snapshot().verify_indexes().unwrap();
    }
}

#[test]
fn actual_backend_write_and_sync_crashes_during_compaction_recover_atomically() {
    for kind in ["write", "sync", "torn_write"] {
        let (dir, mut durable, mut state, tid, ids) = setup(MAX_OVERLAY_KEYS as usize + 2);
        apply(
            &mut durable,
            &mut state,
            ids[..MAX_OVERLAY_KEYS as usize]
                .iter()
                .map(|id| put(tid, *id, "before"))
                .collect(),
        );
        durable.checkpoint(&state).unwrap();
        drop(durable);
        let path = dir.path().join("db");
        let output = child(&path, "auto", None, Some((kind, 1)));
        assert_eq!(
            output.status.code(),
            Some(92),
            "{kind}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let db = Database::open_with_options(&path, manual()).unwrap();
        let snapshot = db.admin().snapshot();
        assert!(matches!(snapshot.revision(), 2 | 3));
        let after = snapshot.revision() == 3;
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
        snapshot.verify_indexes().unwrap();
    }
}

#[test]
fn partial_compaction_crashes_preserve_remainder_and_atomic_authority() {
    for point in [
        "checkpoint_before_compaction",
        "checkpoint_during_compaction",
        "checkpoint_during_overlay_removal",
        "checkpoint_after_compaction",
        "checkpoint_after_manifest",
        "checkpoint_after_prune",
        "checkpoint_before_commit",
        "checkpoint_after_commit",
    ] {
        let (dir, mut durable, mut state, tid, ids) = setup(MAX_OVERLAY_KEYS as usize + 2);
        apply(
            &mut durable,
            &mut state,
            ids[..MAX_OVERLAY_KEYS as usize]
                .iter()
                .map(|id| put(tid, *id, "before"))
                .collect(),
        );
        durable.checkpoint(&state).unwrap();
        drop(durable);
        let path = dir.path().join("db");
        let output = child(&path, "partial", Some(point), None);
        assert_eq!(
            output.status.code(),
            Some(91),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let db = Database::open_with_options(&path, manual()).unwrap();
        let after = point == "checkpoint_after_commit";
        let snapshot = db.admin().snapshot();
        assert_eq!(snapshot.revision(), if after { 3 } else { 2 });
        for row in snapshot.scan("core.Person").unwrap() {
            let changed = after && row.id == *ids.last().unwrap();
            assert_eq!(
                row.values[0],
                Value::from(if changed { "after" } else { "before" })
            );
        }
        assert_eq!(snapshot.schema("extra.Flag").is_ok(), after);
        assert_eq!(
            db.session("reader").snapshot().scan("core.Person").is_ok(),
            after
        );
        let storage = db.storage_info().unwrap();
        assert_eq!(
            storage.overlay_keys,
            if after {
                MAX_OVERLAY_KEYS - MIN_COMPACTION_ROWS + 2
            } else {
                MAX_OVERLAY_KEYS
            }
        );
        assert!(storage.overlay_bytes <= MAX_OVERLAY_BYTES);
        snapshot.verify_indexes().unwrap();
    }
}

fn corrupt_overlay(change: impl FnOnce(&redb::WriteTransaction)) {
    let (dir, mut durable, mut state, tid, ids) = setup(2);
    apply(&mut durable, &mut state, vec![put(tid, ids[0], "overlay")]);
    durable.checkpoint(&state).unwrap();
    drop(durable);
    let path = dir.path().join("db");
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        change(&tx);
        tx.commit().unwrap();
    }
    assert!(Durable::open(&path, manual()).is_err());
}
#[test]
fn overlay_corruption_and_invalid_bounds_fail_closed() {
    corrupt_overlay(|tx| {
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
        table.insert(key.as_slice(), [].as_slice()).unwrap();
    });
    for field in 0..4 {
        corrupt_overlay(|tx| {
            let mut table = tx.open_table(CONTROL).unwrap();
            let mut m = Manifest::decode(table.get(MANIFEST).unwrap().unwrap().value()).unwrap();
            match field {
                0 => m.overlay_keys = MAX_OVERLAY_KEYS + 1,
                1 => m.overlay_bytes = MAX_OVERLAY_BYTES + 1,
                2 => m.base_rows += 1,
                _ => m.overlay_bytes += 1,
            }
            table.insert(MANIFEST, m.encode().as_slice()).unwrap();
        });
    }
    corrupt_overlay(|tx| {
        tx.open_table(OVERLAY)
            .unwrap()
            .retain(|_, _| false)
            .unwrap();
    });
    corrupt_overlay(|tx| {
        let mut table = tx.open_table(OVERLAY).unwrap();
        let (key, value) = {
            let (k, v) = table.iter().unwrap().next().unwrap().unwrap();
            (k.value().to_vec(), v.value().to_vec())
        };
        table
            .insert(key.as_slice(), &value[..value.len() - 1])
            .unwrap();
    });
    corrupt_overlay(|tx| {
        let mut table = tx.open_table(OVERLAY).unwrap();
        let (key, value) = {
            let (k, v) = table.iter().unwrap().next().unwrap().unwrap();
            (k.value().to_vec(), v.value().to_vec())
        };
        table.remove(key.as_slice()).unwrap();
        table
            .insert([0u8; 32].as_slice(), value.as_slice())
            .unwrap();
    });
    corrupt_overlay(|tx| {
        let mut table = tx.open_table(CONTROL).unwrap();
        let mut m = Manifest::decode(table.get(MANIFEST).unwrap().unwrap().value()).unwrap();
        m.format = 2;
        table.insert(MANIFEST, m.encode().as_slice()).unwrap();
        table.insert(CATALOG, b"LCC2".as_slice()).unwrap();
        table.insert(FORMAT, b"LDB2".as_slice()).unwrap();
    });
}
