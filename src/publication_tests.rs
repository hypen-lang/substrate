use super::*;

fn fixture() -> (Database, Id) {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)
        .unwrap();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    let id = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.commit().unwrap();
    (db, id)
}

#[test]
fn unchanged_generation_fences_do_not_wait_for_the_head_mutex() {
    let (db, _) = fixture();
    let reader = db.session("reader").snapshot();
    let admin = db.admin().snapshot();
    std::thread::scope(|threads| {
        let head = db.head.lock().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        threads.spawn(move || {
            let result = reader
                .live
                .check()
                .and_then(|_| reader.live.check_attached())
                .and_then(|_| admin.live.check_attached());
            sent.send(result).unwrap();
        });
        let result = received.recv_timeout(std::time::Duration::from_secs(2));
        // Always unlock before joining, including on a timeout regression.
        drop(head);
        assert!(matches!(result, Ok(Ok(()))), "{result:?}");
    });
}

#[test]
fn capture_before_revocation_cannot_use_the_new_publication_generation() {
    let (db, _) = fixture();
    let session = db.session("reader");
    let (state, generation) = {
        let head = db.head.lock().unwrap();
        (head.state.clone(), head.publication.load(Ordering::Relaxed))
    };
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    // Simulate publication between snapshot capture and slower scope resolution.
    let (_, guard) = session.authorize_state(state, false, generation);
    assert!(matches!(guard.check(), Err(Error::Denied)));
    assert!(matches!(guard.check_attached(), Err(Error::Denied)));
}

#[test]
fn changed_generation_keeps_data_snapshots_but_fences_authority_changes() {
    let (db, id) = fixture();
    let reader = db.session("reader").snapshot();
    let generation = reader.live.authorized_generation;
    let mut write = db.admin().transaction();
    write
        .update("core.Person", id, vec!["Grace".into()])
        .unwrap();
    write.commit().unwrap();
    assert!(db.publication.load(Ordering::Acquire) > generation);
    assert_eq!(
        reader.get("core.Person", id).unwrap().unwrap().values[0],
        "Ada".into()
    );
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    assert!(matches!(reader.scan("core.Person"), Err(Error::Denied)));
}

#[test]
fn public_visibility_changes_fence_anonymous_snapshots() {
    let (db, _) = fixture();
    let mut public = db.admin().transaction();
    public
        .set_schema_visibility("core.Person", Visibility::Public)
        .unwrap();
    public.commit().unwrap();
    let anonymous = db.anonymous().snapshot();
    assert_eq!(anonymous.scan("core.Person").unwrap().len(), 1);
    let mut private = db.admin().transaction();
    private
        .set_schema_visibility("core.Person", Visibility::Private)
        .unwrap();
    private.commit().unwrap();
    assert!(matches!(anonymous.scan("core.Person"), Err(Error::Denied)));
}

#[test]
fn poison_and_fail_stop_deny_even_unchanged_snapshot_generations() {
    let (db, _) = fixture();
    let reader = db.session("reader").snapshot();
    let admin = db.admin().snapshot();
    let poisoned = db.clone();
    assert!(
        std::thread::spawn(move || {
            let _held = poisoned.head.lock().unwrap();
            panic!("poison publication mutex");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        db.publication.load(Ordering::Acquire),
        reader.live.authorized_generation
    );
    assert!(matches!(reader.live.check(), Err(Error::Denied)));
    assert!(matches!(admin.live.check_attached(), Err(Error::Denied)));

    let (db, _) = fixture();
    let reader = db.session("reader").snapshot();
    let admin = db.admin().snapshot();
    db.replication_fail();
    assert_eq!(db.publication.load(Ordering::Acquire), u64::MAX);
    assert!(matches!(reader.live.check(), Err(Error::Denied)));
    assert!(matches!(admin.live.check_attached(), Err(Error::Denied)));
}

#[test]
fn unchanged_generation_still_requires_attachment_and_expiry_checks() {
    let (db, _) = fixture();
    db.clock.set(100);
    let mut reader = db.session("reader").snapshot();
    reader.live.expires_at = Some(101);
    db.clock.set(101);
    assert!(matches!(reader.live.check(), Err(Error::Denied)));
    assert!(matches!(reader.live.check_attached(), Err(Error::Denied)));
    let admin = db.admin().snapshot();
    drop(db);
    assert!(matches!(admin.live.check_attached(), Err(Error::Denied)));
    // Ordinary privileged in-memory snapshot behavior is unchanged.
    assert!(admin.live.check().is_ok());
}

#[test]
fn replication_publication_advances_generation_even_at_the_same_revision() {
    let (db, _) = fixture();
    let reader = db.session("reader").snapshot();
    let state = (*db.head.lock().unwrap().state).clone();
    db.replication_publish(state.clone()).unwrap();
    assert_eq!(
        db.publication.load(Ordering::Acquire),
        reader.live.authorized_generation + 1
    );
    assert!(reader.live.check().is_ok());
    let mut revoked = state;
    let tid = revoked.names["core.Person"];
    revoked.revision += 1;
    revoked.apply(&Op::Grant("reader".into(), tid, None), revoked.revision);
    db.replication_publish(revoked).unwrap();
    assert!(matches!(reader.live.check(), Err(Error::Denied)));
}

#[test]
fn generation_exhaustion_permanently_falls_back_to_locked_authority_checks() {
    let (db, _) = fixture();
    {
        let head = db.head.lock().unwrap();
        head.publication.store(u64::MAX - 1, Ordering::Release);
    }
    let before = db.session("reader").snapshot();
    let state = (*db.head.lock().unwrap().state).clone();
    db.replication_publish(state).unwrap();
    assert_eq!(db.publication.load(Ordering::Acquire), u64::MAX);
    let exhausted = db.session("reader").snapshot();
    assert_eq!(exhausted.live.authorized_generation, u64::MAX);
    assert!(exhausted.live.check().is_ok());
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    assert_eq!(db.publication.load(Ordering::Acquire), u64::MAX);
    assert!(matches!(before.live.check(), Err(Error::Denied)));
    assert!(matches!(exhausted.live.check(), Err(Error::Denied)));
}
