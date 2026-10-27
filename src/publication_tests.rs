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
        (head.state.clone(), head.publication.load().generation)
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
    assert!(db.publication.load().generation > generation);
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
        db.publication.load().generation,
        reader.live.authorized_generation
    );
    assert!(matches!(reader.live.check(), Err(Error::Denied)));
    assert!(matches!(admin.live.check_attached(), Err(Error::Denied)));

    let (db, _) = fixture();
    let reader = db.session("reader").snapshot();
    let admin = db.admin().snapshot();
    db.replication_fail();
    assert_eq!(db.publication.load().generation, u64::MAX);
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
        db.publication.load().generation,
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
fn generation_exhaustion_permanently_checks_current_authority() {
    let (db, _) = fixture();
    {
        let head = db.head.lock().unwrap();
        head.publication.store(Arc::new(PublishedHead {
            state: head.state.clone(),
            failed: false,
            generation: u64::MAX - 1,
        }));
    }
    let before = db.session("reader").snapshot();
    let state = (*db.head.lock().unwrap().state).clone();
    db.replication_publish(state).unwrap();
    assert_eq!(db.publication.load().generation, u64::MAX);
    let exhausted = db.session("reader").snapshot();
    assert_eq!(exhausted.live.authorized_generation, u64::MAX);
    assert!(exhausted.live.check().is_ok());
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    assert_eq!(db.publication.load().generation, u64::MAX);
    assert!(matches!(before.live.check(), Err(Error::Denied)));
    assert!(matches!(exhausted.live.check(), Err(Error::Denied)));
}

#[test]
fn fresh_snapshots_and_changed_generation_fences_do_not_wait_for_writer_mutex() {
    let (db, id) = fixture();
    let session = db.session("reader");
    let retained = session.snapshot();
    let mut write = db.admin().transaction();
    write
        .update("core.Person", id, vec!["Grace".into()])
        .unwrap();
    write.commit().unwrap();
    std::thread::scope(|threads| {
        let head = db.head.lock().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        threads.spawn(move || {
            let current = session.snapshot();
            let old = retained.get("core.Person", id).unwrap().unwrap();
            let new = current.get("core.Person", id).unwrap().unwrap();
            sent.send((old.values[0].clone(), new.values[0].clone()))
                .unwrap();
        });
        let result = received.recv_timeout(std::time::Duration::from_secs(2));
        // Unlock before joining even when a regression causes a timeout.
        drop(head);
        assert_eq!(result.unwrap(), ("Ada".into(), "Grace".into()));
    });
}

#[test]
fn concurrent_snapshot_capture_keeps_state_and_authority_generation_together() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (db, id) = fixture();
    let done = AtomicBool::new(false);
    let barrier = std::sync::Barrier::new(5);
    std::thread::scope(|threads| {
        threads.spawn(|| {
            barrier.wait();
            for revision in 2..=201 {
                let mut tx = db.admin().transaction();
                tx.update("core.Person", id, vec![Value::Text(revision.to_string())])
                    .unwrap();
                assert_eq!(tx.commit().unwrap(), revision);
            }
            done.store(true, Ordering::Release);
        });
        for _ in 0..4 {
            let db = &db;
            let done = &done;
            let barrier = &barrier;
            threads.spawn(move || {
                let session = db.session("reader");
                barrier.wait();
                loop {
                    let snapshot = session.snapshot();
                    assert_eq!(snapshot.state.revision, snapshot.live.authorized_generation);
                    let row = snapshot.get("core.Person", id).unwrap().unwrap();
                    let expected = if snapshot.state.revision == 1 {
                        "Ada".to_owned()
                    } else {
                        snapshot.state.revision.to_string()
                    };
                    assert_eq!(row.values[0], Value::Text(expected));
                    if done.load(Ordering::Acquire) {
                        break;
                    }
                }
            });
        }
    });
}

#[test]
fn poisoned_writer_makes_fresh_unprivileged_snapshots_fail_closed() {
    let (db, _) = fixture();
    let poisoned = db.clone();
    assert!(
        std::thread::spawn(move || {
            let _head = poisoned.head.lock().unwrap();
            panic!("poison writer publication");
        })
        .join()
        .is_err()
    );
    assert!(matches!(
        db.session("reader").snapshot().scan("core.Person"),
        Err(Error::Denied)
    ));
}

#[test]
fn fresh_snapshot_authority_is_shared_only_for_the_exact_source_and_principal() {
    let (db, id) = fixture();
    let session = db.session("reader");
    let first = session.snapshot();
    let second = session.snapshot();
    assert!(Arc::ptr_eq(&first.live.0, &second.live.0));
    let mut sibling = session.clone();
    sibling.principal = Some("stranger".into());
    assert!(matches!(
        sibling.snapshot().get("core.Person", id),
        Err(Error::Denied)
    ));
    assert_eq!(
        session
            .snapshot()
            .get("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        "Ada".into()
    );
    let mut write = db.admin().transaction();
    write
        .update("core.Person", id, vec!["Grace".into()])
        .unwrap();
    write.commit().unwrap();
    let latest = session.snapshot();
    assert!(!Arc::ptr_eq(&first.live.0, &latest.live.0));
    assert_eq!(
        latest.get("core.Person", id).unwrap().unwrap().values[0],
        "Grace".into()
    );
    assert_eq!(
        first.get("core.Person", id).unwrap().unwrap().values[0],
        "Ada".into()
    );
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    assert!(matches!(first.get("core.Person", id), Err(Error::Denied)));
    assert!(matches!(
        session.snapshot().get("core.Person", id),
        Err(Error::Denied)
    ));
}

#[test]
fn authority_cache_does_not_retain_old_row_state() {
    let (db, id) = fixture();
    let session = db.session("reader");
    let old = session.snapshot();
    let state = Arc::downgrade(&old.state);
    drop(old);
    let mut write = db.admin().transaction();
    write
        .update("core.Person", id, vec!["Grace".into()])
        .unwrap();
    write.commit().unwrap();
    // The session still caches the old authority, but only a weak row-state key.
    assert!(state.upgrade().is_none());
    assert_eq!(
        session
            .snapshot()
            .get("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        "Grace".into()
    );
}

#[test]
fn transaction_expiry_mutation_cannot_change_shared_snapshot_authority() {
    let (db, id) = fixture();
    db.clock.set(100);
    let session = db.session("reader");
    let reader = session.snapshot();
    let mut transaction = session.transaction();
    assert!(Arc::ptr_eq(&reader.live.0, &transaction.live.0));
    transaction.expire_at(Some(101));
    assert!(!Arc::ptr_eq(&reader.live.0, &transaction.live.0));
    db.clock.set(101);
    assert!(matches!(
        transaction.get("core.Person", id),
        Err(Error::Denied)
    ));
    assert!(reader.get("core.Person", id).unwrap().is_some());
    assert!(session.snapshot().get("core.Person", id).unwrap().is_some());
}

#[test]
fn session_point_reads_are_fresh_and_preserve_retained_snapshot_semantics() {
    let (db, id) = fixture();
    let session = db.session("reader");
    let retained = session.snapshot();
    for name in ["Grace", "Lin", "Ada"] {
        let mut tx = db.admin().transaction();
        tx.update("core.Person", id, vec![name.into()]).unwrap();
        tx.commit().unwrap();
        for _ in 0..2 {
            assert_eq!(
                session.get("core.Person", id).unwrap().unwrap().values[0],
                name.into()
            );
        }
    }
    assert_eq!(
        retained.get("core.Person", id).unwrap().unwrap().values[0],
        "Ada".into()
    );
    assert!(matches!(
        session.get("missing.Secret", id),
        Err(Error::Denied)
    ));
    assert!(session.get("core.Person", Id::new()).unwrap().is_none());
    let mut revoke = db.admin().transaction();
    revoke.revoke("reader", "core.Person").unwrap();
    revoke.commit().unwrap();
    assert!(matches!(session.get("core.Person", id), Err(Error::Denied)));
}

#[test]
fn session_point_reads_preserve_filters_and_private_reference_closures() {
    let (db, ada) = fixture();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::fitness(), Visibility::Private)
        .unwrap();
    let bob = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    let goal = tx
        .insert("fitness.TrainingGoal", vec![Value::Ref(bob), "run".into()])
        .unwrap();
    let mut grant = Grant::read_only();
    grant.filter = RowFilter::Equals {
        role: "name".into(),
        value: "Ada".into(),
    };
    tx.grant("reader", "core.Person", grant).unwrap();
    tx.grant("reader", "fitness.TrainingGoal", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let session = db.session("reader");
    for _ in 0..2 {
        assert!(session.get("core.Person", ada).unwrap().is_some());
        assert!(session.get("core.Person", bob).unwrap().is_none());
        assert!(session.get("fitness.TrainingGoal", goal).unwrap().is_none());
    }
}

#[test]
fn ordinary_scalar_reads_reuse_grants_without_narrowing_ambient_visibility() {
    let (db, ada) = fixture();
    let mut tx = db.admin().transaction();
    let bob = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.commit().unwrap();
    let reader = db.session("reader");
    let other = db.session("other");
    let anonymous = db.anonymous();
    for explicit_read in [true, false] {
        for visibility in [
            Visibility::Private,
            Visibility::Authenticated,
            Visibility::Public,
        ] {
            let mut tx = db.admin().transaction();
            tx.grant(
                "reader",
                "core.Person",
                Grant {
                    read: explicit_read,
                    write: true,
                    filter: RowFilter::Equals {
                        role: "name".into(),
                        value: "Ada".into(),
                    },
                    ..Grant::read_only()
                },
            )
            .unwrap();
            tx.set_schema_visibility("core.Person", visibility).unwrap();
            tx.commit().unwrap();
            // The first access resolves changed authority; repetitions exercise
            // the ordinary scalar path with its single compiled grant lookup.
            for _ in 0..3 {
                if visibility == Visibility::Private && !explicit_read {
                    assert!(matches!(reader.get("core.Person", ada), Err(Error::Denied)));
                    assert!(matches!(reader.get("core.Person", bob), Err(Error::Denied)));
                } else {
                    assert!(reader.get("core.Person", ada).unwrap().is_some());
                    assert_eq!(
                        reader.get("core.Person", bob).unwrap().is_some(),
                        visibility != Visibility::Private
                    );
                }
                if visibility == Visibility::Private {
                    assert!(matches!(other.get("core.Person", bob), Err(Error::Denied)));
                } else {
                    assert!(other.get("core.Person", bob).unwrap().is_some());
                }
                if visibility == Visibility::Public {
                    assert!(anonymous.get("core.Person", bob).unwrap().is_some());
                } else {
                    assert!(matches!(
                        anonymous.get("core.Person", bob),
                        Err(Error::Denied)
                    ));
                }
            }
        }
    }
}

#[test]
fn session_point_reads_keep_anonymous_visibility_and_fail_closed() {
    let (db, id) = fixture();
    let anonymous = db.anonymous();
    assert!(matches!(
        anonymous.get("core.Person", id),
        Err(Error::Denied)
    ));
    let mut tx = db.admin().transaction();
    tx.set_schema_visibility("core.Person", Visibility::Public)
        .unwrap();
    tx.commit().unwrap();
    assert!(anonymous.get("core.Person", id).unwrap().is_some());
    assert!(anonymous.get("core.Person", id).unwrap().is_some());
    db.replication_fail();
    assert!(matches!(
        anonymous.get("core.Person", id),
        Err(Error::Denied)
    ));

    let (db, id) = fixture();
    let reader = db.session("reader");
    reader.get("core.Person", id).unwrap();
    let poisoned = db.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned.head.lock().unwrap();
            panic!("poison head for borrowed read");
        })
        .join()
        .is_err()
    );
    assert!(matches!(reader.get("core.Person", id), Err(Error::Denied)));
}

#[test]
fn borrowed_authority_release_fence_rejects_concurrent_revocation() {
    let (db, id) = fixture();
    let session = db.session("reader");
    session.get("core.Person", id).unwrap();
    let captured = db.publication.load();
    let authority = session.authority_cache.load();
    let guard = &authority.as_ref().unwrap().guard;
    assert!(guard.check_borrowed(&db.head, &db.publication).is_ok());
    let tentative = captured
        .state
        .row(captured.state.names["core.Person"], id)
        .unwrap();
    let writer = db.clone();
    std::thread::spawn(move || {
        let mut tx = writer.admin().transaction();
        tx.revoke("reader", "core.Person").unwrap();
        tx.commit().unwrap();
    })
    .join()
    .unwrap();
    assert_eq!(tentative.values[0], "Ada".into());
    // Holding old borrowed guards must never let a result cross the new fence.
    assert!(matches!(
        guard.check_borrowed(&db.head, &db.publication),
        Err(Error::Denied)
    ));
    assert!(matches!(session.get("core.Person", id), Err(Error::Denied)));
}

#[test]
fn session_point_reads_keep_credential_scope_rotation_and_expiry() {
    use crate::capability::{CapabilityScope, RelationCapability};
    let (db, id) = fixture();
    db.clock.set(100);
    let mut tx = db.admin().transaction();
    let hidden = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.set_schema_visibility("core.Person", Visibility::Public)
        .unwrap();
    tx.commit().unwrap();
    let mut grant = Grant::read_only();
    grant.filter = RowFilter::Equals {
        role: "name".into(),
        value: "Ada".into(),
    };
    let cap = db
        .issue_capability(
            Id::new(),
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                    grant,
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let token = db.issue_credential("reader", cap.id, 200).unwrap();
    let session = db.authenticate(&token.token).unwrap();
    for _ in 0..2 {
        assert!(session.get("core.Person", id).unwrap().is_some());
        assert!(session.get("core.Person", hidden).unwrap().is_none());
    }
    let rotated = db.rotate_credential(token.id, 200).unwrap();
    assert!(matches!(session.get("core.Person", id), Err(Error::Denied)));
    let session = db.authenticate(&rotated.token).unwrap();
    assert!(session.get("core.Person", id).unwrap().is_some());
    db.clock.set(200);
    assert!(matches!(session.get("core.Person", id), Err(Error::Denied)));
}

fn scoped_reader(db: &Database, name: &str) -> (Session, Id) {
    use crate::capability::{CapabilityScope, RelationCapability};
    let cap = db
        .issue_capability(
            Id::new(),
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                    grant: Grant {
                        filter: RowFilter::Equals {
                            role: "name".into(),
                            value: name.into(),
                        },
                        ..Grant::read_only()
                    },
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    (
        db.session("reader").with_capability(cap.id).unwrap(),
        cap.id,
    )
}

#[test]
fn scoped_scalar_cache_keeps_scope_keys_and_retains_no_row_state() {
    let (db, ada) = fixture();
    let mut tx = db.admin().transaction();
    let bob = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.set_schema_visibility("core.Person", Visibility::Public)
        .unwrap();
    tx.commit().unwrap();
    let (_, cap) = scoped_reader(&db, "Ada");
    let (_, other_cap) = scoped_reader(&db, "Bob");
    let base = db.session("reader");
    let session = base.clone().with_capability(cap).unwrap();
    let other = base.clone().with_capability(other_cap).unwrap();
    // These sessions share the authority cache but have different scopes. The
    // ordinary owned path must not reuse a scoped cache entry, or vice versa.
    for _ in 0..3 {
        assert!(session.get("core.Person", ada).unwrap().is_some());
        assert!(session.get_shared("core.Person", bob).unwrap().is_none());
        assert!(base.get("core.Person", bob).unwrap().is_some());
        assert!(session.get("core.Person", bob).unwrap().is_none());
        assert!(other.get("core.Person", bob).unwrap().is_some());
        assert!(other.get_shared("core.Person", ada).unwrap().is_none());
        assert!(base.get("core.Person", ada).unwrap().is_some());
    }
    session.get("core.Person", ada).unwrap();
    let old = Arc::downgrade(&session.snapshot().state);
    let original = session
        .authority_cache
        .load()
        .as_ref()
        .unwrap()
        .guard
        .authorized_state
        .clone();
    let mut tx = db.admin().transaction();
    tx.update("core.Person", bob, vec!["Robert".into()])
        .unwrap();
    tx.commit().unwrap();
    // Replace the existing scoped snapshot cache, leaving the point authority
    // cache untouched. Its compiled policy must not keep that old overlay alive.
    drop(session.snapshot());
    assert!(old.upgrade().is_none());
    assert!(original.upgrade().is_none());
    assert!(session.get("core.Person", ada).unwrap().is_some());
    assert!(other.get("core.Person", bob).unwrap().is_none());
}

#[test]
fn shared_point_results_do_not_copy_payload_or_mutate_after_publication() {
    let (db, id) = fixture();
    let payload = "x".repeat(64 * 1024);
    let mut tx = db.admin().transaction();
    tx.update("core.Person", id, vec![Value::Text(payload.clone())])
        .unwrap();
    tx.commit().unwrap();
    let reader = db.session("reader");
    let first = reader.get_shared("core.Person", id).unwrap().unwrap();
    let second = reader.get_shared("core.Person", id).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let current = db.publication.load();
    assert!(Arc::ptr_eq(
        &first,
        current
            .state
            .row(current.state.names["core.Person"], id)
            .unwrap()
    ));
    drop(current);
    let mut tx = db.admin().transaction();
    tx.update("core.Person", id, vec!["updated".into()])
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(first.values[0], Value::Text(payload));
    assert_eq!(
        reader
            .get_shared("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        "updated".into()
    );
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        reader.get_shared("core.Person", id),
        Err(Error::Denied)
    ));
}

#[test]
fn scoped_point_result_mapping_is_fenced_after_revocation_on_cold_and_warm_paths() {
    for warm in [false, true] {
        let (db, id) = fixture();
        let (session, cap) = scoped_reader(&db, "Ada");
        if warm {
            session.get("core.Person", id).unwrap();
            assert!(
                session
                    .authority_cache
                    .load()
                    .as_ref()
                    .unwrap()
                    .capability
                    .is_some()
            );
        }
        let result = session.get_point("core.Person", id, |row| {
            db.revoke_capability(cap).unwrap();
            row.clone()
        });
        assert!(matches!(result, Err(Error::Denied)));
        assert!(matches!(
            session.get_shared("core.Person", id),
            Err(Error::Denied)
        ));
    }
}

#[test]
fn scoped_shared_reads_cannot_expose_private_referenced_rows() {
    use crate::capability::{CapabilityScope, RelationCapability};
    let (db, ada) = fixture();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::fitness(), Visibility::Public)
        .unwrap();
    let goal = tx
        .insert("fitness.TrainingGoal", vec![ada.into(), "run".into()])
        .unwrap();
    tx.grant("reader", "fitness.TrainingGoal", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let cap = db
        .issue_capability(
            Id::new(),
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: db
                        .admin()
                        .snapshot()
                        .schema("fitness.TrainingGoal")
                        .unwrap()
                        .id,
                    grant: Grant::read_only(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let reader = db.session("reader").with_capability(cap.id).unwrap();
    for _ in 0..2 {
        assert!(
            reader
                .get_shared("fitness.TrainingGoal", goal)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            reader.get_shared("core.Person", ada),
            Err(Error::Denied)
        ));
    }
}

#[test]
fn credential_expiry_during_result_copy_denies_cold_and_cached_point_reads() {
    for warm in [false, true] {
        let (db, id) = fixture();
        db.clock.set(100);
        let (_, cap) = scoped_reader(&db, "Ada");
        let credential = db.issue_credential("reader", cap, 200).unwrap();
        let reader = db.authenticate(&credential.token).unwrap();
        if warm {
            reader.get("core.Person", id).unwrap();
        }
        assert!(matches!(
            reader.get_point("core.Person", id, |row| {
                db.clock.set(200);
                row.clone()
            }),
            Err(Error::Denied)
        ));
    }
}
