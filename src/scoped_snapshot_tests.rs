use super::*;
use crate::capability::{CapabilityScope, RelationCapability};

fn fixture() -> (Database, Session, Id, Id) {
    let db = Database::memory();
    db.clock.set(100);
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)
        .unwrap();
    tx.grant("owner", "core.Person", Grant::read_write())
        .unwrap();
    let row = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.commit().unwrap();
    let relation = db.admin().snapshot().schema("core.Person").unwrap().id;
    let cap = db
        .issue_capability_until(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation,
                    grant: Grant::read_write(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
            Some(200),
        )
        .unwrap();
    let session = db.session("owner").with_capability(cap.id).unwrap();
    (db, session, row, cap.id)
}

#[test]
fn scoped_snapshot_reuses_only_the_same_published_revision() {
    let (db, session, id, _) = fixture();
    let first = session.snapshot();
    let repeated = session.clone().snapshot();
    assert!(Arc::ptr_eq(&first.state, &repeated.state));
    let mut write = db.admin().transaction();
    write
        .update("core.Person", id, vec!["Grace".into()])
        .unwrap();
    write.commit().unwrap();
    let current = session.snapshot();
    assert!(!Arc::ptr_eq(&first.state, &current.state));
    assert_eq!(
        first.get("core.Person", id).unwrap().unwrap().values[0],
        "Ada".into()
    );
    assert_eq!(
        current.get("core.Person", id).unwrap().unwrap().values[0],
        "Grace".into()
    );
}

#[test]
fn contended_scope_cache_falls_back_without_waiting_or_reusing_stale_authority() {
    let (db, session, id, cap) = fixture();
    let cached = session.snapshot();
    // Hold the optimization lock on this same thread: snapshot must never wait
    // for it, even on a cache hit or after the published state changes.
    let _held = session.scoped_snapshot.as_ref().unwrap().lock().unwrap();
    let fallback = session.snapshot();
    assert!(!Arc::ptr_eq(&cached.state, &fallback.state));
    assert_eq!(
        fallback.get("core.Person", id).unwrap(),
        cached.get("core.Person", id).unwrap()
    );
    let mut write = db.admin().transaction();
    write
        .update("core.Person", id, vec!["Grace".into()])
        .unwrap();
    write.commit().unwrap();
    assert_eq!(
        session
            .snapshot()
            .get("core.Person", id)
            .unwrap()
            .unwrap()
            .values[0],
        "Grace".into()
    );
    db.revoke_capability(cap).unwrap();
    assert!(matches!(
        session.snapshot().scan("core.Person"),
        Err(Error::Denied)
    ));
}

#[test]
fn cached_scope_still_checks_expiry_and_fail_stop() {
    let (db, session, _, _) = fixture();
    let first = session.snapshot();
    db.clock.set(200);
    let expired = session.snapshot();
    assert!(Arc::ptr_eq(&first.state, &expired.state));
    assert!(matches!(expired.scan("core.Person"), Err(Error::Denied)));
    db.clock.set(100);
    db.head.lock().unwrap().fail();
    assert!(matches!(
        session.snapshot().scan("core.Person"),
        Err(Error::Denied)
    ));
    assert!(matches!(first.scan("core.Person"), Err(Error::Denied)));
}

#[test]
fn cached_scope_cannot_survive_revocation_or_credential_rotation() {
    let (db, session, _, cap) = fixture();
    let issued = db.issue_credential("owner", cap, 180).unwrap();
    let authenticated = db.authenticate(&issued.token).unwrap();
    let old = authenticated.snapshot();
    assert!(Arc::ptr_eq(&old.state, &authenticated.snapshot().state));
    let rotated = db.rotate_credential(issued.id, 190).unwrap();
    assert!(matches!(old.scan("core.Person"), Err(Error::Denied)));
    assert!(matches!(
        authenticated.snapshot().scan("core.Person"),
        Err(Error::Denied)
    ));
    assert!(
        db.authenticate(&rotated.token)
            .unwrap()
            .snapshot()
            .scan("core.Person")
            .is_ok()
    );
    let before = session.snapshot();
    db.revoke_capability(cap).unwrap();
    assert!(matches!(before.scan("core.Person"), Err(Error::Denied)));
    assert!(matches!(
        session.snapshot().scan("core.Person"),
        Err(Error::Denied)
    ));
}

#[test]
fn sibling_capability_selections_do_not_share_authority() {
    let (db, _, _, _) = fixture();
    let base = db.session("owner");
    let tid = base.snapshot().schema("core.Person").unwrap().id;
    let issue = |name: &str| {
        db.issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: tid,
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
        .unwrap()
    };
    let ada = base.with_capability(issue("Ada").id).unwrap();
    let bob = base.with_capability(issue("Bob").id).unwrap();
    for _ in 0..3 {
        assert_eq!(ada.snapshot().scan("core.Person").unwrap().len(), 1);
        assert!(bob.snapshot().scan("core.Person").unwrap().is_empty());
    }
}
