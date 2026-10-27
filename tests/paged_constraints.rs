use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};
fn schema(cascade: bool) -> Module {
    Module::new(
        "fk",
        1,
        vec![],
        vec![
            Relation::new("fk.Parent", vec![Role::new("n", DataType::Int)]),
            Relation::new(
                "fk.Child",
                vec![
                    Role::new("parent", DataType::Ref("fk.Parent".into()).nullable())
                        .deferrable()
                        .on_delete(if cascade {
                            OnDelete::Cascade
                        } else {
                            OnDelete::Restrict
                        }),
                ],
            ),
        ],
    )
}
fn open(dir: &tempfile::TempDir) -> PagedDatabase {
    PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap()
}
#[test]
fn staged_references_require_reference_grant_without_requiring_read_access() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let grant = |reference| Grant {
        read: false,
        write: true,
        reference,
        filter: RowFilter::All,
    };
    let mut setup = db.admin().transaction().unwrap();
    setup
        .install_owned(schema(false), "owner", Visibility::Private)
        .unwrap();
    setup.grant("app", "fk.Parent", grant(false)).unwrap();
    setup.grant("app", "fk.Child", grant(false)).unwrap();
    setup.commit().unwrap();
    let app = db.session("app").unwrap();
    let parent = Id::from_u128(1);
    let child = Id::from_u128(2);
    let mut denied = app.transaction().unwrap();
    denied
        .insert_with_id("fk.Parent", parent, vec![1i64.into()])
        .unwrap();
    assert!(matches!(
        denied.insert_with_id("fk.Child", child, vec![parent.into()]),
        Err(Error::Denied)
    ));
    assert!(matches!(denied.commit(), Err(Error::Aborted)));
    assert!(db.admin().get("fk.Parent", parent).unwrap().is_none());

    let mut setup = db.admin().transaction().unwrap();
    setup.grant("app", "fk.Parent", grant(true)).unwrap();
    setup.commit().unwrap();
    let mut allowed = app.transaction().unwrap();
    allowed
        .insert_with_id("fk.Parent", parent, vec![1i64.into()])
        .unwrap();
    allowed
        .insert_with_id("fk.Child", child, vec![parent.into()])
        .unwrap();
    allowed.commit().unwrap();
    assert!(matches!(app.get("fk.Parent", parent), Err(Error::Denied)));
    assert_eq!(
        db.admin().get("fk.Child", child).unwrap().unwrap().values[0],
        Value::Ref(parent)
    );

    let removed = Id::from_u128(3);
    let mut deleted = app.transaction().unwrap();
    deleted
        .insert_with_id("fk.Parent", removed, vec![3i64.into()])
        .unwrap();
    deleted.delete("fk.Parent", removed).unwrap();
    assert!(matches!(
        deleted.insert_with_id("fk.Child", Id::from_u128(4), vec![removed.into()]),
        Err(Error::Denied)
    ));
    assert!(matches!(deleted.commit(), Err(Error::Aborted)));
    assert!(db.admin().get("fk.Parent", removed).unwrap().is_none());
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn paged_deferred_forward_refs_commit_atomically_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let mut tx = db.admin().transaction().unwrap();
    tx.install(schema(false)).unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.defer_constraints().unwrap();
    tx.insert_with_id("fk.Child", Id::from_u128(2), vec![Id::from_u128(1).into()])
        .unwrap();
    tx.insert_with_id("fk.Parent", Id::from_u128(1), vec![1i64.into()])
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.defer_constraints().unwrap();
    tx.insert_with_id("fk.Child", Id::from_u128(3), vec![Id::from_u128(99).into()])
        .unwrap();
    assert!(tx.commit().is_err());
    assert!(
        db.admin()
            .snapshot()
            .unwrap()
            .get("fk.Child", Id::from_u128(3))
            .unwrap()
            .is_none()
    );
    drop(db);
    let db = open(&dir);
    db.admin().snapshot().unwrap().verify().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.defer_constraints().unwrap();
    tx.delete("fk.Parent", Id::from_u128(1)).unwrap();
    tx.update("fk.Child", Id::from_u128(2), vec![Value::Null])
        .unwrap();
    tx.commit().unwrap();
    db.admin().snapshot().unwrap().verify().unwrap();
}
#[test]
fn paged_cascade_requires_each_write_permission_and_keeps_pinned_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let mut tx = db.admin().transaction().unwrap();
    tx.install(schema(true)).unwrap();
    tx.insert_with_id("fk.Parent", Id::from_u128(1), vec![1i64.into()])
        .unwrap();
    tx.insert_with_id("fk.Child", Id::from_u128(2), vec![Id::from_u128(1).into()])
        .unwrap();
    tx.grant("app", "fk.Parent", Grant::read_write()).unwrap();
    tx.grant("app", "fk.Child", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    let old = db.admin().snapshot().unwrap();
    let mut tx = db.session("app").unwrap().transaction().unwrap();
    assert!(matches!(
        tx.delete("fk.Parent", Id::from_u128(1)),
        Err(Error::Denied)
    ));
    assert!(tx.commit().is_err());
    assert!(
        db.admin()
            .snapshot()
            .unwrap()
            .get("fk.Parent", Id::from_u128(1))
            .unwrap()
            .is_some()
    );
    let mut tx = db.admin().transaction().unwrap();
    tx.grant("app", "fk.Child", Grant::read_write()).unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("app").unwrap().transaction().unwrap();
    tx.delete("fk.Parent", Id::from_u128(1)).unwrap();
    tx.commit().unwrap();
    let now = db.admin().snapshot().unwrap();
    assert!(now.get("fk.Parent", Id::from_u128(1)).unwrap().is_none());
    assert!(now.get("fk.Child", Id::from_u128(2)).unwrap().is_none());
    now.verify().unwrap();
    assert!(old.get("fk.Child", Id::from_u128(2)).unwrap().is_some());
}
#[test]
fn paged_cascade_conflicts_with_new_incoming_reference() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let mut tx = db.admin().transaction().unwrap();
    tx.install(schema(true)).unwrap();
    tx.insert_with_id("fk.Parent", Id::from_u128(1), vec![1i64.into()])
        .unwrap();
    tx.commit().unwrap();
    let mut deletion = db.admin().transaction().unwrap();
    deletion.delete("fk.Parent", Id::from_u128(1)).unwrap();
    let mut insertion = db.admin().transaction().unwrap();
    insertion
        .insert("fk.Child", vec![Id::from_u128(1).into()])
        .unwrap();
    insertion.commit().unwrap();
    assert!(matches!(deletion.commit(), Err(Error::Conflict)));
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn paged_cascade_cycle_is_atomic_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new(
        "cycle",
        1,
        vec![],
        vec![Relation::new(
            "cycle.Node",
            vec![
                Role::new("next", DataType::Ref("cycle.Node".into()))
                    .deferrable()
                    .on_delete(OnDelete::Cascade),
            ],
        )],
    ))
    .unwrap();
    tx.defer_constraints().unwrap();
    for (id, next) in [(1, 2), (2, 3), (3, 1)] {
        tx.insert_with_id(
            "cycle.Node",
            Id::from_u128(id),
            vec![Id::from_u128(next).into()],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    // The persisted graph can exceed a single write's configured cascade budget.
    let options = PagedOptions {
        max_transaction_rows: 2,
        ..PagedOptions::default()
    };
    let db = PagedDatabase::open(dir.path().join("db"), options).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    assert!(matches!(
        tx.delete("cycle.Node", Id::from_u128(1)),
        Err(Error::Constraint(_))
    ));
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    for id in 1..=3 {
        assert!(
            db.admin()
                .snapshot()
                .unwrap()
                .get("cycle.Node", Id::from_u128(id))
                .unwrap()
                .is_some()
        );
    }
    drop(db);
    let db = open(&dir);
    let mut tx = db.admin().transaction().unwrap();
    tx.delete("cycle.Node", Id::from_u128(1)).unwrap();
    tx.commit().unwrap();
    for id in 1..=3 {
        assert!(
            db.admin()
                .snapshot()
                .unwrap()
                .get("cycle.Node", Id::from_u128(id))
                .unwrap()
                .is_none()
        );
    }
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn paged_deferral_preserves_reference_permission_and_live_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(schema(false), "owner", Visibility::Private)
        .unwrap();
    tx.insert_with_id("fk.Parent", Id::from_u128(1), vec![1i64.into()])
        .unwrap();
    tx.grant("app", "fk.Child", Grant::read_write()).unwrap();
    tx.grant("app", "fk.Parent", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("app").unwrap().transaction().unwrap();
    tx.defer_constraints().unwrap();
    tx.insert_with_id("fk.Child", Id::from_u128(2), vec![Id::from_u128(1).into()])
        .unwrap();
    assert!(matches!(tx.commit(), Err(Error::Denied)));
    let mut admin = db.admin().transaction().unwrap();
    let mut grant = Grant::read_only();
    grant.reference = true;
    admin.grant("app", "fk.Parent", grant).unwrap();
    admin.commit().unwrap();
    let mut tx = db.session("app").unwrap().transaction().unwrap();
    tx.defer_constraints().unwrap();
    tx.insert_with_id("fk.Child", Id::from_u128(2), vec![Id::from_u128(1).into()])
        .unwrap();
    let mut admin = db.admin().transaction().unwrap();
    admin.grant("app", "fk.Parent", Grant::read_only()).unwrap();
    admin.commit().unwrap();
    assert!(tx.commit().is_err());
    assert!(
        db.admin()
            .snapshot()
            .unwrap()
            .get("fk.Child", Id::from_u128(2))
            .unwrap()
            .is_none()
    );
    db.admin().snapshot().unwrap().verify().unwrap();
}
