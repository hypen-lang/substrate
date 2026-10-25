use substrate_liquid::{partition::*, *};

fn map() -> RoutingMap {
    RoutingMap::new(Id::from_u128(100), vec![Id::from_u128(1), Id::from_u128(2)]).unwrap()
}
fn provision(db: &Database) {
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "app",
            1,
            vec![],
            vec![
                Relation::new("app.Person", vec![Role::new("name", DataType::Text)]),
                Relation::new(
                    "app.Friend",
                    vec![
                        Role::new("left", DataType::Ref("app.Person".into())),
                        Role::new("right", DataType::Ref("app.Person".into())),
                    ],
                ),
            ],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    for name in ["app.Person", "app.Friend"] {
        tx.grant("writer", name, Grant::read_write()).unwrap();
    }
    tx.commit().unwrap();
}
fn setup() -> (RoutingMap, Database, Database, PartitionRouter) {
    let map = map();
    let a = Database::memory();
    let b = Database::memory();
    provision(&a);
    provision(&b);
    let router = PartitionRouter::bind(
        map.clone(),
        vec![
            (map.partitions()[0], a.session("writer")),
            (map.partitions()[1], b.session("writer")),
        ],
    )
    .unwrap();
    (map, a, b, router)
}

#[test]
fn routing_roundtrip_is_stable_and_allocations_preserve_uuid_v4() {
    for n in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
        let map =
            RoutingMap::new(Id::new(), (0..n).map(|i| Id::from_u128(i + 1)).collect()).unwrap();
        let restored = RoutingMap::from_json(&map.to_json()).unwrap();
        assert_eq!(map.fingerprint(), restored.fingerprint());
        for partition in map.partitions() {
            for _ in 0..8 {
                let id = map.allocate(*partition).unwrap();
                assert_eq!(map.partition_for(id), *partition);
                assert_eq!(restored.partition_for(id), *partition);
                assert_eq!(id.0.as_bytes()[6] >> 4, 4);
                assert_eq!(id.0.as_bytes()[8] >> 6, 2);
            }
        }
    }
}

#[test]
fn invalid_versions_counts_duplicates_bindings_and_changed_topology_fail() {
    assert!(RoutingMap::new(Id::new(), vec![]).is_err());
    assert!(RoutingMap::new(Id::new(), vec![Id::new(); 2]).is_err());
    assert!(RoutingMap::new(Id::new(), (0..3).map(Id::from_u128).collect()).is_err());
    let map = map();
    assert!(
        RoutingMap::from_json(
            &map.to_json()
                .replace("\"format_version\":1", "\"format_version\":2")
        )
        .is_err()
    );
    let changed =
        RoutingMap::new(map.id(), map.partitions().iter().copied().rev().collect()).unwrap();
    assert!(changed.check_fingerprint(map.fingerprint()).is_err());
    let db = Database::memory();
    assert!(PartitionRouter::bind(map.clone(), vec![]).is_err());
    assert!(PartitionRouter::bind(map.clone(), vec![(Id::new(), db.admin())]).is_err());
    assert!(
        PartitionRouter::bind(
            map.clone(),
            vec![
                (map.partitions()[0], db.admin()),
                (map.partitions()[0], db.admin())
            ]
        )
        .is_err()
    );
}

#[test]
fn local_relationships_are_atomic_and_partitions_remain_independent() {
    let (map, a, b, router) = setup();
    let mut tx = router.partition(map.partitions()[0]).unwrap().transaction();
    let ada = tx.insert("app.Person", vec!["Ada".into()]).unwrap();
    let bob = tx.insert("app.Person", vec!["Bob".into()]).unwrap();
    tx.insert("app.Friend", vec![ada.into(), bob.into()])
        .unwrap();
    assert_eq!(a.admin().snapshot().scan("app.Person").unwrap().len(), 0);
    tx.commit().unwrap();
    assert_eq!(
        router
            .for_identity(ada)
            .snapshot()
            .get("app.Person", ada)
            .unwrap()
            .unwrap()
            .values[0],
        "Ada".into()
    );
    assert_eq!(a.admin().snapshot().scan("app.Person").unwrap().len(), 2);
    assert!(b.admin().snapshot().scan("app.Person").unwrap().is_empty());
    assert_eq!(
        router
            .transaction_for(&[ada, bob])
            .unwrap()
            .lookup("app.Person", "name", &"Ada".into())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn ignored_cross_partition_reference_error_aborts_preceding_writes() {
    let (map, a, _, router) = setup();
    let left = router.partition(map.partitions()[0]).unwrap();
    let foreign = map.allocate(map.partitions()[1]).unwrap();
    let mut tx = left.transaction();
    let local = tx.insert("app.Person", vec!["Ada".into()]).unwrap();
    assert!(matches!(
        tx.insert("app.Friend", vec![local.into(), foreign.into()]),
        Err(Error::Constraint(_))
    ));
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    assert!(a.admin().snapshot().scan("app.Person").unwrap().is_empty());
    assert!(router.transaction_for(&[local, foreign]).is_err());
    assert!(router.transaction_for(&[]).is_err());
    let mut tx = left.transaction();
    assert!(
        tx.insert_with_id("app.Person", foreign, vec!["Other".into()])
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}

#[test]
fn wrong_partition_updates_deletes_and_reads_are_rejected() {
    let (map, _, _, router) = setup();
    let a = router.partition(map.partitions()[0]).unwrap();
    let foreign = map.allocate(map.partitions()[1]).unwrap();
    assert!(a.snapshot().get("app.Person", foreign).is_err());
    assert!(
        a.snapshot()
            .lookup("app.Friend", "left", &foreign.into())
            .is_err()
    );
    let mut tx = a.transaction();
    assert!(
        tx.update("app.Person", foreign, vec!["Wrong".into()])
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    let mut tx = a.transaction();
    assert!(tx.delete("app.Person", foreign).is_err());
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}

#[test]
fn wrapper_retains_authorization_and_live_revocation() {
    let (map, a, b, router) = setup();
    let session = router.partition(map.partitions()[0]).unwrap();
    let snapshot = session.snapshot();
    let mut pending = session.transaction();
    pending.insert("app.Person", vec!["Ada".into()]).unwrap();
    let mut admin = a.admin().transaction();
    admin.revoke("writer", "app.Person").unwrap();
    admin.commit().unwrap();
    assert!(matches!(snapshot.scan("app.Person"), Err(Error::Denied)));
    assert!(matches!(
        pending.commit(),
        Err(Error::Denied | Error::Conflict)
    ));
    let denied = PartitionRouter::bind(
        map.clone(),
        vec![
            (map.partitions()[0], a.session("nobody")),
            (map.partitions()[1], b.session("nobody")),
        ],
    )
    .unwrap();
    assert!(matches!(
        denied
            .partition(map.partitions()[0])
            .unwrap()
            .transaction()
            .insert("app.Person", vec!["Ada".into()]),
        Err(Error::Denied)
    ));
}

#[test]
fn host_misplaced_rows_are_not_returned_by_wrapped_reads() {
    let (map, a, _, router) = setup();
    let wrong = map.allocate(map.partitions()[1]).unwrap();
    let mut tx = a.admin().transaction();
    tx.insert_with_id("app.Person", wrong, vec!["Misplaced".into()])
        .unwrap();
    tx.commit().unwrap();
    let session = router.partition(map.partitions()[0]).unwrap();
    assert!(session.snapshot().scan("app.Person").is_err());
    let mut tx = session.transaction();
    assert!(tx.scan("app.Person").is_err());
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}

#[test]
fn durable_partitions_reopen_with_pinned_map_and_original_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path_a = dir.path().join("a.redb");
    let path_b = dir.path().join("b.redb");
    let map = map();
    let saved = map.to_json();
    let pin = map.fingerprint();
    let identity;
    {
        let a = Database::open(&path_a).unwrap();
        let b = Database::open(&path_b).unwrap();
        provision(&a);
        provision(&b);
        let router = PartitionRouter::bind(
            map.clone(),
            vec![
                (map.partitions()[0], a.session("writer")),
                (map.partitions()[1], b.session("writer")),
            ],
        )
        .unwrap();
        let mut tx = router.partition(map.partitions()[0]).unwrap().transaction();
        identity = tx.insert("app.Person", vec!["Ada".into()]).unwrap();
        tx.commit().unwrap();
        a.checkpoint().unwrap();
    }
    let restored = RoutingMap::from_json(&saved).unwrap();
    restored.check_fingerprint(pin).unwrap();
    let a = Database::open(&path_a).unwrap();
    let b = Database::open(&path_b).unwrap();
    let router = PartitionRouter::bind(
        restored,
        vec![
            (map.partitions()[0], a.session("writer")),
            (map.partitions()[1], b.session("writer")),
        ],
    )
    .unwrap();
    assert_eq!(
        router
            .for_identity(identity)
            .snapshot()
            .get("app.Person", identity)
            .unwrap()
            .unwrap()
            .values[0],
        "Ada".into()
    );
}
