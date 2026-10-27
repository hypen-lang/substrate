use super::*;
fn id(n: u128) -> Id {
    Id::from_u128(n)
}
fn module() -> Module {
    Module::new(
        "disk",
        1,
        vec![],
        vec![
            Relation::new("disk.Item", vec![Role::new("value", DataType::Int)]).unique(&["value"]),
        ],
    )
}
fn fixture(path: &Path) -> PagedDatabase {
    let db = PagedDatabase::open(
        path,
        PagedOptions {
            decoded_cache_bytes: 512,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(module(), "owner", Visibility::Private)
        .unwrap();
    tx.insert_with_id("disk.Item", id(1), vec![1.into()])
        .unwrap();
    tx.commit().unwrap();
    db
}
#[test]
fn inaccessible_present_and_absent_writes_have_same_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    for n in [1, 99] {
        let mut tx = db.session("stranger").unwrap().transaction().unwrap();
        assert!(matches!(
            tx.update("disk.Item", id(n), vec![2.into()]),
            Err(Error::Denied)
        ));
        let mut tx = db.session("stranger").unwrap().transaction().unwrap();
        assert!(matches!(tx.delete("disk.Item", id(n)), Err(Error::Denied)));
    }
}
#[test]
fn deleted_identity_cannot_be_reallocated_and_absent_read_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let mut old = db.admin().transaction().unwrap();
    assert!(old.get("disk.Item", id(2)).unwrap().is_none());
    let mut tx = db.admin().transaction().unwrap();
    tx.insert_with_id("disk.Item", id(2), vec![2.into()])
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(old.commit(), Err(Error::Conflict)));
    let mut tx = db.admin().transaction().unwrap();
    tx.delete("disk.Item", id(1)).unwrap();
    tx.commit().unwrap();
    drop(db);
    let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    assert!(
        tx.insert_with_id("disk.Item", id(1), vec![9.into()])
            .is_err()
    );
}
#[test]
fn all_ff_relation_id_scan_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
    let mut m = module();
    m.relations[0].id = id(u128::MAX);
    let mut tx = db.admin().transaction().unwrap();
    tx.install(m).unwrap();
    tx.insert_with_id("disk.Item", id(1), vec![1.into()])
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .unwrap()
            .scan_page("disk.Item", None, 4)
            .unwrap()
            .rows
            .len(),
        1
    );
}
#[test]
fn corrupt_row_is_error_and_reopen_does_not_materialize_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    // This test deliberately edits the legacy per-row physical encoding.
    drop(empty_format(&path, PagedFormat::V2));
    let db = fixture(&path);
    let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
    drop(db);
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx.open_table(ROWS).unwrap();
            rows.insert(row_key(tid, id(1)).as_slice(), b"bad".as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    }
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
    assert!(matches!(
        db.admin().get("disk.Item", id(1)),
        Err(Error::Storage(_))
    ));
}
#[test]
fn unknown_format_and_missing_table_fail_closed() {
    for malformed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(fixture(&path));
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            if malformed {
                tx.delete_table(INDEX).unwrap();
            } else {
                let mut meta = tx.open_table(META).unwrap();
                meta.insert("format", b"LPG999".as_slice()).unwrap();
            }
            tx.commit().unwrap();
        }
        assert!(PagedDatabase::open(&path, Default::default()).is_err());
    }
}
#[test]
fn commit_crash_boundaries_preserve_rows_indexes_and_authority() {
    for point in ["paged_before_commit", "paged_after_persist"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(fixture(&path));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "paged::tests::crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("PAGED_TEST_PATH", &path)
            .env("SUBSTRATE_TEST_CRASH_POINT", point)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91));
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let committed = point == "paged_after_persist";
        assert_eq!(
            db.admin().get("disk.Item", id(1)).unwrap().unwrap().values[0],
            Value::Int(if committed { 2 } else { 1 })
        );
        let result = db.anonymous().get("disk.Item", id(1));
        assert_eq!(result.is_ok(), committed);
        assert_eq!(
            db.admin()
                .snapshot()
                .unwrap()
                .lookup_page("disk.Item", "value", &Value::Int(2), None, 2)
                .unwrap()
                .rows
                .len(),
            usize::from(committed)
        );
    }
}
#[test]
#[ignore]
fn crash_child() {
    let Some(path) = std::env::var_os("PAGED_TEST_PATH") else {
        return;
    };
    let db = PagedDatabase::open(Path::new(&path), Default::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.update("disk.Item", id(1), vec![2.into()]).unwrap();
    tx.set_visibility("disk.Item", Visibility::Public).unwrap();
    tx.commit().unwrap();
    panic!("crash hook did not run");
}
#[test]
fn default_host_owner_is_not_an_application_principal() {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install(module()).unwrap();
    tx.insert_with_id("disk.Item", id(1), vec![1.into()])
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("host").unwrap().transaction().unwrap();
    assert!(matches!(
        tx.update("disk.Item", id(1), vec![2.into()]),
        Err(Error::Denied)
    ));
    assert!(db.session(HOST).is_err());
}
#[test]
fn catalog_ownership_corruption_is_rejected() {
    for kind in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        drop(fixture(&path));
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                let mut table = tx.open_table(META).unwrap();
                let mut data: CatalogData =
                    serde_json::from_slice(table.get("catalog").unwrap().unwrap().value()).unwrap();
                match kind {
                    0 => data.owners.clear(),
                    1 => {
                        data.owners.insert("orphan".into(), "owner".into());
                    }
                    2 => {
                        data.owners.insert("disk".into(), String::new());
                    }
                    _ => {
                        data.visibility.values_mut().next().unwrap().0 = "wrong".into();
                    }
                }
                table
                    .insert("catalog", serde_json::to_vec(&data).unwrap().as_slice())
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        assert!(PagedDatabase::open(&path, Default::default()).is_err());
    }
}
#[test]
fn writer_poison_denies_fresh_and_retained_reads() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let snapshot = db.admin().snapshot().unwrap();
    let inner = db.inner.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = inner.writer.lock().unwrap();
            panic!("simulated panic around persistence");
        })
        .join()
        .is_err()
    );
    assert!(matches!(
        snapshot.get("disk.Item", id(1)),
        Err(Error::Storage(_))
    ));
    assert!(db.admin().snapshot().is_err());
}
#[test]
fn unrelated_multimap_database_is_not_initialized() {
    const FOREIGN: redb::MultimapTableDefinition<u64, u64> =
        redb::MultimapTableDefinition::new("foreign");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_multimap_table(FOREIGN)
            .unwrap()
            .insert(1, 2)
            .unwrap();
        tx.commit().unwrap();
    }
    assert!(PagedDatabase::open(&path, Default::default()).is_err());
    let db = redb::Database::create(&path).unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(read.list_tables().unwrap().count(), 0);
    assert!(read.open_multimap_table(FOREIGN).is_ok());
}
#[test]
fn decoded_cache_isolates_pinned_revisions_updates_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let before = db.admin().snapshot().unwrap();
    assert_eq!(
        before.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(1)
    );
    let mut tx = db.admin().transaction().unwrap();
    tx.update("disk.Item", id(1), vec![2.into()]).unwrap();
    tx.commit().unwrap();
    let after = db.admin().snapshot().unwrap();
    assert_eq!(
        after.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(2)
    );
    assert_eq!(
        before.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(1)
    );
    {
        let cache = db.inner.cache.lock().unwrap();
        assert!(
            cache
                .rows
                .keys()
                .any(|(_, _, revision)| *revision == before.revision())
        );
        assert!(
            cache
                .rows
                .keys()
                .any(|(_, _, revision)| *revision == after.revision())
        );
        assert!(cache.bytes <= 512);
    }
    let mut tx = db.admin().transaction().unwrap();
    tx.delete("disk.Item", id(1)).unwrap();
    tx.commit().unwrap();
    assert!(db.admin().get("disk.Item", id(1)).unwrap().is_none());
    assert_eq!(
        after.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(2)
    );
    assert_eq!(
        before.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(1)
    );
}
#[test]
fn impossible_authorization_epoch_and_zero_row_revision_are_rejected() {
    for metadata in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // This test deliberately edits the legacy per-row physical encoding.
        drop(empty_format(&path, PagedFormat::V2));
        let db = fixture(&path);
        let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
        drop(db);
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            if metadata {
                tx.open_table(META)
                    .unwrap()
                    .insert("auth_epoch", u64::MAX.to_le_bytes().as_slice())
                    .unwrap();
            } else {
                let mut rows = tx.open_table(ROWS).unwrap();
                let key = row_key(tid, id(1));
                let mut bytes = rows.get(key.as_slice()).unwrap().unwrap().value().to_vec();
                bytes[4..12].fill(0);
                rows.insert(key.as_slice(), bytes.as_slice()).unwrap();
            }
            tx.commit().unwrap();
        }
        let reopened = PagedDatabase::open(&path, Default::default());
        if metadata {
            assert!(reopened.is_err());
        } else {
            assert!(reopened.unwrap().admin().get("disk.Item", id(1)).is_err());
        }
    }
}

#[test]
fn staged_unique_keys_follow_updates_deletes_and_atomic_reassignment() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let before = db.admin().snapshot().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.update("disk.Item", id(1), vec![2.into()]).unwrap();
    tx.update("disk.Item", id(1), vec![3.into()]).unwrap();
    tx.insert_with_id("disk.Item", id(2), vec![2.into()])
        .unwrap();
    tx.delete("disk.Item", id(2)).unwrap();
    tx.insert_with_id("disk.Item", id(3), vec![2.into()])
        .unwrap();
    tx.insert_with_id("disk.Item", id(4), vec![1.into()])
        .unwrap();
    tx.commit().unwrap();
    let after = db.admin().snapshot().unwrap();
    for (value, expected) in [(1, 4), (2, 3), (3, 1)] {
        let page = after
            .lookup_page("disk.Item", "value", &Value::Int(value), None, 1)
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].id, id(expected));
        assert!(
            after
                .lookup_page("disk.Item", "value", &Value::Int(value), page.next, 1)
                .unwrap()
                .rows
                .is_empty()
        );
    }
    assert_eq!(
        before
            .lookup_page("disk.Item", "value", &1.into(), None, 1)
            .unwrap()
            .rows[0]
            .id,
        id(1)
    );
    assert!(
        before
            .lookup_page("disk.Item", "value", &2.into(), None, 1)
            .unwrap()
            .rows
            .is_empty()
    );
    after.verify().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.insert_with_id("disk.Item", id(5), vec![5.into()])
        .unwrap();
    assert!(
        tx.insert_with_id("disk.Item", id(6), vec![5.into()])
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}

#[test]
fn unique_probe_applies_row_grants_and_snapshot_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.insert_with_id("disk.Item", id(2), vec![2.into()])
        .unwrap();
    tx.grant(
        "reader",
        "disk.Item",
        Grant {
            read: true,
            write: false,
            reference: false,
            filter: RowFilter::Equals {
                role: "value".into(),
                value: 1.into(),
            },
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let reader = db.session("reader").unwrap().snapshot().unwrap();
    assert_eq!(
        reader
            .lookup_page("disk.Item", "value", &1.into(), None, 8)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        reader
            .lookup_page("disk.Item", "value", &2.into(), None, 8)
            .unwrap()
            .rows
            .is_empty()
    );
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "disk.Item").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        reader.lookup_page("disk.Item", "value", &1.into(), None, 8),
        Err(Error::Denied)
    ));
}

#[test]
fn version_coalescing_is_bounded_and_deduplicates_identical_keys() {
    let mut changes = VersionChanges::new(70);
    changes.insert(vec![0; 6]).unwrap();
    changes.insert(vec![0; 6]).unwrap();
    assert_eq!(changes.keys.len(), 1);
    assert!(changes.insert(vec![1]).is_err());
}

#[test]
fn grant_replacement_never_resurrects_capabilities_or_credentials() {
    use crate::capability::{CapabilityScope, RelationCapability};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path);
    let tid = db.admin().snapshot().unwrap().def("disk.Item").unwrap().id;
    let mut tx = db.admin().transaction().unwrap();
    tx.grant("reader", "disk.Item", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    db.issue_capability(
        id(600),
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
    let credential = db
        .issue_credential("reader", id(600), 4_000_000_000)
        .unwrap();
    assert!(db.authenticate(&credential.token).is_ok());
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "disk.Item").unwrap();
    tx.grant("reader", "disk.Item", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    assert!(db.authenticate(&credential.token).is_err());
    assert!(
        db.session("reader")
            .unwrap()
            .with_capability(id(600))
            .is_err()
    );
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    assert!(db.authenticate(&credential.token).is_err());
    assert!(
        db.session("reader")
            .unwrap()
            .with_capability(id(600))
            .is_err()
    );
    assert!(
        db.session("reader")
            .unwrap()
            .get("disk.Item", id(1))
            .unwrap()
            .is_some()
    );
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn synthesized_policy_and_control_type_ids_cannot_alias_application_identities() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    for reserved in [
        crate::auth::TYPE_ID,
        crate::capability::TYPE_ID,
        crate::views::TYPE_ID,
    ] {
        let mut tx = db.admin().transaction().unwrap();
        assert!(
            tx.insert_with_id("disk.Item", reserved, vec![9.into()])
                .is_err()
        );
    }
    let relation = Relation {
        id: id(700),
        name: "next.Item".into(),
        roles: vec![Role::new("value", DataType::Int)],
        unique: vec![],
        checks: vec![],
    };
    let policy_id = authority::visibility_id(relation.id);
    let mut tx = db.admin().transaction().unwrap();
    tx.insert_with_id("disk.Item", policy_id, vec![9.into()])
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    assert!(
        tx.install(Module::new("next", 1, vec![], vec![relation]))
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}

#[test]
fn managed_namespace_owner_cannot_bypass_proposals_with_native_install() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    db.configure_namespace(crate::schema_control::NamespacePolicy {
        namespace: "disk".into(),
        owner: "owner".into(),
        auto_approve: false,
        enabled: true,
        quotas: Default::default(),
    })
    .unwrap();
    let mut tx = db.session("owner").unwrap().transaction().unwrap();
    assert!(matches!(
        tx.install_owned(
            Module::new(
                "disk",
                2,
                vec![],
                vec![Relation::new(
                    "disk.Extension",
                    vec![Role::new("value", DataType::Int)]
                )]
            ),
            "owner",
            Visibility::Private
        ),
        Err(Error::Denied)
    ));
}

#[test]
fn legacy_catalog_without_control_reservations_opens_and_seals_on_install() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path);
    let view = db.inner.head.read().unwrap().clone();
    let internal: Vec<_> = view
        .catalog
        .state
        .identities
        .iter()
        .copied()
        .chain(
            view.catalog
                .state
                .relations
                .keys()
                .filter(|id| authority::reserved_type(**id))
                .copied(),
        )
        .collect();
    let mut wire = serde_json::to_value(&view.catalog.data).unwrap();
    wire.as_object_mut().unwrap().remove("reserved");
    wire.as_object_mut().unwrap().remove("control");
    drop(view);
    let tx = db.inner.db.begin_write().unwrap();
    tx.open_table(META)
        .unwrap()
        .insert("catalog", serde_json::to_vec(&wire).unwrap().as_slice())
        .unwrap();
    for id in internal {
        tx.open_table(IDS)
            .unwrap()
            .remove(id.0.as_bytes().as_slice())
            .unwrap();
        tx.open_table(VERS)
            .unwrap()
            .remove(tagged(b'a', id.0.as_bytes()).as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    db.admin().snapshot().unwrap().verify().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new(
        "extra",
        1,
        vec![],
        vec![Relation::new(
            "extra.Item",
            vec![Role::new("value", DataType::Int)],
        )],
    ))
    .unwrap();
    tx.commit().unwrap();
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    assert!(!snapshot.view.catalog.data.reserved.is_empty());
    snapshot.verify().unwrap();
    assert_eq!(
        snapshot.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(1)
    );
}
#[test]
fn cancellable_paging_checks_denied_candidates_and_reference_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    for n in 2..=20 {
        tx.insert_with_id("disk.Item", id(n), vec![(n as i64).into()])
            .unwrap();
    }
    tx.grant(
        "limited",
        "disk.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "value".into(),
                value: 20.into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.install_owned(
        Module::new(
            "edges",
            1,
            vec![("disk".into(), 1)],
            vec![Relation::new(
                "edges.Link",
                vec![Role::new("target", DataType::Ref("disk.Item".into()))],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    tx.insert_with_id("edges.Link", id(100), vec![Value::Ref(id(1))])
        .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("limited").unwrap().snapshot().unwrap();
    let mut calls = 0;
    let result = snapshot.page_checked("disk.Item", None, None, 1, &mut || {
        calls += 1;
        if calls >= 5 {
            Err(Error::Query("cancelled".into()))
        } else {
            Ok(())
        }
    });
    assert!(matches!(result,Err(Error::Query(message)) if message=="cancelled"));
    assert_eq!(calls, 5);
    let snapshot = db.admin().snapshot().unwrap();
    let mut calls = 0;
    let result = snapshot.get_checked("edges.Link", id(100), &mut || {
        calls += 1;
        if calls >= 3 {
            Err(Error::Query("cancelled".into()))
        } else {
            Ok(())
        }
    });
    assert!(matches!(result,Err(Error::Query(message)) if message=="cancelled"));
    assert_eq!(calls, 3);
}

#[test]
fn packed_row_revisions_preserve_occ_for_present_absent_deleted_and_reextended_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new(
        "anchor",
        1,
        vec![],
        vec![Relation::new(
            "anchor.Row",
            vec![Role::new("value", DataType::Int)],
        )],
    ))
    .unwrap();
    tx.extend("disk.Item", "anchor.Row", id(1), vec![7.into()])
        .unwrap();
    tx.commit().unwrap();
    let pinned = db.admin().snapshot().unwrap();
    let mut stale = db.admin().transaction().unwrap();
    assert!(stale.get("disk.Item", id(1)).unwrap().is_some());
    let mut tx = db.admin().transaction().unwrap();
    tx.update("disk.Item", id(1), vec![2.into()]).unwrap();
    tx.commit().unwrap();
    assert!(matches!(stale.commit(), Err(Error::Conflict)));
    assert_eq!(
        pinned.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(1)
    );

    let mut absent = db.admin().transaction().unwrap();
    assert!(absent.get("disk.Item", id(77)).unwrap().is_none());
    let mut tx = db.admin().transaction().unwrap();
    tx.insert_with_id("disk.Item", id(77), vec![77.into()])
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(absent.commit(), Err(Error::Conflict)));

    let before_delete = db.admin().snapshot().unwrap();
    let mut stale = db.admin().transaction().unwrap();
    stale.get("disk.Item", id(1)).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.delete("disk.Item", id(1)).unwrap();
    tx.commit().unwrap();
    assert!(matches!(stale.commit(), Err(Error::Conflict)));
    let mut absent = db.admin().transaction().unwrap();
    assert!(absent.get("disk.Item", id(1)).unwrap().is_none());
    let mut tx = db.admin().transaction().unwrap();
    tx.extend("anchor.Row", "disk.Item", id(1), vec![3.into()])
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(absent.commit(), Err(Error::Conflict)));
    assert_eq!(
        before_delete
            .get("disk.Item", id(1))
            .unwrap()
            .unwrap()
            .values[0],
        Value::Int(2)
    );

    let mut first = db.admin().transaction().unwrap();
    let mut second = db.admin().transaction().unwrap();
    first.update("disk.Item", id(1), vec![4.into()]).unwrap();
    second.update("disk.Item", id(77), vec![78.into()]).unwrap();
    first.commit().unwrap();
    second.commit().unwrap();
    let mut first = db.admin().transaction().unwrap();
    let mut second = db.admin().transaction().unwrap();
    first.update("disk.Item", id(1), vec![90.into()]).unwrap();
    second.update("disk.Item", id(77), vec![90.into()]).unwrap();
    first.commit().unwrap();
    assert!(matches!(second.commit(), Err(Error::Conflict)));
    let mut first = db.admin().transaction().unwrap();
    let mut second = db.admin().transaction().unwrap();
    first
        .insert_with_id("disk.Item", id(999), vec![999.into()])
        .unwrap();
    second
        .insert_with_id("anchor.Row", id(999), vec![999.into()])
        .unwrap();
    first.commit().unwrap();
    assert!(matches!(second.commit(), Err(Error::Conflict)));

    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new(
        "link",
        1,
        vec![("disk".into(), 1)],
        vec![Relation::new(
            "link.Edge",
            vec![Role::new("target", DataType::Ref("disk.Item".into()))],
        )],
    ))
    .unwrap();
    tx.commit().unwrap();
    let mut deletion = db.admin().transaction().unwrap();
    deletion.delete("disk.Item", id(77)).unwrap();
    let mut insertion = db.admin().transaction().unwrap();
    insertion.insert("link.Edge", vec![id(77).into()]).unwrap();
    insertion.commit().unwrap();
    assert!(matches!(deletion.commit(), Err(Error::Conflict)));
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn legacy_bucket_family_and_redundant_row_versions_remain_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path);
    let view = db.inner.head.read().unwrap().clone();
    let tid = view.catalog.state.names["disk.Item"];
    let row = row_key(tid, id(1));
    let revision = row_revision(view.rows.get(row.as_slice()).unwrap().unwrap().value()).unwrap();
    assert!(
        view.versions
            .get(tagged(b'r', &row).as_slice())
            .unwrap()
            .is_none()
    );
    let family = tagged(b'f', tid.0.as_bytes());
    let bucket = tagged(b'b', &index_prefix(tid, 0, &1.into()));
    assert!(view.versions.get(family.as_slice()).unwrap().is_none());
    assert!(view.versions.get(bucket.as_slice()).unwrap().is_none());
    drop(view);
    let tx = db.inner.db.begin_write().unwrap();
    {
        let mut versions = tx.open_table(VERS).unwrap();
        versions
            .insert(tagged(b'r', &row).as_slice(), revision)
            .unwrap();
        versions.insert(family.as_slice(), revision).unwrap();
        versions.insert(bucket.as_slice(), revision).unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    db.admin().snapshot().unwrap().verify().unwrap();
    let pinned = db.admin().snapshot().unwrap();
    let mut stale = db.admin().transaction().unwrap();
    stale.get("disk.Item", id(1)).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.update("disk.Item", id(1), vec![2.into()]).unwrap();
    tx.commit().unwrap();
    assert!(matches!(stale.commit(), Err(Error::Conflict)));
    assert_eq!(
        pinned.get("disk.Item", id(1)).unwrap().unwrap().values[0],
        Value::Int(1)
    );
    drop(pinned);
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    db.admin().snapshot().unwrap().verify().unwrap();
    let view = db.inner.head.read().unwrap();
    assert!(
        view.versions
            .get(tagged(b'r', &row).as_slice())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        view.versions
            .get(bucket.as_slice())
            .unwrap()
            .unwrap()
            .value(),
        revision
    );
}

#[test]
fn expiry_after_staging_disk_changes_aborts_before_persistence() {
    use crate::capability::{CapabilityScope, RelationCapability};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path);
    db.inner.clock.set(100);
    let tid = db.admin().snapshot().unwrap().def("disk.Item").unwrap().id;
    let mut tx = db.admin().transaction().unwrap();
    tx.grant("writer", "disk.Item", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    db.issue_capability_until(
        id(650),
        "writer",
        CapabilityScope {
            relations: vec![RelationCapability {
                relation: tid,
                grant: Grant::read_write(),
            }],
            schema_namespace: None,
            delegations: 0,
        },
        Some(200),
    )
    .unwrap();
    let credential = db.issue_credential("writer", id(650), 200).unwrap();
    let session = db.authenticate(&credential.token).unwrap();
    let mut tx = session.transaction().unwrap();
    tx.insert_with_id("disk.Item", id(651), vec![651.into()])
        .unwrap();
    let clock = db.inner.clock.clone();
    tx.before_persist_test_hook = Some(Box::new(move || clock.set(200)));
    let revision = db.stats().unwrap().revision;
    assert!(matches!(tx.commit(), Err(Error::Denied)));
    assert_eq!(db.stats().unwrap().revision, revision);
    assert!(db.admin().get("disk.Item", id(651)).unwrap().is_none());
    drop(session);
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    assert_eq!(db.stats().unwrap().revision, revision);
    assert!(db.admin().get("disk.Item", id(651)).unwrap().is_none());
    db.admin().snapshot().unwrap().verify().unwrap();
}

/// Create an empty legacy file before installing any definitions or rows. This
/// is a test fixture only: public opens never relabel an existing layout.
pub(super) fn empty_format(path: &Path, format: PagedFormat) -> PagedDatabase {
    let db = PagedDatabase::open(path, Default::default()).unwrap();
    let tx = db.inner.db.begin_write().unwrap();
    tx.open_table(META)
        .unwrap()
        .insert("format", format.bytes())
        .unwrap();
    tx.commit().unwrap();
    drop(db);
    PagedDatabase::open(path, Default::default()).unwrap()
}

#[test]
fn unique_role_layouts_preserve_pages_references_writes_and_reopen() {
    for format in [PagedFormat::V1, PagedFormat::V2, PagedFormat::V3] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = empty_format(&path, format);
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "layout",
            1,
            vec![],
            vec![
                Relation::new(
                    "layout.Person",
                    vec![
                        Role::new("name", DataType::Text),
                        Role::new("group", DataType::Bytes),
                    ],
                )
                .unique(&["name"]),
                Relation::new(
                    "layout.Edge",
                    vec![
                        Role::new("person", DataType::Ref("layout.Person".into())),
                        Role::new("label", DataType::Text),
                    ],
                )
                .unique(&["person"])
                .unique(&["person", "label"]),
            ],
        ))
        .unwrap();
        for n in 1..=9 {
            tx.insert_with_id(
                "layout.Person",
                id(n),
                vec![
                    Value::Text(format!("name{n}")),
                    Value::Bytes(vec![0, 255, 0]),
                ],
            )
            .unwrap();
        }
        tx.insert_with_id("layout.Edge", id(20), vec![id(1).into(), "link".into()])
            .unwrap();
        tx.commit().unwrap();
        let pinned = db.admin().snapshot().unwrap();
        let before = pinned.verify().unwrap();
        assert_eq!(
            before.role_index_entries,
            if format == PagedFormat::V1 { 20 } else { 10 }
        );
        let mut after = None;
        let mut found = vec![];
        loop {
            let page = pinned
                .lookup_page(
                    "layout.Person",
                    "group",
                    &Value::Bytes(vec![0, 255, 0]),
                    after,
                    2,
                )
                .unwrap();
            found.extend(page.rows.into_iter().map(|r| r.id));
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        assert_eq!(found, (1..=9).map(id).collect::<Vec<_>>());
        assert_eq!(
            pinned
                .lookup_page("layout.Person", "name", &"name1".into(), None, 1)
                .unwrap()
                .rows[0]
                .id,
            id(1)
        );
        assert!(
            pinned
                .lookup_page("layout.Person", "name", &"name1".into(), Some(id(1)), 1)
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(
            pinned
                .lookup_page("layout.Edge", "person", &id(1).into(), None, 1)
                .unwrap()
                .rows[0]
                .id,
            id(20)
        );
        let mut denied = db.admin().transaction().unwrap();
        assert!(denied.delete("layout.Person", id(1)).is_err());
        let mut duplicate = db.admin().transaction().unwrap();
        assert!(
            duplicate
                .insert("layout.Person", vec!["name1".into(), Value::Bytes(vec![])])
                .is_err()
        );
        let mut tx = db.admin().transaction().unwrap();
        tx.update(
            "layout.Person",
            id(1),
            vec!["renamed".into(), Value::Bytes(vec![0, 255, 0])],
        )
        .unwrap();
        tx.delete("layout.Edge", id(20)).unwrap();
        tx.commit().unwrap();
        assert_eq!(pinned.verify().unwrap(), before);
        drop(pinned);
        drop(denied);
        drop(duplicate);
        drop(db);
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        assert_eq!(db.inner.head.read().unwrap().format, format);
        let snapshot = db.admin().snapshot().unwrap();
        snapshot.verify().unwrap();
        assert!(
            snapshot
                .lookup_page("layout.Person", "name", &"name1".into(), None, 1)
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(
            snapshot
                .lookup_page("layout.Person", "name", &"renamed".into(), None, 1)
                .unwrap()
                .rows[0]
                .id,
            id(1)
        );
        assert!(
            snapshot
                .lookup_page("layout.Edge", "person", &id(1).into(), None, 1)
                .unwrap()
                .rows
                .is_empty()
        );
        drop(snapshot);
        let mut tx = db.admin().transaction().unwrap();
        tx.delete("layout.Person", id(1)).unwrap();
        tx.commit().unwrap();
        db.admin().snapshot().unwrap().verify().unwrap();
    }
}

#[test]
fn fresh_insert_uses_global_allocation_occ_without_redundant_row_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new(
        "other",
        1,
        vec![],
        vec![Relation::new(
            "other.Item",
            vec![Role::new("value", DataType::Int)],
        )],
    ))
    .unwrap();
    tx.commit().unwrap();
    let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
    let mut absent = db.admin().transaction().unwrap();
    assert!(absent.get("disk.Item", id(100)).unwrap().is_none());
    let mut first = db.admin().transaction().unwrap();
    let mut second = db.admin().transaction().unwrap();
    first
        .insert_with_id("disk.Item", id(100), vec![100.into()])
        .unwrap();
    second
        .insert_with_id("other.Item", id(100), vec![100.into()])
        .unwrap();
    assert!(
        first
            .reads
            .contains_key(&tagged(b'a', id(100).0.as_bytes()))
    );
    assert!(
        !first
            .reads
            .contains_key(&tagged(b'r', &row_key(tid, id(100))))
    );
    first.commit().unwrap();
    assert!(matches!(second.commit(), Err(Error::Conflict)));
    assert!(matches!(absent.commit(), Err(Error::Conflict)));
    assert!(db.admin().get("other.Item", id(100)).unwrap().is_none());
    db.admin().snapshot().unwrap().verify().unwrap();
}

#[test]
fn compact_layout_audit_rejects_redundant_unique_role_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = fixture(&path);
    assert_eq!(db.inner.head.read().unwrap().format, PagedFormat::V3);
    let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
    let tx = db.inner.db.begin_write().unwrap();
    tx.open_table(INDEX)
        .unwrap()
        .insert(index_key(tid, 0, &1.into(), id(1)).as_slice(), 0)
        .unwrap();
    tx.commit().unwrap();
    drop(db);
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    assert!(db.admin().snapshot().unwrap().verify().is_err());
}

fn visitor_reference_fixture(path: &Path) -> PagedDatabase {
    let db = fixture(path);
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "edges",
            1,
            vec![("disk".into(), 1)],
            vec![Relation::new(
                "edges.Link",
                vec![Role::new("target", DataType::Ref("disk.Item".into()))],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    tx.insert_with_id("edges.Link", id(100), vec![Value::Ref(id(1))])
        .unwrap();
    tx.commit().unwrap();
    db
}

#[test]
fn aggregate_visitor_validates_corrupt_and_missing_referenced_rows() {
    for malformed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // This test deliberately edits the legacy per-row physical encoding.
        drop(empty_format(&path, PagedFormat::V2));
        let db = visitor_reference_fixture(&path);
        let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
        drop(db);
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                let mut rows = tx.open_table(ROWS).unwrap();
                let key = row_key(tid, id(1));
                if malformed {
                    rows.insert(key.as_slice(), b"bad".as_slice()).unwrap();
                } else {
                    rows.remove(key.as_slice()).unwrap();
                }
            }
            tx.commit().unwrap();
        }
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        let snapshot = db.session("owner").unwrap().snapshot().unwrap();
        let mut count = 0;
        let result = snapshot.visit_authorized_rows("edges.Link", &mut || Ok(()), &mut |_| {
            count += 1;
            Ok(())
        });
        assert!(matches!(result, Err(Error::Storage(_))));
        assert_eq!(count, 0);
    }
}

#[test]
fn aggregate_visitor_terminal_reference_respects_visibility_bound_and_cache_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(visitor_reference_fixture(&path));
    for limit in [1, 2] {
        let db = PagedDatabase::open(
            &path,
            PagedOptions {
                max_visibility_rows: limit,
                decoded_cache_bytes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let snapshot = db.session("owner").unwrap().snapshot().unwrap();
        let expected = snapshot.scan_page("edges.Link", None, 1);
        let mut count = 0;
        let actual = snapshot.visit_authorized_rows("edges.Link", &mut || Ok(()), &mut |_| {
            count += 1;
            Ok(())
        });
        assert_eq!(actual.is_ok(), expected.is_ok());
        assert_eq!(count, usize::from(limit == 2));
        assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
    }
}

#[test]
fn aggregate_visitor_revocation_during_callback_fails_final_release() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.grant("reader", "disk.Item", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    let result = snapshot.visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| {
        let mut tx = db.admin().transaction()?;
        tx.revoke("reader", "disk.Item")?;
        tx.commit()?;
        Ok(())
    });
    assert!(matches!(result, Err(Error::Denied)));
}

#[test]
fn aggregate_visitor_cycle_fallback_preserves_node_and_edge_limits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    // This test deliberately edits the legacy per-row physical encoding.
    drop(empty_format(&path, PagedFormat::V2));
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "graph",
            1,
            vec![],
            vec![Relation::new(
                "graph.Node",
                vec![
                    Role::new("left", DataType::Ref("graph.Node".into())),
                    Role::new("right", DataType::Ref("graph.Node".into())),
                ],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    let def = snapshot.def("graph.Node").unwrap();
    let revision = snapshot.revision();
    drop(snapshot);
    drop(db);
    // Seed a valid typed cycle directly: the public API intentionally requires
    // references to pre-exist. This isolates read traversal and edge budgets.
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx.open_table(ROWS).unwrap();
            let row = Row {
                id: id(1),
                values: vec![Value::Ref(id(1)), Value::Ref(id(1))].into(),
            };
            rows.insert(
                row_key(def.id, row.id).as_slice(),
                encode_row(&def, &row, revision).unwrap().as_slice(),
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    for limit in [1, 2] {
        let db = PagedDatabase::open(
            &path,
            PagedOptions {
                max_visibility_rows: limit,
                ..Default::default()
            },
        )
        .unwrap();
        let snapshot = db.session("owner").unwrap().snapshot().unwrap();
        let expected = snapshot.scan_page("graph.Node", None, 1);
        let mut count = 0;
        let actual = snapshot.visit_authorized_rows("graph.Node", &mut || Ok(()), &mut |_| {
            count += 1;
            Ok(())
        });
        assert_eq!(actual.is_ok(), expected.is_ok());
        assert_eq!(count, usize::from(limit == 2));
    }
}

#[test]
fn aggregate_visitor_byte_rollover_retains_denied_candidate_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        dir.path().join("db"),
        PagedOptions {
            max_page_rows: 2,
            max_page_bytes: 28, // PG01 header plus one i64; exactly one row fits.
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "work",
            1,
            vec![],
            vec![Relation::new(
                "work.Item",
                vec![Role::new("visible", DataType::Int)],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    for n in 1..=203 {
        tx.insert_with_id(
            "work.Item",
            id(n),
            vec![i64::from([1, 102, 203].contains(&n)).into()],
        )
        .unwrap();
    }
    tx.grant(
        "reader",
        "work.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "visible".into(),
                value: 1.into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    let first = snapshot.scan_page("work.Item", None, 2).unwrap();
    assert_eq!(first.rows.len(), 1);
    assert!(matches!(
        snapshot.scan_page("work.Item", first.next, 2),
        Err(Error::Query(_))
    ));
    assert!(matches!(
        snapshot.visit_authorized_rows("work.Item", &mut || Ok(()), &mut |_| Ok(())),
        Err(Error::Query(_))
    ));
}

#[test]
fn aggregate_terminal_cursor_matches_native_reads_across_reference_orders() {
    let dir = tempfile::tempdir().unwrap();
    let db = visitor_reference_fixture(&dir.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    for n in 2..=80 {
        tx.insert_with_id("disk.Item", id(n), vec![(n as i64).into()])
            .unwrap();
    }
    let patterns = [
        (1..=80).collect::<Vec<u128>>(),
        (1..=80).rev().collect(),
        (0..80).map(|n| (n * 37) % 80 + 1).collect(),
        vec![1, 1, 1, 2, 2, 80, 80, 1, 1],
        vec![1, 40, 80, 2, 70, 3, 79],
    ];
    let mut next = 1000;
    for targets in patterns {
        for target in targets {
            tx.insert_with_id("edges.Link", id(next), vec![Value::Ref(id(target))])
                .unwrap();
            next += 1;
        }
    }
    tx.grant("reader", "edges.Link", Grant::read_only())
        .unwrap();
    tx.grant(
        "reader",
        "disk.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "value".into(),
                value: 2.into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    for principal in ["owner", "reader"] {
        let snapshot = db.session(principal).unwrap().snapshot().unwrap();
        let expected = snapshot.scan_page("edges.Link", None, 1024).unwrap().rows;
        let before = db.stats().unwrap().decoded_cache_entries;
        let mut actual = Vec::new();
        snapshot
            .visit_authorized_rows("edges.Link", &mut || Ok(()), &mut |row| {
                actual.push(row.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(db.stats().unwrap().decoded_cache_entries, before);
    }
}

#[test]
fn aggregate_terminal_cursor_dangling_gaps_and_unrelated_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    // This test deliberately edits the legacy per-row physical encoding.
    drop(empty_format(&path, PagedFormat::V2));
    let db = fixture(&path);
    let mut tx = db.admin().transaction().unwrap();
    for n in 2..=40 {
        tx.insert_with_id("disk.Item", id(n), vec![(n as i64).into()])
            .unwrap();
    }
    tx.commit().unwrap();
    let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
    drop(db);
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx.open_table(ROWS).unwrap();
            rows.insert(row_key(tid, id(3)).as_slice(), b"bad".as_slice())
                .unwrap();
            rows.remove(row_key(tid, id(5)).as_slice()).unwrap();
        }
        tx.commit().unwrap();
    }
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    let def = snapshot.def("disk.Item").unwrap();
    let mut directional = TerminalCursor::default();
    // Descending neighbors keep one reverse iterator, while switches and gaps
    // exceeding the fixed lookahead perform one indexed seek.
    for (n, seeks) in [
        (40, 1),
        (39, 2),
        (38, 2),
        (37, 2),
        (10, 3),
        (9, 3),
        (8, 3),
        (8, 3),
        (20, 4),
        (21, 4),
        (7, 5),
        (6, 5),
    ] {
        let bytes = directional.get(&snapshot.view.rows, tid, id(n)).unwrap();
        assert_eq!(
            snapshot.decode_uncached(&def, id(n), bytes).unwrap().values[0],
            Value::Int(n as i64)
        );
        assert_eq!(directional.seek_count, seeks);
    }
    // A reverse cursor must not substitute row4 for the missing row5. It may
    // subsequently reuse row4, and skip unrelated corrupt row3 to reach row2.
    assert!(directional.get(&snapshot.view.rows, tid, id(5)).is_err());
    assert_eq!(directional.seek_count, 5);
    for n in [4, 2] {
        let bytes = directional.get(&snapshot.view.rows, tid, id(n)).unwrap();
        assert_eq!(
            snapshot.decode_uncached(&def, id(n), bytes).unwrap().values[0],
            Value::Int(n as i64)
        );
    }
    assert!(directional.get(&snapshot.view.rows, tid, id(0)).is_err());
    assert!(directional.get(&snapshot.view.rows, tid, id(41)).is_err());
    let mut cursor = TerminalCursor::default();
    for n in [1, 4, 40, 2, 39] {
        let bytes = cursor.get(&snapshot.view.rows, tid, id(n)).unwrap();
        assert_eq!(
            snapshot.decode_uncached(&def, id(n), bytes).unwrap().values[0],
            Value::Int(n as i64)
        );
    }
    for n in [0, 5, 41] {
        assert!(matches!(
            cursor.get(&snapshot.view.rows, tid, id(n)),
            Err(Error::Storage(_))
        ));
    }
    let bytes = cursor.get(&snapshot.view.rows, tid, id(3)).unwrap();
    assert!(matches!(
        snapshot.decode_uncached(&def, id(3), bytes),
        Err(Error::Storage(_))
    ));
}

fn long_scan_fixture(path: &Path) -> PagedDatabase {
    let db = fixture(path);
    for first in (2..=3500).step_by(128) {
        let mut tx = db.admin().transaction().unwrap();
        for n in first..=(first + 127).min(3500) {
            tx.insert_with_id("disk.Item", id(n), vec![(n as i64).into()])
                .unwrap();
        }
        tx.commit().unwrap();
    }
    let mut tx = db.admin().transaction().unwrap();
    tx.grant("reader", "disk.Item", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    db
}

#[test]
fn long_aggregate_visitor_recovers_after_callback_errors_and_panics() {
    let dir = tempfile::tempdir().unwrap();
    let db = long_scan_fixture(&dir.path().join("db"));
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    // Exit after traversing several storage pages, then ensure the same pinned
    // snapshot remains usable and no cursor state or scratch row leaks through.
    for fail_at in [1025, 1500, 2500] {
        let mut count = 0;
        let result = snapshot.visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| {
            count += 1;
            if count == fail_at {
                return Err(Error::Query("deliberate visitor cancellation".into()));
            }
            Ok(())
        });
        assert!(
            matches!(result, Err(Error::Query(message)) if message == "deliberate visitor cancellation")
        );
        assert_eq!(count, fail_at);
    }
    let mut count = 0;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        snapshot.visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| {
            count += 1;
            assert_ne!(count, 1500, "deliberate visitor panic");
            Ok(())
        })
    }));
    assert!(panic.is_err());
    let mut count = 0;
    snapshot
        .visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |row| {
            count += 1;
            assert_eq!(row.id, id(count));
            assert_eq!(row.values[0], Value::Int(count as i64));
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 3500);
    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
}

#[test]
fn long_aggregate_visitor_fences_live_revocation_across_storage_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = long_scan_fixture(&dir.path().join("db"));
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    let mut count = 0;
    let result = snapshot.visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| {
        count += 1;
        if count == 1500 {
            let mut tx = db.admin().transaction()?;
            tx.revoke("reader", "disk.Item")?;
            tx.commit()?;
        }
        Ok(())
    });
    assert!(matches!(result, Err(Error::Denied)));
    assert!((1500..=1756).contains(&count));
    assert!(matches!(
        snapshot.visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| Ok(())),
        Err(Error::Denied)
    ));
    let mut visible = 0;
    db.admin()
        .snapshot()
        .unwrap()
        .visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| {
            visible += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(visible, 3500);
}

#[test]
fn long_aggregate_visitor_preserves_page_visibility_work_limits() {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        dir.path().join("db"),
        PagedOptions {
            max_page_rows: 3,
            decoded_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "scan",
            1,
            vec![],
            vec![Relation::new(
                "scan.Item",
                vec![Role::new("allowed", DataType::Bool)],
            )],
        ),
        "owner",
        Visibility::Private,
    )
    .unwrap();
    for n in 1..=1600 {
        tx.insert_with_id("scan.Item", id(n), vec![Value::Bool(n <= 1100)])
            .unwrap();
    }
    tx.grant(
        "reader",
        "scan.Item",
        Grant {
            filter: RowFilter::Equals {
                role: "allowed".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = db.session("reader").unwrap().snapshot().unwrap();
    let mut count = 0;
    let result = snapshot.visit_authorized_rows("scan.Item", &mut || Ok(()), &mut |_| {
        count += 1;
        Ok(())
    });
    assert!(
        matches!(result, Err(Error::Query(message)) if message.contains("page visibility work budget"))
    );
    assert_eq!(count, 1100);
    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
}

#[test]
fn long_aggregate_visitor_preserves_error_order_across_storage_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    // This test deliberately edits the legacy per-row physical encoding.
    drop(empty_format(&path, PagedFormat::V2));
    let db = long_scan_fixture(&path);
    let tid = db.inner.head.read().unwrap().catalog.state.names["disk.Item"];
    drop(db);
    {
        let db = redb::Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx.open_table(ROWS).unwrap();
            // Place corruption after many complete storage pages so a long scan
            // must return an error rather than release a partial aggregate.
            let mut key = row_key(tid, id(1499));
            key.push(0);
            rows.insert(key.as_slice(), b"bad".as_slice()).unwrap();
        }
        tx.commit().unwrap();
    }
    let db = PagedDatabase::open(&path, Default::default()).unwrap();
    let snapshot = db.admin().snapshot().unwrap();
    for _ in 0..4 {
        let mut count = 0;
        let result = snapshot.visit_authorized_rows("disk.Item", &mut || Ok(()), &mut |_| {
            count += 1;
            Ok(())
        });
        assert!(
            matches!(result, Err(Error::Storage(message)) if message.contains("malformed row key"))
        );
        assert_eq!(count, 1499);
    }
    // The failing row still receives its ordinary source-work check before
    // packed-key validation; storage optimization must not change error precedence.
    let mut checks = 0;
    let mut visible = 0;
    let result = snapshot.visit_authorized_rows(
        "disk.Item",
        &mut || {
            checks += 1;
            if checks == 1501 {
                return Err(Error::Query("source budget wins".into()));
            }
            Ok(())
        },
        &mut |_| {
            visible += 1;
            Ok(())
        },
    );
    assert!(matches!(result, Err(Error::Query(message)) if message == "source budget wins"));
    assert_eq!(visible, 1499);
}
