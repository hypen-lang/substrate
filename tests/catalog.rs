use substrate_liquid::{catalog::*, service::*, *};

fn fixture() -> (Database, Gateway, Id) {
    let db = Database::memory();
    let secret = Relation::new("vault.Secret", vec![Role::new("secret", DataType::Text)]);
    let hidden_id = secret.id;
    let public = Relation::new(
        "app.Link",
        vec![
            Role::new("owner", DataType::Ref("vault.Secret".into())),
            Role::new("label", DataType::Text),
        ],
    )
    .unique(&["owner", "label"]);
    let mut tx = db.admin().transaction();
    tx.install(Module::new("vault", 1, vec![], vec![secret]))
        .unwrap();
    tx.install(Module::new(
        "app",
        1,
        vec![("vault".into(), 1)],
        vec![public, Relation::new("app.Empty", vec![])],
    ))
    .unwrap();
    tx.grant(
        "agent",
        "app.Link",
        Grant {
            filter: RowFilter::Equals {
                role: "label".into(),
                value: "private-policy-value".into(),
            },
            ..Grant::read_write()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let g = Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                principal: "agent".into(),
                token: "catalog-test-credential-000000000000".into(),
            }],
        },
    )
    .unwrap();
    (db, g, hidden_id)
}
fn call(g: &Gateway, options: CatalogRequest) -> Response {
    g.handle(Request {
        version: 1,
        token: "catalog-test-credential-000000000000".into(),
        command: Command::Catalog { options },
    })
}
#[test]
fn catalog_redacts_hidden_targets_imports_and_policy_values() {
    let (db, g, hidden) = fixture();
    let Response::Ok(Output::Catalog { page }) = call(&g, CatalogRequest::default()) else {
        panic!()
    };
    let text = serde_json::to_string(&page).unwrap();
    for secret in [
        "vault",
        "Secret",
        "private-policy-value",
        "app.Empty",
        &hidden.to_string(),
    ] {
        assert!(!text.contains(secret), "{text}");
    }
    assert_eq!(page.relations.len(), 1);
    let r = &page.relations[0];
    assert!(r.row_restricted && r.operations.read && r.operations.insert && r.operations.reference);
    assert_eq!(r.module.version, "1");
    assert_eq!(
        r.unique,
        vec![vec![
            RoleId {
                relation: r.id,
                slot: 0
            },
            RoleId {
                relation: r.id,
                slot: 1
            }
        ]]
    );
    assert!(matches!(
        r.roles[0].data_type,
        CatalogType::Ref { target: None }
    ));
    assert!(r.roles.iter().all(|r| r.required && r.equality_index));
    assert!(r.fingerprint.is_none());
    assert!(
        !text.contains(
            &db.admin()
                .snapshot()
                .schema("app.Link")
                .unwrap()
                .fingerprint()
        )
    );
    // Legacy capabilities cannot represent an opaque target and must omit this family.
    let response = g.handle(Request {
        version: 1,
        token: "catalog-test-credential-000000000000".into(),
        command: Command::Capabilities,
    });
    assert!(
        matches!(response,Response::Ok(Output::Capabilities{relations,..}) if relations.is_empty())
    );
}
#[test]
fn missing_and_denied_names_are_indistinguishable_and_do_not_expand_imports() {
    let (_, g, _) = fixture();
    let query = |name: &str| match call(
        &g,
        CatalogRequest {
            relation: Some(name.into()),
            ..Default::default()
        },
    ) {
        Response::Error(e) => (e.code, e.message),
        _ => panic!(),
    };
    assert_eq!(query("vault.Secret"), query("missing.Type"));
    assert_eq!(query("app.Empty"), query("vault.Secret"));
    assert_eq!(query("vault.Secret").0, "denied");
}
#[test]
fn reference_only_write_only_and_empty_grants_have_explicit_semantics() {
    let (db, _, _) = fixture();
    let mut tx = db.admin().transaction();
    tx.grant("agent", "vault.Secret", Grant::reference_only())
        .unwrap();
    tx.grant(
        "writer",
        "app.Empty",
        Grant {
            read: false,
            write: true,
            reference: false,
            filter: RowFilter::All,
        },
    )
    .unwrap();
    tx.grant(
        "none",
        "app.Empty",
        Grant {
            read: false,
            write: false,
            reference: false,
            filter: RowFilter::All,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let catalog = db
        .session("agent")
        .snapshot()
        .catalog(&Default::default())
        .unwrap();
    let secret = catalog
        .relations
        .iter()
        .find(|r| r.name == "vault.Secret")
        .unwrap();
    assert_eq!(
        secret.fingerprint.as_deref(),
        Some(
            db.admin()
                .snapshot()
                .schema("vault.Secret")
                .unwrap()
                .fingerprint()
                .as_str()
        )
    );
    assert!(secret.operations.reference && !secret.operations.read && !secret.operations.insert);
    let link = catalog
        .relations
        .iter()
        .find(|r| r.name == "app.Link")
        .unwrap();
    assert!(
        matches!(&link.roles[0].data_type,CatalogType::Ref{target:Some(t)} if t.name=="vault.Secret")
    );
    assert!(db.session("agent").snapshot().scan("vault.Secret").is_err());
    let writer = db
        .session("writer")
        .snapshot()
        .catalog(&Default::default())
        .unwrap();
    assert!(writer.relations[0].operations.insert && !writer.relations[0].operations.read);
    assert!(
        db.session("none")
            .snapshot()
            .catalog(&Default::default())
            .unwrap()
            .relations
            .is_empty()
    );
}
#[test]
fn pagination_uses_visible_ids_and_revision_guard_without_global_counts() {
    let (db, _, _) = fixture();
    let mut tx = db.admin().transaction();
    tx.grant("agent", "app.Empty", Grant::read_only()).unwrap();
    tx.commit().unwrap();
    let session = db.session("agent");
    let old = session.snapshot();
    let first = old
        .catalog(&CatalogRequest {
            limit: Some(1),
            ..Default::default()
        })
        .unwrap();
    let second = old
        .catalog(&CatalogRequest {
            limit: Some(1),
            after: first.next,
            expected_revision: Some(first.revision.clone()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first.relations.len(), 1);
    assert_eq!(second.relations.len(), 1);
    assert_ne!(first.relations[0].id, second.relations[0].id);
    assert!(second.next.is_none());
    let mut tx = db.admin().transaction();
    tx.revoke("agent", "app.Empty").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        session.snapshot().catalog(&CatalogRequest {
            expected_revision: Some(first.revision),
            ..Default::default()
        }),
        Err(Error::Conflict)
    ));
    assert!(matches!(
        old.catalog(&Default::default()),
        Err(Error::Denied)
    ));
    assert_eq!(
        session
            .snapshot()
            .catalog(&Default::default())
            .unwrap()
            .relations
            .len(),
        1
    );
}
#[test]
fn online_addition_does_not_change_old_descriptor_or_pinned_catalog() {
    let (db, _, _) = fixture();
    let old = db.session("agent").snapshot();
    let before = old.catalog(&Default::default()).unwrap();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "app",
        2,
        vec![],
        vec![Relation::new(
            "app.New",
            vec![Role::new("n", DataType::Int)],
        )],
    ))
    .unwrap();
    tx.grant("agent", "app.New", Grant::read_write()).unwrap();
    tx.commit().unwrap();
    let now = db
        .session("agent")
        .snapshot()
        .catalog(&Default::default())
        .unwrap();
    assert_eq!(now.relations.len(), 2);
    let unchanged = now.relations.iter().find(|r| r.name == "app.Link").unwrap();
    assert_eq!(
        serde_json::to_value(unchanged).unwrap(),
        serde_json::to_value(&before.relations[0]).unwrap()
    );
    assert_eq!(
        serde_json::to_value(old.catalog(&Default::default()).unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}
#[test]
fn invalid_options_and_unknown_wire_fields_fail_closed() {
    let (db, _, _) = fixture();
    let s = db.session("agent").snapshot();
    for limit in [0, 101, usize::MAX] {
        assert!(
            s.catalog(&CatalogRequest {
                limit: Some(limit),
                ..Default::default()
            })
            .is_err()
        );
    }
    assert!(
        s.catalog(&CatalogRequest {
            after: Some(Id::new()),
            relation: Some("app.Link".into()),
            ..Default::default()
        })
        .is_err()
    );
    let command: Command =
        serde_json::from_str(r#"{"op":"catalog","options":{"limit":1}}"#).unwrap();
    assert!(matches!(command, Command::Catalog { .. }));
    assert!(
        serde_json::from_str::<Command>(r#"{"op":"catalog","options":{"principal":"admin"}}"#)
            .is_err()
    );
}
#[test]
fn namespace_install_permission_does_not_disclose_existing_families() {
    let (db, g, _) = fixture();
    let mut tx = db.admin().transaction();
    tx.revoke("agent", "app.Link").unwrap();
    tx.grant_schema("agent", "app").unwrap();
    tx.commit().unwrap();
    assert!(
        matches!(call(&g,Default::default()),Response::Ok(Output::Catalog{page}) if page.relations.is_empty())
    );
}
#[test]
fn scalar_metadata_preserves_exact_types_without_data_values() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "all",
        1,
        vec![],
        vec![Relation::new(
            "all.Values",
            vec![
                Role::new("b", DataType::Bool),
                Role::new("i", DataType::Int),
                Role::new("d", DataType::Decimal { scale: 18 }),
                Role::new("t", DataType::Timestamp),
                Role::new("bytes", DataType::Bytes),
            ],
        )],
    ))
    .unwrap();
    tx.commit().unwrap();
    let page = db.admin().snapshot().catalog(&Default::default()).unwrap();
    assert!(matches!(
        page.relations[0].roles[2].data_type,
        CatalogType::Decimal { scale: 18 }
    ));
    assert_eq!(page.relations[0].roles.len(), 5);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(96))]
    #[test]
    fn paged_discovery_matches_explicit_capabilities_without_hidden_targets(
        flags in proptest::collection::vec((proptest::bool::ANY,proptest::bool::ANY,proptest::bool::ANY),1..30),
        page_size in 1usize..8,
    ) {
        let db=Database::memory();
        let relations=(0..flags.len()).map(|i|Relation::new(&format!("test.R{i}"),if i==0{vec![]}else{vec![Role::new("parent",DataType::Ref("test.R0".into()))]})).collect();
        let mut tx=db.admin().transaction();
        tx.install(Module::new("test",1,vec![],relations)).unwrap();
        for (i,(read,write,reference)) in flags.iter().copied().enumerate() {
            tx.grant("agent",&format!("test.R{i}"),Grant{read,write,reference,filter:RowFilter::All}).unwrap();
        }
        tx.commit().unwrap();
        let snapshot=db.session("agent").snapshot();let mut after=None;let mut seen=std::collections::BTreeSet::new();
        let target_visible=flags[0].0||flags[0].1||flags[0].2;
        loop {
            let page=snapshot.catalog(&CatalogRequest{after,limit:Some(page_size),..Default::default()}).unwrap();
            for relation in page.relations {
                proptest::prop_assert!(seen.insert(relation.name.clone()));
                let index:usize=relation.name.strip_prefix("test.R").unwrap().parse().unwrap();
                let (read,write,reference)=flags[index];
                proptest::prop_assert!(read||write||reference);
                proptest::prop_assert_eq!((relation.operations.read,relation.operations.insert,relation.operations.reference),(read,write,reference));
                for role in relation.roles {
                    let CatalogType::Ref{target}=role.data_type else {panic!()};
                    proptest::prop_assert_eq!(target.is_some(),target_visible);
                }
            }
            after=page.next;if after.is_none(){break;}
        }
        let expected=flags.iter().enumerate().filter(|(_, (r,w,x))|*r||*w||*x).map(|(i,_)|format!("test.R{i}")).collect::<std::collections::BTreeSet<_>>();
        proptest::prop_assert_eq!(seen,expected);
    }
}
