use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use substrate_liquid::{capability::*, paged::*, schema_control::*, service::*, *};
#[path = "support/tls.rs"]
mod tls;
const TOKEN: &str = "paged-local-token-with-at-least-32-bytes";
fn fixture() -> (tempfile::TempDir, PagedDatabase, Id) {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(dir.path().join("paged.redb"), PagedOptions::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(demo::core(), "owner", Visibility::Private)
        .unwrap();
    let id = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.commit().unwrap();
    (dir, db, id)
}
fn gateway(db: PagedDatabase) -> Gateway {
    Gateway::new(
        db,
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: TOKEN.into(),
                principal: "owner".into(),
            }],
        },
    )
    .unwrap()
}
fn request(g: &Gateway, command: Command) -> Response {
    g.handle(Request {
        version: 1,
        token: TOKEN.into(),
        command,
    })
}
fn relation(db: &PagedDatabase) -> Relation {
    db.admin()
        .snapshot()
        .unwrap()
        .schema("core.Person")
        .unwrap()
}
fn query() -> Command {
    Command::Query {
        sql: "SELECT name FROM core.Person ORDER BY name".into(),
        params: vec![],
    }
}
fn good(r: Response) -> Output {
    match r {
        Response::Ok(o) => o,
        r => panic!("{r:?}"),
    }
}
#[test]
fn existing_gateway_protocol_named_batch_pages_sql_and_rollback() {
    let (_dir, db, ada) = fixture();
    let g = gateway(db.clone());
    let def = relation(&db);
    let catalog = db
        .session("owner")
        .unwrap()
        .snapshot()
        .unwrap()
        .catalog(&catalog::CatalogRequest::default())
        .unwrap();
    let contract = Contract {
        relation: def.name.clone(),
        schema_id: def.id,
        fingerprint: catalog.relations[0].fingerprint.clone().unwrap(),
    };
    let insert = Mutation::Insert {
        relation: def.name.clone(),
        schema_id: def.id,
        fields: BTreeMap::from([("name".into(), WireValue::Text("Bob".into()))]),
        alias: Some("bob".into()),
    };
    let bob = match good(request(
        &g,
        Command::Checked {
            contracts: vec![contract.clone()],
            command: Box::new(Command::Batch {
                operations: vec![insert.clone()],
            }),
        },
    )) {
        Output::Batch { ids } => ids[0],
        o => panic!("{o:?}"),
    };
    good(request(
        &g,
        Command::Batch {
            operations: vec![Mutation::Update {
                relation: def.name.clone(),
                schema_id: def.id,
                id: bob,
                fields: BTreeMap::from([("name".into(), WireValue::Text("Bobby".into()))]),
            }],
        },
    ));
    match good(request(&g, query())) {
        Output::Query { rows, .. } => assert_eq!(rows.len(), 2),
        o => panic!("{o:?}"),
    }
    let bad = Mutation::Insert {
        relation: def.name.clone(),
        schema_id: def.id,
        fields: BTreeMap::new(),
        alias: None,
    };
    assert!(matches!(
        request(
            &g,
            Command::Batch {
                operations: vec![insert, bad]
            }
        ),
        Response::Error(_)
    ));
    match good(request(
        &g,
        Command::Page {
            relation: def.name.clone(),
            schema_id: def.id,
            after: None,
            limit: 10,
            expected_revision: None,
            filter: Some(("name".into(), WireValue::Text("Ada".into()))),
        },
    )) {
        Output::Page { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, ada)
        }
        o => panic!("{o:?}"),
    }
    good(request(
        &g,
        Command::Batch {
            operations: vec![Mutation::Delete {
                relation: def.name,
                schema_id: def.id,
                id: bob,
            }],
        },
    ));
    assert_eq!(
        db.admin()
            .snapshot()
            .unwrap()
            .scan_page("core.Person", None, 10)
            .unwrap()
            .rows
            .len(),
        1
    );
}
#[test]
fn same_catalog_and_schema_proposal_protocol_installs_online() {
    let (_dir, db, ada) = fixture();
    db.configure_namespace(NamespacePolicy {
        namespace: "calendar".into(),
        owner: "owner".into(),
        auto_approve: true,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
    let g = gateway(db.clone());
    let module = Module::new(
        "calendar",
        1,
        vec![("core".into(), 1)],
        vec![Relation::new(
            "calendar.Note",
            vec![
                Role::new("person", DataType::Ref("core.Person".into())),
                Role::new("text", DataType::Text),
            ],
        )],
    );
    let id = Id::new();
    let proposal = match good(request(
        &g,
        Command::SchemaPropose {
            id,
            module: module.clone(),
        },
    )) {
        Output::SchemaProposal { proposal } => proposal,
        o => panic!("{o:?}"),
    };
    assert_eq!(proposal.plan.existing_record_rewrites, 0);
    good(request(
        &g,
        Command::SchemaApply {
            id,
            digest: proposal.plan.digest,
        },
    ));
    good(request(
        &g,
        Command::Batch {
            operations: vec![Mutation::Insert {
                relation: "calendar.Note".into(),
                schema_id: module.relations[0].id,
                fields: BTreeMap::from([
                    ("person".into(), WireValue::Ref(ada.to_string())),
                    ("text".into(), WireValue::Text("meeting".into())),
                ]),
                alias: None,
            }],
        },
    ));
    match good(request(
        &g,
        Command::Query {
            sql: "SELECT p.name,n.text FROM core.Person p JOIN calendar.Note n ON p.id=n.person"
                .into(),
            params: vec![],
        },
    )) {
        Output::Query { rows, .. } => assert_eq!(rows.len(), 1),
        o => panic!("{o:?}"),
    }
    drop(g);
    drop(db);
    let reopened =
        PagedDatabase::open(_dir.path().join("paged.redb"), PagedOptions::default()).unwrap();
    assert_eq!(
        reopened
            .session("owner")
            .unwrap()
            .schema_proposal(id)
            .unwrap()
            .status,
        ProposalStatus::Installed
    );
}
#[test]
fn nested_reads_share_bounded_batched_sql_execution() {
    let (_dir, db, ada) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    let m = Module::new(
        "notes",
        1,
        vec![("core".into(), 1)],
        vec![Relation::new(
            "notes.Note",
            vec![
                Role::new("person", DataType::Ref("core.Person".into())),
                Role::new("text", DataType::Text),
            ],
        )],
    );
    tx.install_owned(m, "owner", Visibility::Private).unwrap();
    for text in ["a", "b", "c"] {
        tx.insert("notes.Note", vec![Value::Ref(ada), text.into()])
            .unwrap();
    }
    tx.commit().unwrap();
    let snapshot = db.session("owner").unwrap().snapshot().unwrap();
    let contract = |name: &str| {
        let c = snapshot
            .catalog(&catalog::CatalogRequest {
                relation: Some(name.into()),
                ..Default::default()
            })
            .unwrap()
            .relations
            .remove(0);
        Contract {
            relation: c.name,
            schema_id: c.id,
            fingerprint: c.fingerprint.unwrap(),
        }
    };
    let plan = nested::ReadPlan {
        contract: contract("core.Person"),
        filter: None,
        order: vec![],
        limit: 10,
        include: BTreeMap::from([(
            "notes".into(),
            nested::Include {
                direction: nested::Direction::Many,
                role: "person".into(),
                plan: nested::ReadPlan {
                    contract: contract("notes.Note"),
                    filter: None,
                    order: vec![nested::Order {
                        field: "text".into(),
                        descending: false,
                    }],
                    limit: 2,
                    include: BTreeMap::new(),
                },
            },
        )]),
    };
    let result = snapshot.read_nested(&plan).unwrap();
    assert_eq!(result.stats.sql_statements, 2);
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0].related["notes"] {
        nested::NestedValue::Many(rows) => assert_eq!(rows.len(), 2),
        _ => panic!(),
    }
    let g = gateway(db);
    assert!(matches!(
        request(&g, Command::Read { plan }),
        Response::Ok(Output::Read { .. })
    ));
}
#[test]
fn authenticated_tls_preserves_scope_rotation_and_live_revocation() {
    let (dir, db, _) = fixture();
    let mut tx = db.admin().transaction().unwrap();
    tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.commit().unwrap();
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 120;
    let root = db
        .issue_capability_until(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: relation(&db).id,
                    grant: Grant {
                        filter: RowFilter::Equals {
                            role: "name".into(),
                            value: "Ada".into(),
                        },
                        ..Grant::read_only()
                    },
                }],
                schema_namespace: None,
                delegations: 1,
            },
            Some(end),
        )
        .unwrap();
    let issued = db.issue_credential("owner", root.id, end).unwrap();
    let held = db.authenticate(&issued.token).unwrap().snapshot().unwrap();
    let (ca, cert, key) = tls::certificates(dir.path());
    let server =
        remote::RemoteServer::bind(db.clone(), "127.0.0.1:0".parse().unwrap(), &cert, &key)
            .unwrap();
    let address = server.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shutdown = stop.clone();
    let worker = std::thread::spawn(move || server.run(shutdown).unwrap());
    let client = |token: &str| remote::RemoteClient::new(address, "localhost", &ca, token).unwrap();
    let old = client(&issued.token);
    let result = || {
        match good(old.request(query()).unwrap()) {
            Output::Query { rows, .. } => assert_eq!(rows.len(), 1),
            o => panic!("{o:?}"),
        }
        let next = db.rotate_credential(issued.id, end).unwrap();
        assert!(matches!(old.request(query()).unwrap(), Response::Error(_)));
        assert!(held.scan_page("core.Person", None, 10).is_err());
        match good(client(&next.token).request(query()).unwrap()) {
            Output::Query { rows, .. } => assert_eq!(rows.len(), 1),
            o => panic!("{o:?}"),
        }
        db.revoke_capability(root.id).unwrap();
        assert!(matches!(
            client(&next.token).request(query()).unwrap(),
            Response::Error(_)
        ));
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(result));
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    outcome.unwrap();
}
#[allow(dead_code)]
#[path = "../examples/dx/generated/schema.rs"]
mod generated;
#[test]
fn existing_generated_rust_record_and_manifest_work_with_paged_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(dir.path().join("records.redb"), PagedOptions::default()).unwrap();
    let manifest = SchemaManifest::from_json(include_str!("../examples/dx/manifest.json")).unwrap();
    db.admin()
        .transact(RetryPolicy::default(), |tx| tx.install_manifest(&manifest))
        .unwrap();
    let plan = db
        .admin()
        .transact(RetryPolicy::default(), |tx| tx.install_manifest(&manifest))
        .unwrap();
    assert!(plan.additions.is_empty());
    let record = generated::core::Person { name: "Ada".into() };
    let id = db
        .admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_record(record.clone())
        })
        .unwrap();
    assert_eq!(
        db.admin().snapshot().unwrap().get_record(id).unwrap(),
        Some(record)
    );
    db.admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.replace_record(
                id,
                generated::core::Person {
                    name: "Grace".into(),
                },
            )
        })
        .unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .unwrap()
            .get_record(id)
            .unwrap()
            .unwrap()
            .name,
        "Grace"
    );
    let descriptor = db
        .admin()
        .snapshot()
        .unwrap()
        .describe("core.Person")
        .unwrap();
    db.admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.update_dynamic(
                &descriptor,
                id.raw(),
                Fields::from([("name".into(), "Margaret".into())]),
            )
        })
        .unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .unwrap()
            .get_dynamic(&descriptor, id.raw())
            .unwrap()
            .unwrap()["name"],
        Value::Text("Margaret".into())
    );
}
#[cfg(unix)]
#[test]
fn existing_unix_client_uses_generated_records_and_anonymous_visibility() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let db = PagedDatabase::open(dir.path().join("local.redb"), PagedOptions::default()).unwrap();
    let manifest = SchemaManifest::from_json(include_str!("../examples/dx/manifest.json")).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_manifest_with_visibility(&manifest, Visibility::Private)
        .unwrap();
    tx.grant("owner", "core.Person", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    let socket = dir.path().join("db.sock");
    let server = local::LocalServer::bind(
        db.clone(),
        &socket,
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: TOKEN.into(),
                principal: "owner".into(),
            }],
        },
    )
    .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shutdown = stop.clone();
    let worker = std::thread::spawn(move || server.run(shutdown).unwrap());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let client = local::LocalClient::new(&socket, TOKEN);
        let id = client
            .insert_record(generated::core::Person { name: "Ada".into() })
            .unwrap();
        assert_eq!(
            db.admin()
                .snapshot()
                .unwrap()
                .get_record(id)
                .unwrap()
                .unwrap()
                .name,
            "Ada"
        );
        assert!(matches!(
            local::LocalClient::anonymous(&socket)
                .request(query())
                .unwrap(),
            Response::Error(_)
        ));
        let mut tx = db.admin().transaction().unwrap();
        tx.set_visibility("core.Person", Visibility::Public)
            .unwrap();
        tx.commit().unwrap();
        match good(
            local::LocalClient::anonymous(&socket)
                .request(query())
                .unwrap(),
        ) {
            Output::Query { rows, .. } => assert_eq!(rows.len(), 1),
            o => panic!("{o:?}"),
        }
        assert!(matches!(
            local::LocalClient::new(&socket, "invalid")
                .request(query())
                .unwrap(),
            Response::Error(_)
        ));
    }));
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    outcome.unwrap();
}
#[test]
fn scoped_gateway_cannot_use_ambient_visibility_or_change_namespace() {
    let (_dir, db, ada) = fixture();
    db.configure_namespace(NamespacePolicy {
        namespace: "calendar".into(),
        owner: "owner".into(),
        auto_approve: true,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
    let root = db
        .issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: relation(&db).id,
                    grant: Grant::read_only(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let g = gateway(db.clone());
    let using = |command| Command::UsingCapability {
        capability: root.id,
        command: Box::new(command),
    };
    assert!(matches!(
        request(
            &g,
            using(Command::SchemaPropose {
                id: Id::new(),
                module: Module::new(
                    "calendar",
                    1,
                    vec![],
                    vec![Relation::new("calendar.Note", vec![])]
                )
            })
        ),
        Response::Error(_)
    ));
    assert!(matches!(
        request(
            &g,
            using(Command::Batch {
                operations: vec![Mutation::Delete {
                    relation: "core.Person".into(),
                    schema_id: relation(&db).id,
                    id: ada
                }]
            })
        ),
        Response::Error(_)
    ));
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "public",
            1,
            vec![],
            vec![Relation::new(
                "public.Secret",
                vec![Role::new("text", DataType::Text)],
            )],
        ),
        "another",
        Visibility::Public,
    )
    .unwrap();
    tx.insert("public.Secret", vec!["hidden from this scope".into()])
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        request(
            &g,
            using(Command::Query {
                sql: "SELECT * FROM public.Secret".into(),
                params: vec![]
            })
        ),
        Response::Error(_)
    ));
    match good(request(
        &g,
        using(Command::Catalog {
            options: Default::default(),
        }),
    )) {
        Output::Catalog { page } => assert_eq!(page.relations.len(), 1),
        o => panic!("{o:?}"),
    }
}
#[test]
fn unauthorized_named_and_generated_writes_do_not_disclose_private_fields_or_contracts() {
    let (_dir, db, existing) = fixture();
    for fields in [
        Fields::new(),
        Fields::from([("name".into(), "Ada".into())]),
        Fields::from([("unknown".into(), "Ada".into())]),
    ] {
        for name in ["core.Person", "core.Absent"] {
            let mut tx = db.session("stranger").unwrap().transaction().unwrap();
            assert!(matches!(
                tx.insert_named(name, fields.clone()),
                Err(Error::Denied)
            ));
            assert!(matches!(tx.commit(), Err(Error::Aborted)));
        }
    }
    // The generated definition deliberately differs from this fixture's private
    // type ID: permission must precede the match/mismatch oracle.
    let mut tx = db.session("stranger").unwrap().transaction().unwrap();
    assert!(matches!(
        tx.insert_record(generated::core::Person { name: "Ada".into() }),
        Err(Error::Denied)
    ));
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    let mut tx = db.session("stranger").unwrap().transaction().unwrap();
    assert!(matches!(
        tx.replace_record(
            TypedId::from_raw(existing),
            generated::core::Person { name: "Ada".into() }
        ),
        Err(Error::Denied)
    ));
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    // A narrow write grant remains usable; only table-level permission is checked
    // before decoding fields, and the real row filter is enforced by insertion.
    let mut tx = db.admin().transaction().unwrap();
    tx.grant(
        "limited",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "Ada".into(),
            },
            ..Grant::read_write()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("limited").unwrap().transaction().unwrap();
    tx.insert_named("core.Person", Fields::from([("name".into(), "Ada".into())]))
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("limited").unwrap().transaction().unwrap();
    assert!(matches!(
        tx.insert_named("core.Person", Fields::from([("name".into(), "Bob".into())])),
        Err(Error::Denied)
    ));
}
