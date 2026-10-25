use substrate_liquid::{
    capability::CapabilityScope,
    catalog::CatalogRequest,
    schema_control::{NamespacePolicy, SchemaQuotas},
    service::{
        ClientCredential, Command, Gateway, Output, Request, Response, ServerConfig, SqlWireValue,
    },
    *,
};
const OWNER: &str = "owner-token-000000000000000000000000";
const OTHER: &str = "other-token-000000000000000000000000";
fn setup() -> Database {
    let db = Database::memory();
    db.configure_namespace(NamespacePolicy {
        namespace: "gaming".into(),
        owner: "owner".into(),
        auto_approve: true,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
    let mut tx = db.admin().transaction();
    tx.install_with_visibilities(
        Module::new(
            "gaming",
            1,
            vec![],
            vec![
                Relation::new(
                    "gaming.GamerProfile",
                    vec![Role::new("name", DataType::Text)],
                ),
                Relation::new(
                    "gaming.GamingMessages",
                    vec![Role::new("text", DataType::Text)],
                ),
                Relation::new("gaming.Notice", vec![Role::new("text", DataType::Text)]),
                Relation::new(
                    "gaming.Link",
                    vec![Role::new(
                        "message",
                        DataType::Ref("gaming.GamingMessages".into()),
                    )],
                ),
            ],
        ),
        [
            ("gaming.GamingMessages".into(), Visibility::Private),
            ("gaming.Notice".into(), Visibility::Public),
            ("gaming.Link".into(), Visibility::Public),
        ]
        .into(),
    )
    .unwrap();
    tx.insert("gaming.GamerProfile", vec!["Gamer".into()])
        .unwrap();
    let message = tx
        .insert("gaming.GamingMessages", vec!["private text".into()])
        .unwrap();
    tx.insert("gaming.Notice", vec!["public text".into()])
        .unwrap();
    tx.insert("gaming.Link", vec![message.into()]).unwrap();
    tx.commit().unwrap();
    db
}
fn gateway(db: &Database) -> Gateway {
    Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![
                ClientCredential {
                    token: OWNER.into(),
                    principal: "owner".into(),
                },
                ClientCredential {
                    token: OTHER.into(),
                    principal: "other".into(),
                },
            ],
        },
    )
    .unwrap()
}
fn request(token: &str, command: Command) -> Request {
    Request {
        version: 1,
        token: token.into(),
        command,
    }
}
fn query(sql: &str) -> Command {
    Command::Query {
        sql: sql.into(),
        params: vec![],
    }
}
fn count(response: Response, n: i64) {
    match response {
        Response::Ok(Output::Query { rows, .. }) => {
            assert_eq!(rows, vec![vec![SqlWireValue::Integer(n.to_string())]])
        }
        r => panic!("{r:?}"),
    }
}
fn denied(response: Response) {
    assert!(matches!(response, Response::Error(_)), "{response:?}");
}
#[test]
fn protocol_public_default_private_and_reference_closure() {
    let db = setup();
    let gateway = gateway(&db);
    count(
        gateway.handle(request("", query("SELECT count(*) FROM gaming.Notice"))),
        1,
    );
    denied(gateway.handle(request(
        "wrong",
        query("SELECT count(*) FROM gaming.Notice"),
    )));
    denied(gateway.handle(request(
        "",
        query("SELECT count(*) FROM gaming.GamerProfile"),
    )));
    count(
        gateway.handle(request(
            OTHER,
            query("SELECT count(*) FROM gaming.GamerProfile"),
        )),
        1,
    );
    denied(gateway.handle(request(
        OTHER,
        query("SELECT count(*) FROM gaming.GamingMessages"),
    )));
    count(
        gateway.handle(request(
            OWNER,
            query("SELECT count(*) FROM gaming.GamingMessages"),
        )),
        1,
    );
    // SQL omits a family whose full reference-domain metadata is private.
    denied(gateway.handle(request("", query("SELECT count(*) FROM gaming.Link"))));
    let public = gateway.handle(request(
        "",
        Command::Catalog {
            options: CatalogRequest::default(),
        },
    ));
    let Response::Ok(Output::Catalog { page }) = public else {
        panic!("{public:?}")
    };
    assert!(
        page.relations
            .iter()
            .all(|r| r.name != "gaming.GamingMessages" && r.name != "gaming.GamerProfile")
    );
    denied(gateway.handle(request(
        "",
        Command::SchemaVisibility {
            relation: "gaming.Notice".into(),
            visibility: Visibility::Private,
        },
    )));
    denied(gateway.handle(request(
        OTHER,
        Command::SchemaVisibility {
            relation: "gaming.Notice".into(),
            visibility: Visibility::Private,
        },
    )));
    assert!(matches!(
        gateway.handle(request(
            OWNER,
            Command::SchemaVisibility {
                relation: "gaming.Notice".into(),
                visibility: Visibility::Private
            }
        )),
        Response::Ok(Output::SchemaChanged)
    ));
    denied(gateway.handle(request("", query("SELECT count(*) FROM gaming.Notice"))));
}
#[test]
fn explicit_grants_and_checked_contract_reads_preserve_visibility() {
    let db = setup();
    let gateway = gateway(&db);
    let s = db.admin().snapshot();
    let schema = s.schema("gaming.Notice").unwrap();
    let contract = substrate_liquid::service::Contract {
        relation: schema.name.clone(),
        schema_id: schema.id,
        fingerprint: schema.fingerprint(),
    };
    count(
        gateway.handle(request(
            "",
            Command::Checked {
                contracts: vec![contract],
                command: Box::new(query("SELECT count(*) FROM gaming.Notice")),
            },
        )),
        1,
    );
    let mut tx = db.admin().transaction();
    tx.grant("other", "gaming.GamingMessages", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    count(
        gateway.handle(request(
            OTHER,
            query("SELECT count(*) FROM gaming.GamingMessages"),
        )),
        1,
    );
    count(
        gateway.handle(request(OTHER, query("SELECT count(*) FROM gaming.Link"))),
        1,
    );
    assert!(
        db.session("other")
            .transaction()
            .insert("gaming.GamerProfile", vec!["no write".into()])
            .is_err()
    );
}
#[path = "support/tls.rs"]
mod tls;
#[test]
fn tls_anonymous_reads_namespace_owner_updates_and_scoped_credentials() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use substrate_liquid::remote::{RemoteClient, RemoteServer};
    let dir = tempfile::tempdir().unwrap();
    let (ca, cert, key) = tls::certificates(dir.path());
    let db = setup();
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let cap = db
        .issue_capability_until(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![],
                schema_namespace: Some("gaming".into()),
                delegations: 0,
            },
            Some(end),
        )
        .unwrap();
    let credential = db.issue_credential("owner", cap.id, end).unwrap();
    let server = Arc::new(
        RemoteServer::bind(db.clone(), "127.0.0.1:0".parse().unwrap(), &cert, &key).unwrap(),
    );
    let address = server.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let running = server.clone();
    let signal = stop.clone();
    let worker = std::thread::spawn(move || running.run(signal));
    // Always stop the host even when an assertion fails.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let public = RemoteClient::anonymous(address, "localhost", &ca).unwrap();
        let mut persistent = public.connect().unwrap();
        count(
            persistent
                .request(query("SELECT count(*) FROM gaming.Notice"))
                .unwrap(),
            1,
        );
        denied(
            persistent
                .request(query("SELECT count(*) FROM gaming.GamingMessages"))
                .unwrap(),
        );
        let owner = RemoteClient::new(address, "localhost", &ca, &credential.token).unwrap();
        // Namespace control does not imply data reads, even for public schemas.
        denied(
            owner
                .request(query("SELECT count(*) FROM gaming.Notice"))
                .unwrap(),
        );
        assert!(matches!(
            owner
                .request(Command::SchemaVisibility {
                    relation: "gaming.Notice".into(),
                    visibility: Visibility::Private
                })
                .unwrap(),
            Response::Ok(Output::SchemaChanged)
        ));
        denied(
            persistent
                .request(query("SELECT count(*) FROM gaming.Notice"))
                .unwrap(),
        );
        denied(
            RemoteClient::new(address, "localhost", &ca, "invalid")
                .unwrap()
                .request(Command::Capabilities)
                .unwrap(),
        );
        persistent.close();
    }));
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap().unwrap();
    result.unwrap();
}
