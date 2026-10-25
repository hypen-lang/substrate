use substrate_liquid::{
    catalog::CatalogRequest,
    disk_snapshot::SpillOptions,
    nested::ReadPlan,
    service::{
        ClientCredential, Command, Contract, Gateway, Output, Request, Response, ServerConfig,
    },
    *,
};

const TOKEN: &str = "visibility-review-reader-000000000000";

#[test]
fn internal_host_owner_marker_is_not_an_application_read_capability() {
    let (db, _, _) = fixture();
    let host_marker = "\0host";
    assert!(
        db.session(host_marker)
            .snapshot()
            .scan("review.Secret")
            .is_err()
    );
    let gateway = Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: TOKEN.into(),
                principal: host_marker.into(),
            }],
        },
    )
    .unwrap();
    let response = call(
        &gateway,
        TOKEN,
        Command::Catalog {
            options: CatalogRequest {
                relation: Some("review.Secret".into()),
                ..Default::default()
            },
        },
    );
    assert!(
        matches!(response, Response::Error(ref e) if e.code == "denied"),
        "{response:?}"
    );
    // Only an explicit host grant can authorize this otherwise ordinary principal.
    let mut tx = db.admin().transaction();
    tx.grant(host_marker, "review.Secret", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.session(host_marker)
            .snapshot()
            .scan("review.Secret")
            .unwrap()
            .len(),
        1
    );
}

fn fixture() -> (Database, Gateway, Relation) {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibilities(
        Module::new(
            "review",
            1,
            vec![],
            vec![
                Relation::new("review.Secret", vec![Role::new("body", DataType::Text)]),
                Relation::new(
                    "review.Link",
                    vec![Role::new("target", DataType::Ref("review.Secret".into()))],
                ),
            ],
        ),
        [
            ("review.Secret".into(), Visibility::Private),
            ("review.Link".into(), Visibility::Public),
        ]
        .into(),
    )
    .unwrap();
    let secret = tx.insert("review.Secret", vec!["private".into()]).unwrap();
    tx.insert("review.Link", vec![secret.into()]).unwrap();
    tx.commit().unwrap();
    let link = db.admin().snapshot().schema("review.Link").unwrap();
    let gateway = Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: TOKEN.into(),
                principal: "reader".into(),
            }],
        },
    )
    .unwrap();
    (db, gateway, link)
}

fn checked(link: &Relation, fingerprint: String) -> Command {
    Command::Checked {
        contracts: vec![Contract {
            relation: link.name.clone(),
            schema_id: link.id,
            fingerprint,
        }],
        command: Box::new(Command::Catalog {
            options: CatalogRequest {
                relation: Some(link.name.clone()),
                ..Default::default()
            },
        }),
    }
}

fn call(gateway: &Gateway, token: &str, command: Command) -> Response {
    gateway.handle(Request {
        version: 1,
        token: token.into(),
        command,
    })
}

#[test]
fn opaque_reference_contracts_never_expose_fingerprint_match_oracles() {
    let (db, gateway, link) = fixture();
    for token in ["", TOKEN] {
        for fingerprint in [link.fingerprint(), "0".repeat(64)] {
            let response = call(&gateway, token, checked(&link, fingerprint.clone()));
            assert!(
                matches!(response, Response::Error(ref e) if e.code == "denied"),
                "{response:?}"
            );
            let response = call(
                &gateway,
                token,
                Command::Read {
                    plan: ReadPlan {
                        contract: Contract {
                            relation: link.name.clone(),
                            schema_id: link.id,
                            fingerprint,
                        },
                        filter: None,
                        order: vec![],
                        limit: 10,
                        include: Default::default(),
                    },
                },
            );
            assert!(
                matches!(response, Response::Error(ref e) if e.code == "denied"),
                "{response:?}"
            );
        }
    }
    assert!(db.anonymous().snapshot().describe(&link.name).is_err());
    assert!(
        db.session("reader")
            .snapshot()
            .describe(&link.name)
            .is_err()
    );

    // Reference authority allows schema discovery, but still does not authorize
    // reading the target or rows whose reference closure includes it.
    let mut tx = db.admin().transaction();
    tx.grant(
        "reader",
        "review.Secret",
        Grant {
            read: false,
            write: false,
            reference: true,
            filter: RowFilter::All,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let response = call(&gateway, TOKEN, checked(&link, link.fingerprint()));
    assert!(
        matches!(response, Response::Ok(Output::Catalog { .. })),
        "{response:?}"
    );
    let snapshot = db.session("reader").snapshot();
    assert!(snapshot.describe(&link.name).is_ok());
    assert!(snapshot.scan(&link.name).unwrap().is_empty());
    assert!(snapshot.scan("review.Secret").is_err());
}

#[test]
fn concurrent_visibility_mutations_conflict_and_invalidate_disk_leases() {
    let (db, _, _) = fixture();
    let mut make_target_visible = db.admin().transaction();
    make_target_visible
        .set_schema_visibility("review.Secret", Visibility::Public)
        .unwrap();
    make_target_visible.commit().unwrap();
    let snapshot = db.anonymous().snapshot();
    let directory = tempfile::tempdir().unwrap();
    let lease = snapshot
        .spill(
            directory.path().join("public.redb"),
            &["review.Link"],
            SpillOptions::default(),
        )
        .unwrap();
    assert_eq!(lease.stats().unwrap().rows, 1);
    let mut first = db.admin().transaction();
    let mut second = db.admin().transaction();
    first
        .set_schema_visibility("review.Link", Visibility::Private)
        .unwrap();
    second
        .set_schema_visibility("review.Link", Visibility::Authenticated)
        .unwrap();
    first.commit().unwrap();
    assert!(matches!(second.commit(), Err(Error::Conflict)));
    assert!(snapshot.scan("review.Link").is_err());
    assert!(lease.stats().is_err());
    assert!(lease.page("review.Link", None, None, 10).is_err());
    assert!(db.anonymous().snapshot().scan("review.Link").is_err());
    assert!(db.session("reader").snapshot().scan("review.Link").is_err());
}
