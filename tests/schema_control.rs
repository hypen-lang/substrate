use substrate_liquid::{catalog::CatalogRequest, schema_control::*, service::*, *};
fn policy(auto: bool) -> NamespacePolicy {
    NamespacePolicy {
        namespace: "agent".into(),
        owner: "alice".into(),
        auto_approve: auto,
        enabled: true,
        quotas: SchemaQuotas::default(),
    }
}
fn setup(db: &Database, auto: bool) -> Id {
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
        .unwrap();
    let person = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.grant(
        "alice",
        "core.Person",
        Grant {
            reference: true,
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db.configure_namespace(policy(auto)).unwrap();
    person
}
fn module(version: u64, name: &str) -> Module {
    Module::new(
        "agent",
        version,
        vec![("core".into(), 1)],
        vec![
            Relation::new(
                &format!("agent.{name}"),
                vec![
                    Role::new("person", DataType::Ref("core.Person".into())),
                    Role::new("value", DataType::Int),
                ],
            )
            .unique(&["person"]),
        ],
    )
}
fn install(db: &Database, version: u64, name: &str) -> SchemaProposal {
    let s = db.session("alice");
    let p = s
        .propose_schema_with_visibility(
            Id::new(),
            module(version, name),
            [(format!("agent.{name}"), Visibility::Private)].into(),
        )
        .unwrap();
    if p.status == ProposalStatus::Pending {
        db.decide_schema_proposal(p.id, &p.plan.digest, true)
            .unwrap();
    }
    s.apply_schema_proposal(p.id, &p.plan.digest).unwrap()
}
#[test]
fn proposal_requires_exact_host_approval_then_installs_atomically() {
    let db = Database::memory();
    setup(&db, false);
    let s = db.session("alice");
    let before = s.snapshot();
    let p = s.propose_schema(Id::new(), module(1, "Score")).unwrap();
    assert_eq!(p.status, ProposalStatus::Pending);
    assert_eq!(p.plan.existing_record_rewrites, 0);
    assert_eq!(p.plan.costs[0].minimum_packed_put_bytes, 57);
    assert_eq!(p.plan.costs[0].reference_index_entries_per_row, 1);
    assert!(s.snapshot().schema("agent.Score").is_err());
    assert!(s.apply_schema_proposal(p.id, &p.plan.digest).is_err());
    assert!(db.decide_schema_proposal(p.id, "wrong", true).is_err());
    db.decide_schema_proposal(p.id, &p.plan.digest, true)
        .unwrap();
    let done = s.apply_schema_proposal(p.id, &p.plan.digest).unwrap();
    assert_eq!(done.status, ProposalStatus::Installed);
    assert_eq!(
        s.snapshot().schema("agent.Score").unwrap(),
        p.plan.module.relations[0]
    );
    assert!(before.schema("agent.Score").is_err());
    assert!(db.session("bob").snapshot().schema("agent.Score").is_err());
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn repeated_submission_and_apply_are_idempotent_and_not_bearer_authority() {
    let db = Database::memory();
    setup(&db, true);
    let s = db.session("alice");
    let id = Id::new();
    let m = module(1, "Score");
    let p = s.propose_schema(id, m.clone()).unwrap();
    let revision = s.snapshot().revision();
    assert_eq!(
        s.propose_schema(id, m.clone()).unwrap().plan.digest,
        p.plan.digest
    );
    assert_eq!(s.snapshot().revision(), revision);
    assert!(s.propose_schema(id, module(1, "Different")).is_err());
    assert!(db.session("bob").schema_proposal(id).is_err());
    assert!(
        db.session("bob")
            .apply_schema_proposal(id, &p.plan.digest)
            .is_err()
    );
    s.apply_schema_proposal(id, &p.plan.digest).unwrap();
    let revision = s.snapshot().revision();
    s.apply_schema_proposal(id, &p.plan.digest).unwrap();
    assert_eq!(s.snapshot().revision(), revision);
    assert_eq!(
        s.propose_schema(id, m).unwrap().status,
        ProposalStatus::Installed
    );
}
#[test]
fn unknown_private_reference_and_import_guesses_do_not_disclose_catalog() {
    let db = Database::memory();
    setup(&db, true);
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "vault",
            1,
            vec![],
            vec![Relation::new("vault.Secret", vec![])],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.commit().unwrap();
    let s = db.session("alice");
    let attempt = |name: &str| {
        let mut m = module(1, "Score");
        m.relations[0].roles[0].data_type = DataType::Ref(name.into());
        s.propose_schema(Id::new(), m).unwrap_err().to_string()
    };
    assert_eq!(attempt("vault.Secret"), attempt("missing.Secret"));
    let mut unused = module(1, "Score");
    unused.imports.push(("vault".into(), 999));
    assert!(s.propose_schema(Id::new(), unused).is_err());
    assert!(
        db.session("bob")
            .propose_schema(Id::new(), module(1, "Score"))
            .is_err()
    );
    assert!(db.session("bob").schema_namespaces().unwrap().is_empty());
    assert_eq!(s.schema_namespaces().unwrap()[0].usage.proposals, 0);
}
#[test]
fn quota_changes_and_reference_revocation_invalidate_prior_approval() {
    let db = Database::memory();
    setup(&db, true);
    let s = db.session("alice");
    let p = s.propose_schema(Id::new(), module(1, "Score")).unwrap();
    db.configure_namespace(policy(true)).unwrap();
    assert!(matches!(
        s.apply_schema_proposal(p.id, &p.plan.digest),
        Err(Error::Conflict)
    ));
    let p = s.propose_schema(Id::new(), module(1, "Score")).unwrap();
    let mut tx = db.admin().transaction();
    tx.revoke("alice", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        s.apply_schema_proposal(p.id, &p.plan.digest),
        Err(Error::Denied)
    ));
    assert!(s.snapshot().schema("agent.Score").is_err());
    assert_eq!(
        s.schema_proposal(p.id).unwrap().status,
        ProposalStatus::Approved
    );
}
#[test]
fn quotas_count_history_and_cancelled_proposals_without_reserving_schema() {
    let db = Database::memory();
    setup(&db, true);
    let mut p = policy(true);
    p.quotas.relations = 1;
    p.quotas.proposals = 2;
    db.configure_namespace(p).unwrap();
    let s = db.session("alice");
    let a = s.propose_schema(Id::new(), module(1, "A")).unwrap();
    s.cancel_schema_proposal(a.id).unwrap();
    assert!(s.apply_schema_proposal(a.id, &a.plan.digest).is_err());
    let b = s.propose_schema(Id::new(), module(1, "B")).unwrap();
    s.apply_schema_proposal(b.id, &b.plan.digest).unwrap();
    assert!(s.propose_schema(Id::new(), module(2, "C")).is_err());
    let info = &s.schema_namespaces().unwrap()[0];
    assert_eq!(info.usage.relations, 1);
    assert_eq!(info.usage.proposals, 2);
    s.set_schema_lifecycle("agent.B", Lifecycle::Retired)
        .unwrap();
    assert_eq!(s.schema_namespaces().unwrap()[0].usage.relations, 1);
}
#[test]
fn competing_same_version_installs_have_one_winner_and_retryable_conflict() {
    let db = Database::memory();
    setup(&db, true);
    let s = db.session("alice");
    let a = s.propose_schema(Id::new(), module(1, "A")).unwrap();
    let b = s.propose_schema(Id::new(), module(1, "B")).unwrap();
    let barrier = std::sync::Barrier::new(2);
    let outcomes = std::thread::scope(|scope| {
        let handles = [a, b]
            .into_iter()
            .map(|p| {
                let s = &s;
                let b = &barrier;
                scope.spawn(move || {
                    b.wait();
                    s.apply_schema_proposal(p.id, &p.plan.digest)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(Error::Conflict)))
            .count(),
        1
    );
    assert_eq!(s.schema_namespaces().unwrap()[0].usage.relations, 1);
}
#[test]
fn readers_continue_during_online_installation() {
    let db = Database::memory();
    let person = setup(&db, true);
    let s = db.session("alice");
    let pinned = s.snapshot();
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            for _ in 0..100 {
                let snap = s.snapshot();
                assert_eq!(snap.scan("core.Person").unwrap()[0].id, person);
                let names = snap.catalog(&CatalogRequest::default()).unwrap();
                assert!(names.relations.iter().any(|r| r.name == "core.Person"));
            }
        });
        barrier.wait();
        for i in 1..=8 {
            install(&db, i, &format!("R{i}"));
        }
    });
    assert_eq!(
        pinned.scan("core.Person").unwrap(),
        s.snapshot().scan("core.Person").unwrap()
    );
    assert!(pinned.schema("agent.R1").is_err());
    assert_eq!(s.schema_namespaces().unwrap()[0].latest_version, "8");
}
#[test]
fn dynamic_rust_records_need_no_generation_and_keep_contracts_and_auth() {
    let db = Database::memory();
    let person = setup(&db, true);
    install(&db, 1, "Score");
    let s = db.session("alice");
    let d = s.snapshot().describe("agent.Score").unwrap();
    let id = s
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_dynamic(
                &d,
                [
                    ("person".into(), person.into()),
                    ("value".into(), 42.into()),
                ]
                .into(),
            )
        })
        .unwrap();
    assert_eq!(
        s.snapshot().get_dynamic(&d, id).unwrap().unwrap()["value"],
        Value::Int(42)
    );
    s.transact(RetryPolicy::default(), |tx| {
        tx.update_dynamic(&d, id, [("value".into(), 43.into())].into())
    })
    .unwrap();
    assert_eq!(
        s.snapshot().get_dynamic(&d, id).unwrap().unwrap()["value"],
        Value::Int(43)
    );
    assert!(db.session("bob").snapshot().get_dynamic(&d, id).is_err());
    assert!(
        s.transact(RetryPolicy::default(), |tx| tx
            .insert_dynamic(&d, [("value".into(), "wrong".into())].into()))
            .is_err()
    );
    s.transact(RetryPolicy::default(), |tx| tx.delete_dynamic(&d, id))
        .unwrap();
    assert!(s.snapshot().get_dynamic(&d, id).unwrap().is_none());
    let other = Database::memory();
    let mut tx = other.admin().transaction();
    tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
        .unwrap();
    let mut m = module(1, "Score");
    m.relations[0].id = d.relation().id;
    m.relations[0].roles[1].data_type = DataType::Text;
    tx.install_with_visibility(m, substrate_liquid::Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    let mut tx = other.admin().transaction();
    assert!(tx.insert_dynamic(&d, Fields::new()).is_err());
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}
#[test]
fn immutable_aliases_and_monotonic_retirement_preserve_old_reads_and_rows() {
    let db = Database::memory();
    let person = setup(&db, true);
    install(&db, 1, "Score");
    let s = db.session("alice");
    s.create_schema_alias("agent.Rating", "agent.Score")
        .unwrap();
    s.create_schema_alias("agent.Rating", "agent.Score")
        .unwrap();
    let d = s.snapshot().describe("agent.Rating").unwrap();
    assert_eq!(d.relation().name, "agent.Score");
    assert!(
        db.session("bob")
            .snapshot()
            .describe("agent.Rating")
            .is_err()
    );
    assert!(s.propose_schema(Id::new(), module(2, "Rating")).is_err());
    let id = s
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_dynamic(
                &d,
                [("person".into(), person.into()), ("value".into(), 1.into())].into(),
            )
        })
        .unwrap();
    s.set_schema_lifecycle("agent.Score", Lifecycle::Deprecated)
        .unwrap();
    s.set_schema_lifecycle("agent.Score", Lifecycle::Retired)
        .unwrap();
    assert!(
        s.set_schema_lifecycle("agent.Score", Lifecycle::Active)
            .is_err()
    );
    assert!(s.snapshot().get_dynamic(&d, id).unwrap().is_some());
    let cat = s
        .snapshot()
        .catalog(&CatalogRequest {
            relation: Some("agent.Rating".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(cat.relations[0].lifecycle, Lifecycle::Retired);
    assert_eq!(cat.relations[0].aliases, vec!["agent.Rating"]);
    let m = Module::new(
        "agent",
        2,
        vec![],
        vec![Relation::new(
            "agent.New",
            vec![Role::new("old", DataType::Ref("agent.Score".into()))],
        )],
    );
    assert!(s.propose_schema(Id::new(), m).is_err());
}
#[test]
fn checkpoint_reopen_preserves_approval_policy_and_install_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let id;
    {
        let db = Database::open(&path).unwrap();
        setup(&db, false);
        let p = db
            .session("alice")
            .propose_schema(Id::new(), module(1, "Score"))
            .unwrap();
        id = p.id;
        db.decide_schema_proposal(id, &p.plan.digest, true).unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let s = db.session("alice");
        let p = s.schema_proposal(id).unwrap();
        assert_eq!(p.status, ProposalStatus::Approved);
        s.apply_schema_proposal(id, &p.plan.digest).unwrap();
        s.create_schema_alias("agent.Rating", "agent.Score")
            .unwrap();
        s.set_schema_lifecycle("agent.Score", Lifecycle::Deprecated)
            .unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let s = db.session("alice");
    let p = s.schema_proposal(id).unwrap();
    assert_eq!(
        s.apply_schema_proposal(id, &p.plan.digest).unwrap().status,
        ProposalStatus::Installed
    );
    assert_eq!(
        s.snapshot()
            .describe("agent.Rating")
            .unwrap()
            .relation()
            .name,
        "agent.Score"
    );
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn terminal_decisions_disabled_namespaces_and_owner_changes_fail_closed() {
    let db = Database::memory();
    setup(&db, false);
    let s = db.session("alice");
    let p = s.propose_schema(Id::new(), module(1, "Score")).unwrap();
    db.decide_schema_proposal(p.id, &p.plan.digest, false)
        .unwrap();
    assert!(s.apply_schema_proposal(p.id, &p.plan.digest).is_err());
    assert!(
        db.decide_schema_proposal(p.id, &p.plan.digest, true)
            .is_err()
    );
    let mut config = policy(true);
    config.owner = "bob".into();
    assert!(db.configure_namespace(config).is_err());
    let mut config = policy(true);
    config.enabled = false;
    db.configure_namespace(config).unwrap();
    assert!(s.propose_schema(Id::new(), module(1, "Score")).is_err());
}
#[test]
fn gateway_does_not_expose_host_approval_or_control_tables() {
    let db = Database::memory();
    setup(&db, false);
    let g = Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                principal: "alice".into(),
                token: "schema-control-credential-00000000".into(),
            }],
        },
    )
    .unwrap();
    let id = Id::new();
    let response = g.handle(Request {
        version: 1,
        token: "schema-control-credential-00000000".into(),
        command: Command::SchemaPropose {
            id,
            module: module(1, "Score"),
        },
    });
    assert!(
        matches!(response,Response::Ok(Output::SchemaProposal{proposal}) if proposal.status==ProposalStatus::Pending)
    );
    assert!(
        serde_json::from_str::<Command>(
            r#"{"op":"schema_approve","id":"00000000-0000-0000-0000-000000000001"}"#
        )
        .is_err()
    );
    assert!(
        db.session("alice")
            .snapshot()
            .schema("liquid_control.Namespace")
            .is_err()
    );
    assert!(
        db.session("alice")
            .snapshot()
            .catalog(&CatalogRequest::default())
            .unwrap()
            .relations
            .iter()
            .all(|r| !r.name.starts_with("liquid_control"))
    );
}

#[test]
fn every_quota_dimension_is_enforced_before_installation() {
    for which in 0..5 {
        let db = Database::memory();
        setup(&db, true);
        let mut config = policy(true);
        match which {
            0 => config.quotas.relations = 1,
            1 => config.quotas.roles = 1,
            2 => config.quotas.versions = 1,
            3 => config.quotas.schema_bytes = 1,
            _ => config.quotas.proposals = 1,
        }
        db.configure_namespace(config).unwrap();
        let s = db.session("alice");
        if which == 0 || which == 2 {
            install(&db, 1, "First");
            assert!(s.propose_schema(Id::new(), module(2, "Second")).is_err());
        } else if which == 4 {
            let p = s.propose_schema(Id::new(), module(1, "First")).unwrap();
            s.cancel_schema_proposal(p.id).unwrap();
            assert!(s.propose_schema(Id::new(), module(1, "Second")).is_err());
        } else {
            assert!(s.propose_schema(Id::new(), module(1, "First")).is_err());
        }
        assert!(s.snapshot().schema("agent.Second").is_err());
    }
}
#[test]
fn schema_control_crash_child() {
    let Ok(path) = std::env::var("SUBSTRATE_SCHEMA_CRASH_PATH") else {
        return;
    };
    let db = Database::open(path).unwrap();
    let s = db.session("alice");
    let id =
        Id(uuid::Uuid::parse_str(&std::env::var("SUBSTRATE_SCHEMA_PROPOSAL").unwrap()).unwrap());
    let p = s.schema_proposal(id).unwrap();
    match std::env::var("SUBSTRATE_SCHEMA_CRASH_PHASE")
        .unwrap()
        .as_str()
    {
        "uncommitted" => {
            let mut tx = db.admin().transaction();
            tx.install_with_visibility(p.plan.module, substrate_liquid::Visibility::Private)
                .unwrap();
            std::process::exit(0);
        }
        "approve" => {
            db.decide_schema_proposal(id, &p.plan.digest, true).unwrap();
        }
        "apply" => {
            s.apply_schema_proposal(id, &p.plan.digest).unwrap();
        }
        _ => panic!(),
    }
    std::process::exit(0);
}
#[test]
fn abrupt_process_recovery_never_loses_acknowledged_decisions_or_installs_partial_state() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("schema.redb");
    let id;
    {
        let db = Database::open(&path).unwrap();
        setup(&db, false);
        id = db
            .session("alice")
            .propose_schema(Id::new(), module(1, "Score"))
            .unwrap()
            .id;
    }
    for (phase, expected) in [
        ("uncommitted", ProposalStatus::Pending),
        ("approve", ProposalStatus::Approved),
        ("apply", ProposalStatus::Installed),
    ] {
        assert!(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "schema_control_crash_child"])
                .env("SUBSTRATE_SCHEMA_CRASH_PATH", &path)
                .env("SUBSTRATE_SCHEMA_PROPOSAL", id.to_string())
                .env("SUBSTRATE_SCHEMA_CRASH_PHASE", phase)
                .status()
                .unwrap()
                .success()
        );
        let db = Database::open(&path).unwrap();
        let s = db.session("alice");
        assert_eq!(s.schema_proposal(id).unwrap().status, expected);
        assert_eq!(
            s.snapshot().schema("agent.Score").is_ok(),
            expected == ProposalStatus::Installed
        );
        db.admin().snapshot().verify_indexes().unwrap();
    }
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn install_cancel_histories_match_bounded_catalog_model(actions in proptest::collection::vec(proptest::bool::ANY,1..30)) {
        let db=Database::memory();setup(&db,true);let mut config=policy(true);config.quotas.relations=8;db.configure_namespace(config).unwrap();let s=db.session("alice");let mut installed=0u64;let mut proposed=0u32;
        for (i,apply) in actions.into_iter().enumerate() {
            let p=s.propose_schema(Id::new(),module(installed+1,&format!("R{i}")));
            if installed==8 {proptest::prop_assert!(p.is_err());continue;}
            let p=p.unwrap();proposed+=1;
            if apply {s.apply_schema_proposal(p.id,&p.plan.digest).unwrap();installed+=1;}else{s.cancel_schema_proposal(p.id).unwrap();}
            let info=&s.schema_namespaces().unwrap()[0];proptest::prop_assert_eq!(info.usage.relations,installed as u32);proptest::prop_assert_eq!(info.usage.proposals,proposed);
        }
        proptest::prop_assert_eq!(s.snapshot().relations().iter().filter(|r|r.name.starts_with("agent.")).count(),installed as usize);
        db.admin().snapshot().verify_indexes().unwrap();
    }
}

#[test]
fn wire_proposals_require_explicit_ids_and_hidden_target_hashes_are_redacted() {
    let missing = serde_json::json!({"op":"schema_propose","id":Id::new(),"module":{"name":"agent","version":1,"imports":[],"relations":[{"name":"agent.New","roles":[],"unique":[]}]}});
    assert!(serde_json::from_value::<Command>(missing).is_err());
    let db = Database::memory();
    setup(&db, true);
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "vault",
            1,
            vec![],
            vec![
                Relation::new("vault.Hidden", vec![]),
                Relation::new(
                    "vault.PublicLink",
                    vec![Role::new("hidden", DataType::Ref("vault.Hidden".into()))],
                ),
            ],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.grant("alice", "vault.PublicLink", Grant::reference_only())
        .unwrap();
    tx.commit().unwrap();
    let module = Module::new(
        "agent",
        1,
        vec![("vault".into(), 1)],
        vec![Relation::new(
            "agent.Link",
            vec![Role::new(
                "target",
                DataType::Ref("vault.PublicLink".into()),
            )],
        )],
    );
    let p = db
        .session("alice")
        .propose_schema(Id::new(), module)
        .unwrap();
    assert!(p.plan.references[0].fingerprint.is_none());
    let serialized = serde_json::to_string(&p).unwrap();
    assert!(!serialized.contains("vault.Hidden"));
    assert!(
        !serialized.contains(
            &db.admin()
                .snapshot()
                .schema("vault.PublicLink")
                .unwrap()
                .fingerprint()
        )
    );
}

#[test]
fn managed_namespaces_cannot_bypass_approval_with_legacy_native_install_grants() {
    let db = Database::memory();
    setup(&db, false);
    let mut tx = db.admin().transaction();
    tx.grant_schema("alice", "agent").unwrap();
    tx.grant_schema("alice", "liquid_control").unwrap();
    tx.commit().unwrap();
    let mut tx = db.session("alice").transaction();
    assert!(matches!(
        tx.install_with_visibility(module(1, "Bypass"), substrate_liquid::Visibility::Private),
        Err(Error::Denied)
    ));
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    let mut tx = db.session("alice").transaction();
    assert!(matches!(
        tx.install_with_visibility(
            Module::new("liquid_control", 2, vec![], vec![]),
            substrate_liquid::Visibility::Private
        ),
        Err(Error::Denied)
    ));
    assert_eq!(
        db.session("alice")
            .propose_schema(Id::new(), module(1, "Score"))
            .unwrap()
            .status,
        ProposalStatus::Pending
    );
}

#[test]
fn enrolling_namespace_races_legacy_install_with_a_transaction_conflict() {
    let db = Database::memory();
    setup(&db, true);
    let mut tx = db.admin().transaction();
    tx.grant_schema("legacy", "newapp").unwrap();
    tx.commit().unwrap();
    let mut legacy = db.session("legacy").transaction();
    legacy
        .install_with_visibility(
            Module::new(
                "newapp",
                1,
                vec![],
                vec![Relation::new("newapp.Bypass", vec![])],
            ),
            substrate_liquid::Visibility::Private,
        )
        .unwrap();
    let mut managed = policy(false);
    managed.namespace = "newapp".into();
    db.configure_namespace(managed).unwrap();
    assert!(matches!(legacy.commit(), Err(Error::Conflict)));
    assert!(db.admin().snapshot().schema("newapp.Bypass").is_err());
}
