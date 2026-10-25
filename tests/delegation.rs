use substrate_liquid::{capability::*, schema_control::*, *};
fn fixture(db: &Database) -> (Id, Id, Id) {
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    let ada = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    let bob = tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.grant("owner", "core.Person", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    (
        db.admin().snapshot().schema("core.Person").unwrap().id,
        ada,
        bob,
    )
}
fn scope(tid: Id, depth: u8) -> CapabilityScope {
    CapabilityScope {
        relations: vec![RelationCapability {
            relation: tid,
            grant: Grant::read_write(),
        }],
        schema_namespace: None,
        delegations: depth,
    }
}
fn root(db: &Database, scope: CapabilityScope) -> DelegatedCapability {
    db.issue_capability(Id::new(), "owner", scope).unwrap()
}
fn child(
    db: &Database,
    parent: &DelegatedCapability,
    subject: &str,
    scope: CapabilityScope,
) -> DelegatedCapability {
    db.session(&parent.subject)
        .delegate_capability(parent.id, Id::new(), subject, scope)
        .unwrap()
}
fn selected(db: &Database, cap: &DelegatedCapability) -> Session {
    db.session(&cap.subject).with_capability(cap.id).unwrap()
}
fn denied<T: std::fmt::Debug>(r: Result<T>) {
    assert!(matches!(r, Err(Error::Denied)), "{r:?}");
}
#[test]
fn host_root_must_fit_current_grants_and_cannot_name_internal_families() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    assert!(
        db.issue_capability(Id::new(), "stranger", scope(tid, 1))
            .is_err()
    );
    let cap = root(&db, scope(tid, 1));
    assert_eq!(cap.subject, "owner");
    let internal = db
        .admin()
        .snapshot()
        .schema("liquid_security.Capability")
        .unwrap()
        .id;
    assert!(
        db.issue_capability(Id::new(), "owner", scope(internal, 1))
            .is_err()
    );
    assert!(
        db.session("owner")
            .snapshot()
            .schema("liquid_security.Capability")
            .is_err()
    );
}
#[test]
fn delegated_runtime_operations_enforce_row_attenuation_on_old_and_new_values() {
    let db = Database::memory();
    let (tid, ada, bob) = fixture(&db);
    let parent = root(&db, scope(tid, 2));
    let mut narrow = scope(tid, 1);
    narrow.relations[0].grant.filter = RowFilter::Equals {
        role: "name".into(),
        value: "Ada".into(),
    };
    let cap = child(&db, &parent, "worker", narrow.clone());
    let s = selected(&db, &cap);
    let descriptor = s.snapshot().describe("core.Person").unwrap();
    assert_eq!(s.snapshot().scan("core.Person").unwrap().len(), 1);
    assert!(
        s.snapshot()
            .get_dynamic(&descriptor, bob)
            .unwrap()
            .is_none()
    );
    let mut tx = s.transaction();
    denied(tx.update_dynamic(&descriptor, bob, [("name".into(), "Ada".into())].into()));
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    let mut tx = s.transaction();
    denied(tx.update_dynamic(&descriptor, ada, [("name".into(), "Bob".into())].into()));
    let mut tx = s.transaction();
    tx.insert_dynamic(&descriptor, [("name".into(), "Ada".into())].into())
        .unwrap();
    tx.commit().unwrap();
    narrow.delegations = 0;
    narrow.relations[0].grant.write = false;
    let leaf = child(&db, &cap, "reader", narrow);
    let reader = selected(&db, &leaf);
    assert_eq!(
        reader
            .snapshot()
            .sql()
            .unwrap()
            .query("SELECT count(*) FROM core.Person", &[])
            .unwrap()
            .rows[0][0],
        rusqlite::types::Value::Integer(2)
    );
    denied(reader.transaction().delete_dynamic(&descriptor, ada));
}
#[test]
fn scope_is_selected_not_union_with_ambient_authority_and_is_subject_bound() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    let parent = root(&db, scope(tid, 1));
    let mut narrow = scope(tid, 0);
    narrow.relations[0].grant = Grant::read_only();
    let cap = child(&db, &parent, "worker", narrow);
    let mut tx = db.admin().transaction();
    tx.grant("worker", "core.Person", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    assert!(db.session("stranger").with_capability(cap.id).is_err());
    denied(
        selected(&db, &cap)
            .transaction()
            .insert("core.Person", vec!["C".into()]),
    );
    assert!(selected(&db, &cap).with_capability(parent.id).is_err());
    assert!(
        db.session("worker")
            .transaction()
            .insert("core.Person", vec!["C".into()])
            .is_ok()
    );
}
#[test]
fn cannot_amplify_filters_rights_namespace_or_delegation_depth() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    let mut restricted = scope(tid, 1);
    restricted.relations[0].grant = Grant {
        filter: RowFilter::Equals {
            role: "name".into(),
            value: "Ada".into(),
        },
        ..Grant::read_only()
    };
    let parent = root(&db, restricted.clone());
    for change in 0..5 {
        let mut c = restricted.clone();
        c.delegations = 0;
        match change {
            0 => c.relations[0].grant.write = true,
            1 => c.relations[0].grant.reference = true,
            2 => c.relations[0].grant.filter = RowFilter::All,
            3 => c.schema_namespace = Some("other".into()),
            _ => c.delegations = 1,
        };
        denied(
            db.session("owner")
                .delegate_capability(parent.id, Id::new(), "worker", c),
        );
    }
    restricted.delegations = 0;
    let leaf = child(&db, &parent, "worker", restricted.clone());
    denied(selected(&db, &leaf).delegate_capability(leaf.id, Id::new(), "third", restricted));
}
#[test]
fn ancestor_revocation_fences_retained_native_runtime_sql_catalog_and_writes() {
    let db = Database::memory();
    let (tid, ada, _) = fixture(&db);
    let parent = root(&db, scope(tid, 2));
    let cap = child(&db, &parent, "worker", scope(tid, 1));
    let leaf = child(&db, &cap, "reader", scope(tid, 0));
    let s = selected(&db, &leaf);
    let old = s.snapshot();
    let descriptor = old.describe("core.Person").unwrap();
    let sql = old.sql().unwrap();
    let mut prepared = sql.prepare("SELECT name FROM core.Person").unwrap();
    let mut write = s.transaction();
    write
        .update_dynamic(&descriptor, ada, [("name".into(), "Changed".into())].into())
        .unwrap();
    db.session("owner").revoke_capability(cap.id).unwrap();
    denied(old.scan("core.Person"));
    denied(old.get_dynamic(&descriptor, ada));
    denied(old.catalog(&Default::default()));
    denied(old.page("core.Person", None, None, 10));
    denied(prepared.query(&[]));
    denied(sql.query("SELECT 1", &[]));
    denied(write.commit());
    denied(s.snapshot().scan("core.Person"));
    assert!(db.session("reader").with_capability(leaf.id).is_err());
    assert!(old.relations().is_empty());
    assert_eq!(
        db.admin()
            .snapshot()
            .get("core.Person", ada)
            .unwrap()
            .unwrap()
            .values[0],
        Value::Text("Ada".into())
    );
}
#[test]
fn direct_revocation_invalidates_cached_queries_and_regrant_cannot_resurrect_capabilities() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    let parent = root(&db, scope(tid, 1));
    let cap = child(&db, &parent, "worker", scope(tid, 0));
    let old = db.session("owner").snapshot();
    let delegated = selected(&db, &cap).snapshot();
    let mut tx = db.admin().transaction();
    tx.revoke("owner", "core.Person").unwrap();
    tx.commit().unwrap();
    denied(old.scan("core.Person"));
    denied(delegated.scan("core.Person"));
    let mut tx = db.admin().transaction();
    tx.grant("owner", "core.Person", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    assert!(db.session("worker").with_capability(cap.id).is_err());
    assert_eq!(
        db.session("owner")
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn revocation_is_scoped_and_does_not_invalidate_sibling_capabilities_or_data_snapshots() {
    let db = Database::memory();
    let (tid, ada, _) = fixture(&db);
    let parent = root(&db, scope(tid, 1));
    let first = child(&db, &parent, "first", scope(tid, 0));
    let second = child(&db, &parent, "second", scope(tid, 0));
    let old = selected(&db, &second).snapshot();
    let mut tx = db.admin().transaction();
    tx.update("core.Person", ada, vec!["new".into()]).unwrap();
    tx.commit().unwrap();
    db.session("first").revoke_capability(first.id).unwrap();
    assert_eq!(
        old.get("core.Person", ada).unwrap().unwrap().values[0],
        Value::Text("Ada".into())
    );
    assert!(db.session("first").with_capability(first.id).is_err());
    denied(db.session("stranger").revoke_capability(second.id));
}
fn namespace(db: &Database) {
    db.configure_namespace(NamespacePolicy {
        namespace: "app".into(),
        owner: "owner".into(),
        auto_approve: false,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
}
fn extension(version: u64) -> Module {
    Module::new(
        "app",
        version,
        vec![("core".into(), 1)],
        vec![Relation::new(
            &format!("app.Note{version}"),
            vec![
                Role::new("person", DataType::Ref("core.Person".into())),
                Role::new("text", DataType::Text),
            ],
        )],
    )
}
#[test]
fn delegated_schema_install_preserves_approval_reference_scope_and_owner_only_grants() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    namespace(&db);
    let mut rights = scope(tid, 1);
    rights.schema_namespace = Some("app".into());
    let parent = root(&db, rights.clone());
    rights.delegations = 0;
    let cap = child(&db, &parent, "builder", rights);
    let builder = selected(&db, &cap);
    let proposal = builder.propose_schema(Id::new(), extension(1)).unwrap();
    assert_eq!(proposal.owner, "owner");
    denied(builder.apply_schema_proposal(proposal.id, &proposal.plan.digest));
    db.decide_schema_proposal(proposal.id, &proposal.plan.digest, true)
        .unwrap();
    builder
        .apply_schema_proposal(proposal.id, &proposal.plan.digest)
        .unwrap();
    denied(builder.snapshot().describe("app.Note1"));
    assert!(db.session("owner").snapshot().describe("app.Note1").is_ok());
    denied(builder.create_schema_alias("app.Alias", "app.Note1"));
    denied(builder.set_schema_lifecycle("app.Note1", Lifecycle::Retired));
    assert_eq!(builder.schema_namespaces().unwrap().len(), 1);
    let next = builder.propose_schema(Id::new(), extension(2)).unwrap();
    db.decide_schema_proposal(next.id, &next.plan.digest, true)
        .unwrap();
    db.revoke_capability(cap.id).unwrap();
    denied(builder.apply_schema_proposal(next.id, &next.plan.digest));
    assert!(db.admin().snapshot().schema("app.Note2").is_err());
}
#[test]
fn schema_only_scope_cannot_borrow_owners_unrelated_reference_grants() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    namespace(&db);
    let mut all = scope(tid, 1);
    all.schema_namespace = Some("app".into());
    let parent = root(&db, all);
    let cap = child(
        &db,
        &parent,
        "builder",
        CapabilityScope {
            relations: vec![],
            schema_namespace: Some("app".into()),
            delegations: 0,
        },
    );
    let s = selected(&db, &cap);
    denied(s.propose_schema(Id::new(), extension(1)));
    let p = db
        .session("owner")
        .propose_schema(Id::new(), extension(1))
        .unwrap();
    db.decide_schema_proposal(p.id, &p.plan.digest, true)
        .unwrap();
    denied(s.schema_proposal(p.id));
    denied(s.cancel_schema_proposal(p.id));
    denied(s.apply_schema_proposal(p.id, &p.plan.digest));
    let other = Module::new("other", 1, vec![], vec![Relation::new("other.R", vec![])]);
    denied(s.propose_schema(Id::new(), other));
}
#[test]
fn namespace_policy_generation_invalidates_existing_schema_capability() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    namespace(&db);
    let mut rights = scope(tid, 1);
    rights.schema_namespace = Some("app".into());
    let parent = root(&db, rights.clone());
    rights.delegations = 0;
    let cap = child(&db, &parent, "worker", rights);
    let s = selected(&db, &cap);
    let old = s.snapshot();
    namespace(&db);
    denied(s.schema_namespaces());
    denied(old.scan("core.Person"));
    assert!(db.session("worker").with_capability(cap.id).is_err());
}
#[test]
fn reference_closure_never_falls_back_to_authority_or_ambient_grants() {
    let db = Database::memory();
    let (tid, ada, _) = fixture(&db);
    namespace(&db);
    let mut tx = db.admin().transaction();
    tx.install(extension(1)).unwrap();
    tx.insert("app.Note1", vec![ada.into(), "secret".into()])
        .unwrap();
    tx.grant("owner", "app.Note1", Grant::read_write()).unwrap();
    tx.commit().unwrap();
    let note = db.admin().snapshot().schema("app.Note1").unwrap().id;
    let mut all = scope(tid, 1);
    all.relations.push(RelationCapability {
        relation: note,
        grant: Grant::read_only(),
    });
    let parent = root(&db, all);
    let cap = child(
        &db,
        &parent,
        "worker",
        CapabilityScope {
            relations: vec![RelationCapability {
                relation: note,
                grant: Grant::read_only(),
            }],
            schema_namespace: None,
            delegations: 0,
        },
    );
    let snap = selected(&db, &cap).snapshot();
    assert!(snap.scan("app.Note1").unwrap().is_empty());
    assert!(
        snap.catalog(&Default::default()).unwrap().relations[0]
            .fingerprint
            .is_none()
    );
    denied(snap.describe("app.Note1"));
    denied(snap.schema("app.Note1"));
    assert!(snap.relations().is_empty());
}
#[test]
fn active_and_revoked_capabilities_survive_checkpoint_and_tail_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (active, revoked) = {
        let db = Database::open(&path).unwrap();
        let (tid, _, _) = fixture(&db);
        let parent = root(&db, scope(tid, 1));
        let active = child(&db, &parent, "active", scope(tid, 0));
        let revoked = child(&db, &parent, "revoked", scope(tid, 0));
        db.checkpoint().unwrap();
        db.revoke_capability(revoked.id).unwrap();
        (active, revoked)
    };
    for _ in 0..2 {
        let db = Database::open(&path).unwrap();
        assert_eq!(
            selected(&db, &active)
                .snapshot()
                .scan("core.Person")
                .unwrap()
                .len(),
            2
        );
        assert!(db.session("revoked").with_capability(revoked.id).is_err());
        db.admin().snapshot().verify_indexes().unwrap();
        db.checkpoint().unwrap();
    }
}
#[test]
fn malformed_and_oversized_scopes_fail_before_persistence() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    for mut bad in [scope(tid, 9), scope(tid, 1), scope(tid, 1)] {
        if bad.delegations == 1 {
            bad.relations.push(bad.relations[0].clone());
        }
        assert!(
            db.issue_capability(Id::new(), "owner", bad.clone())
                .is_err()
        );
        bad.relations.clear();
    }
    let mut bad = scope(tid, 1);
    bad.relations[0].grant.filter = RowFilter::Equals {
        role: "name".into(),
        value: 42.into(),
    };
    assert!(db.issue_capability(Id::new(), "owner", bad).is_err());
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(96))]
    #[test]
    fn arbitrary_delegated_row_reads_match_independent_filter_model(allowed in proptest::bool::ANY, name in proptest::sample::select(vec!["Ada","Bob","missing"])) {
        let db=Database::memory();let(tid,_,_)=fixture(&db);let parent=root(&db,scope(tid,1));let mut c=scope(tid,0);
        c.relations[0].grant=Grant{read:true,write:false,reference:allowed,filter:RowFilter::Equals{role:"name".into(),value:name.into()}};
        let cap=child(&db,&parent,"worker",c);let old=selected(&db,&cap).snapshot();
        let expected:Vec<_>=["Ada","Bob"].into_iter().filter(|n| *n==name).map(|n| Value::Text(n.into())).collect();
        proptest::prop_assert_eq!(old.scan("core.Person").unwrap().iter().map(|r|r.values[0].clone()).collect::<Vec<_>>(),expected);
        db.revoke_capability(parent.id).unwrap();proptest::prop_assert!(old.scan("core.Person").is_err());
    }
}

#[test]
fn capability_protocol_keeps_i64_filters_exact_and_rejects_principal_injection() {
    use substrate_liquid::service::*;
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    let parent = root(&db, scope(tid, 1));
    let gateway = Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![
                ClientCredential {
                    token: "o".repeat(32),
                    principal: "owner".into(),
                },
                ClientCredential {
                    token: "w".repeat(32),
                    principal: "worker".into(),
                },
            ],
        },
    )
    .unwrap();
    let cap = child(&db, &parent, "worker", scope(tid, 0));
    let response = gateway.handle(Request {
        version: 1,
        token: "w".repeat(32),
        command: Command::UsingCapability {
            capability: cap.id,
            command: Box::new(Command::Catalog {
                options: Default::default(),
            }),
        },
    });
    assert!(matches!(response, Response::Ok(Output::Catalog { .. })));
    let wrong = gateway.handle(Request {
        version: 1,
        token: "o".repeat(32),
        command: Command::UsingCapability {
            capability: cap.id,
            command: Box::new(Command::Capabilities),
        },
    });
    assert!(matches!(wrong, Response::Error(_)));
    let mut exact = scope(tid, 0);
    exact.relations[0].grant.filter = RowFilter::Equals {
        role: "n".into(),
        value: Value::Int(9007199254740993),
    };
    let json = serde_json::to_value(&exact).unwrap();
    assert_eq!(
        json["relations"][0]["grant"]["filter"]["Equals"]["value"]["Int"],
        "9007199254740993"
    );
    assert_eq!(
        serde_json::from_value::<CapabilityScope>(json.clone()).unwrap(),
        exact
    );
    let mut bad = json;
    bad["relations"][0]["grant"]["filter"]["Equals"]["value"]["Int"] =
        serde_json::json!(9007199254740993i64);
    assert!(serde_json::from_value::<CapabilityScope>(bad).is_err());
    assert!(serde_json::from_str::<Command>(r#"{"op":"using_capability","capability":"00000000-0000-0000-0000-000000000001","principal":"owner","command":{"op":"capabilities"}}"#).is_err());
    db.revoke_capability(parent.id).unwrap();
    assert!(matches!(
        gateway.handle(Request {
            version: 1,
            token: "w".repeat(32),
            command: Command::UsingCapability {
                capability: cap.id,
                command: Box::new(Command::Capabilities)
            }
        }),
        Response::Error(_)
    ));
}
#[test]
fn capability_ledger_bounds_collisions_and_reserved_namespace_are_enforced() {
    let db = Database::memory();
    let (tid, _, _) = fixture(&db);
    let parent = root(&db, scope(tid, 1));
    assert!(
        db.issue_capability(parent.id, "owner", scope(tid, 1))
            .is_err()
    );
    let mut policy = NamespacePolicy {
        namespace: "liquid_security".into(),
        owner: "owner".into(),
        auto_approve: true,
        enabled: true,
        quotas: SchemaQuotas::default(),
    };
    assert!(db.configure_namespace(policy.clone()).is_err());
    policy.namespace = "LIQUID_SECURITY".into();
    assert!(db.configure_namespace(policy).is_err());
    // Root and 255 descendants exhaust this issuer's lifetime ledger budget.
    for i in 0..255 {
        child(&db, &parent, &format!("reader{i}"), scope(tid, 0));
    }
    assert!(
        db.session("owner")
            .delegate_capability(parent.id, Id::new(), "overflow", scope(tid, 0))
            .is_err()
    );
    assert_eq!(
        selected(&db, &parent)
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn retained_restricted_snapshot_fails_closed_after_host_handle_is_dropped() {
    let old = {
        let db = Database::memory();
        fixture(&db);
        db.session("owner").snapshot()
    };
    denied(old.scan("core.Person"));
}
#[test]
fn capability_crash_child() {
    let Ok(path) = std::env::var("SUBSTRATE_CAP_CRASH_PATH") else {
        return;
    };
    let db = Database::open(path).unwrap();
    if std::env::var("SUBSTRATE_CAP_CRASH_PHASE").unwrap() == "issue" {
        let (tid, _, _) = fixture(&db);
        db.issue_capability(Id::from_u128(100), "owner", scope(tid, 1))
            .unwrap();
        db.session("owner")
            .delegate_capability(
                Id::from_u128(100),
                Id::from_u128(101),
                "worker",
                scope(tid, 0),
            )
            .unwrap();
    } else {
        db.revoke_capability(Id::from_u128(100)).unwrap();
    }
    std::process::exit(0); // no destructors/checkpoint on shutdown
}
#[test]
fn abrupt_process_recovery_preserves_issuance_and_cascading_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    for phase in ["issue", "revoke"] {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "capability_crash_child"])
            .env("SUBSTRATE_CAP_CRASH_PATH", &path)
            .env("SUBSTRATE_CAP_CRASH_PHASE", phase)
            .output()
            .unwrap();
        assert!(result.status.success(), "{:?}", result);
        let db = Database::open(&path).unwrap();
        let selected = db.session("worker").with_capability(Id::from_u128(101));
        if phase == "issue" {
            assert_eq!(
                selected
                    .unwrap()
                    .snapshot()
                    .scan("core.Person")
                    .unwrap()
                    .len(),
                2
            );
            db.checkpoint().unwrap();
        } else {
            assert!(selected.is_err());
        }
        db.admin().snapshot().verify_indexes().unwrap();
    }
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(96))]
    #[test]
    fn revocation_tree_matches_independent_ancestor_model(revoked in proptest::collection::vec(0usize..7,0..9)) {
        let db=Database::memory();let(tid,_,_)=fixture(&db);let mut caps=vec![root(&db,scope(tid,2))];
        caps.push(child(&db,&caps[0],"a",scope(tid,1)));caps.push(child(&db,&caps[0],"b",scope(tid,1)));
        for (parent,name) in [(1,"aa"),(1,"ab"),(2,"ba"),(2,"bb")] {caps.push(child(&db,&caps[parent],name,scope(tid,0)));}
        let retained:Vec<_>=caps.iter().map(|c|selected(&db,c).snapshot()).collect();
        for i in &revoked {db.revoke_capability(caps[*i].id).unwrap();}
        let ancestors=[vec![0],vec![0,1],vec![0,2],vec![0,1,3],vec![0,1,4],vec![0,2,5],vec![0,2,6]];
        for i in 0..7 {let allowed=ancestors[i].iter().all(|p|!revoked.contains(p));
            proptest::prop_assert_eq!(retained[i].scan("core.Person").is_ok(),allowed);
            proptest::prop_assert_eq!(db.session(&caps[i].subject).with_capability(caps[i].id).is_ok(),allowed);
        }
    }
}

#[test]
fn delegated_proposal_redacts_reference_hashes_and_host_can_review_opaque_requirements() {
    let db = Database::memory();
    fixture(&db);
    namespace(&db);
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "linked",
        1,
        vec![("core".into(), 1)],
        vec![Relation::new(
            "linked.Link",
            vec![Role::new("person", DataType::Ref("core.Person".into()))],
        )],
    ))
    .unwrap();
    tx.grant("owner", "linked.Link", Grant::reference_only())
        .unwrap();
    tx.commit().unwrap();
    let link = db.admin().snapshot().schema("linked.Link").unwrap().id;
    let parent = root(
        &db,
        CapabilityScope {
            relations: vec![RelationCapability {
                relation: link,
                grant: Grant::reference_only(),
            }],
            schema_namespace: Some("app".into()),
            delegations: 1,
        },
    );
    let cap = child(
        &db,
        &parent,
        "builder",
        CapabilityScope {
            delegations: 0,
            ..parent.scope.clone()
        },
    );
    let s = selected(&db, &cap);
    let module = Module::new(
        "app",
        1,
        vec![("linked".into(), 1)],
        vec![Relation::new(
            "app.UseLink",
            vec![Role::new("link", DataType::Ref("linked.Link".into()))],
        )],
    );
    let plan = s.propose_schema(Id::new(), module).unwrap();
    assert!(plan.plan.references[0].fingerprint.is_none());
    db.decide_schema_proposal(plan.id, &plan.plan.digest, true)
        .unwrap();
    s.apply_schema_proposal(plan.id, &plan.plan.digest).unwrap();
    // A receipt originally produced under the owner's broader visibility is also redacted.
    let next = db
        .session("owner")
        .propose_schema(
            Id::new(),
            Module::new(
                "app",
                2,
                vec![("linked".into(), 1)],
                vec![Relation::new(
                    "app.Other",
                    vec![Role::new("link", DataType::Ref("linked.Link".into()))],
                )],
            ),
        )
        .unwrap();
    assert!(next.plan.references[0].fingerprint.is_some());
    assert!(
        s.schema_proposal(next.id).unwrap().plan.references[0]
            .fingerprint
            .is_none()
    );
}
