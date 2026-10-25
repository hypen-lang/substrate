use std::collections::BTreeMap;
use substrate_liquid::{
    capability::{CapabilityScope, RelationCapability},
    catalog::{CatalogRequest, CatalogType},
    schema_control::{NamespacePolicy, SchemaQuotas},
    *,
};

fn gaming() -> Module {
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
                vec![Role::new("body", DataType::Text)],
            ),
        ],
    )
}
fn owned(db: &Database) {
    db.configure_namespace(NamespacePolicy {
        namespace: "gaming".into(),
        owner: "game".into(),
        auto_approve: true,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
}
fn installed(db: &Database) -> (Id, Id) {
    owned(db);
    let app = db.session("game");
    let proposal = app
        .propose_schema_with_visibility(
            Id::new(),
            gaming(),
            BTreeMap::from([("gaming.GamingMessages".into(), Visibility::Private)]),
        )
        .unwrap();
    assert_eq!(proposal.plan.format, 2);
    assert_eq!(
        proposal.plan.visibility.as_ref().unwrap()["gaming.GamerProfile"],
        Visibility::Authenticated
    );
    app.apply_schema_proposal(proposal.id, &proposal.plan.digest)
        .unwrap();
    let mut tx = app.transaction();
    let profile = tx
        .insert("gaming.GamerProfile", vec!["Ada".into()])
        .unwrap();
    let message = tx
        .insert("gaming.GamingMessages", vec!["secret".into()])
        .unwrap();
    tx.commit().unwrap();
    (profile, message)
}
#[test]
fn default_authenticated_private_owner_recipients_and_anonymous_public() {
    let db = Database::memory();
    let (profile, message) = installed(&db);
    let stranger = db.session("other").snapshot();
    assert!(
        stranger
            .get("gaming.GamerProfile", profile)
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        stranger.get("gaming.GamingMessages", message),
        Err(Error::Denied)
    ));
    let page = stranger.catalog(&CatalogRequest::default()).unwrap();
    assert_eq!(page.relations.len(), 1);
    assert_eq!(
        page.relations[0].visibility,
        Some(Visibility::Authenticated)
    );
    assert!(page.relations[0].operations.read);
    assert!(!page.relations[0].operations.insert);
    assert!(!page.relations[0].operations.reference);
    assert!(db.anonymous().snapshot().relations().is_empty());
    assert!(
        db.session("other")
            .transaction()
            .insert("gaming.GamerProfile", vec!["write".into()])
            .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.grant("recipient", "gaming.GamingMessages", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    assert!(
        db.session("recipient")
            .snapshot()
            .get("gaming.GamingMessages", message)
            .unwrap()
            .is_some()
    );
    db.session("game")
        .set_schema_visibility("gaming.GamerProfile", Visibility::Public)
        .unwrap();
    let anon = db.anonymous().snapshot();
    assert_eq!(
        anon.catalog(&CatalogRequest::default())
            .unwrap()
            .relations
            .len(),
        1
    );
    assert!(anon.get("gaming.GamerProfile", profile).unwrap().is_some());
    assert!(anon.get("gaming.GamingMessages", message).is_err());
    assert_eq!(
        anon.sql()
            .unwrap()
            .query("SELECT COUNT(*) FROM gaming.GamerProfile", &[])
            .unwrap()
            .rows[0][0],
        rusqlite::types::Value::Integer(1)
    );
}
#[test]
fn policies_do_not_grant_reference_or_write_and_anonymous_cannot_be_elevated() {
    let db = Database::memory();
    let (profile, _) = installed(&db);
    db.session("game")
        .set_schema_visibility("gaming.GamerProfile", Visibility::Public)
        .unwrap();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "links",
        1,
        vec![("gaming".into(), 1)],
        vec![Relation::new(
            "links.Link",
            vec![Role::new(
                "profile",
                DataType::Ref("gaming.GamerProfile".into()),
            )],
        )],
    ))
    .unwrap();
    tx.grant("other", "links.Link", Grant::read_write())
        .unwrap();
    tx.grant("\0anonymous", "gaming.GamerProfile", Grant::read_write())
        .unwrap();
    tx.grant("\0anonymous", "gaming.GamingMessages", Grant::read_write())
        .unwrap();
    tx.grant_schema("\0anonymous", "anon").unwrap();
    tx.commit().unwrap();
    assert!(
        db.session("other")
            .transaction()
            .insert("links.Link", vec![profile.into()])
            .is_err()
    );
    let mut anon = db.anonymous().transaction();
    assert!(
        anon.insert("gaming.GamerProfile", vec!["bad".into()])
            .is_err()
    );
    assert!(anon.commit().is_err());
    assert!(
        db.anonymous()
            .transaction()
            .delete("gaming.GamerProfile", profile)
            .is_err()
    );
    assert!(
        db.anonymous()
            .snapshot()
            .scan("gaming.GamingMessages")
            .is_err()
    );
    assert!(
        db.anonymous()
            .transaction()
            .install(Module::new("anon", 1, vec![], vec![]))
            .is_err()
    );
}
#[test]
fn visibility_changes_invalidate_retained_native_sql_joins_and_aggregates() {
    let db = Database::memory();
    installed(&db);
    let app = db.session("game");
    app.set_schema_visibility("gaming.GamerProfile", Visibility::Public)
        .unwrap();
    let snap = db.anonymous().snapshot();
    let sql = snap.sql().unwrap();
    let texts = [
        "SELECT name FROM gaming.GamerProfile",
        "SELECT COUNT(*) FROM gaming.GamerProfile",
        "SELECT a.name FROM gaming.GamerProfile a JOIN gaming.GamerProfile b ON a.id=b.id",
        "SELECT name FROM gaming.GamerProfile LIMIT 0",
    ];
    let mut queries: Vec<_> = texts.iter().map(|s| sql.prepare(s).unwrap()).collect();
    for q in &mut queries {
        q.query(&[]).unwrap();
    }
    let original = db.admin().snapshot().schema("gaming.GamerProfile").unwrap();
    app.set_schema_visibility("gaming.GamerProfile", Visibility::Private)
        .unwrap();
    assert!(snap.scan("gaming.GamerProfile").is_err());
    assert!(snap.catalog(&CatalogRequest::default()).is_err());
    for q in &mut queries {
        assert!(q.query(&[]).is_err());
    }
    assert!(
        db.session("other")
            .snapshot()
            .scan("gaming.GamerProfile")
            .is_err()
    );
    assert_eq!(
        original,
        db.admin().snapshot().schema("gaming.GamerProfile").unwrap()
    );
}
#[test]
fn private_reference_closure_and_catalog_redaction_survive_public_outer_family() {
    let db = Database::memory();
    let (_, message) = installed(&db);
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "links",
            1,
            vec![("gaming".into(), 1)],
            vec![Relation::new(
                "links.Link",
                vec![Role::new(
                    "message",
                    DataType::Ref("gaming.GamingMessages".into()),
                )],
            )],
        ),
        Visibility::Public,
    )
    .unwrap();
    tx.insert("links.Link", vec![message.into()]).unwrap();
    tx.commit().unwrap();
    let anon = db.anonymous().snapshot();
    assert!(anon.scan("links.Link").unwrap().is_empty());
    let cat = anon
        .catalog(&CatalogRequest {
            relation: Some("links.Link".into()),
            ..Default::default()
        })
        .unwrap();
    assert!(matches!(
        cat.relations[0].roles[0].data_type,
        CatalogType::Ref { target: None }
    ));
    assert!(cat.relations[0].fingerprint.is_none());
    assert!(
        anon.sql()
            .unwrap()
            .query("SELECT COUNT(*) FROM links.Link", &[])
            .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.grant("recipient", "gaming.GamingMessages", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.session("recipient")
            .snapshot()
            .scan("links.Link")
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn credential_scopes_cap_ambient_reads_and_namespace_authority_updates_visibility() {
    let db = Database::memory();
    installed(&db);
    let app = db.session("game");
    app.set_schema_visibility("gaming.GamerProfile", Visibility::Public)
        .unwrap();
    let profile = db
        .admin()
        .snapshot()
        .schema("gaming.GamerProfile")
        .unwrap()
        .id;
    let scope = CapabilityScope {
        relations: vec![RelationCapability {
            relation: profile,
            grant: Grant {
                filter: RowFilter::Equals {
                    role: "name".into(),
                    value: "not Ada".into(),
                },
                ..Grant::read_only()
            },
        }],
        schema_namespace: None,
        delegations: 0,
    };
    let cap = db.issue_capability(Id::new(), "game", scope).unwrap();
    let scoped = app.with_capability(cap.id).unwrap();
    assert!(
        scoped
            .snapshot()
            .scan("gaming.GamerProfile")
            .unwrap()
            .is_empty()
    );
    assert!(scoped.snapshot().scan("gaming.GamingMessages").is_err());
    assert!(
        scoped
            .set_schema_visibility("gaming.GamerProfile", Visibility::Private)
            .is_err()
    );
    let token = db.issue_credential("game", cap.id, 253402300799).unwrap();
    let credential = db.authenticate(&token.token).unwrap();
    assert!(
        credential
            .snapshot()
            .scan("gaming.GamerProfile")
            .unwrap()
            .is_empty()
    );
    let ns = db
        .issue_capability(
            Id::new(),
            "game",
            CapabilityScope {
                relations: vec![],
                schema_namespace: Some("gaming".into()),
                delegations: 0,
            },
        )
        .unwrap();
    let owner = app.with_capability(ns.id).unwrap();
    owner
        .set_schema_visibility("gaming.GamerProfile", Visibility::Private)
        .unwrap();
    assert!(owner.snapshot().scan("gaming.GamerProfile").is_err());
    db.revoke_capability(ns.id).unwrap();
    assert!(
        owner
            .set_schema_visibility("gaming.GamerProfile", Visibility::Public)
            .is_err()
    );
}
#[test]
fn creation_is_atomic_unknown_names_fail_and_additive_install_keeps_old_readers() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "old",
        1,
        vec![],
        vec![Relation::new(
            "old.Row",
            vec![Role::new("v", DataType::Int)],
        )],
    ))
    .unwrap();
    tx.insert("old.Row", vec![1.into()]).unwrap();
    tx.commit().unwrap();
    let old = db.session("reader").snapshot();
    let before = db.admin().snapshot().stats().unwrap();
    let mut tx = db.admin().transaction();
    assert!(
        tx.install_with_visibilities(
            gaming(),
            BTreeMap::from([("unknown.Name".into(), Visibility::Public)])
        )
        .is_err()
    );
    assert!(tx.commit().is_err());
    assert_eq!(db.admin().snapshot().stats().unwrap(), before);
    let mut tx = db.admin().transaction();
    tx.install(gaming()).unwrap();
    drop(tx);
    assert!(db.admin().snapshot().schema("gaming.GamerProfile").is_err());
    let mut tx = db.admin().transaction();
    tx.install(gaming()).unwrap();
    tx.commit().unwrap();
    assert_eq!(old.scan("old.Row").unwrap().len(), 1);
    assert!(old.schema("gaming.GamerProfile").is_err());
    assert!(
        db.session("reader")
            .snapshot()
            .schema("gaming.GamerProfile")
            .is_ok()
    );
    let selected = db
        .session("reader")
        .snapshot()
        .sql_selected(&["old.Row"])
        .unwrap();
    assert!(
        selected
            .query("SELECT * FROM gaming.GamerProfile", &[])
            .is_err()
    );
}
#[test]
fn visibility_and_owner_survive_redo_and_checkpoint_without_schema_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.redb");
    {
        let db = Database::open(&path).unwrap();
        installed(&db);
        db.session("game")
            .set_schema_visibility("gaming.GamerProfile", Visibility::Public)
            .unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.anonymous()
                .snapshot()
                .scan("gaming.GamerProfile")
                .unwrap()
                .len(),
            1
        );
        assert!(
            db.session("other")
                .snapshot()
                .scan("gaming.GamingMessages")
                .is_err()
        );
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.anonymous()
            .snapshot()
            .scan("gaming.GamerProfile")
            .unwrap()
            .len(),
        1
    );
    db.session("game")
        .set_schema_visibility("gaming.GamerProfile", Visibility::Private)
        .unwrap();
    assert!(
        db.anonymous()
            .snapshot()
            .scan("gaming.GamerProfile")
            .is_err()
    );
    db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn namespace_ownership_and_visibility_conflicts_are_enforced() {
    let db = Database::memory();
    installed(&db);
    assert!(
        db.session("stranger")
            .set_schema_visibility("gaming.GamerProfile", Visibility::Public)
            .is_err()
    );
    let mut reader = db.session("other").transaction();
    reader.scan("gaming.GamerProfile").unwrap();
    db.session("game")
        .set_schema_visibility("gaming.GamerProfile", Visibility::Private)
        .unwrap();
    assert!(reader.commit().is_err());
    db.configure_namespace(NamespacePolicy {
        namespace: "gaming".into(),
        owner: "game".into(),
        auto_approve: true,
        enabled: false,
        quotas: SchemaQuotas::default(),
    })
    .unwrap();
    assert!(
        db.session("game")
            .set_schema_visibility("gaming.GamerProfile", Visibility::Public)
            .is_err()
    );
}

#[test]
fn host_installs_respect_namespace_owner_and_later_provisioning_adopts_policy() {
    for configure_first in [false, true] {
        let db = Database::memory();
        if configure_first {
            owned(&db);
        }
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(gaming(), Visibility::Private)
            .unwrap();
        let id = tx
            .insert("gaming.GamingMessages", vec!["private".into()])
            .unwrap();
        tx.commit().unwrap();
        if !configure_first {
            owned(&db);
        }
        assert!(
            db.session("game")
                .snapshot()
                .get("gaming.GamingMessages", id)
                .unwrap()
                .is_some()
        );
        assert!(
            db.session("other")
                .snapshot()
                .scan("gaming.GamingMessages")
                .is_err()
        );
        assert!(
            db.session("game")
                .transaction()
                .insert("gaming.GamingMessages", vec!["requires grant".into()])
                .is_err()
        );
    }
}

#[test]
fn malformed_visibility_rows_fail_atomically_even_for_trusted_host() {
    let db = Database::memory();
    installed(&db);
    let policy = db
        .admin()
        .snapshot()
        .scan("liquid_visibility.Policy")
        .unwrap()
        .remove(0);
    let before = db.admin().snapshot().stats().unwrap();
    for (role, value) in [
        ("visibility", "unknown"),
        ("relation", "not-a-uuid"),
        ("owner", "\0anonymous"),
    ] {
        let mut tx = db.admin().transaction();
        let mut values = policy.values.to_vec();
        let index = match role {
            "relation" => 0,
            "owner" => 1,
            _ => 2,
        };
        values[index] = value.into();
        assert!(
            tx.update("liquid_visibility.Policy", policy.id, values)
                .is_err()
        );
        assert!(tx.commit().is_err());
        assert_eq!(db.admin().snapshot().stats().unwrap(), before);
    }
}
