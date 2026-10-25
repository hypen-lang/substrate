//! Run with `cargo run --example schema_visibility`.
use std::collections::BTreeMap;
use substrate_liquid::{
    schema_control::{NamespacePolicy, SchemaQuotas},
    *,
};

fn main() -> Result<()> {
    let db = Database::memory();
    db.configure_namespace(NamespacePolicy {
        namespace: "gaming".into(),
        owner: "game".into(),
        auto_approve: true,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })?;
    let app = db.session("game");
    let module = Module::new(
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
    );
    let proposal = app.propose_schema_with_visibility(
        Id::new(),
        module,
        BTreeMap::from([("gaming.GamingMessages".into(), Visibility::Private)]),
    )?;
    app.apply_schema_proposal(proposal.id, &proposal.plan.digest)?;
    let mut tx = app.transaction();
    tx.insert("gaming.GamerProfile", vec!["Ada".into()])?;
    tx.insert("gaming.GamingMessages", vec!["Friends only".into()])?;
    tx.commit()?;

    // Trusted embedding code assigns these identities. Remote apps use credentials.
    let another_app = db.session("social").snapshot();
    assert_eq!(another_app.scan("gaming.GamerProfile")?.len(), 1);
    assert!(another_app.scan("gaming.GamingMessages").is_err());
    assert!(
        db.anonymous()
            .snapshot()
            .scan("gaming.GamerProfile")
            .is_err()
    );

    app.set_schema_visibility("gaming.GamerProfile", Visibility::Public)?;
    let public = db.anonymous().snapshot();
    assert_eq!(public.scan("gaming.GamerProfile")?.len(), 1);
    assert_eq!(
        public
            .sql()?
            .query("SELECT COUNT(*) FROM gaming.GamerProfile", &[])?
            .rows
            .len(),
        1
    );
    app.set_schema_visibility("gaming.GamerProfile", Visibility::Private)?;
    assert!(public.scan("gaming.GamerProfile").is_err()); // Live revocation.
    println!(
        "Authenticated profiles, private messages, public anonymous SQL, and live visibility changes verified."
    );
    Ok(())
}
