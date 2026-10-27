//! Local scoped reads without copying large payloads. No transport is involved.
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use substrate_liquid::{
    capability::{CapabilityScope, RelationCapability},
    *,
};

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "assets",
            1,
            vec![],
            vec![Relation::new(
                "assets.Document",
                vec![
                    Role::new("workspace", DataType::Text),
                    Role::new("content", DataType::Bytes),
                ],
            )],
        ),
        Visibility::Private,
    )?;
    tx.grant("images", "assets.Document", Grant::read_write())?;
    let visible = tx.insert(
        "assets.Document",
        vec!["alpha".into(), Value::Bytes(vec![1; 65536])],
    )?;
    let hidden = tx.insert(
        "assets.Document",
        vec!["beta".into(), Value::Bytes(vec![9; 65536])],
    )?;
    tx.commit()?;

    let scope = CapabilityScope {
        relations: vec![RelationCapability {
            relation: db.admin().snapshot().schema("assets.Document")?.id,
            grant: Grant {
                filter: RowFilter::Equals {
                    role: "workspace".into(),
                    value: "alpha".into(),
                },
                ..Grant::read_only()
            },
        }],
        schema_namespace: None,
        delegations: 0,
    };
    let cap = db.issue_capability(Id::new(), "images", scope)?;
    let expires = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 300;
    let credential = db.issue_credential("images", cap.id, expires)?;
    let reader = db.authenticate(&credential.token)?;
    let original = reader
        .get_shared("assets.Document", visible)?
        .expect("visible row");
    let again = reader
        .get_shared("assets.Document", visible)?
        .expect("visible row");
    assert!(Arc::ptr_eq(&original, &again));
    assert!(reader.get_shared("assets.Document", hidden)?.is_none());

    let mut tx = db.admin().transaction();
    tx.update(
        "assets.Document",
        visible,
        vec!["alpha".into(), Value::Bytes(vec![2; 65536])],
    )?;
    tx.commit()?;
    let updated = reader
        .get_shared("assets.Document", visible)?
        .expect("updated row");
    assert_eq!(original.values[1], Value::Bytes(vec![1; 65536]));
    assert_eq!(updated.values[1], Value::Bytes(vec![2; 65536]));
    db.revoke_credential(credential.id)?;
    assert!(matches!(
        reader.get_shared("assets.Document", visible),
        Err(Error::Denied)
    ));
    // Already delivered data stays readable. Releasing these handles frees old
    // payloads once no snapshots or callers need them.
    assert_eq!(original.values[1], Value::Bytes(vec![1; 65536]));
    println!(
        "Shared payloads, scoped filtering, immutable old results, and live revocation verified."
    );
    Ok(())
}
