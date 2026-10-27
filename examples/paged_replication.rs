//! Offline bootstrap, durable incremental transfer and live read revocation.
use substrate_liquid::paged::{PagedDatabase, PagedReplica};
use substrate_liquid::{DataType, Grant, Module, Relation, Role, Visibility};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.redb");
    let follower_path = directory.path().join("follower.redb");
    let db = PagedDatabase::open(&source_path, Default::default())?;
    let mut transaction = db.admin().transaction()?;
    transaction.install_owned(
        Module::new(
            "contacts",
            1,
            vec![],
            vec![
                Relation::new("contacts.Person", vec![Role::new("name", DataType::Text)])
                    .unique(&["name"]),
            ],
        ),
        "contacts-app",
        Visibility::Private,
    )?;
    let person = transaction.insert("contacts.Person", vec!["Alice".into()])?;
    transaction.grant("reader-app", "contacts.Person", Grant::read_only())?;
    transaction.commit()?;
    let cursor = db.enable_replication()?;
    drop(db); // Bootstrap explicitly locks a closed source.
    let follower =
        PagedReplica::create_from_file(&source_path, &follower_path, Default::default())?;
    let retained = follower.snapshot_eventual("reader-app")?;
    assert!(retained.get("contacts.Person", person)?.is_some());

    let db = PagedDatabase::open(&source_path, Default::default())?;
    let mut transaction = db.session("contacts-app")?.transaction()?;
    transaction.update("contacts.Person", person, vec!["Alice Updated".into()])?;
    transaction.revoke("reader-app", "contacts.Person")?;
    transaction.commit()?;
    let event = db.replication_next(cursor)?.ok_or("expected an event")?;
    follower.apply(&event)?;
    let head = db.replication_cursor()?;
    assert_eq!(follower.cursor()?, head);
    assert!(retained.get("contacts.Person", person).is_err());
    let rows = follower
        .snapshot_at_cursor("contacts-app", head)?
        .lookup_page("contacts.Person", "name", &"Alice Updated".into(), None, 10)?;
    assert_eq!(rows.rows.len(), 1);
    println!(
        "Replicated revision {} with matching indexes; the retained reader was revoked.",
        head.revision
    );
    Ok(())
}
