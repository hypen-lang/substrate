//! Run with `cargo run --offline --example durable_views`.
use substrate_liquid::*;

fn main() -> Result<()> {
    let directory = tempfile::tempdir().expect("temporary database directory");
    let path = directory.path().join("views.redb");
    let db = Database::open(&path)?;
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)?;
    tx.insert("core.Person", vec!["Ada".into()])?;
    tx.insert("core.Person", vec!["Bob".into()])?;
    tx.grant(
        "app",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "Ada".into(),
            },
            ..Grant::read_only()
        },
    )?;
    tx.commit()?;
    let id = Id::new();
    db.define_view(
        id,
        "People",
        "SELECT name FROM core.Person ORDER BY name",
        &["core.Person"],
    )?;
    db.grant_view(id, "app")?;
    let view = db.session("app").snapshot().open_view(id)?;
    println!("Invoker result: {:?}", view.query()?.rows);
    assert_eq!(view.query()?.rows.len(), 1);
    db.checkpoint()?;
    drop(view);
    drop(db);

    let db = Database::open(&path)?;
    let view = db.session("app").snapshot().open_view(id)?;
    println!("After checkpoint and restart: {:?}", view.query()?.rows);
    let mut tx = db.admin().transaction();
    tx.revoke("app", "core.Person")?;
    tx.commit()?;
    assert!(view.query().is_err());
    assert!(db.session("app").snapshot().view_definition(id).is_err());
    println!("Revoking underlying access blocks retained views and definition inspection.");
    Ok(())
}
