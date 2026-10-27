//! Explicit offline resident-to-paged conversion, preserving IDs and permissions.
//! cargo run --example paged_import
use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};
fn main() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let source = directory.path().join("resident.redb");
    let destination = directory.path().join("paged.redb");
    let db = Database::open(&source)?;
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)?;
    let person = tx.insert("core.Person", vec!["Ada".into()])?;
    tx.grant("contacts", "core.Person", Grant::read_only())?;
    tx.commit()?;
    db.checkpoint()?;
    let mut tx = db.admin().transaction();
    tx.update("core.Person", person, vec!["Ada Lovelace".into()])?;
    tx.commit()?;
    drop(db); // The source must be offline before conversion.
    let report =
        PagedDatabase::import_resident_file(&source, &destination, PagedOptions::default())?;
    let paged = PagedDatabase::open(&destination, PagedOptions::default())?;
    assert_eq!(
        paged
            .session("contacts")?
            .get("core.Person", person)?
            .unwrap()
            .values[0],
        Value::from("Ada Lovelace")
    );
    assert!(matches!(
        paged.anonymous().get("core.Person", person),
        Err(Error::Denied)
    ));
    paged.admin().snapshot()?.verify()?;
    println!(
        "Imported {} application rows from {} at unchanged revision {}; identity {} retained",
        report.rows, report.source_format, report.revision, person
    );
    Ok(())
}
