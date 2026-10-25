//! A trusted local host transfers explicit bounded bytes; no network protocol implied.
use substrate_liquid::{
    replication::{Replica, Source},
    *,
};
fn main() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let leader = Database::open_with_options(
        directory.path().join("leader.redb"),
        DatabaseOptions {
            checkpoint_policy: None,
        },
    )?;
    let mut tx = leader.admin().transaction();
    tx.install(demo::core())?;
    let ada = tx.insert("core.Person", vec!["Ada".into()])?;
    tx.grant("reporting", "core.Person", Grant::read_only())?;
    tx.commit()?;
    let mut source = Source::new(&leader)?;
    let path = directory.path().join("follower.redb");
    let follower = Replica::create(&path, source.seed())?;
    let mut tx = leader.admin().transaction();
    tx.update("core.Person", ada, vec!["Ada Lovelace".into()])?;
    tx.commit()?;
    let packet = source.next_packet()?.expect("new revision");
    follower.apply(&packet)?;
    let snapshot = follower.snapshot_at_cursor("reporting", source.cursor())?;
    assert_eq!(
        snapshot.get("core.Person", ada)?.unwrap().values[0],
        Value::from("Ada Lovelace")
    );
    println!(
        "Follower reached revision {} with unchanged identity {ada}",
        source.cursor().revision
    );
    drop(follower);
    let follower = Replica::open(&path)?;
    let retained = follower.snapshot_eventual("reporting");
    let mut tx = leader.admin().transaction();
    tx.revoke("reporting", "core.Person")?;
    tx.commit()?;
    assert!(retained.scan("core.Person").is_ok()); // Explicitly eventual until apply.
    follower.apply(&source.next_packet()?.expect("revocation"))?;
    assert!(retained.scan("core.Person").is_err());
    println!("Restart preserved the cursor; applying revocation invalidated retained reads");
    Ok(())
}
