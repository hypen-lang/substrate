//! Run with `cargo run --offline --example partitioned`.
use substrate_liquid::{partition::*, *};

fn main() -> Result<()> {
    let dir = tempfile::tempdir().expect("temporary partition directory");
    let paths = [dir.path().join("east.redb"), dir.path().join("west.redb")];
    let partitions = [Id::from_u128(1), Id::from_u128(2)];
    let map = RoutingMap::new(Id::from_u128(100), partitions.to_vec())?;
    let manifest = map.to_json();
    let expected_fingerprint = map.fingerprint();
    let ada;
    {
        let dbs = [Database::open(&paths[0])?, Database::open(&paths[1])?];
        // Trusted provisioning is separate from application routing. Reuse the same
        // schema identities across partitions; grants remain local to each database.
        let schema = demo::core();
        for db in &dbs {
            let mut tx = db.admin().transaction();
            tx.install(schema.clone())?;
            tx.grant("people-app", "core.Person", Grant::read_write())?;
            tx.commit()?;
        }
        let router = PartitionRouter::bind(
            map,
            partitions
                .iter()
                .zip(&dbs)
                .map(|(id, db)| (*id, db.session("people-app")))
                .collect(),
        )?;
        let mut tx = router.partition(partitions[0])?.transaction();
        ada = tx.insert("core.Person", vec!["Ada".into()])?;
        tx.commit()?;
        let foreign = router.partition(partitions[1])?.allocate_identity();
        assert!(router.transaction_for(&[ada, foreign]).is_err());
        dbs[0].checkpoint()?;
        println!(
            "Committed Ada in partition {}; cross-partition transaction rejected",
            partitions[0]
        );
    }
    let restored = RoutingMap::from_json(&manifest)?;
    restored.check_fingerprint(expected_fingerprint)?;
    let dbs = [Database::open(&paths[0])?, Database::open(&paths[1])?];
    let router = PartitionRouter::bind(
        restored,
        partitions
            .iter()
            .zip(&dbs)
            .map(|(id, db)| (*id, db.session("people-app")))
            .collect(),
    )?;
    let row = router
        .for_identity(ada)
        .snapshot()
        .get("core.Person", ada)?
        .expect("persisted person");
    println!("Reopened identity {ada}: {:?}", row.values);
    Ok(())
}
