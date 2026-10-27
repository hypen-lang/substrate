//! `cargo run --example sql_writes`: atomic SQL mutations through the normal
//! authenticated local protocol, persisted in bounded-memory paged storage.
use std::os::unix::fs::PermissionsExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use substrate_liquid::{local::*, paged::*, service::*, *};
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    let db = PagedDatabase::open(dir.path().join("demo.redb"), PagedOptions::default())?;
    let mut tx = db.admin().transaction()?;
    tx.install_owned(demo::core(), "app", Visibility::Private)?;
    tx.commit()?;
    let socket = dir.path().join("socket");
    let token = "example-only-token-at-least-thirty-two-bytes";
    let server = LocalServer::bind(
        db.clone(),
        &socket,
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: token.into(),
                principal: "app".into(),
            }],
        },
    )?;
    let stop = Arc::new(AtomicBool::new(false));
    let shutdown = stop.clone();
    let worker = std::thread::spawn(move || server.run(shutdown));
    let result = (|| -> substrate_liquid::Result<()> {
        let client = LocalClient::new(&socket, token);
        let inserted = client.execute(
            "INSERT INTO core.Person (name) VALUES ($1),($2)",
            vec![
                SqlWireValue::Text("Ada".into()),
                SqlWireValue::Text("Grace".into()),
            ],
        )?;
        println!("Inserted {} stable identities", inserted.affected_rows);
        let changed = client.execute(
            "UPDATE core.Person SET name=$1 WHERE id=$2",
            vec![
                SqlWireValue::Text("Ada Lovelace".into()),
                SqlWireValue::Text(inserted.ids[0].to_string()),
            ],
        )?;
        assert_eq!(changed.affected_rows, 1);
        let removed = client.execute(
            "DELETE FROM core.Person WHERE id=$1",
            vec![SqlWireValue::Text(inserted.ids[1].to_string())],
        )?;
        assert_eq!(removed.affected_rows, 1);
        println!("Updated and deleted through authorized native transactions");
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    worker.join().map_err(|_| "server panicked")??;
    result?;
    Ok(())
}
