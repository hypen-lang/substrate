//! Existing TypeScript client and TLS protocol backed by paged current storage.
//! Run `npm run demo:paged` to verify discovery, typed writes, SQL, rotation and expiry.
use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use substrate_liquid::{
    capability::*,
    paged::{PagedDatabase, PagedOptions},
    remote::{RemoteLimits, RemoteOptions, RemoteServer},
    *,
};
#[path = "../tests/support/tls.rs"]
mod tls;
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (ca, cert, key) = tls::certificates(dir.path());
    let db = PagedDatabase::open(dir.path().join("paged.redb"), PagedOptions::default())?;
    let mut tx = db.admin().transaction()?;
    tx.install_owned(demo::core(), "owner", Visibility::Private)?;
    tx.insert("core.Person", vec!["Ada".into()])?;
    tx.insert("core.Person", vec!["Bob".into()])?;
    tx.grant("owner", "core.Person", Grant::read_write())?;
    tx.commit()?;
    let scope = CapabilityScope {
        relations: vec![RelationCapability {
            relation: db.admin().snapshot()?.schema("core.Person")?.id,
            grant: Grant {
                filter: RowFilter::Equals {
                    role: "name".into(),
                    value: "Ada".into(),
                },
                ..Grant::read_write()
            },
        }],
        schema_namespace: None,
        delegations: 0,
    };
    let root = db.issue_capability_until(Id::new(), "owner", scope.clone(), Some(now() + 300))?;
    let old = db.issue_credential("owner", root.id, now() + 120)?;
    let held = db.authenticate(&old.token)?.snapshot()?;
    let (audit, audit_events) = std::sync::mpsc::sync_channel(128);
    let server = RemoteServer::bind_with_options(
        db.clone(),
        "127.0.0.1:0".parse()?,
        &cert,
        &key,
        RemoteOptions {
            limits: RemoteLimits {
                max_connections_per_peer: 4,
                idle_timeout: std::time::Duration::from_secs(5),
                ..RemoteLimits::default()
            },
            audit: Some(audit),
        },
    )?;
    let port = server.local_addr()?.port();
    let shutdown = Arc::new(AtomicBool::new(false));
    let stop = shutdown.clone();
    let worker = std::thread::spawn(move || server.run(stop));
    let result = (|| -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut child = Command::new("node")
            .arg("target/dx-ts/examples/dx/paged-app.js")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        fn send(input: &mut impl Write, value: serde_json::Value) -> std::io::Result<()> {
            writeln!(input, "{value}")?;
            input.flush()
        }
        fn wait(output: &mut impl BufRead) -> std::io::Result<()> {
            let mut line = String::new();
            output.read_line(&mut line)?;
            if line.trim() != "ok" {
                return Err(std::io::Error::other("Node verification failed"));
            }
            Ok(())
        }
        send(
            &mut input,
            serde_json::json!({"host":"127.0.0.1","port":port,"servername":"localhost","ca":String::from_utf8(ca)?,"token":old.token}),
        )?;
        wait(&mut output)?;
        println!("TLS hostname verification and scoped runtime reads/writes passed.");
        let new = db.rotate_credential(old.id, now() + 120)?;
        assert!(held.scan_page("core.Person", None, 100).is_err());
        send(&mut input, serde_json::json!({"token":new.token}))?;
        wait(&mut output)?;
        println!("Atomic credential rotation denied old Node client and retained native snapshot.");
        db.revoke_capability(root.id)?;
        send(&mut input, serde_json::json!({"revoked":true}))?;
        wait(&mut output)?;
        println!("Parent revocation denied the replacement credential.");
        let end = now() + 3;
        let short = db.issue_capability_until(Id::new(), "owner", scope, Some(end))?;
        let credential = db.issue_credential("owner", short.id, end)?;
        send(
            &mut input,
            serde_json::json!({"token":credential.token,"expires_at":end}),
        )?;
        wait(&mut output)?;
        assert!(child.wait()?.success());
        println!("Capability expiry denied the connected application's next operation.");
        db.admin().snapshot()?.verify()?;
        Ok(())
    })();
    shutdown.store(true, Ordering::Relaxed);
    worker.join().expect("TLS host panicked")?;
    result?;
    println!(
        "Observed {} bounded security audit events without tokens or request payloads.",
        audit_events.try_iter().count()
    );
    drop(held);
    drop(db);
    let reopened = PagedDatabase::open(dir.path().join("paged.redb"), PagedOptions::default())?;
    assert!(reopened.authenticate(&old.token).is_err());
    println!("Paged recovery preserved credential revocation without loading current rows.");
    Ok(())
}
