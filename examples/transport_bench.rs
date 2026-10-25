//! Warm loopback request latency; real TLS/auth/query path, no durability writes.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use substrate_liquid::{capability::*, remote::*, service::*, *};
#[path = "../tests/support/tls.rs"]
mod tls;
fn query() -> Command {
    Command::Query {
        sql: "SELECT 1".into(),
        params: vec![],
    }
}
fn measure(mut run: impl FnMut() -> Response) -> serde_json::Value {
    for _ in 0..5 {
        assert!(matches!(run(), Response::Ok(_)));
    }
    let mut samples = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        assert!(matches!(run(), Response::Ok(_)));
        samples.push(start.elapsed().as_secs_f64() * 1e6);
    }
    samples.sort_by(f64::total_cmp);
    serde_json::json!({"requests":samples.len(),"median_us":samples[50],"p95_us":samples[95],"minimum_us":samples[0]})
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    let (ca, cert, key) = tls::certificates(dir.path());
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core())?;
    tx.grant("bench", "core.Person", Grant::read_only())?;
    tx.commit()?;
    let root = db.issue_capability(
        Id::new(),
        "bench",
        CapabilityScope {
            relations: vec![RelationCapability {
                relation: db.admin().snapshot().schema("core.Person")?.id,
                grant: Grant::read_only(),
            }],
            schema_namespace: None,
            delegations: 0,
        },
    )?;
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        + 300;
    let credential = db.issue_credential("bench", root.id, end)?;
    let server = RemoteServer::bind(db.clone(), "127.0.0.1:0".parse()?, &cert, &key)?;
    let addr = server.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let shutdown = stop.clone();
    let worker = std::thread::spawn(move || server.run(shutdown).unwrap());
    let remote = RemoteClient::new(addr, "localhost", &ca, credential.token)?;
    let tls = measure(|| remote.request(query()).unwrap());
    let mut session = remote.connect()?;
    let tls_persistent = measure(|| session.request(query()).unwrap());
    session.close();
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    #[cfg(unix)]
    let local = {
        use substrate_liquid::local::*;
        let path = dir.path().join("bench.sock");
        let token = "benchmark-token-01234567890123456789";
        let server = LocalServer::bind(
            db,
            &path,
            ServerConfig {
                version: 1,
                clients: vec![ClientCredential {
                    token: token.into(),
                    principal: "bench".into(),
                }],
            },
        )?;
        let stop = Arc::new(AtomicBool::new(false));
        let shutdown = stop.clone();
        let worker = std::thread::spawn(move || server.run(shutdown).unwrap());
        let client = LocalClient::new(path, token);
        let results = measure(|| client.request(query()).unwrap());
        let mut session = client.connect()?;
        let persistent = measure(|| session.request(query()).unwrap());
        session.close();
        stop.store(true, Ordering::Relaxed);
        worker.join().unwrap();
        serde_json::json!({"one_shot":results,"persistent":persistent})
    };
    #[cfg(not(unix))]
    let local = serde_json::Value::Null;
    println!(
        "{}",
        serde_json::json!({"tls_one_shot":tls,"tls_persistent":tls_persistent,"unix":local,"query":"SELECT 1","note":"warm same-host release build; 5 warmups,100 sequential requests; same query and host; one-shot vs one reused connection; not production latency"})
    );
    Ok(())
}
