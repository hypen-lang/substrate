//! Run `npm run demo:schema`. A trusted host owns approval; the Node app receives
//! only an ordinary principal credential and discovers every descriptor at runtime.
#[cfg(unix)]
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::{
        io::{BufRead, BufReader},
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use substrate_liquid::{schema_control::*, service::*, *};
    let dir = tempfile::tempdir()?;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    let path = dir.path().join("schema.redb");
    let socket = dir.path().join("service.sock");
    let db = Database::open(&path)?;
    let mut tx = db.admin().transaction();
    tx.install(demo::core())?;
    let person = tx.insert("core.Person", vec!["Ada".into()])?;
    tx.grant(
        "agent",
        "core.Person",
        Grant {
            reference: true,
            ..Grant::read_only()
        },
    )?;
    tx.commit()?;
    for (namespace, auto) in [("agent", true), ("reviewed", false)] {
        db.configure_namespace(NamespacePolicy {
            namespace: namespace.into(),
            owner: "agent".into(),
            auto_approve: auto,
            enabled: true,
            quotas: SchemaQuotas::default(),
        })?;
    }
    let token = format!("{}{}", Id::new().0.simple(), Id::new().0.simple());
    let other = format!("{}{}", Id::new().0.simple(), Id::new().0.simple());
    let server = local::LocalServer::bind(
        db.clone(),
        &socket,
        ServerConfig {
            version: 1,
            clients: vec![
                ClientCredential {
                    principal: "agent".into(),
                    token: token.clone(),
                },
                ClientCredential {
                    principal: "other".into(),
                    token: other.clone(),
                },
            ],
        },
    )?;
    let stop = Arc::new(AtomicBool::new(false));
    let signal = stop.clone();
    let host = std::thread::spawn(move || server.run(signal));
    let reads = Arc::new(AtomicUsize::new(0));
    let counter = reads.clone();
    let signal = stop.clone();
    let session = db.session("agent");
    let reader = std::thread::spawn(move || {
        while !signal.load(Ordering::Relaxed) {
            let row = session
                .snapshot()
                .get("core.Person", person)
                .unwrap()
                .unwrap();
            assert_eq!(row.values[0], Value::Text("Ada".into()));
            counter.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });
    let result = (|| -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut child = Command::new("node")
            .arg("target/dx-ts/examples/dx/dynamic-app.js")
            .env("SUBSTRATE_SOCKET", &socket)
            .env("SUBSTRATE_TOKEN", &token)
            .env("SUBSTRATE_OTHER_TOKEN", &other)
            .stdout(Stdio::piped())
            .spawn()?;
        for line in BufReader::new(child.stdout.take().expect("piped")).lines() {
            let line = line?;
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line)
                && let (Some(id), Some(digest)) =
                    (value["review"].as_str(), value["digest"].as_str())
            {
                let id = Id(uuid::Uuid::parse_str(id)?);
                let proposal = db.review_schema_proposal(id)?;
                assert_eq!(proposal.owner, "agent");
                assert_eq!(proposal.plan.module.name, "reviewed");
                assert_eq!(proposal.plan.module.relations[0].name, "reviewed.Note");
                assert_eq!(proposal.plan.existing_record_rewrites, 0);
                db.decide_schema_proposal(id, digest, true)?;
                println!("Host approved the reviewed.Note plan");
                continue;
            }
            println!("{line}");
        }
        if !child.wait()?.success() {
            return Err("dynamic application failed".into());
        }
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    host.join().expect("server thread")?;
    reader.join().expect("reader");
    result?;
    assert!(reads.load(Ordering::Relaxed) > 0);
    println!(
        "{} concurrent reads retained the original Person",
        reads.load(Ordering::Relaxed)
    );
    db.checkpoint()?;
    drop(db);
    let db = Database::open(path)?;
    let s = db.session("agent").snapshot();
    assert_eq!(
        s.scan("agent.Score")?[0].values[1],
        Value::Int(9007199254740994)
    );
    assert_eq!(s.describe("agent.Rating")?.relation().name, "agent.Score");
    assert_eq!(s.scan("reviewed.Note")?.len(), 1);
    db.admin().snapshot().verify_indexes()?;
    println!("PASS: checkpoint/reopen preserved dynamic schemas, rows and lifecycle");
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("The schema host example requires Unix sockets");
}
