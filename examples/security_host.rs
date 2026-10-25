//! `npm run demo:security`: principal-bound Node clients, nested reads, schema
//! approval, cascading revocation and durable recovery through a private host.
#[cfg(unix)]
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::{
        io::{BufRead, BufReader},
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    use substrate_liquid::{capability::*, schema_control::*, service::*, *};
    let dir = tempfile::tempdir()?;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    let path = dir.path().join("security.redb");
    let socket = dir.path().join("service.sock");
    let db = Database::open(&path)?;
    let mut tx = db.admin().transaction();
    tx.install(demo::core())?;
    let ada = tx.insert("core.Person", vec!["Ada".into()])?;
    let bob = tx.insert("core.Person", vec!["Bob".into()])?;
    tx.install(Module::new(
        "app",
        1,
        vec![("core".into(), 1)],
        vec![Relation::new(
            "app.Entry",
            vec![
                Role::new("person", DataType::Ref("core.Person".into())),
                Role::new("value", DataType::Int),
            ],
        )],
    ))?;
    tx.grant("owner", "core.Person", Grant::read_write())?;
    tx.grant("owner", "app.Entry", Grant::read_write())?;
    tx.commit()?;
    db.configure_namespace(NamespacePolicy {
        namespace: "app".into(),
        owner: "owner".into(),
        auto_approve: false,
        enabled: true,
        quotas: SchemaQuotas::default(),
    })?;
    let snap = db.admin().snapshot();
    let person = snap.schema("core.Person")?.id;
    let entry = snap.schema("app.Entry")?.id;
    let root = db.issue_capability(
        Id::new(),
        "owner",
        CapabilityScope {
            relations: vec![
                RelationCapability {
                    relation: person,
                    grant: Grant::read_write(),
                },
                RelationCapability {
                    relation: entry,
                    grant: Grant::read_write(),
                },
            ],
            schema_namespace: Some("app".into()),
            delegations: 2,
        },
    )?;
    let token = || format!("{}{}", Id::new().0.simple(), Id::new().0.simple());
    let owner = token();
    let worker = token();
    let reader = token();
    let server = local::LocalServer::bind(
        db.clone(),
        &socket,
        ServerConfig {
            version: 1,
            clients: vec![
                ClientCredential {
                    principal: "owner".into(),
                    token: owner.clone(),
                },
                ClientCredential {
                    principal: "worker".into(),
                    token: worker.clone(),
                },
                ClientCredential {
                    principal: "reader".into(),
                    token: reader.clone(),
                },
            ],
        },
    )?;
    let stop = Arc::new(AtomicBool::new(false));
    let signal = stop.clone();
    let host = std::thread::spawn(move || server.run(signal));
    let result = (|| -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut app = Command::new("node")
            .arg("target/dx-ts/examples/dx/security-app.js")
            .env("SUBSTRATE_SOCKET", &socket)
            .env("OWNER_TOKEN", owner)
            .env("WORKER_TOKEN", worker)
            .env("READER_TOKEN", reader)
            .env("ROOT_CAPABILITY", root.id.to_string())
            .env("PERSON_TYPE", person.to_string())
            .env("ENTRY_TYPE", entry.to_string())
            .env("ADA", ada.to_string())
            .env("BOB", bob.to_string())
            .stdout(Stdio::piped())
            .spawn()?;
        for line in BufReader::new(app.stdout.take().unwrap()).lines() {
            let line = line?;
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line)
                && let (Some(id), Some(digest)) = (v["review"].as_str(), v["digest"].as_str())
            {
                let id = Id(uuid::Uuid::parse_str(id)?);
                let plan = db.review_schema_proposal(id)?;
                assert_eq!(plan.owner, "owner");
                assert_eq!(plan.plan.module.name, "app");
                assert_eq!(plan.plan.module.version, 2);
                db.decide_schema_proposal(id, digest, true)?;
            } else {
                println!("{line}");
            }
        }
        if !app.wait()?.success() {
            return Err("security application failed".into());
        }
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    host.join().expect("server")?;
    result?;
    // Native retained handles and staged writes also observe root revocation.
    let s = db.session("owner").with_capability(root.id)?;
    let retained = s.snapshot();
    let sql = retained.sql()?;
    let mut write = s.transaction();
    write.insert("app.Entry", vec![ada.into(), 42.into()])?;
    db.revoke_capability(root.id)?;
    assert!(retained.scan("core.Person").is_err());
    assert!(sql.query("SELECT * FROM core.Person", &[]).is_err());
    assert!(write.commit().is_err());
    drop(s);
    drop(sql);
    db.checkpoint()?;
    drop(db);
    let reopened = Database::open(path)?;
    assert!(reopened.session("owner").with_capability(root.id).is_err());
    assert_eq!(reopened.admin().snapshot().scan("app.Entry")?.len(), 1);
    reopened.admin().snapshot().schema("app.NewNote")?;
    reopened.admin().snapshot().verify_indexes()?;
    println!(
        "PASS: retained SQL/native handles and staged writes denied after revocation; checkpoint/reopen preserved revocation and committed data"
    );
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("The security example requires Unix sockets");
}
