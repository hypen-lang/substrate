use substrate_liquid::*;
mod query_profile_cli;
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("bench-query") => query_profile_cli::run(&args[1..])?,
        #[cfg(unix)]
        Some("serve") if args.len() == 4 => {
            use std::os::unix::fs::PermissionsExt;
            if std::fs::metadata(&args[3])?.permissions().mode() & 0o077 != 0 {
                return Err("credential file must be private (chmod 600)".into());
            }
            let config: service::ServerConfig =
                serde_json::from_str(&std::fs::read_to_string(&args[3])?)?;
            let server = local::LocalServer::bind(open_existing(&args[1])?, &args[2], config)?;
            println!("Listening on {}", args[2]);
            server.run(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )))?;
        }
        Some("schema-lock") if args.len() == 3 => {
            let previous = if std::path::Path::new(&args[2]).exists() {
                Some(SchemaManifest::from_json(&std::fs::read_to_string(
                    &args[2],
                )?)?)
            } else {
                None
            };
            let manifest =
                SchemaManifest::lock(&std::fs::read_to_string(&args[1])?, previous.as_ref())?;
            write_changed(std::path::Path::new(&args[2]), &manifest.to_json()?)?;
            println!("Locked {} module versions", manifest.modules.len());
        }
        Some("generate") if args.len() >= 3 => {
            let manifest = SchemaManifest::from_json(&std::fs::read_to_string(&args[1])?)?;
            let roots = args[3..]
                .iter()
                .map(|s| {
                    s.split_once('@')
                        .ok_or("expected namespace@version")
                        .and_then(|(n, v)| {
                            v.parse::<u64>()
                                .map(|v| (n.to_string(), v))
                                .map_err(|_| "invalid version")
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let manifest = if roots.is_empty() {
                manifest
            } else {
                manifest.select(&roots)?
            };
            let code = manifest.generate()?;
            let dir = std::path::Path::new(&args[2]);
            std::fs::create_dir_all(dir)?;
            write_changed(&dir.join("schema.rs"), &code.rust)?;
            write_changed(&dir.join("schema.ts"), &code.typescript)?;
            println!(
                "Generated schema.rs and schema.ts (contract {})",
                manifest.fingerprint()?
            );
        }
        Some(command @ ("manifest-plan" | "manifest-install" | "manifest-export"))
            if args.len() == 3 =>
        {
            let db = if command == "manifest-install" {
                Database::open(&args[1])?
            } else {
                open_existing(&args[1])?
            };
            if command == "manifest-export" {
                write_changed(
                    std::path::Path::new(&args[2]),
                    &db.admin().snapshot().manifest()?.to_json()?,
                )?;
            } else {
                let manifest = SchemaManifest::from_json(&std::fs::read_to_string(&args[2])?)?;
                let mut tx = db.admin().transaction();
                let plan = tx.install_manifest(&manifest)?;
                println!("{}", serde_json::to_string_pretty(&plan)?);
                if command == "manifest-install" {
                    println!("Committed revision {}", tx.commit()?);
                }
            }
        }
        Some("demo") if args.len() <= 2 => {
            let db = if let Some(path) = args.get(1) {
                Database::open(path)?
            } else {
                Database::memory()
            };
            demo::run(&db)?;
        }
        Some("query") if args.len() == 4 => {
            let db = open_existing(&args[1])?;
            // Local CLI is a trusted administrator tool, not an authentication server.
            let session = if args[2] == "admin" {
                db.admin()
            } else {
                db.session(&args[2])
            };
            let table = session.snapshot().sql()?.query(&args[3], &[])?;
            println!("{}", table.columns.join(" | "));
            for row in table.rows {
                println!("{row:?}");
            }
        }
        Some("inspect") if args.len() == 2 => {
            let db = open_existing(&args[1])?;
            let snapshot = db.admin().snapshot();
            println!("{:?}", snapshot.stats()?);
            println!("{:?}", db.recovery_info()?);
            println!("{:?}", db.storage_info()?);
            for def in snapshot.relations() {
                println!("{} [{}]: {:?}", def.name, def.id, def.roles);
            }
            snapshot.verify_indexes()?;
            println!("Indexes verified.");
        }
        Some("checkpoint") if args.len() == 2 => {
            let db = open_existing(&args[1])?;
            println!("{:?}", db.checkpoint()?);
        }
        Some(command @ ("schema-check" | "schema-diff" | "install")) if args.len() == 3 => {
            let module: Module = serde_json::from_slice(&std::fs::read(&args[2])?)?;
            let db = if command == "install" {
                Database::open(&args[1])?
            } else {
                open_existing(&args[1])?
            };
            let mut tx = db.admin().transaction();
            tx.install(module.clone())?;
            println!(
                "{}@{}: conservative additive extension",
                module.name, module.version
            );
            for relation in &module.relations {
                println!("+ {}", relation.name);
            }
            println!("Schema layout backfills: 0; checkpoint maintenance may copy unchanged rows");
            if command == "install" {
                println!("Committed revision {}", tx.commit()?);
            }
        }
        _ => println!(
            "Substrate Liquid SQL prototype\n\n  substrate-liquid serve database.redb socket-path credentials.json\n  substrate-liquid schema-lock declarations.json manifest.json\n  substrate-liquid generate manifest.json output-directory [namespace@version ...]\n  substrate-liquid manifest-plan database.redb manifest.json\n  substrate-liquid manifest-install database.redb manifest.json\n  substrate-liquid manifest-export database.redb manifest.json\n  substrate-liquid demo [new.redb]\n  substrate-liquid query database.redb PRINCIPAL 'SELECT ...'\n  substrate-liquid bench-query database.redb PRINCIPAL query.sql [OPTIONS]\n  substrate-liquid inspect database.redb\n  substrate-liquid checkpoint database.redb\n  substrate-liquid schema-check database.redb module.json\n  substrate-liquid schema-diff database.redb module.json\n  substrate-liquid install database.redb module.json\n\nThis local CLI is a trusted host tool; PRINCIPAL=admin selects administrative access."
        ),
    }
    Ok(())
}
fn open_existing(path: &str) -> Result<Database> {
    if !std::path::Path::new(path).is_file() {
        return Err(Error::Storage("database does not exist".into()));
    }
    Database::open(path)
}

fn write_changed(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    if std::fs::read_to_string(path).is_ok_and(|old| old == text) {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let temporary = parent.join(format!(".substrate-{}.tmp", Id::new()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
