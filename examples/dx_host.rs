//! Create a fresh local two-application fixture and private credentials.
use std::io::Write;
use substrate_liquid::*;
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .ok_or("usage: dx_host NEW_DIRECTORY")?;
    let dir = std::path::Path::new(&dir);
    std::fs::create_dir(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let db = Database::open(dir.join("database.redb"))?;
    let source = if let Some(path) = std::env::args().nth(2) {
        std::fs::read_to_string(path)?
    } else {
        include_str!("dx/manifest.json").into()
    };
    let manifest = SchemaManifest::from_json(&source)?;
    let mut tx = db.admin().transaction();
    tx.install_manifest(&manifest)?;
    let benchmark_people = std::env::args()
        .nth(3)
        .map(|s| s.parse::<usize>())
        .transpose()?;
    if let Some(n) = benchmark_people {
        if !(1..=1000).contains(&n) {
            return Err("benchmark people must be 1..1000".into());
        }
        for i in 0..n {
            let person = tx.insert_with_id(
                "core.Person",
                Id::from_u128(i as u128 + 42),
                vec![format!("Person {i}").as_str().into()],
            )?;
            for minutes in [30, 31] {
                let workout = tx.insert("fitness.Workout", vec![minutes.into()])?;
                tx.insert("fitness.Performed", vec![person.into(), workout.into()])?;
            }
        }
    } else {
        tx.insert_with_id("core.Person", Id::from_u128(42), vec!["Ada".into()])?;
    }
    for app in ["fitness", "crm"] {
        tx.grant(
            app,
            "core.Person",
            Grant {
                reference: true,
                ..Grant::read_only()
            },
        )?;
        tx.grant_schema(app, app)?;
        for def in manifest
            .modules
            .iter()
            .flat_map(|m| &m.relations)
            .filter(|r| r.name.starts_with(&format!("{app}.")))
        {
            tx.grant(app, &def.name, Grant::read_write())?;
        }
    }
    for def in manifest.modules.iter().flat_map(|m| &m.relations) {
        tx.grant("reporter", &def.name, Grant::read_only())?;
    }
    tx.commit()?;
    db.checkpoint()?;
    let config = service::ServerConfig {
        version: 1,
        clients: ["fitness", "crm", "reporter"]
            .into_iter()
            .map(|principal| service::ClientCredential {
                principal: principal.into(),
                token: format!("{}{}", Id::new().0.simple(), Id::new().0.simple()),
            })
            .collect(),
    };
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(dir.join("credentials.json"))?;
    file.write_all(serde_json::to_string_pretty(&config)?.as_bytes())?;
    file.sync_all()?;
    println!(
        "Created database and private application credentials in {}",
        dir.display()
    );
    Ok(())
}
