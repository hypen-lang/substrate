//! Explicit offline format conversion. Only the final, verified file is published.
//! The source is exclusively opened by redb and is never logically modified.
use super::*;
use std::{fs, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PagedImportStats {
    pub source_format: String,
    pub revision: u64,
    pub rows: u64,
    pub allocated_ids: u64,
}

struct Staging(PathBuf);
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(err)?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

impl PagedDatabase {
    /// Convert an offline LPG1/LPG2/LPG3 file into a new dense LPG3 file.
    ///
    /// The source must be closed by all other processes. This preserves exact
    /// identities, revisions, catalog/credentials/capabilities, policy metadata,
    /// deletion history and conflict timestamps. It does not import resident
    /// checkpoint files; those have a different logical control-plane encoding.
    ///
    /// Application data is streamed with bounded write batches. Both source and
    /// candidate receive a full integrity audit. The destination must not exist;
    /// a same-directory hard link publishes only the complete verified file,
    /// without replacing an existing destination. On Unix the directory is
    /// synchronized before success. A crash can leave an unreferenced staging
    /// file, but never a partially imported destination. The returned revision
    /// is unchanged; importing does not manufacture a new logical transaction.
    pub fn import_paged_file(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        options: PagedOptions,
    ) -> Result<PagedImportStats> {
        Self::import_file(source, destination, options, false)
    }
    pub(super) fn import_file(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        options: PagedOptions,
        follower: bool,
    ) -> Result<PagedImportStats> {
        let destination = destination.as_ref();
        if destination.try_exists().map_err(err)? {
            return Err(err("import destination already exists"));
        }
        let source = Self::open_with_factory(options, |builder| {
            let db = builder.open(source.as_ref()).map_err(err)?;
            // Do not initialize a valid but empty redb file as a side effect.
            if !db
                .begin_read()
                .map_err(err)?
                .list_tables()
                .map_err(err)?
                .any(|table| table.name() == "liquid_paged_control_v1")
            {
                return Err(err("source is not a paged database"));
            }
            Ok(db)
        })?;
        if follower {
            source.replication_cursor()?;
        }
        let snapshot = source.admin().snapshot()?;
        let audited = snapshot.verify()?;
        let view = &snapshot.view;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let staging = Staging(parent.join(format!(".substrate-import-{}", Id::new())));
        let mut file_options = fs::OpenOptions::new();
        file_options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file_options.mode(0o600);
        }
        let file = file_options.open(&staging.0).map_err(err)?;
        let target =
            Self::open_with_factory(options, |builder| builder.create_file(file).map_err(err))?;

        copy_rows(view, &target.inner.db, options)?;
        copy_auxiliary(view, &target.inner.db, options)?;
        replication::copy_journal(view, &target.inner.db, follower)?;
        {
            let mut tx = target.inner.db.begin_write().map_err(err)?;
            tx.set_durability(redb::Durability::Immediate)
                .map_err(err)?;
            {
                let source_meta = view.read.open_table(META).map_err(err)?;
                let mut meta = tx.open_table(META).map_err(err)?;
                for entry in source_meta.iter().map_err(err)? {
                    let (key, value) = entry.map_err(err)?;
                    meta.insert(key.value(), value.value()).map_err(err)?;
                }
                meta.insert("format", PagedFormat::V3.bytes())
                    .map_err(err)?;
            }
            crate::durable::failpoint("paged_import_before_complete");
            tx.commit().map_err(err)?;
        }
        // Drop the stale initial head and reopen through normal validation.
        drop(target);
        let verified = Self::open(&staging.0, options)?;
        let result = verified.admin().snapshot()?.verify()?;
        if result.revision != audited.revision
            || result.rows != audited.rows
            || result.allocated_ids != audited.allocated_ids
            || result.unique_index_entries != audited.unique_index_entries
            || result.incoming_index_entries != audited.incoming_index_entries
            || result.relation_types != audited.relation_types
        {
            return Err(err("import verification differs from source"));
        }
        drop(verified);
        crate::durable::failpoint("paged_import_before_publish");
        // hard_link is atomic and fails if another publisher won the name.
        fs::hard_link(&staging.0, destination).map_err(err)?;
        sync_directory(parent)?;
        crate::durable::failpoint("paged_import_after_publish");
        fs::remove_file(&staging.0).map_err(err)?;
        sync_directory(parent)?;
        Ok(PagedImportStats {
            source_format: String::from_utf8_lossy(view.format.bytes()).into_owned(),
            revision: result.revision,
            rows: result.rows,
            allocated_ids: result.allocated_ids,
        })
    }
}

fn copy_rows(view: &View, db: &redb::Database, options: PagedOptions) -> Result<()> {
    let mut source = view.rows.iter().map_err(err)?.peekable();
    while source.peek().is_some() {
        let tx = db.begin_write().map_err(err)?;
        {
            let mut rows = RowWriter::new(
                tx.open_table(ROWS).map_err(err)?,
                PagedFormat::V3,
                options.max_row_bytes,
            );
            let mut bytes = 0usize;
            let mut count = 0usize;
            while let Some(next) = source.peek() {
                let cost = match next {
                    Ok((key, value)) => key
                        .value()
                        .len()
                        .saturating_add(value.value().len())
                        .saturating_add(128),
                    Err(_) => {
                        source.next().unwrap()?;
                        unreachable!()
                    }
                };
                if cost > options.max_transaction_bytes {
                    return Err(err("one imported row exceeds transaction budget"));
                }
                if count == options.max_transaction_rows
                    || bytes.saturating_add(cost) > options.max_transaction_bytes
                {
                    break;
                }
                let (key, value) = source.next().unwrap()?;
                rows.insert(key.value(), value.value().to_vec())?;
                bytes += cost;
                count += 1;
            }
            rows.finish()?;
        }
        tx.commit().map_err(err)?;
        crate::durable::failpoint("paged_import_after_batch");
    }
    Ok(())
}

fn copy_auxiliary(view: &View, db: &redb::Database, options: PagedOptions) -> Result<()> {
    // Every table is already structurally validated. Iterate a pinned source
    // snapshot and keep only one entry plus one bounded destination batch live.
    macro_rules! copy {
        ($definition:expr, $cost:expr, $keep:expr) => {{
            let source = view.read.open_table($definition).map_err(err)?;
            let mut entries = source.iter().map_err(err)?.peekable();
            while entries.peek().is_some() {
                let tx = db.begin_write().map_err(err)?;
                {
                    let mut target = tx.open_table($definition).map_err(err)?;
                    let mut bytes = 0usize;
                    let mut count = 0usize;
                    while let Some(next) = entries.peek() {
                        let cost = match next {
                            Ok((key, value)) => ($cost)(key.value(), value.value()),
                            Err(_) => {
                                entries.next().unwrap().map_err(err)?;
                                unreachable!()
                            }
                        };
                        if cost > options.max_transaction_bytes {
                            return Err(err("one imported index entry exceeds transaction budget"));
                        }
                        if count == options.max_transaction_rows
                            || bytes.saturating_add(cost) > options.max_transaction_bytes
                        {
                            break;
                        }
                        let (key, value) = entries.next().unwrap().map_err(err)?;
                        if ($keep)(key.value())? {
                            target.insert(key.value(), value.value()).map_err(err)?;
                        }
                        bytes += cost;
                        count += 1;
                    }
                }
                tx.commit().map_err(err)?;
                crate::durable::failpoint("paged_import_after_batch");
            }
        }};
    }
    let all = |_: &[u8]| -> Result<bool> { Ok(true) };
    copy!(IDS, |k: &[u8], _: u8| k.len() + 129, all);
    copy!(VERS, |k: &[u8], _: u64| k.len() + 136, all);
    copy!(INCOMING, |k: &[u8], _: u8| k.len() + 129, all);
    copy!(
        UNIQUE,
        |k: &[u8], v: &[u8]| k.len().saturating_add(v.len()).saturating_add(128),
        all
    );
    copy!(
        INDEX,
        |k: &[u8], _: u8| k.len() + 129,
        |key: &[u8]| -> Result<bool> {
            let tid = Id(uuid::Uuid::from_bytes(
                key.get(..16)
                    .ok_or_else(|| err("invalid index key"))?
                    .try_into()
                    .map_err(err)?,
            ));
            let slot = usize::try_from(u64::from_be_bytes(
                key.get(16..24)
                    .ok_or_else(|| err("invalid index key"))?
                    .try_into()
                    .map_err(err)?,
            ))
            .map_err(err)?;
            let def = view
                .catalog
                .state
                .relations
                .get(&tid)
                .ok_or_else(|| err("unknown indexed type"))?;
            if slot >= def.roles.len() {
                return Err(err("invalid indexed role"));
            }
            Ok(PagedFormat::V3.role_index(def, slot))
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(path: &Path, format: PagedFormat) -> (Id, Id) {
        drop(PagedDatabase::open(path, PagedOptions::default()).unwrap());
        let disk = redb::Database::open(path).unwrap();
        let tx = disk.begin_write().unwrap();
        tx.open_table(META)
            .unwrap()
            .insert("format", format.bytes())
            .unwrap();
        tx.commit().unwrap();
        drop(disk);
        let db = PagedDatabase::open(path, PagedOptions::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "import",
                1,
                vec![],
                vec![
                    Relation::new("import.Person", vec![Role::new("name", DataType::Text)])
                        .unique(&["name"]),
                    Relation::new(
                        "import.Extension",
                        vec![
                            Role::new("person", DataType::Ref("import.Person".into())),
                            Role::new("score", DataType::Int),
                        ],
                    ),
                ],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        let person = tx.insert("import.Person", vec!["Alice".into()]).unwrap();
        let deleted = tx.insert("import.Person", vec!["Deleted".into()]).unwrap();
        tx.extend(
            "import.Person",
            "import.Extension",
            person,
            vec![Value::Ref(person), 7.into()],
        )
        .unwrap();
        tx.grant("reader", "import.Person", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.delete("import.Person", deleted).unwrap();
        tx.commit().unwrap();
        (person, deleted)
    }

    #[test]
    fn converts_all_paged_formats_without_changing_logical_state_or_permissions() {
        for format in [PagedFormat::V1, PagedFormat::V2, PagedFormat::V3] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let destination = dir.path().join("destination");
            let (person, deleted) = fixture(&source, format);
            let options = PagedOptions {
                max_transaction_rows: 1,
                max_transaction_bytes: 4096,
                ..Default::default()
            };
            let report = PagedDatabase::import_paged_file(&source, &destination, options).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
            assert_eq!(report.source_format.as_bytes(), format.bytes());
            assert_eq!(report.revision, 2);
            assert_eq!(report.rows, 2);
            let imported = PagedDatabase::open(&destination, options).unwrap();
            let snapshot = imported.admin().snapshot().unwrap();
            assert_eq!(snapshot.view.format, PagedFormat::V3);
            assert_eq!(
                snapshot
                    .get("import.Person", person)
                    .unwrap()
                    .unwrap()
                    .values,
                vec!["Alice".into()].into_boxed_slice()
            );
            assert!(snapshot.get("import.Person", deleted).unwrap().is_none());
            assert!(
                imported
                    .session("reader")
                    .unwrap()
                    .get("import.Person", person)
                    .unwrap()
                    .is_some()
            );
            assert!(matches!(
                imported
                    .session("reader")
                    .unwrap()
                    .get("import.Extension", person),
                Err(Error::Denied)
            ));
            assert!(matches!(
                imported.anonymous().get("import.Person", person),
                Err(Error::Denied)
            ));
            let mut tx = imported.admin().transaction().unwrap();
            assert!(
                tx.insert_with_id("import.Person", deleted, vec!["reused".into()])
                    .is_err()
            );
            let source_db = PagedDatabase::open(&source, options).unwrap();
            assert_eq!(source_db.admin().snapshot().unwrap().view.format, format);
            source_db.admin().snapshot().unwrap().verify().unwrap();
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
        }
    }

    #[test]
    fn rejects_existing_destination_missing_source_and_too_small_batch() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        fixture(&source, PagedFormat::V2);
        fs::write(&destination, b"keep me").unwrap();
        assert!(
            PagedDatabase::import_paged_file(&source, &destination, Default::default()).is_err()
        );
        assert_eq!(fs::read(&destination).unwrap(), b"keep me");
        fs::remove_file(&destination).unwrap();
        assert!(
            PagedDatabase::import_paged_file(
                dir.path().join("missing"),
                &destination,
                Default::default()
            )
            .is_err()
        );
        assert!(!destination.exists());
        let options = PagedOptions {
            max_transaction_bytes: 1,
            ..Default::default()
        };
        assert!(PagedDatabase::import_paged_file(&source, &destination, options).is_err());
        assert!(!destination.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn rejects_corrupt_source_before_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        fixture(&source, PagedFormat::V2);
        let disk = redb::Database::open(&source).unwrap();
        let tx = disk.begin_write().unwrap();
        tx.open_table(INDEX)
            .unwrap()
            .insert(b"bad index".as_slice(), 0)
            .unwrap();
        tx.commit().unwrap();
        drop(disk);
        assert!(
            PagedDatabase::import_paged_file(&source, &destination, Default::default()).is_err()
        );
        assert!(!destination.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[cfg(test)]
mod crash_tests {
    use super::*;
    #[test]
    fn import_crashes_never_publish_an_incomplete_destination() {
        for point in [
            "paged_import_after_batch",
            "paged_import_before_complete",
            "paged_import_before_publish",
            "paged_import_after_publish",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let destination = dir.path().join("destination");
            let db = PagedDatabase::open(&source, Default::default()).unwrap();
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "crash",
                1,
                vec![],
                vec![Relation::new(
                    "crash.Item",
                    vec![Role::new("value", DataType::Int)],
                )],
            ))
            .unwrap();
            tx.insert_with_id("crash.Item", Id::from_u128(7), vec![7.into()])
                .unwrap();
            tx.commit().unwrap();
            drop(db);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "paged::transfer::crash_tests::import_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("SUBSTRATE_IMPORT_SOURCE", &source)
                .env("SUBSTRATE_IMPORT_DESTINATION", &destination)
                .env("SUBSTRATE_TEST_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(91));
            let original = PagedDatabase::open(&source, Default::default()).unwrap();
            assert_eq!(
                original.admin().snapshot().unwrap().verify().unwrap().rows,
                1
            );
            assert_eq!(destination.exists(), point == "paged_import_after_publish");
            if destination.exists() {
                let imported = PagedDatabase::open(&destination, Default::default()).unwrap();
                assert_eq!(
                    imported.admin().snapshot().unwrap().verify().unwrap().rows,
                    1
                );
                assert_eq!(imported.stats().unwrap().revision, 1);
            }
        }
    }
    #[test]
    #[ignore]
    fn import_child() {
        PagedDatabase::import_paged_file(
            std::env::var_os("SUBSTRATE_IMPORT_SOURCE").unwrap(),
            std::env::var_os("SUBSTRATE_IMPORT_DESTINATION").unwrap(),
            Default::default(),
        )
        .unwrap();
        panic!("crash point did not execute");
    }
}
