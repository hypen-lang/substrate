//! Prepared singleton reads used by SQL. Immutable snapshot metadata/policies
//! are compiled once; live fences still guard every execution. Results borrow
//! the bounded decoded cache through Arc rather than cloning unprojected data.
use super::*;

pub(super) struct PointProbe {
    snapshot: PagedSnapshot,
    def: Arc<CompiledRelation>,
    // (role slot, single-column uniqueness constraint). None means identity.
    unique: Option<(usize, usize)>,
    policy: ScanReadPolicy,
    terminal: Option<(usize, Arc<CompiledRelation>, ScanReadPolicy)>,
}
impl PointProbe {
    pub(super) fn compile(snapshot: &PagedSnapshot, name: &str, column: usize) -> Option<Self> {
        let def = snapshot.def(name).ok()?;
        if !discover(
            &snapshot.view.catalog,
            snapshot.principal.as_deref(),
            snapshot.scope.as_deref(),
            def.id,
        ) {
            return None;
        }
        let unique = if let Some(slot) = column.checked_sub(1) {
            let role = def.roles.get(slot)?;
            Some((
                slot,
                def.unique
                    .iter()
                    .position(|key| key.len() == 1 && key[0] == role.name)?,
            ))
        } else {
            None
        };
        let terminal = if let [(slot, target)] = def.reference_slots.as_slice()
            && !def.roles[*slot].data_type.is_nullable()
        {
            let target = snapshot.def(target).ok()?;
            target.reference_slots.is_empty().then(|| {
                let policy = snapshot.scan_read_policy(target.id);
                (*slot, target, policy)
            })
        } else {
            None
        };
        Some(Self {
            snapshot: snapshot.clone(),
            policy: snapshot.scan_read_policy(def.id),
            def,
            unique,
            terminal,
        })
    }

    pub(super) fn read(
        &self,
        value: &Value,
        check: &mut impl FnMut() -> Result<()>,
    ) -> Result<Option<Arc<Row>>> {
        check()?;
        self.snapshot.fence()?;
        let result = self.read_inner(value, check);
        // Do not release either data or data-dependent errors after revocation.
        // The callback can itself change authority, so the fence is last.
        let completed = check();
        self.snapshot.fence()?;
        completed?;
        result
    }

    fn read_inner(
        &self,
        value: &Value,
        check: &mut impl FnMut() -> Result<()>,
    ) -> Result<Option<Arc<Row>>> {
        let id = if let Some((slot, constraint)) = self.unique {
            if !self.def.roles[slot].data_type.accepts(value) {
                return Err(Error::Query("index value type mismatch".into()));
            }
            let fixed = unique_scalar_key(self.def.id, constraint, value);
            let dynamic;
            let key = if let Some((bytes, len)) = &fixed {
                &bytes[..*len]
            } else {
                dynamic = unique_key(self.def.id, constraint, std::slice::from_ref(value));
                dynamic.as_slice()
            };
            let Some(id) = self.snapshot.view.unique.get(key).map_err(err)? else {
                return Ok(None);
            };
            Id(uuid::Uuid::from_slice(id.value()).map_err(err)?)
        } else {
            let Value::Ref(id) = value else {
                return Err(Error::Query("invalid identity probe".into()));
            };
            *id
        };
        let Some((row, packed_len)) = self.snapshot.raw_sized(self.def.id, id, Some(&self.def))?
        else {
            return if self.unique.is_some() {
                Err(err("dangling index"))
            } else {
                Ok(None)
            };
        };
        if let Some((slot, _)) = self.unique
            && &row.values[slot] != value
        {
            return Err(err("index value mismatch"));
        }
        if !self.policy.allows(&row) {
            return Ok(None);
        }
        if let Some((slot, target, policy)) = &self.terminal {
            // Match the reference walk's root + target bound, even if the
            // target policy would deny access. No referenced data leaks.
            if self.snapshot.db.inner.options.max_visibility_rows < 2 {
                return Err(Error::Query("reference visibility budget exceeded".into()));
            }
            check()?;
            let Value::Ref(id) = row.values[*slot] else {
                return Err(err("invalid typed reference"));
            };
            let target = self
                .snapshot
                .raw_sized(target.id, id, Some(target))?
                .ok_or_else(|| err("dangling typed reference"))?;
            if !policy.allows(&target.0) {
                return Ok(None);
            }
        } else if !self.def.reference_slots.is_empty()
            && !self.snapshot.visible_checked(self.def.id, id, check)?
        {
            return Ok(None);
        }
        // Single-column unique probes previously used page_checked; retain
        // that byte bound even though no page or full row copy is constructed.
        if self.unique.is_some() && packed_len > self.snapshot.db.inner.options.max_page_bytes {
            return Err(Error::Query("row exceeds page byte bound".into()));
        }
        Ok(Some(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(options: PagedOptions) -> (tempfile::TempDir, PagedDatabase) {
        fixture_format(options, None)
    }

    fn fixture_format(
        options: PagedOptions,
        format: Option<PagedFormat>,
    ) -> (tempfile::TempDir, PagedDatabase) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(format) = format {
            drop(super::super::tests::empty_format(
                &dir.path().join("db"),
                format,
            ));
        }
        let db = PagedDatabase::open(dir.path().join("db"), options).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new(
                "test",
                1,
                vec![],
                vec![
                    Relation::new(
                        "test.Person",
                        vec![
                            Role::new("number", DataType::Int),
                            Role::new("text", DataType::Text),
                        ],
                    )
                    .unique(&["number"]),
                    Relation::new(
                        "test.Link",
                        vec![
                            Role::new("person", DataType::Ref("test.Person".into())),
                            Role::new("number", DataType::Int),
                        ],
                    )
                    .unique(&["number"]),
                    Relation::new(
                        "test.Chain",
                        vec![Role::new("link", DataType::Ref("test.Link".into()))],
                    )
                    .unique(&["link"]),
                ],
            ),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        for n in 1..=2 {
            tx.insert_with_id(
                "test.Person",
                Id::from_u128(n),
                vec![
                    Value::Int(n as i64),
                    Value::Text("wide unprojected text".repeat(64)),
                ],
            )
            .unwrap();
            tx.insert_with_id(
                "test.Link",
                Id::from_u128(10 + n),
                vec![Value::Ref(Id::from_u128(n)), Value::Int(n as i64)],
            )
            .unwrap();
            tx.insert_with_id(
                "test.Chain",
                Id::from_u128(20 + n),
                vec![Value::Ref(Id::from_u128(10 + n))],
            )
            .unwrap();
        }
        for name in ["test.Person", "test.Link", "test.Chain"] {
            tx.grant("reader", name, Grant::read_only()).unwrap();
        }
        tx.commit().unwrap();
        (dir, db)
    }
    fn read(probe: &PointProbe, value: Value) -> Result<Option<Arc<Row>>> {
        probe.read(&value, &mut || Ok(()))
    }

    #[test]
    fn prepared_probes_share_rows_and_do_not_copy_unprojected_values() {
        let (_dir, db) = fixture(PagedOptions::default());
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        let point = PointProbe::compile(&snapshot, "test.Person", 0).unwrap();
        let unique = PointProbe::compile(&snapshot, "test.Person", 1).unwrap();
        let a = read(&point, Value::Ref(Id::from_u128(1))).unwrap().unwrap();
        let b = read(&unique, Value::Int(1)).unwrap().unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(PointProbe::compile(&snapshot, "test.Person", 2).is_none());
        assert!(read(&unique, Value::Int(3)).unwrap().is_none());
        assert!(
            read(&point, Value::Ref(Id::from_u128(3)))
                .unwrap()
                .is_none()
        );
        assert!(read(&unique, Value::Text("1".into())).is_err());
    }

    #[test]
    fn cached_packed_lengths_stay_bound_to_their_snapshot() {
        for format in [PagedFormat::V2, PagedFormat::V3] {
            let (_dir, db) = fixture_format(PagedOptions::default(), Some(format));
            let old = db.session("reader").unwrap().snapshot().unwrap();
            let def = old.def("test.Person").unwrap();
            let id = Id::from_u128(1);
            let (row, bytes) = old.raw_sized(def.id, id, Some(&def)).unwrap().unwrap();
            assert_eq!(bytes, row_encoded_len(&def, &row).unwrap());
            let (cached, cached_bytes) = old.raw_sized(def.id, id, None).unwrap().unwrap();
            assert!(Arc::ptr_eq(&row, &cached));
            assert_eq!(bytes, cached_bytes);
            let old_probe = PointProbe::compile(&old, "test.Person", 1).unwrap();
            let mut tx = db.admin().transaction().unwrap();
            tx.update(
                "test.Person",
                id,
                vec![Value::Int(1), Value::Text("short".into())],
            )
            .unwrap();
            tx.commit().unwrap();
            // Data-only publication must not revoke an older consistent read.
            assert_eq!(
                read(&old_probe, Value::Int(1)).unwrap().unwrap().values,
                row.values
            );
            let fresh = db.session("reader").unwrap().snapshot().unwrap();
            let (new, new_bytes) = fresh.raw_sized(def.id, id, Some(&def)).unwrap().unwrap();
            assert_eq!(new_bytes, row_encoded_len(&def, &new).unwrap());
            assert!(new_bytes < bytes);
            let mut tx = db.admin().transaction().unwrap();
            tx.grant("reader", "test.Person", Grant::read_only())
                .unwrap();
            tx.commit().unwrap();
            assert!(matches!(
                read(&old_probe, Value::Int(1)),
                Err(Error::Denied)
            ));
            let fresh = db.session("reader").unwrap().snapshot().unwrap();
            assert!(
                read(
                    &PointProbe::compile(&fresh, "test.Person", 1).unwrap(),
                    Value::Int(1)
                )
                .unwrap()
                .is_some()
            );
        }
    }

    #[test]
    fn poisoned_publication_still_fails_closed_with_atomic_fences() {
        let (_dir, db) = fixture(PagedOptions::default());
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        let probe = PointProbe::compile(&snapshot, "test.Person", 0).unwrap();
        let inner = db.inner.clone();
        assert!(
            std::thread::spawn(move || {
                let _guard = inner.head.write().unwrap();
                panic!("test publication failure");
            })
            .join()
            .is_err()
        );
        assert!(matches!(
            read(&probe, Value::Ref(Id::from_u128(1))),
            Err(Error::Storage(_))
        ));
    }

    #[test]
    fn prepared_probes_keep_row_reference_and_transitive_filters_live() {
        let (_dir, db) = fixture(PagedOptions::default());
        let mut tx = db.admin().transaction().unwrap();
        tx.grant(
            "reader",
            "test.Person",
            Grant {
                filter: RowFilter::Equals {
                    role: "number".into(),
                    value: Value::Int(1),
                },
                ..Grant::read_only()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        let unique = PointProbe::compile(&snapshot, "test.Person", 1).unwrap();
        let terminal = PointProbe::compile(&snapshot, "test.Link", 2).unwrap();
        let chain = PointProbe::compile(&snapshot, "test.Chain", 0).unwrap();
        for (probe, yes, no) in [
            (&unique, Value::Int(1), Value::Int(2)),
            (&terminal, Value::Int(1), Value::Int(2)),
            (
                &chain,
                Value::Ref(Id::from_u128(21)),
                Value::Ref(Id::from_u128(22)),
            ),
        ] {
            assert!(read(probe, yes).unwrap().is_some());
            assert!(read(probe, no).unwrap().is_none());
        }
        let mut tx = db.admin().transaction().unwrap();
        tx.revoke("reader", "test.Person").unwrap();
        tx.commit().unwrap();
        for probe in [&unique, &terminal, &chain] {
            assert!(matches!(read(probe, Value::Int(1)), Err(Error::Denied)));
        }
    }

    #[test]
    fn singleton_probes_preserve_page_and_reference_bounds() {
        let (_dir, db) = fixture(PagedOptions {
            max_page_bytes: 128,
            max_visibility_rows: 1,
            ..PagedOptions::default()
        });
        let snapshot = db.session("reader").unwrap().snapshot().unwrap();
        // Identity reads did not use page bounds; unique index reads did.
        let point = PointProbe::compile(&snapshot, "test.Person", 0).unwrap();
        assert!(
            read(&point, Value::Ref(Id::from_u128(1)))
                .unwrap()
                .is_some()
        );
        let unique = PointProbe::compile(&snapshot, "test.Person", 1).unwrap();
        assert!(
            read(&unique, Value::Int(1))
                .unwrap_err()
                .to_string()
                .contains("page byte bound")
        );
        let terminal = PointProbe::compile(&snapshot, "test.Link", 2).unwrap();
        assert!(
            read(&terminal, Value::Int(1))
                .unwrap_err()
                .to_string()
                .contains("reference visibility budget")
        );
    }

    #[test]
    fn singleton_probe_rechecks_authority_before_releasing_data() {
        for revoke_at in [2, 3] {
            let (_dir, db) = fixture(PagedOptions::default());
            let snapshot = db.session("reader").unwrap().snapshot().unwrap();
            let probe = PointProbe::compile(&snapshot, "test.Link", 2).unwrap();
            let mut checks = 0;
            let result = probe.read(&Value::Int(1), &mut || {
                checks += 1;
                if checks == revoke_at {
                    let mut tx = db.admin().transaction().unwrap();
                    tx.revoke("reader", "test.Person").unwrap();
                    tx.commit().unwrap();
                }
                Ok(())
            });
            assert!(matches!(result, Err(Error::Denied)));
        }
    }

    #[test]
    fn prepared_probe_scope_attenuates_public_owner_reads_and_rotation_invalidates_it() {
        use crate::capability::{CapabilityScope, RelationCapability};
        let (_dir, db) = fixture(PagedOptions::default());
        let mut tx = db.admin().transaction().unwrap();
        tx.set_visibility("test.Person", Visibility::Public)
            .unwrap();
        tx.set_visibility("test.Link", Visibility::Public).unwrap();
        tx.commit().unwrap();
        let snapshot = db.admin().snapshot().unwrap();
        let person = snapshot.def("test.Person").unwrap().id;
        let link = snapshot.def("test.Link").unwrap().id;
        let scope = CapabilityScope {
            relations: vec![
                RelationCapability {
                    relation: person,
                    grant: Grant {
                        filter: RowFilter::Equals {
                            role: "number".into(),
                            value: Value::Int(1),
                        },
                        ..Grant::read_only()
                    },
                },
                RelationCapability {
                    relation: link,
                    grant: Grant::read_only(),
                },
            ],
            schema_namespace: None,
            delegations: 0,
        };
        let capability = db.issue_capability(Id::new(), "owner", scope).unwrap();
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 120;
        let credential = db
            .issue_credential("owner", capability.id, expires)
            .unwrap();
        let snapshot = db
            .authenticate(&credential.token)
            .unwrap()
            .snapshot()
            .unwrap();
        let point = PointProbe::compile(&snapshot, "test.Person", 1).unwrap();
        let terminal = PointProbe::compile(&snapshot, "test.Link", 2).unwrap();
        assert!(PointProbe::compile(&snapshot, "test.Chain", 0).is_none());
        for probe in [&point, &terminal] {
            assert!(read(probe, Value::Int(1)).unwrap().is_some());
            assert!(read(probe, Value::Int(2)).unwrap().is_none());
        }
        db.rotate_credential(credential.id, expires).unwrap();
        for probe in [&point, &terminal] {
            assert!(matches!(read(probe, Value::Int(1)), Err(Error::Denied)));
        }
    }

    #[test]
    fn singleton_probe_validates_even_unrestricted_reference_targets() {
        for malformed in [false, true] {
            // Deliberate PG01 corruption uses the legacy row table.
            let (dir, db) = fixture_format(PagedOptions::default(), Some(PagedFormat::V2));
            let def = db.admin().snapshot().unwrap().def("test.Person").unwrap();
            let tx = db.inner.db.begin_write().unwrap();
            {
                let mut rows = tx.open_table(ROWS).unwrap();
                let key = row_key(def.id, Id::from_u128(1));
                if malformed {
                    rows.insert(key.as_slice(), b"bad".as_slice()).unwrap();
                } else {
                    rows.remove(key.as_slice()).unwrap();
                }
            }
            tx.commit().unwrap();
            drop(db);
            let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
            let snapshot = db.session("reader").unwrap().snapshot().unwrap();
            let probe = PointProbe::compile(&snapshot, "test.Link", 2).unwrap();
            assert!(matches!(
                read(&probe, Value::Int(1)),
                Err(Error::Storage(_))
            ));
        }
    }

    #[test]
    fn unique_probe_rejects_mismatched_and_dangling_indexes() {
        for dangling in [false, true] {
            let (_dir, db) = fixture(PagedOptions {
                decoded_cache_bytes: 0,
                ..PagedOptions::default()
            });
            let def = db.admin().snapshot().unwrap().def("test.Person").unwrap();
            let tx = db.inner.db.begin_write().unwrap();
            {
                let mut table = tx.open_table(UNIQUE).unwrap();
                let key = unique_key(def.id, 0, &[Value::Int(1)]);
                let id = Id::from_u128(if dangling { 99 } else { 2 });
                table
                    .insert(key.as_slice(), id.0.as_bytes().as_slice())
                    .unwrap();
            }
            tx.commit().unwrap();
            drop(db);
            let db = PagedDatabase::open(_dir.path().join("db"), PagedOptions::default()).unwrap();
            let snapshot = db.session("reader").unwrap().snapshot().unwrap();
            let probe = PointProbe::compile(&snapshot, "test.Person", 1).unwrap();
            let error = read(&probe, Value::Int(1)).unwrap_err().to_string();
            assert!(
                error.contains(if dangling {
                    "dangling index"
                } else {
                    "index value mismatch"
                }),
                "{error}"
            );
        }
    }
}
