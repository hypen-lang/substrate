//! Fixed-width aggregate source: direct streaming, full typed validation and the
//! same authority/reference contract as the general row visitor.
use super::encoding::PackedValidationLayout;
use super::*;

pub(super) struct TerminalPlan {
    pub(super) slot: usize,
    pub(super) def: Arc<CompiledRelation>,
    pub(super) allowed: bool,
    pub(super) validation: Option<PackedValidationLayout>,
}

pub(super) struct FixedScanPlan {
    pub(super) def: Arc<CompiledRelation>,
    pub(super) layout: FixedPackedLayout,
    pub(super) terminal: Option<TerminalPlan>,
}

impl PagedSnapshot {
    pub(super) fn fixed_scan_plan(&self, name: &str) -> Result<Option<FixedScanPlan>> {
        self.fence()?;
        let def = self.def(name)?;
        if !discover(
            &self.view.catalog,
            self.principal.as_deref(),
            self.scope.as_deref(),
            def.id,
        ) {
            return Err(Error::Denied);
        }
        let Some(layout) = FixedPackedLayout::compile(&def) else {
            return Ok(None);
        };
        if !matches!(self.scan_read_policy(def.id), ScanReadPolicy::All) {
            return Ok(None);
        }
        let terminal = if def.reference_slots.is_empty() {
            None
        } else if let [(slot, name)] = def.reference_slots.as_slice() {
            let tid = self.view.catalog.state.names[name];
            let target = &self.view.catalog.state.relations[&tid];
            if !target.reference_slots.is_empty() {
                return Ok(None);
            }
            let policy = self.scan_read_policy(tid);
            if matches!(policy, ScanReadPolicy::Filter(_)) {
                return Ok(None);
            }
            Some(TerminalPlan {
                slot: *slot,
                def: target.clone(),
                allowed: matches!(policy, ScanReadPolicy::All),
                validation: PackedValidationLayout::compile(target),
            })
        } else {
            return Ok(None);
        };
        Ok(Some(FixedScanPlan {
            def,
            layout,
            terminal,
        }))
    }

    #[inline(always)]
    pub(super) fn validate_terminal_row(&self, target: &TerminalPlan, packed: &[u8]) -> Result<()> {
        if let Some(validation) = &target.validation {
            self.count(metrics::ReadCounter::ValidatedPackedRow);
            if packed.len() > self.db.inner.options.max_row_bytes {
                return Err(err("row exceeds configured bound"));
            }
            let revision = validation.validate(packed)?;
            if revision == 0 || revision > self.view.revision {
                return Err(err("row revision is outside snapshot history"));
            }
            Ok(())
        } else {
            self.validate_uncached(&target.def, packed)
        }
    }

    #[cfg(test)]
    pub(super) fn has_dense_fixed_scan(&self, name: &str) -> Result<bool> {
        if !matches!(self.view.rows, dense::RowStore::Dense(_)) {
            return Ok(false);
        }
        Ok(self.fixed_scan_plan(name)?.is_some())
    }

    /// Returns false before invoking the visitor when the schema/policy needs
    /// the general graph/row-filter path. No partial-result fallback is allowed.
    pub(super) fn visit_fixed_authorized_rows(
        &self,
        name: &str,
        check: &mut impl FnMut() -> Result<()>,
        visitor: &mut impl FnMut(Id, FixedPackedRow<'_, '_>) -> Result<()>,
    ) -> Result<bool> {
        let Some(FixedScanPlan {
            def,
            layout,
            terminal,
        }) = self.fixed_scan_plan(name)?
        else {
            return Ok(false);
        };
        check()?;
        let options = self.db.inner.options;
        let mut cursor = dense::ScanCursor::default();
        let mut examined = 0usize;
        let mut scanned = 0usize;
        let mut last_accepted_examined = 0usize;
        let mut page_rows = 0usize;
        let mut page_bytes = 0usize;
        self.view
            .rows
            .visit_family(def.id.0.as_bytes(), &mut |item| {
                check()?;
                if scanned.is_multiple_of(256) {
                    self.fence()?;
                }
                scanned = scanned.wrapping_add(1);
                examined += 1;
                if examined > options.max_page_rows.saturating_mul(64) {
                    return Err(Error::Query("page visibility work budget exceeded".into()));
                }
                let (key, bytes) = item?;
                if key.len() != 32 {
                    return Err(err("malformed row key"));
                }
                if bytes.len() > options.max_row_bytes {
                    return Err(err("row exceeds configured bound"));
                }
                let id = Id(uuid::Uuid::from_bytes(
                    key[16..].try_into().expect("validated 32-byte row key"),
                ));
                self.count(metrics::ReadCounter::RootScanRow);
                self.count(metrics::ReadCounter::ValidatedPackedRow);
                let row = layout.read(bytes)?;
                if row.revision() == 0 || row.revision() > self.view.revision {
                    return Err(err("row revision is outside snapshot history"));
                }
                let visible = if let Some(target) = &terminal {
                    if options.max_visibility_rows == 0 {
                        return Err(Error::Query("reference visibility budget exceeded".into()));
                    }
                    check()?;
                    if options.max_visibility_rows < 2 {
                        return Err(Error::Query("reference visibility budget exceeded".into()));
                    }
                    let Some(target_id) = row.reference(target.slot) else {
                        return Err(err("invalid typed reference"));
                    };
                    self.count(metrics::ReadCounter::ReferenceCheck);
                    let packed = cursor.get(&self.view.rows, target.def.id, target_id)?;
                    self.validate_terminal_row(target, packed)?;
                    target.allowed
                } else {
                    true
                };
                if visible {
                    let size = bytes.len();
                    if size > options.max_page_bytes {
                        return Err(Error::Query("row exceeds page byte bound".into()));
                    }
                    if page_bytes > options.max_page_bytes - size {
                        page_rows = 0;
                        page_bytes = 0;
                        examined -= last_accepted_examined;
                    }
                    visitor(id, row)?;
                    last_accepted_examined = examined;
                    page_rows += 1;
                    page_bytes += size;
                    if page_rows == 256.min(options.max_page_rows) {
                        page_rows = 0;
                        page_bytes = 0;
                        examined = 0;
                        last_accepted_examined = 0;
                    }
                }
                Ok(())
            })?;
        check()?;
        self.fence()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_terminal_validation_keeps_filtered_rows_and_callback_error_order() {
        for format in [PagedFormat::V2, PagedFormat::V3] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let db = crate::paged::tests::empty_format(&path, format);
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "validate",
                1,
                vec![],
                vec![
                    Relation::new(
                        "validate.Target",
                        vec![
                            Role::new("text", DataType::Text),
                            Role::new("flag", DataType::Bool),
                            Role::new("number", DataType::Int),
                        ],
                    ),
                    Relation::new(
                        "validate.Root",
                        vec![
                            Role::new("target", DataType::Ref("validate.Target".into())),
                            Role::new("number", DataType::Int),
                        ],
                    ),
                ],
            ))
            .unwrap();
            for n in 1..=2 {
                tx.insert_with_id(
                    "validate.Target",
                    Id::from_u128(n),
                    vec!["x".into(), Value::Bool(true), Value::Int(1)],
                )
                .unwrap();
                tx.insert_with_id(
                    "validate.Root",
                    Id::from_u128(10 + n),
                    vec![Id::from_u128(n).into(), Value::Int(0)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
            let (def, revision) = {
                let snapshot = db.admin().snapshot().unwrap();
                (
                    snapshot.def("validate.Target").unwrap(),
                    snapshot.revision(),
                )
            };
            let bad = Row {
                id: Id::from_u128(2),
                values: vec!["x".into(), Value::Bool(true), Value::Int(1)].into_boxed_slice(),
            };
            let mut bytes = encode_row(&def, &bad, revision).unwrap();
            // Preserve framing/dense directory; damage an unprojected UTF-8
            // field in the second reference target only.
            bytes[24] = 0xff;
            let tx = db.inner.db.begin_write().unwrap();
            {
                let mut writer = RowWriter::new(
                    tx.open_table(ROWS).unwrap(),
                    format,
                    db.inner.options.max_row_bytes,
                );
                writer.insert(&row_key(def.id, bad.id), bytes).unwrap();
                writer.finish().unwrap();
            }
            tx.commit().unwrap();
            drop(db);
            let db = PagedDatabase::open(&path, Default::default()).unwrap();
            let snapshot = db.admin().snapshot().unwrap();
            let result = snapshot.visit_fixed_authorized_rows(
                "validate.Root",
                &mut || Ok(()),
                &mut |_, _| Err(Error::Query("earlier callback".into())),
            );
            assert!(matches!(result, Err(Error::Query(message)) if message == "earlier callback"));
            let mut delivered = 0;
            let result = snapshot.visit_fixed_authorized_rows(
                "validate.Root",
                &mut || Ok(()),
                &mut |_, _| {
                    delivered += 1;
                    Ok(())
                },
            );
            assert!(matches!(result, Err(Error::Storage(_))));
            assert_eq!(delivered, 1);
            // A full-scan SQL predicate excludes every root, but still cannot
            // hide corruption in its referenced rows.
            let sql = snapshot.sql().unwrap();
            assert!(
                sql.query(
                    "SELECT number,COUNT(*) FROM validate.Root WHERE number<>0 GROUP BY number",
                    &[]
                )
                .is_err()
            );
        }
    }
}
