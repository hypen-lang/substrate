//! Opt-in logical read diagnostics and pinned table accounting. Neither reports
//! physical disk operations: redb and the OS can satisfy logical reads in cache.
use super::*;
use redb::ReadableTableMetadata;
use std::sync::atomic::AtomicU64;

macro_rules! io_counters {
    ($($field:ident),+ $(,)?) => {
        #[derive(Debug, Default)]
        struct IoCounters { $($field: AtomicU64),+ }

        /// Requests delivered to redb's stock file backend. Read requests bypassed
        /// redb's page cache (or are metadata/recovery requests); they may still
        /// hit the OS cache. Byte counts are requested bytes, including failures,
        /// not confirmed transfers or physical disk I/O. Counts include all
        /// database work, opening, background work, and closing while attached.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
        pub struct PagedIoCounts { $(pub $field: u64),+ }
        impl PagedIoCounts {
            /// Read at quiescent boundaries for per-operation attribution.
            pub fn delta_since(self, earlier: Self) -> Self {
                Self { $($field: self.$field.saturating_sub(earlier.$field)),+ }
            }
        }
        impl IoCounters {
            fn snapshot(&self) -> PagedIoCounts {
                PagedIoCounts { $($field: self.$field.load(Ordering::Relaxed)),+ }
            }
        }
    }
}
io_counters! {
    read_calls, read_requested_bytes, write_calls, write_requested_bytes,
    sync_calls, resize_calls, length_calls, close_calls, errors,
}

/// Host-only instrumentation enabled by `PagedDatabase::open_with_io_metrics`.
/// Ordinary `open` uses the original stock backend without counter updates.
/// Cloned handles share totals. Field sampling is not an atomic global snapshot.
#[derive(Clone, Debug, Default)]
pub struct PagedIoMetrics {
    counters: Arc<IoCounters>,
}
impl PagedIoMetrics {
    pub fn snapshot(&self) -> PagedIoCounts {
        self.counters.snapshot()
    }
}

#[derive(Debug)]
struct MeasuredBackend<B> {
    inner: B,
    metrics: PagedIoMetrics,
}
impl<B> MeasuredBackend<B> {
    fn result<T>(&self, result: std::io::Result<T>) -> std::io::Result<T> {
        if result.is_err() {
            self.metrics.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}
impl<B: redb::StorageBackend> redb::StorageBackend for MeasuredBackend<B> {
    fn len(&self) -> std::io::Result<u64> {
        self.metrics
            .counters
            .length_calls
            .fetch_add(1, Ordering::Relaxed);
        self.result(self.inner.len())
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.metrics
            .counters
            .read_calls
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .counters
            .read_requested_bytes
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        self.result(self.inner.read(offset, out))
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.metrics
            .counters
            .resize_calls
            .fetch_add(1, Ordering::Relaxed);
        self.result(self.inner.set_len(len))
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.metrics
            .counters
            .sync_calls
            .fetch_add(1, Ordering::Relaxed);
        self.result(self.inner.sync_data())
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.metrics
            .counters
            .write_calls
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .counters
            .write_requested_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.result(self.inner.write(offset, data))
    }
    fn close(&self) -> std::io::Result<()> {
        self.metrics
            .counters
            .close_calls
            .fetch_add(1, Ordering::Relaxed);
        self.result(self.inner.close())
    }
}

macro_rules! read_counters {
    ($($variant:ident => $field:ident),+ $(,)?) => {
        #[derive(Clone, Copy)]
        pub(super) enum ReadCounter { $($variant),+ }

        #[derive(Default)]
        pub(super) struct ReadCounters { $($field: AtomicU64),+ }

        /// Cumulative logical work performed by an instrumented snapshot and
        /// its clones. Counters include unsuccessful and unauthorized reads.
        /// Concurrent snapshots/clones sharing a handle contribute together;
        /// reading fields is not an atomic cross-counter snapshot.
        ///
        /// Cache hits/misses count `raw` decoded-cache probes; misses include
        /// disabled caching and absent rows. Evictions count FIFO removals by
        /// this operation, even when another snapshot inserted the removed row.
        /// `row_table_gets` counts direct row-table point requests through `raw`
        /// or the adaptive range root reader,
        /// not index reads or cursor traversal. Root scan/index counters count
        /// candidates in the instrumented streaming visitors. Decode/validation
        /// counters count attempts, including corrupt input. Visibility nodes
        /// count distinct nodes per generic traversal; reference checks count
        /// attempted edges, not distinct target identities. These counters do
        /// not measure redb page-cache misses, physical reads or bytes transferred.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
        pub struct PagedReadCounts { $(pub $field: u64),+ }

        impl ReadCounters {
            pub(super) fn increment(&self, counter: ReadCounter) {
                let value = match counter { $(ReadCounter::$variant => &self.$field),+ };
                value.fetch_add(1, Ordering::Relaxed);
            }
            fn snapshot(&self) -> PagedReadCounts {
                PagedReadCounts { $($field: self.$field.load(Ordering::Relaxed)),+ }
            }
        }
        impl PagedReadCounts {
            /// Difference from an earlier reading of the same handle. Call only
            /// at quiescent boundaries for exact per-operation attribution.
            /// Saturating subtraction avoids underflow for mismatched readings.
            pub fn delta_since(self, earlier: Self) -> Self {
                Self { $($field: self.$field.saturating_sub(earlier.$field)),+ }
            }
        }
    };
}

read_counters! {
    DecodedCacheHit => decoded_cache_hits,
    DecodedCacheMiss => decoded_cache_misses,
    DecodedCacheEviction => decoded_cache_evictions,
    RowTableGet => row_table_gets,
    RootScanRow => root_scan_rows,
    IndexCandidate => index_candidates,
    DecodedRow => decoded_rows,
    ValidatedPackedRow => validated_packed_rows,
    VisibilityNode => visibility_nodes,
    ReferenceCheck => reference_checks,
}

/// An opt-in diagnostic handle. Metrics do not contain row values or identities.
/// No counters are allocated or updated by an ordinary snapshot.
#[derive(Clone, Default)]
pub struct PagedReadMetrics {
    pub(super) counters: Arc<ReadCounters>,
}
impl PagedReadMetrics {
    pub fn snapshot(&self) -> PagedReadCounts {
        self.counters.snapshot()
    }
}

/// Storage attributed by redb to one physical table in a pinned revision.
/// `stored_bytes` includes application keys and values; `metadata_bytes`
/// includes internal branch keys. Fragmentation is reported separately.
#[derive(Clone, Debug, Serialize)]
pub struct PagedTableStorage {
    pub name: String,
    /// Physical entries: LPG3 row-table entries are blocks, not logical rows.
    pub entries: u64,
    pub tree_height: u32,
    pub leaf_pages: u64,
    pub branch_pages: u64,
    pub stored_bytes: u64,
    pub metadata_bytes: u64,
    pub fragmented_bytes: u64,
}
impl PagedTableStorage {
    fn read(name: &str, table: &impl ReadableTableMetadata) -> Result<Self> {
        let stats = table.stats().map_err(err)?;
        Ok(Self {
            name: name.into(),
            entries: table.len().map_err(err)?,
            tree_height: stats.tree_height(),
            leaf_pages: stats.leaf_pages(),
            branch_pages: stats.branch_pages(),
            stored_bytes: stats.stored_bytes(),
            metadata_bytes: stats.metadata_bytes(),
            fragmented_bytes: stats.fragmented_bytes(),
        })
    }
}

/// Current table accounting, not whole-file allocated size. Excludes database
/// headers, allocator tables, free pages and older pages retained by snapshots.
/// Computing redb statistics may traverse table pages and warm storage caches.
#[derive(Clone, Debug, Serialize)]
pub struct PagedStorageBreakdown {
    pub revision: u64,
    pub format: &'static str,
    pub tables: Vec<PagedTableStorage>,
}

impl PagedDatabase {
    /// Open with optional backend request counters. File flags, OS locking,
    /// durability, error propagation, and redb cache budgets match `open`.
    /// Use separately from ordinary latency measurements: instrumentation adds
    /// atomic updates only to this explicitly selected backend.
    pub fn open_with_io_metrics(
        path: impl AsRef<Path>,
        options: PagedOptions,
    ) -> Result<(Self, PagedIoMetrics)> {
        let metrics = PagedIoMetrics::default();
        let db = Self::open_with_factory(options, |builder| {
            // These are exactly redb Builder::create's file-opening flags. Keep
            // stock FileBackend::new responsible for platform file locking.
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(err)?;
            let inner = redb::backends::FileBackend::new(file).map_err(err)?;
            builder
                .create_with_backend(MeasuredBackend {
                    inner,
                    metrics: metrics.clone(),
                })
                .map_err(err)
        })?;
        Ok((db, metrics))
    }

    /// Attach fresh logical-work counters to a snapshot of this database. This
    /// host handle can already create administrator sessions; restricted snapshot
    /// holders alone cannot opt in and observe hidden candidate cardinalities.
    /// Existing snapshot clones are unaffected; later clones share the handle.
    /// This never changes the snapshot's principal, scope or authority fences.
    pub fn with_read_metrics(
        &self,
        mut snapshot: PagedSnapshot,
    ) -> Result<(PagedSnapshot, PagedReadMetrics)> {
        if !Arc::ptr_eq(&self.inner, &snapshot.db.inner) {
            return Err(Error::Denied);
        }
        snapshot.fence()?;
        let metrics = PagedReadMetrics::default();
        snapshot.metrics = Some(metrics.counters.clone());
        Ok((snapshot, metrics))
    }
}

impl PagedSnapshot {
    pub(super) fn count(&self, counter: ReadCounter) {
        if let Some(metrics) = &self.metrics {
            metrics.increment(counter);
        }
    }

    /// Host-administrator-only accounting for this pinned revision. Call after
    /// timed queries; this is an explicit diagnostic, not a constant-time API.
    pub fn storage_breakdown(&self) -> Result<PagedStorageBreakdown> {
        if self.principal.is_some() {
            return Err(Error::Denied);
        }
        self.fence()?;
        let meta = self.view.read.open_table(META).map_err(err)?;
        let tables = vec![
            PagedTableStorage::read(ROWS.name(), self.view.rows.physical())?,
            PagedTableStorage::read(INDEX.name(), &self.view.index)?,
            PagedTableStorage::read(UNIQUE.name(), &self.view.unique)?,
            PagedTableStorage::read(INCOMING.name(), &self.view.incoming)?,
            PagedTableStorage::read(IDS.name(), &self.view.ids)?,
            PagedTableStorage::read(VERS.name(), &self.view.versions)?,
            PagedTableStorage::read(META.name(), &meta)?,
        ];
        self.fence()?;
        Ok(PagedStorageBreakdown {
            revision: self.view.revision,
            format: match self.view.format {
                PagedFormat::V1 => "LPG1",
                PagedFormat::V2 => "LPG2",
                PagedFormat::V3 => "LPG3",
            },
            tables,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::StorageBackend;

    #[derive(Debug)]
    struct FailingBackend;
    impl StorageBackend for FailingBackend {
        fn len(&self) -> std::io::Result<u64> {
            Err(std::io::Error::other("len"))
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
            assert_eq!(offset, 17);
            out[0] = 9;
            Err(std::io::Error::other("read"))
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            assert_eq!(len, 41);
            Err(std::io::Error::other("resize"))
        }
        fn sync_data(&self) -> std::io::Result<()> {
            Err(std::io::Error::other("sync"))
        }
        fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            assert_eq!(offset, 19);
            assert_eq!(data, b"abc");
            Err(std::io::Error::other("write"))
        }
        fn close(&self) -> std::io::Result<()> {
            Err(std::io::Error::other("close"))
        }
    }

    #[test]
    fn measured_backend_preserves_all_errors_and_partial_read_buffers() {
        let metrics = PagedIoMetrics::default();
        let backend = MeasuredBackend {
            inner: FailingBackend,
            metrics: metrics.clone(),
        };
        let mut out = [0; 2];
        assert_eq!(backend.len().unwrap_err().to_string(), "len");
        assert_eq!(backend.read(17, &mut out).unwrap_err().to_string(), "read");
        assert_eq!(out, [9, 0]);
        assert_eq!(backend.set_len(41).unwrap_err().to_string(), "resize");
        assert_eq!(backend.sync_data().unwrap_err().to_string(), "sync");
        assert_eq!(backend.write(19, b"abc").unwrap_err().to_string(), "write");
        assert_eq!(backend.close().unwrap_err().to_string(), "close");
        assert_eq!(metrics.snapshot().errors, 6);
        assert_eq!(metrics.snapshot().read_requested_bytes, 2);
        assert_eq!(metrics.snapshot().write_requested_bytes, 3);
    }

    #[test]
    fn measured_file_backend_forwards_bytes_eof_resize_sync_and_close() {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("raw"))
            .unwrap();
        let metrics = PagedIoMetrics::default();
        let backend = MeasuredBackend {
            inner: redb::backends::FileBackend::new(file).unwrap(),
            metrics: metrics.clone(),
        };
        backend.set_len(8).unwrap();
        backend.write(2, b"test").unwrap();
        let mut out = [0u8; 8];
        backend.read(0, &mut out).unwrap();
        assert_eq!(&out, b"\0\0test\0\0");
        assert_eq!(backend.len().unwrap(), 8);
        assert_eq!(
            backend.read(7, &mut [0; 2]).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        backend.read(0, &mut []).unwrap();
        backend.sync_data().unwrap();
        backend.close().unwrap();
        let counts = metrics.snapshot();
        assert_eq!(counts.read_calls, 3);
        assert_eq!(counts.read_requested_bytes, 10);
        assert_eq!(counts.write_calls, 1);
        assert_eq!(counts.write_requested_bytes, 4);
        assert_eq!(counts.resize_calls, 1);
        assert_eq!(counts.length_calls, 1);
        assert_eq!(counts.sync_calls, 1);
        assert_eq!(counts.close_calls, 1);
        assert_eq!(counts.errors, 1);
        assert_eq!(counts.delta_since(counts), PagedIoCounts::default());
    }

    #[test]
    fn diagnostic_open_preserves_locking_durable_reopen_and_invalid_limits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let invalid = dir.path().join("invalid");
        assert!(
            PagedDatabase::open_with_io_metrics(
                &invalid,
                PagedOptions {
                    max_page_rows: 0,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(!invalid.exists());
        let (db, metrics) = PagedDatabase::open_with_io_metrics(&path, Default::default()).unwrap();
        #[cfg(any(unix, windows))]
        {
            assert!(PagedDatabase::open(&path, Default::default()).is_err());
            assert!(PagedDatabase::open_with_io_metrics(&path, Default::default()).is_err());
        }
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "sample",
            1,
            vec![],
            vec![Relation::new(
                "sample.Item",
                vec![Role::new("number", DataType::Int)],
            )],
        ))
        .unwrap();
        tx.insert_with_id("sample.Item", Id::from_u128(1), vec![1.into()])
            .unwrap();
        tx.commit().unwrap();
        assert!(metrics.snapshot().write_requested_bytes > 0);
        assert!(metrics.snapshot().sync_calls > 0);
        drop(db);
        assert_eq!(metrics.snapshot().close_calls, 1);
        let db = PagedDatabase::open(&path, Default::default()).unwrap();
        assert_eq!(
            db.admin()
                .get("sample.Item", Id::from_u128(1))
                .unwrap()
                .unwrap()
                .values
                .as_ref(),
            &[1.into()]
        );
        drop(db);
        let (db, metrics) = PagedDatabase::open_with_io_metrics(&path, Default::default()).unwrap();
        assert!(
            db.admin()
                .get("sample.Item", Id::from_u128(1))
                .unwrap()
                .is_some()
        );
        assert!(metrics.snapshot().read_calls > 0);
    }

    #[test]
    fn counters_share_clones_and_report_quiescent_deltas() {
        let metrics = PagedReadMetrics::default();
        let earlier = metrics.snapshot();
        let clone = metrics.clone();
        clone.counters.increment(ReadCounter::DecodedCacheHit);
        metrics.counters.increment(ReadCounter::DecodedCacheHit);
        metrics.counters.increment(ReadCounter::RowTableGet);
        let delta = metrics.snapshot().delta_since(earlier);
        assert_eq!(delta.decoded_cache_hits, 2);
        assert_eq!(delta.row_table_gets, 1);
        assert_eq!(delta.decoded_cache_misses, 0);
        assert_eq!(earlier.delta_since(metrics.snapshot()), earlier);
    }

    #[test]
    fn cache_metrics_distinguish_hits_absence_and_fifo_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(
            dir.path().join("db"),
            PagedOptions {
                // One scalar row, including its cached validated packed length.
                decoded_cache_bytes: 128
                    + std::mem::size_of::<usize>()
                    + std::mem::size_of::<Value>(),
                ..Default::default()
            },
        )
        .unwrap();
        let relation = Relation::new("sample.Item", vec![Role::new("number", DataType::Int)]);
        let tid = relation.id;
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new("sample", 1, vec![], vec![relation]))
            .unwrap();
        for n in 1..=2 {
            tx.insert_with_id("sample.Item", Id::from_u128(n), vec![Value::Int(n as i64)])
                .unwrap();
        }
        tx.commit().unwrap();
        let ordinary = db.admin().snapshot().unwrap();
        assert!(ordinary.metrics.is_none());
        let (snapshot, metrics) = db.with_read_metrics(ordinary.clone()).unwrap();
        for n in [1, 1, 2, 1] {
            assert!(snapshot.raw(tid, Id::from_u128(n)).unwrap().is_some());
        }
        assert!(snapshot.raw(tid, Id::from_u128(99)).unwrap().is_none());
        let counts = metrics.snapshot();
        assert_eq!(counts.decoded_cache_hits, 1);
        assert_eq!(counts.decoded_cache_misses, 4);
        assert_eq!(counts.row_table_gets, 4);
        assert_eq!(counts.decoded_rows, 3);
        assert_eq!(counts.decoded_cache_evictions, 2);
        ordinary.raw(tid, Id::from_u128(99)).unwrap();
        assert_eq!(metrics.snapshot(), counts);
        assert!(db.stats().unwrap().decoded_cache_bytes <= db.inner.options.decoded_cache_bytes);
    }

    #[test]
    fn host_metrics_preserve_capability_scope_revocation_and_database_identity() {
        use crate::capability::{CapabilityScope, RelationCapability};
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let other = PagedDatabase::open(dir.path().join("other"), Default::default()).unwrap();
        assert!(matches!(
            db.with_read_metrics(other.admin().snapshot().unwrap()),
            Err(Error::Denied)
        ));
        let relation = Relation::new("sample.Item", vec![Role::new("number", DataType::Int)]);
        let tid = relation.id;
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(
            Module::new("sample", 1, vec![], vec![relation]),
            "owner",
            Visibility::Private,
        )
        .unwrap();
        for n in 1..=2 {
            tx.insert_with_id("sample.Item", Id::from_u128(n), vec![Value::Int(n as i64)])
                .unwrap();
        }
        tx.grant("reader", "sample.Item", Grant::read_only())
            .unwrap();
        tx.commit().unwrap();
        let capability = Id::from_u128(600);
        db.issue_capability(
            capability,
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: tid,
                    grant: Grant {
                        filter: RowFilter::Equals {
                            role: "number".into(),
                            value: 1.into(),
                        },
                        ..Grant::read_only()
                    },
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
        let scoped = db
            .session("reader")
            .unwrap()
            .with_capability(capability)
            .unwrap()
            .snapshot()
            .unwrap();
        let (scoped, metrics) = db.with_read_metrics(scoped).unwrap();
        assert!(
            scoped
                .get("sample.Item", Id::from_u128(1))
                .unwrap()
                .is_some()
        );
        assert!(
            scoped
                .get("sample.Item", Id::from_u128(2))
                .unwrap()
                .is_none()
        );
        assert!(metrics.snapshot().row_table_gets > 0);
        assert!(matches!(scoped.storage_breakdown(), Err(Error::Denied)));
        db.revoke_capability(capability).unwrap();
        assert!(matches!(
            scoped.get("sample.Item", Id::from_u128(1)),
            Err(Error::Denied)
        ));
        assert!(matches!(db.with_read_metrics(scoped), Err(Error::Denied)));
    }

    #[test]
    fn storage_accounting_is_pinned_read_only_and_host_only() {
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), Default::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install(Module::new(
            "sample",
            1,
            vec![],
            vec![Relation::new(
                "sample.Item",
                vec![Role::new("number", DataType::Int)],
            )],
        ))
        .unwrap();
        tx.insert_with_id("sample.Item", Id::from_u128(1), vec![1.into()])
            .unwrap();
        tx.commit().unwrap();
        let pinned = db.admin().snapshot().unwrap();
        let before = pinned.storage_breakdown().unwrap();
        assert_eq!(before.tables.len(), 7);
        let rows = before
            .tables
            .iter()
            .find(|table| table.name == ROWS.name())
            .unwrap();
        assert_eq!(rows.entries, 1);
        assert!(rows.stored_bytes > 0);
        let mut tx = db.admin().transaction().unwrap();
        tx.insert_with_id("sample.Item", Id::from_u128(2), vec![2.into()])
            .unwrap();
        tx.commit().unwrap();
        let after = pinned.storage_breakdown().unwrap();
        assert_eq!(before.revision, after.revision);
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        let current = db.admin().snapshot().unwrap().storage_breakdown().unwrap();
        assert_eq!(
            current
                .tables
                .iter()
                .find(|table| table.name == ROWS.name())
                .unwrap()
                .entries,
            1
        );
        assert_eq!(db.admin().snapshot().unwrap().verify().unwrap().rows, 2);
        assert!(matches!(
            db.session("reader")
                .unwrap()
                .snapshot()
                .unwrap()
                .storage_breakdown(),
            Err(Error::Denied)
        ));
        assert!(matches!(
            db.anonymous().snapshot().unwrap().storage_breakdown(),
            Err(Error::Denied)
        ));
    }
}
