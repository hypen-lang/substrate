//! Fixed-work SQL/native comparison. Invoke MODE PERSON_ROWS.
//! Modes: paged|resident|sqlite|postgres|native-paged|native-resident.
//! Each process uses its own file; run serially through experiments/paged_sql/run.py.
use rusqlite::{Connection, types::Value as S};
use serde_json::json;
use std::{hint::black_box, time::Instant};
use substrate_liquid::{
    paged::{PagedDatabase, PagedIoCounts, PagedIoMetrics, PagedOptions, PagedSqlSnapshot},
    *,
};
type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const CACHE: usize = 8 * 1024 * 1024;
// Fixed family IDs make physical family order identical across processes and
// before/after executables. Occurrences use only small integers below 2,000,001.
const PERSON_TYPE: Id = Id::from_u128(0x01000000000000000000000000000001);
const SCORE_TYPE: Id = Id::from_u128(0x02000000000000000000000000000002);
const FIXTURE_VERSION: &str = "fixed-schema-ids-v1";
#[path = "paged_sql_bench/native.rs"]
mod native;
#[cfg(unix)]
#[path = "paged_sql_bench/postgres.rs"]
mod postgres;
#[derive(Clone, Copy, PartialEq, Eq)]
enum Suite {
    Reads,
    Aggregates,
    AggregatesWide,
}
impl Suite {
    fn from_environment() -> AnyResult<Self> {
        match std::env::var("SUBSTRATE_PAGED_SQL_SUITE") {
            Ok(value) => match value.as_str() {
                "reads" => Ok(Self::Reads),
                "aggregates" => Ok(Self::Aggregates),
                "aggregates-wide" => Ok(Self::AggregatesWide),
                _ => Err("suite must be reads|aggregates|aggregates-wide".into()),
            },
            Err(std::env::VarError::NotPresent) => Ok(Self::Reads),
            Err(error) => Err(error.into()),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Reads => "reads",
            Self::Aggregates => "aggregates",
            Self::AggregatesWide => "aggregates-wide",
        }
    }
    fn buckets(self) -> usize {
        if self == Self::AggregatesWide {
            1024
        } else {
            32
        }
    }
    fn score_roles(self) -> Vec<Role> {
        let mut roles = vec![
            Role::new("person", DataType::Ref("core.Person".into())),
            Role::new("score", DataType::Int),
        ];
        if self != Self::Reads {
            roles.push(Role::new("bucket", DataType::Int));
        }
        roles
    }
    fn score_values(self, person: usize, n: usize) -> Vec<Value> {
        let mut values = vec![Value::Ref(id(person)), Value::Int(n as i64 * 10)];
        if self != Self::Reads {
            values.push(Value::Int((n % self.buckets()) as i64));
        }
        values
    }
}
/// Fixed-work diagnostic profile. Never recalibrate per engine or warm the
/// changing-parameter sequence beyond its original single query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HotProfile {
    Default,
    Robust,
}
impl HotProfile {
    fn parse(value: Option<&str>) -> AnyResult<Self> {
        match value {
            None | Some("default") => Ok(Self::Default),
            Some("robust") => Ok(Self::Robust),
            _ => Err("hot profile must be default|robust".into()),
        }
    }
    fn from_environment() -> AnyResult<Self> {
        match std::env::var("SUBSTRATE_PAGED_SQL_HOT_PROFILE") {
            Ok(value) => Self::parse(Some(&value)),
            Err(std::env::VarError::NotPresent) => Self::parse(None),
            Err(error) => Err(error.into()),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Robust => "robust",
        }
    }
    fn selected(self, case: &str) -> bool {
        self == Self::Robust
            && matches!(
                case,
                "point" | "equality" | "selective_join" | "aggregate_equality"
            )
    }
    fn iterations(self, case: &str, base: usize) -> usize {
        if self.selected(case) {
            base * 100
        } else {
            base
        }
    }
    fn warmups(self, case: &str) -> usize {
        if self.selected(case) { 1000 } else { 1 }
    }
    fn annotate(self, results: &mut [serde_json::Value]) {
        for result in results {
            let warmups = self.warmups(result["case"].as_str().expect("measured case name"));
            result["warmup_iterations"] = json!(warmups);
        }
    }
}
fn timed_cases(
    rows: usize,
    suite: Suite,
    profile: HotProfile,
) -> Vec<(&'static str, &'static str, usize)> {
    cases(rows, suite)
        .into_iter()
        .map(|(case, sql, count)| (case, sql, profile.iterations(case, count)))
        .collect()
}
#[derive(Clone, Copy)]
enum ReferenceOrder {
    Ordered,
    Reversed,
}
impl ReferenceOrder {
    fn from_environment() -> AnyResult<Self> {
        match std::env::var("SUBSTRATE_PAGED_REFERENCE_ORDER") {
            Ok(value) => match value.as_str() {
                "ordered" => Ok(Self::Ordered),
                "reversed" => Ok(Self::Reversed),
                _ => Err("reference order must be ordered|reversed".into()),
            },
            Err(std::env::VarError::NotPresent) => Ok(Self::Ordered),
            Err(error) => Err(error.into()),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Reversed => "reversed",
        }
    }
    fn target_person(self, n: usize, rows: usize) -> usize {
        match self {
            Self::Ordered => n,
            Self::Reversed => rows - 1 - n,
        }
    }
}
fn id(n: usize) -> Id {
    Id::from_u128(n as u128 + 1)
}
fn name(n: usize) -> String {
    format!("Person{n:08}")
}
fn modules(suite: Suite) -> [Module; 2] {
    let mut modules = [
        Module::new(
            "core",
            1,
            vec![],
            vec![
                Relation::new(
                    "core.Person",
                    vec![
                        Role::new("name", DataType::Text),
                        Role::new("number", DataType::Int),
                    ],
                )
                .unique(&["number"]),
            ],
        ),
        Module::new(
            "app",
            1,
            vec![("core".into(), 1)],
            vec![Relation::new("app.Score", suite.score_roles()).unique(&["person"])],
        ),
    ];
    modules[0].relations[0].id = PERSON_TYPE;
    modules[1].relations[0].id = SCORE_TYPE;
    modules
}
enum Db {
    Paged(PagedDatabase, Option<PagedIoMetrics>),
    Resident(Database),
    Sqlite(Connection),
}
enum Sql {
    Paged(PagedSqlSnapshot),
    Resident(SqlSnapshot),
}
impl Db {
    fn build(
        mode: &str,
        path: &std::path::Path,
        rows: usize,
        order: ReferenceOrder,
        suite: Suite,
    ) -> AnyResult<Self> {
        Self::build_with_io(mode, path, rows, order, suite, false)
    }
    fn build_with_io(
        mode: &str,
        path: &std::path::Path,
        rows: usize,
        order: ReferenceOrder,
        suite: Suite,
        io_diagnostics: bool,
    ) -> AnyResult<Self> {
        if io_diagnostics && mode != "paged" {
            return Err("I/O diagnostics require paged storage".into());
        }
        match mode {
            "paged" => {
                let options = PagedOptions {
                    cache_bytes: CACHE,
                    decoded_cache_bytes: 1024 * 1024,
                    ..Default::default()
                };
                let (db, io) = if io_diagnostics {
                    let (db, io) = PagedDatabase::open_with_io_metrics(path, options)?;
                    (db, Some(io))
                } else {
                    (PagedDatabase::open(path, options)?, None)
                };
                let mut tx = db.admin().transaction()?;
                for module in modules(suite) {
                    tx.install_owned(module, "owner", Visibility::Private)?;
                }
                for relation in ["core.Person", "app.Score"] {
                    tx.grant("reader", relation, Grant::read_only())?;
                }
                tx.commit()?;
                for start in (0..rows).step_by(128) {
                    let mut tx = db.admin().transaction()?;
                    for n in start..(start + 128).min(rows) {
                        tx.insert_with_id(
                            "core.Person",
                            id(order.target_person(n, rows)),
                            vec![
                                Value::Text(name(order.target_person(n, rows))),
                                Value::Int(order.target_person(n, rows) as i64),
                            ],
                        )?;
                        tx.insert_with_id(
                            "app.Score",
                            id(rows + n),
                            suite.score_values(order.target_person(n, rows), n),
                        )?;
                    }
                    tx.commit()?;
                }
                Ok(Self::Paged(db, io))
            }
            "resident" => {
                let db = Database::open_with_storage_options(
                    path,
                    DatabaseOptions::default(),
                    GroupCommitOptions::default(),
                    StorageOptions { cache_bytes: CACHE },
                )?;
                let mut tx = db.admin().transaction();
                for module in modules(suite) {
                    tx.install_with_visibility(module, Visibility::Private)?;
                }
                for relation in ["core.Person", "app.Score"] {
                    tx.grant("reader", relation, Grant::read_only())?;
                }
                tx.commit()?;
                for start in (0..rows).step_by(128) {
                    let mut tx = db.admin().transaction();
                    for n in start..(start + 128).min(rows) {
                        tx.insert_with_id(
                            "core.Person",
                            id(order.target_person(n, rows)),
                            vec![
                                Value::Text(name(order.target_person(n, rows))),
                                Value::Int(order.target_person(n, rows) as i64),
                            ],
                        )?;
                        tx.insert_with_id(
                            "app.Score",
                            id(rows + n),
                            suite.score_values(order.target_person(n, rows), n),
                        )?;
                    }
                    tx.commit()?;
                }
                Ok(Self::Resident(db))
            }
            "sqlite" => {
                let mut db = Connection::open(path)?;
                // One database; quoted dotted names preserve the same logical namespace.
                db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA cache_size=-8192; PRAGMA foreign_keys=ON; CREATE TABLE 'core.Person'(id TEXT PRIMARY KEY,name TEXT NOT NULL,number INTEGER NOT NULL UNIQUE); CREATE INDEX person_name ON 'core.Person'(name); CREATE TABLE 'app.Score'(id TEXT PRIMARY KEY,person TEXT NOT NULL UNIQUE REFERENCES 'core.Person'(id),score INTEGER NOT NULL); CREATE INDEX score_value ON 'app.Score'(score);")?;
                if suite != Suite::Reads {
                    db.execute_batch("ALTER TABLE 'app.Score' ADD COLUMN bucket INTEGER NOT NULL DEFAULT 0; CREATE INDEX score_bucket ON 'app.Score'(bucket);")?;
                }
                for start in (0..rows).step_by(128) {
                    let tx = db.transaction()?;
                    {
                        let mut p = tx.prepare("INSERT INTO 'core.Person' VALUES(?1,?2,?3)")?;
                        let mut s = tx.prepare(if suite != Suite::Reads {
                            "INSERT INTO 'app.Score' VALUES(?1,?2,?3,?4)"
                        } else {
                            "INSERT INTO 'app.Score' VALUES(?1,?2,?3)"
                        })?;
                        for n in start..(start + 128).min(rows) {
                            let person = order.target_person(n, rows);
                            p.execute((id(person).to_string(), name(person), person as i64))?;
                            let mut values = vec![
                                S::Text(id(rows + n).to_string()),
                                S::Text(id(person).to_string()),
                                S::Integer(n as i64 * 10),
                            ];
                            if suite != Suite::Reads {
                                values.push(S::Integer((n % suite.buckets()) as i64));
                            }
                            s.execute(rusqlite::params_from_iter(values))?;
                        }
                    }
                    tx.commit()?;
                }
                Ok(Self::Sqlite(db))
            }
            _ => Err("mode must be paged|resident|sqlite".into()),
        }
    }
    fn io_counts(&self) -> Option<PagedIoCounts> {
        match self {
            Self::Paged(_, Some(metrics)) => Some(metrics.snapshot()),
            _ => None,
        }
    }
    fn io_delta(&self, before: Option<PagedIoCounts>) -> Option<PagedIoCounts> {
        before
            .zip(self.io_counts())
            .map(|(before, after)| after.delta_since(before))
    }
    fn sql(&self) -> Result<Option<Sql>> {
        match self {
            Self::Paged(db, _) => Ok(Some(Sql::Paged(db.session("reader")?.snapshot()?.sql()?))),
            Self::Resident(db) => Ok(Some(Sql::Resident(db.session("reader").snapshot().sql()?))),
            Self::Sqlite(db) => {
                // Match the retained Substrate snapshot: all timed reads share
                // one SQLite read transaction rather than reopening it per call.
                db.execute_batch("BEGIN DEFERRED")
                    .map_err(|e| Error::Query(e.to_string()))?;
                Ok(None)
            }
        }
    }
}
fn cases(rows: usize, suite: Suite) -> Vec<(&'static str, &'static str, usize)> {
    let mut cases = vec![
        ("point", "SELECT name FROM core.Person WHERE id=?1", 5000),
        (
            "equality",
            "SELECT name FROM core.Person WHERE number=?1",
            2000,
        ),
        (
            "changing_parameters",
            "SELECT name FROM core.Person WHERE number=?1",
            2000,
        ),
        (
            "changing_parameters_warm",
            "SELECT name FROM core.Person WHERE number=?1",
            2000,
        ),
        (
            "selective_join",
            "SELECT p.name,s.score FROM core.Person p JOIN app.Score s ON s.person=p.id WHERE p.number=?1",
            2000,
        ),
        (
            "aggregate",
            "SELECT COUNT(*),SUM(score) FROM app.Score",
            if rows <= 100_000 { 3 } else { 1 },
        ),
    ];
    if suite != Suite::Reads {
        cases.extend([
            ("aggregate_equality", "SELECT COUNT(*),SUM(score) FROM app.Score WHERE score=?1", 1000),
            ("aggregate_range", "SELECT COUNT(*),SUM(score) FROM app.Score WHERE score>=?1 AND score<?2", 20),
            ("aggregate_broad", "SELECT COUNT(*),SUM(score) FROM app.Score WHERE score>=?1", 3),
            ("aggregate_grouped", "SELECT bucket,COUNT(*),SUM(score) FROM app.Score GROUP BY bucket ORDER BY bucket", 3),
            ("aggregate_filtered_grouped", "SELECT bucket,COUNT(*),SUM(score) FROM app.Score WHERE score>=?1 GROUP BY bucket ORDER BY bucket", 3),
        ]);
    }
    cases
}
fn binding(case: &str, n: usize, rows: usize) -> Vec<S> {
    let index = if case.starts_with("changing_parameters") {
        (n * 7919) % rows
    } else {
        rows / 2
    };
    match case {
        "point" => vec![S::Text(id(index).to_string())],
        "aggregate" | "aggregate_grouped" => vec![],
        "aggregate_equality" => vec![S::Integer(index as i64 * 10)],
        "aggregate_range" => vec![
            S::Integer(index as i64 * 10),
            S::Integer((index + rows / 100) as i64 * 10),
        ],
        "aggregate_broad" => vec![S::Integer((rows / 10) as i64 * 10)],
        "aggregate_filtered_grouped" => vec![S::Integer(index as i64 * 10)],
        _ => vec![S::Integer(index as i64)],
    }
}
fn expected(case: &str, n: usize, rows: usize, order: ReferenceOrder, suite: Suite) -> Vec<Vec<S>> {
    let index = if case.starts_with("changing_parameters") {
        (n * 7919) % rows
    } else {
        rows / 2
    };
    match case {
        "aggregate_equality" => aggregate_expected(index, index + 1, false, suite.buckets()),
        "aggregate_range" => aggregate_expected(index, index + rows / 100, false, suite.buckets()),
        "aggregate_broad" => aggregate_expected(rows / 10, rows, false, suite.buckets()),
        "aggregate_grouped" => aggregate_expected(0, rows, true, suite.buckets()),
        "aggregate_filtered_grouped" => aggregate_expected(index, rows, true, suite.buckets()),
        "aggregate" => vec![vec![
            S::Integer(rows as i64),
            S::Integer((rows as i64 - 1) * rows as i64 / 2 * 10),
        ]],
        "selective_join" => vec![vec![
            S::Text(name(index)),
            S::Integer(order.target_person(index, rows) as i64 * 10),
        ]],
        _ => vec![vec![S::Text(name(index))]],
    }
}
// The arithmetic reference is prepared before timing. Query execution still
// includes output validation, identically for every engine.
fn aggregate_expected(start: usize, end: usize, grouped: bool, buckets: usize) -> Vec<Vec<S>> {
    if !grouped {
        let count = end - start;
        let sum = (start + end - 1) as i64 * count as i64 / 2 * 10;
        return vec![vec![
            S::Integer(count as i64),
            if count == 0 { S::Null } else { S::Integer(sum) },
        ]];
    }
    (0..buckets)
        .filter_map(|bucket| {
            let first = start + (bucket + buckets - start % buckets) % buckets;
            if first >= end {
                return None;
            }
            let count = (end - 1 - first) / buckets + 1;
            let last = first + (count - 1) * buckets;
            Some(vec![
                S::Integer(bucket as i64),
                S::Integer(count as i64),
                S::Integer((first + last) as i64 * count as i64 / 2 * 10),
            ])
        })
        .collect()
}
// Process CPU includes every client worker thread. For PostgreSQL this excludes
// the server, whose CPU is sampled separately by the runner.
fn client_cpu_seconds() -> Option<f64> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the correctly sized output on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return None;
        }
        // SAFETY: the successful call above initialized every field we read.
        let usage = unsafe { usage.assume_init() };
        Some(
            usage.ru_utime.tv_sec as f64
                + usage.ru_utime.tv_usec as f64 / 1e6
                + usage.ru_stime.tv_sec as f64
                + usage.ru_stime.tv_usec as f64 / 1e6,
        )
    }
    #[cfg(not(unix))]
    {
        None
    }
}
// High-water marks at untimed phase boundaries distinguish loading from read
// and diagnostic peaks. These are not current RSS or per-phase allocations.
fn peak_rss_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes this correctly sized output on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return None;
        }
        // SAFETY: initialized by the successful system call above.
        let bytes = unsafe { usage.assume_init() }.ru_maxrss as u64;
        Some(if cfg!(target_os = "macos") {
            bytes
        } else {
            bytes.saturating_mul(1024)
        })
    }
    #[cfg(not(unix))]
    {
        None
    }
}
// Heap traversal is reserved for the separate diagnostic run; headline timings
// only sample the cheap process high-water counter outside timed loops.
fn allocator_diagnostics() -> Option<serde_json::Value> {
    #[cfg(target_os = "macos")]
    {
        if std::env::var("SUBSTRATE_PAGED_IO_DIAGNOSTICS")
            .ok()
            .as_deref()
            != Some("1")
        {
            return None;
        }
        let mut stats = std::mem::MaybeUninit::<libc::malloc_statistics_t>::zeroed();
        // SAFETY: the platform API writes this correctly sized statistics record.
        // A null zone requests the documented sum of all allocator zones.
        unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), stats.as_mut_ptr()) };
        // SAFETY: initialized by malloc_zone_statistics above.
        let stats = unsafe { stats.assume_init() };
        type MemoryReader = unsafe extern "C" fn(
            libc::task_t,
            libc::vm_address_t,
            libc::vm_size_t,
            *mut *mut libc::c_void,
        ) -> libc::kern_return_t;
        // These public malloc introspection APIs are not declared by libc.
        // Signatures match the platform malloc/malloc.h header.
        unsafe extern "C" {
            fn malloc_get_all_zones(
                task: libc::task_t,
                reader: Option<MemoryReader>,
                addresses: *mut *mut libc::vm_address_t,
                count: *mut libc::c_uint,
            ) -> libc::kern_return_t;
            fn malloc_get_zone_name(zone: *mut libc::malloc_zone_t) -> *const libc::c_char;
        }
        let mut addresses = std::ptr::null_mut();
        let mut count = 0;
        // SAFETY: querying this process with the default local reader. The API
        // writes the addresses pointer and count; both outputs start empty.
        // libc deprecates its Mach wrappers in favor of a separate crate; this
        // diagnostic only needs the existing public current-task accessor.
        #[allow(deprecated)]
        let zone_status = unsafe {
            malloc_get_all_zones(libc::mach_task_self(), None, &mut addresses, &mut count)
        };
        let mut zones = Vec::new();
        let zones_available =
            zone_status == 0 && count <= 1024 && (count == 0 || !addresses.is_null());
        if zones_available && count > 0 {
            // SAFETY: the successful API call returned count local zone
            // addresses. Copy the list before invoking another introspection
            // call. This benchmark does not destroy allocator zones.
            let addresses =
                unsafe { std::slice::from_raw_parts(addresses, count as usize) }.to_vec();
            for address in addresses {
                let zone = address as *mut libc::malloc_zone_t;
                if zone.is_null() {
                    continue;
                }
                let mut zone_stats = std::mem::MaybeUninit::<libc::malloc_statistics_t>::zeroed();
                // SAFETY: zone is a current-process allocator zone returned
                // by the public introspection API; the output has exact size.
                unsafe { libc::malloc_zone_statistics(zone, zone_stats.as_mut_ptr()) };
                let zone_stats = unsafe { zone_stats.assume_init() };
                // SAFETY: the API returns a zone-owned C string or null. Copy
                // the name immediately; no pointer is retained in the result.
                let name = unsafe {
                    let pointer = malloc_get_zone_name(zone);
                    (!pointer.is_null()).then(|| {
                        std::ffi::CStr::from_ptr(pointer)
                            .to_string_lossy()
                            .into_owned()
                    })
                };
                zones.push(json!({"name":name,"blocks_in_use":zone_stats.blocks_in_use,
                    "size_in_use":zone_stats.size_in_use,"max_size_in_use":zone_stats.max_size_in_use,
                    "size_allocated":zone_stats.size_allocated}));
            }
        }
        Some(
            json!({"method":"macOS allocator zone totals; live heap and reserved allocator bytes, not RSS",
            "blocks_in_use":stats.blocks_in_use,"size_in_use":stats.size_in_use,
            "max_size_in_use":stats.max_size_in_use,"size_allocated":stats.size_allocated,
            "zones_available":zones_available,"zones_status":zone_status,"zones":zones}),
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}
fn query_measurement(
    case: &str,
    iterations: usize,
    prepare: f64,
    wall: f64,
    cpu: Option<f64>,
) -> serde_json::Value {
    let cpu_us = cpu
        .zip(client_cpu_seconds())
        .map(|(start, end)| (end - start).max(0.0) * 1e6 / iterations as f64);
    json!({"case":case,"iterations":iterations,"prepare_us":prepare*1e6,
        "query_us":wall*1e6/iterations as f64,"client_cpu_us":cpu_us,
        "peak_rss_bytes_after_case":peak_rss_bytes(), "allocator_after_case":allocator_diagnostics()})
}

/// A point-in-time footprint of this benchmark's known files, while the
/// connection and read snapshot are still open. Do not checkpoint, close the
/// database, recurse through directories, or include unrelated neighbours.
fn file_footprint(mode: &str, path: &std::path::Path) -> AnyResult<serde_json::Value> {
    let sidecars: &[(&str, &str)] = match mode {
        "sqlite" => &[
            ("wal", "-wal"),
            ("shared_memory", "-shm"),
            ("rollback_journal", "-journal"),
        ],
        "resident" | "paged" => &[],
        _ => return Err("file footprint only supports embedded engines".into()),
    };
    let mut logical = 0u64;
    let mut allocated = if cfg!(unix) { Some(0u64) } else { None };
    let mut inspect =
        |role: &str, file: &std::path::Path, required: bool| -> AnyResult<serde_json::Value> {
            let metadata = match std::fs::symlink_metadata(file) {
                Ok(metadata) if metadata.is_file() => Some(metadata),
                Ok(_) => return Err("benchmark-owned database path is not a regular file".into()),
                Err(error) if !required && error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let size = metadata.as_ref().map_or(0, std::fs::Metadata::len);
            #[cfg(unix)]
            let disk = {
                use std::os::unix::fs::MetadataExt;
                Some(
                    metadata
                        .as_ref()
                        .map_or(0, |m| m.blocks())
                        .checked_mul(512)
                        .ok_or("allocated byte count overflow")?,
                )
            };
            #[cfg(not(unix))]
            let disk: Option<u64> = None;
            logical = logical
                .checked_add(size)
                .ok_or("logical byte count overflow")?;
            allocated = allocated
                .zip(disk)
                .map(|(a, b)| a.checked_add(b).ok_or("allocated byte count overflow"))
                .transpose()?;
            Ok(
                json!({"role":role,"filename":file.file_name().map(|name|name.to_string_lossy()),
            "exists":metadata.is_some(),"logical_bytes":size,"allocated_bytes":disk}),
            )
        };
    let main = inspect("main_database", path, true)?;
    let mut files = Vec::with_capacity(sidecars.len());
    for (role, suffix) in sidecars {
        let mut filename = path.as_os_str().to_os_string();
        filename.push(suffix);
        files.push(inspect(role, &std::path::PathBuf::from(filename), false)?);
    }
    Ok(
        json!({"scope":"main database and explicitly known sidecars only",
        "measurement":"after timed queries, while database and snapshot remain open",
        "main_database":main,"sidecars":files,"total_logical_bytes":logical,
        "total_allocated_bytes":allocated,
        "allocated_bytes_source":if cfg!(unix) {Some("filesystem st_blocks multiplied by 512; excludes directory metadata")} else {None}}),
    )
}

fn sqlite_query_plans(
    db: &Connection,
    rows: usize,
    suite: Suite,
) -> AnyResult<Vec<serde_json::Value>> {
    let mut plans = Vec::new();
    for (case, text, _) in cases(rows, suite) {
        if !matches!(
            case,
            "aggregate_broad"
                | "aggregate_range"
                | "aggregate_grouped"
                | "aggregate_filtered_grouped"
        ) {
            continue;
        }
        let sql = text
            .replace("core.Person", "\"core.Person\"")
            .replace("app.Score", "\"app.Score\"");
        let mut statement = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let entries = statement.query_map(rusqlite::params_from_iter(binding(case, 0, rows)), |row| {
            Ok(json!({"id":row.get::<_,i64>(0)?,"parent":row.get::<_,i64>(1)?,"detail":row.get::<_,String>(3)?}))
        })?.collect::<std::result::Result<Vec<_>,_>>()?;
        plans.push(json!({"case":case,"method":"EXPLAIN QUERY PLAN with fixture parameters; no ANALYZE; collected after timed queries",
            "plan":entries}));
    }
    Ok(plans)
}

fn paged_diagnostics(
    db: &Db,
    interface: &str,
    rows: usize,
    order: ReferenceOrder,
    suite: Suite,
) -> AnyResult<Option<serde_json::Value>> {
    let Db::Paged(db, io_metrics) = db else {
        return Ok(None);
    };
    let (snapshot, metrics) = db.with_read_metrics(db.session("reader")?.snapshot()?)?;
    let mut counts = Vec::new();
    if interface == "native" {
        let snapshot = native::NativeSnapshot::Paged(snapshot);
        for (case, _, _) in cases(rows, suite) {
            let before = metrics.snapshot();
            let io_before = io_metrics.as_ref().map(PagedIoMetrics::snapshot);
            let result = snapshot.read(case, 0, rows, suite)?;
            let delta = metrics.snapshot().delta_since(before);
            assert_eq!(result, expected(case, 0, rows, order, suite));
            let io_delta = io_before
                .zip(io_metrics.as_ref().map(PagedIoMetrics::snapshot))
                .map(|(before, after)| after.delta_since(before));
            counts.push(json!({"case":case,"counts":delta,"backend_io_counts":io_delta}));
        }
    } else {
        let sql = snapshot.sql()?;
        for (case, text, _) in cases(rows, suite) {
            let mut prepared = sql.prepare(text)?;
            let before = metrics.snapshot();
            let io_before = io_metrics.as_ref().map(PagedIoMetrics::snapshot);
            let result = prepared.query(&binding(case, 0, rows))?;
            let delta = metrics.snapshot().delta_since(before);
            assert_eq!(result.rows, expected(case, 0, rows, order, suite));
            let io_delta = io_before
                .zip(io_metrics.as_ref().map(PagedIoMetrics::snapshot))
                .map(|(before, after)| after.delta_since(before));
            counts.push(json!({"case":case,"counts":delta,"backend_io_counts":io_delta}));
        }
    }
    // Host-only physical accounting is deliberately separate from reader queries.
    // Traversing table statistics may warm pages, so this comes last.
    let storage = db.admin().snapshot()?.storage_breakdown()?;
    let cache = db.stats()?;
    let commits = db.group_commit_stats()?;
    Ok(Some(json!({
        "read_method":"one untimed instrumented reader execution per case after all timed queries; preparation excluded; cache state follows the timed suite",
        "counter_scope":"logical instrumented operations, not physical I/O or redb page-cache misses",
        "read_counts":counts,
        "storage_method":"host-admin pinned table accounting collected after all queries; excludes file headers, allocators, free and historical pages",
        "storage":storage,
        "commit_method":"completed engine durable groups since open, including fixture setup; not filesystem synchronization call counts or a concurrent TPS measurement",
        "commits":{"durable_groups":commits.durable_groups,"committed_writes":commits.committed_writes,"largest_group":commits.largest_group},
        "cache":{"revision":cache.revision,"decoded_cache_bytes":cache.decoded_cache_bytes,
            "decoded_cache_entries":cache.decoded_cache_entries,"configured_cache_bytes":cache.configured_cache_bytes}
    })))
}

fn parse_io_flag(value: Option<&str>) -> AnyResult<bool> {
    match value {
        None | Some("0") => Ok(false),
        Some("1") => Ok(true),
        _ => Err("SUBSTRATE_PAGED_IO_DIAGNOSTICS must be 0 or 1".into()),
    }
}
fn query_measurement_with_io(
    db: &Db,
    before: Option<PagedIoCounts>,
    case: &str,
    iterations: usize,
    prepare: f64,
    wall: f64,
    cpu: Option<f64>,
) -> serde_json::Value {
    let mut result = query_measurement(case, iterations, prepare, wall, cpu);
    result["backend_io_counts"] = json!(db.io_delta(before));
    result
}

fn mode_metadata(mode: &str) -> AnyResult<(&'static str, &'static str)> {
    match mode {
        "resident" => Ok(("sql", "resident")),
        "paged" => Ok(("sql", "paged")),
        "sqlite" => Ok(("sql", "sqlite")),
        "postgres" => Ok(("sql", "postgres")),
        "native-resident" => Ok(("native", "resident")),
        "native-paged" => Ok(("native", "paged")),
        _ => Err("mode must be resident|paged|sqlite|postgres|native-resident|native-paged".into()),
    }
}

fn main() -> AnyResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: paged_sql_bench MODE PERSON_ROWS".into());
    }
    let (interface, storage) = mode_metadata(&args[0])?;
    let io_environment = match std::env::var("SUBSTRATE_PAGED_IO_DIAGNOSTICS") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let io_diagnostics = parse_io_flag(io_environment.as_deref())?;
    if io_diagnostics && storage != "paged" {
        return Err("I/O diagnostics require paged or native-paged mode".into());
    }
    let rows: usize = args[1].parse()?;
    if !(100..=1_000_000).contains(&rows) {
        return Err("rows must be100..1M".into());
    }
    let order = ReferenceOrder::from_environment()?;
    let suite = Suite::from_environment()?;
    let profile = HotProfile::from_environment()?;
    if args[0] == "postgres" {
        #[cfg(unix)]
        return postgres::run(rows, order, suite, profile);
        #[cfg(not(unix))]
        return Err("the isolated PostgreSQL benchmark requires Unix".into());
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let start = Instant::now();
    let db = if io_diagnostics {
        Db::build_with_io(storage, &path, rows, order, suite, true)?
    } else {
        Db::build(storage, &path, rows, order, suite)?
    };
    let load = start.elapsed().as_secs_f64();
    let load_backend_io_counts = db.io_counts();
    let peak_after_load = peak_rss_bytes();
    let allocator_after_load = allocator_diagnostics();
    let start = Instant::now();
    let native = (interface == "native")
        .then(|| native::NativeSnapshot::new(&db))
        .transpose()?;
    let sql = if native.is_some() { None } else { db.sql()? };
    let setup = start.elapsed().as_secs_f64();
    let peak_after_setup = peak_rss_bytes();
    let allocator_after_setup = allocator_diagnostics();
    let mut results = Vec::new();
    for (case, text, iterations) in timed_cases(rows, suite, profile) {
        let aggregate_expected = case
            .starts_with("aggregate_")
            .then(|| expected(case, 0, rows, order, suite));
        let start = Instant::now();
        if let Some(native) = &native {
            for _ in 0..profile.warmups(case) {
                assert_eq!(
                    native.read(case, 0, rows, suite)?,
                    expected(case, 0, rows, order, suite)
                );
            }
            let io_start = db.io_counts();
            let cpu = client_cpu_seconds();
            let timer = Instant::now();
            for n in 0..iterations {
                let result = native.read(case, n, rows, suite)?;
                if let Some(expected) = &aggregate_expected {
                    assert_eq!(&result, expected);
                } else {
                    assert_eq!(result, expected(case, n, rows, order, suite));
                }
                black_box(result);
            }
            let mut result = query_measurement_with_io(
                &db,
                io_start,
                case,
                iterations,
                0.0,
                timer.elapsed().as_secs_f64(),
                cpu,
            );
            result["execution_path"] = json!(native::execution_path(case));
            result["prepare_kind"] =
                json!("not applicable; public native calls, no SQL or prepared aggregate engine");
            results.push(result);
            continue;
        }
        match &sql {
            Some(Sql::Paged(sql)) => {
                let mut p = sql.prepare(text)?;
                let prepare = start.elapsed().as_secs_f64();
                for _ in 0..profile.warmups(case) {
                    assert_eq!(
                        p.query(&binding(case, 0, rows))?.rows,
                        expected(case, 0, rows, order, suite)
                    );
                }
                let io_start = db.io_counts();
                let cpu = client_cpu_seconds();
                let t = Instant::now();
                for n in 0..iterations {
                    let result = p.query(&binding(case, n, rows))?;
                    if let Some(expected) = &aggregate_expected {
                        assert_eq!(&result.rows, expected);
                    } else {
                        assert_eq!(result.rows, expected(case, n, rows, order, suite));
                    }
                    black_box(result);
                }
                results.push(query_measurement_with_io(
                    &db,
                    io_start,
                    case,
                    iterations,
                    prepare,
                    t.elapsed().as_secs_f64(),
                    cpu,
                ));
            }
            Some(Sql::Resident(sql)) => {
                let mut p = sql.prepare(text)?;
                let prepare = start.elapsed().as_secs_f64();
                for _ in 0..profile.warmups(case) {
                    assert_eq!(
                        p.query(&binding(case, 0, rows))?.rows,
                        expected(case, 0, rows, order, suite)
                    );
                }
                let io_start = db.io_counts();
                let cpu = client_cpu_seconds();
                let t = Instant::now();
                for n in 0..iterations {
                    let result = p.query(&binding(case, n, rows))?;
                    if let Some(expected) = &aggregate_expected {
                        assert_eq!(&result.rows, expected);
                    } else {
                        assert_eq!(result.rows, expected(case, n, rows, order, suite));
                    }
                    black_box(result);
                }
                results.push(query_measurement_with_io(
                    &db,
                    io_start,
                    case,
                    iterations,
                    prepare,
                    t.elapsed().as_secs_f64(),
                    cpu,
                ));
            }
            None => {
                let Db::Sqlite(connection) = &db else {
                    unreachable!()
                };
                let text = text
                    .replace("core.Person", "\"core.Person\"")
                    .replace("app.Score", "\"app.Score\"");
                let mut p = connection.prepare(&text)?;
                let prepare = start.elapsed().as_secs_f64();
                let count = p.column_count();
                let read = |p: &mut rusqlite::Statement<'_>, n| -> rusqlite::Result<Vec<Vec<S>>> {
                    p.query_map(rusqlite::params_from_iter(binding(case, n, rows)), |r| {
                        (0..count).map(|c| r.get(c)).collect()
                    })?
                    .collect()
                };
                for _ in 0..profile.warmups(case) {
                    assert_eq!(read(&mut p, 0)?, expected(case, 0, rows, order, suite));
                }
                let io_start = db.io_counts();
                let cpu = client_cpu_seconds();
                let t = Instant::now();
                for n in 0..iterations {
                    let result = read(&mut p, n)?;
                    if let Some(expected) = &aggregate_expected {
                        assert_eq!(&result, expected);
                    } else {
                        assert_eq!(result, expected(case, n, rows, order, suite));
                    }
                    black_box(result);
                }
                results.push(query_measurement_with_io(
                    &db,
                    io_start,
                    case,
                    iterations,
                    prepare,
                    t.elapsed().as_secs_f64(),
                    cpu,
                ));
            }
        }
    }
    profile.annotate(&mut results);
    let peak_after_queries = peak_rss_bytes();
    let footprint = file_footprint(storage, &path)?;
    let diagnostics = paged_diagnostics(&db, interface, rows, order, suite)?;
    let sqlite_plans = match &db {
        Db::Sqlite(connection) => Some(sqlite_query_plans(connection, rows, suite)?),
        _ => None,
    };
    println!(
        "{}",
        json!({"mode":args[0],"interface":interface,"storage":storage,"timing_class":if io_diagnostics {"instrumented-diagnostic"} else {"headline"},"backend_io_method":if io_diagnostics {Some("stock FileBackend request counts and requested bytes; includes OS-cache hits and failed requests, not physical disk I/O; per-case totals cover timed iterations only, excluding warmup and preparation")} else {None},"fixture_version":FIXTURE_VERSION,"schema_manifest":modules(suite),"hot_profile":profile.name(),"suite":suite.name(),"reference_order":order.name(),"people":rows,"total_rows":rows*2,"relations":2,"namespaces":2,"load_seconds":load,"load_backend_io_counts":load_backend_io_counts,"read_setup_us":setup*1e6,"sql_setup_us":if interface=="sql" {Some(setup*1e6)} else {None},"native_setup_us":if interface=="native" {Some(setup*1e6)} else {None},"page_cache_bytes":CACHE,"decoded_cache_bytes":if storage=="paged" {1024*1024}else{0},"queries":results,"verified":true,"peak_rss_phases":{"method":"process lifetime high-water RSS sampled at untimed phase boundaries, not current memory or phase allocation","after_load":peak_after_load,"after_setup":peak_after_setup,"after_queries":peak_after_queries,"after_diagnostics":peak_rss_bytes()},"allocator_phases":{"after_load":allocator_after_load,"after_setup":allocator_after_setup,"after_diagnostics":allocator_diagnostics()},"file_footprint":footprint,"paged_diagnostics":diagnostics,"sqlite_query_plans":sqlite_plans,"native_source_materialization":if interface=="native" {Some(if storage=="paged" {"256-row public pages; client fold at most fixture bucket count"} else {"public eager scan/lookup Vec<Row>; client fold at most fixture bucket count"})} else {None}})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_io_diagnostics_are_explicit_and_ordinary_open_has_no_handle() {
        assert!(!parse_io_flag(None).unwrap());
        assert!(!parse_io_flag(Some("0")).unwrap());
        assert!(parse_io_flag(Some("1")).unwrap());
        assert!(parse_io_flag(Some("true")).is_err());
        let dir = tempfile::tempdir().unwrap();
        let normal = Db::build(
            "paged",
            &dir.path().join("normal"),
            100,
            ReferenceOrder::Ordered,
            Suite::Aggregates,
        )
        .unwrap();
        assert!(normal.io_counts().is_none());
        let measured = Db::build_with_io(
            "paged",
            &dir.path().join("measured"),
            100,
            ReferenceOrder::Ordered,
            Suite::Aggregates,
            true,
        )
        .unwrap();
        assert!(measured.io_counts().unwrap().write_calls > 0);
        let diagnostics = paged_diagnostics(
            &measured,
            "native",
            100,
            ReferenceOrder::Ordered,
            Suite::Aggregates,
        )
        .unwrap()
        .unwrap();
        let cases = diagnostics["read_counts"].as_array().unwrap();
        assert_eq!(cases.len(), 11);
        assert!(
            cases
                .iter()
                .all(|case| case["backend_io_counts"]["read_calls"].is_u64())
        );
        assert!(
            Db::build_with_io(
                "resident",
                &dir.path().join("invalid"),
                100,
                ReferenceOrder::Ordered,
                Suite::Reads,
                true
            )
            .is_err()
        );
    }

    #[test]
    fn native_matrix_uses_public_apis_and_matches_every_fixture_query() {
        for storage in ["resident", "paged"] {
            for order in [ReferenceOrder::Ordered, ReferenceOrder::Reversed] {
                for suite in [Suite::Reads, Suite::Aggregates, Suite::AggregatesWide] {
                    let dir = tempfile::tempdir().unwrap();
                    let rows = 257;
                    let db =
                        Db::build(storage, &dir.path().join("db"), rows, order, suite).unwrap();
                    let snapshot = native::NativeSnapshot::new(&db).unwrap();
                    for (case, _, _) in cases(rows, suite) {
                        for iteration in [0, 1, 1999] {
                            assert_eq!(
                                snapshot.read(case, iteration, rows, suite).unwrap(),
                                expected(case, iteration, rows, order, suite),
                                "{storage} {case}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn native_pinned_reader_rejects_reads_after_revocation() {
        for storage in ["resident", "paged"] {
            let dir = tempfile::tempdir().unwrap();
            let db = Db::build(
                storage,
                &dir.path().join("db"),
                100,
                ReferenceOrder::Ordered,
                Suite::Aggregates,
            )
            .unwrap();
            let snapshot = native::NativeSnapshot::new(&db).unwrap();
            assert!(
                snapshot
                    .read("aggregate", 0, 100, Suite::Aggregates)
                    .is_ok()
            );
            match &db {
                Db::Resident(db) => {
                    let mut tx = db.admin().transaction();
                    tx.revoke("reader", "app.Score").unwrap();
                    tx.commit().unwrap();
                }
                Db::Paged(db, _) => {
                    let mut tx = db.admin().transaction().unwrap();
                    tx.revoke("reader", "app.Score").unwrap();
                    tx.commit().unwrap();
                }
                Db::Sqlite(_) => unreachable!(),
            }
            assert!(
                snapshot
                    .read("aggregate", 0, 100, Suite::Aggregates)
                    .is_err()
            );
            assert!(
                snapshot
                    .read("selective_join", 0, 100, Suite::Aggregates)
                    .is_err()
            );
        }
    }

    #[test]
    fn benchmark_mode_metadata_never_substitutes_sql_for_native() {
        assert_eq!(mode_metadata("native-paged").unwrap(), ("native", "paged"));
        assert_eq!(
            mode_metadata("native-resident").unwrap(),
            ("native", "resident")
        );
        assert_eq!(mode_metadata("paged").unwrap(), ("sql", "paged"));
        assert!(mode_metadata("native-sqlite").is_err());
    }

    #[test]
    fn file_footprint_counts_only_owned_files_and_reports_missing_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        std::fs::write(&path, b"abc").unwrap();
        std::fs::write(dir.path().join("db-wal"), b"12345").unwrap();
        std::fs::write(dir.path().join("db-shm"), b"1234567").unwrap();
        std::fs::write(dir.path().join("unrelated"), vec![0; 100_000]).unwrap();
        let footprint = file_footprint("sqlite", &path).unwrap();
        assert_eq!(footprint["total_logical_bytes"], 15);
        assert_eq!(footprint["main_database"]["logical_bytes"], 3);
        assert_eq!(footprint["sidecars"][0]["logical_bytes"], 5);
        assert_eq!(footprint["sidecars"][1]["logical_bytes"], 7);
        assert_eq!(footprint["sidecars"][2]["exists"], false);
        assert_eq!(footprint["sidecars"][2]["logical_bytes"], 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let allocated = ["db", "db-wal", "db-shm"]
                .iter()
                .map(|name| std::fs::metadata(dir.path().join(name)).unwrap().blocks() * 512)
                .sum::<u64>();
            assert_eq!(footprint["total_allocated_bytes"], allocated);
        }
        for mode in ["resident", "paged"] {
            let footprint = file_footprint(mode, &path).unwrap();
            assert_eq!(footprint["total_logical_bytes"], 3);
            assert_eq!(footprint["sidecars"], json!([]));
        }
        assert!(file_footprint("postgres", &path).is_err());
        assert!(file_footprint("sqlite", &dir.path().join("missing")).is_err());
        assert!(file_footprint("sqlite", dir.path()).is_err());
    }

    #[test]
    fn sqlite_plan_telemetry_uses_bound_fixture_queries() {
        let dir = tempfile::tempdir().unwrap();
        let Db::Sqlite(connection) = Db::build(
            "sqlite",
            &dir.path().join("db"),
            100,
            ReferenceOrder::Ordered,
            Suite::Aggregates,
        )
        .unwrap() else {
            unreachable!()
        };
        let plans = sqlite_query_plans(&connection, 100, Suite::Aggregates).unwrap();
        assert_eq!(plans.len(), 4);
        for plan in plans {
            assert!(!plan["plan"].as_array().unwrap().is_empty());
            assert!(
                plan["plan"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|entry| entry["detail"].is_string())
            );
        }
        assert!(
            sqlite_query_plans(&connection, 100, Suite::Reads)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn fixture_schema_identities_and_fields_are_deterministic() {
        for suite in [Suite::Reads, Suite::Aggregates, Suite::AggregatesWide] {
            let first = modules(suite);
            let second = modules(suite);
            assert_eq!(first, second);
            assert_eq!(first[0].relations[0].id, PERSON_TYPE);
            assert_eq!(first[1].relations[0].id, SCORE_TYPE);
            assert!(PERSON_TYPE < SCORE_TYPE);
            assert!(id(2_000_000) < PERSON_TYPE);
            assert_eq!(first[0].relations[0].roles.len(), 2);
            assert_eq!(
                first[1].relations[0].roles.len(),
                if suite == Suite::Reads { 2 } else { 3 }
            );
        }
    }

    #[test]
    fn aggregate_reference_matches_naive_grouping_and_default_fixture_stays_small() {
        assert_eq!(cases(100, Suite::Reads).len(), 6);
        assert_eq!(cases(100, Suite::Aggregates).len(), 11);
        assert_eq!(Suite::Reads.score_roles().len(), 2);
        assert_eq!(Suite::Aggregates.score_roles().len(), 3);
        for rows in [100, 257, 1000] {
            for (start, end) in [
                (0, rows),
                (rows / 2, rows),
                (rows / 2, rows / 2 + rows / 100),
                (0, 1),
            ] {
                let naive_count = (end - start) as i64;
                let naive_sum = (start..end).map(|n| n as i64 * 10).sum::<i64>();
                assert_eq!(
                    aggregate_expected(start, end, false, 32),
                    vec![vec![S::Integer(naive_count), S::Integer(naive_sum)]]
                );
                let expected = (0..32)
                    .filter_map(|bucket| {
                        let rows = (start..end)
                            .filter(|n| n % 32 == bucket)
                            .collect::<Vec<_>>();
                        (!rows.is_empty()).then(|| {
                            vec![
                                S::Integer(bucket as i64),
                                S::Integer(rows.len() as i64),
                                S::Integer(rows.iter().map(|&n| n as i64 * 10).sum()),
                            ]
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(aggregate_expected(start, end, true, 32), expected);
            }
        }
    }

    #[test]
    fn wide_groups_match_independent_row_grouping() {
        let suite = Suite::AggregatesWide;
        assert_eq!(suite.buckets(), 1024);
        assert_eq!(cases(10000, suite).len(), 11);
        for (start, end) in [(0, 100), (0, 4097), (2048, 4097)] {
            let mut groups = std::collections::BTreeMap::<i64, (i64, i64)>::new();
            for n in start..end {
                let values = suite.score_values(n, n);
                let Value::Int(bucket) = values[2] else {
                    panic!("bucket");
                };
                let group = groups.entry(bucket).or_default();
                group.0 += 1;
                group.1 += n as i64 * 10;
            }
            let expected = groups
                .into_iter()
                .map(|(key, (count, sum))| {
                    vec![S::Integer(key), S::Integer(count), S::Integer(sum)]
                })
                .collect::<Vec<_>>();
            assert_eq!(
                aggregate_expected(start, end, true, suite.buckets()),
                expected
            );
        }
    }

    #[test]
    fn robust_profile_extends_only_fixed_hot_cases_and_reports_exact_warmups() {
        assert_eq!(HotProfile::parse(None).unwrap(), HotProfile::Default);
        assert_eq!(
            HotProfile::parse(Some("robust")).unwrap(),
            HotProfile::Robust
        );
        assert!(HotProfile::parse(Some("adaptive")).is_err());
        for suite in [Suite::Reads, Suite::Aggregates, Suite::AggregatesWide] {
            let default = cases(500_000, suite);
            assert_eq!(timed_cases(500_000, suite, HotProfile::Default), default);
            let robust = timed_cases(500_000, suite, HotProfile::Robust);
            for ((case, sql, count), (actual, actual_sql, iterations)) in default.iter().zip(robust)
            {
                assert_eq!((*case, *sql), (actual, actual_sql));
                let hot = matches!(
                    *case,
                    "point" | "equality" | "selective_join" | "aggregate_equality"
                );
                assert_eq!(iterations, count * if hot { 100 } else { 1 });
                assert_eq!(HotProfile::Robust.warmups(case), if hot { 1000 } else { 1 });
                assert_eq!(HotProfile::Default.warmups(case), 1);
            }
        }
        let mut measurements = vec![
            json!({"case":"point"}),
            json!({"case":"changing_parameters"}),
            json!({"case":"aggregate_grouped"}),
        ];
        HotProfile::Robust.annotate(&mut measurements);
        assert_eq!(measurements[0]["warmup_iterations"], 1000);
        assert_eq!(measurements[1]["warmup_iterations"], 1);
        assert_eq!(measurements[2]["warmup_iterations"], 1);
    }

    #[test]
    fn reversed_reference_fixture_is_bijective_and_join_expectation_inverts_it() {
        for rows in [100, 257, 1000] {
            for order in [ReferenceOrder::Ordered, ReferenceOrder::Reversed] {
                for score in 0..rows {
                    let person = order.target_person(score, rows);
                    assert!(person < rows);
                    assert_eq!(order.target_person(person, rows), score);
                }
                let expected = expected("selective_join", 0, rows, order, Suite::Reads);
                let S::Integer(score) = expected[0][1] else {
                    panic!("expected score")
                };
                assert_eq!(order.target_person((score / 10) as usize, rows), rows / 2);
            }
        }
    }
}
