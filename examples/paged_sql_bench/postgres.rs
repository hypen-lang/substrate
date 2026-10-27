//! Benchmark-only libpq adapter. No database-engine dependency or server process
//! startup is included in query timing. The runner owns a disposable Unix cluster.
use super::{AnyResult, CACHE, ReferenceOrder, S, Suite, binding, cases, expected, id, name};
use super::{
    FIXTURE_VERSION, HotProfile, client_cpu_seconds, modules, query_measurement, timed_cases,
};
use serde_json::json;
use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    hint::black_box,
    ptr::NonNull,
    time::Instant,
};

type Ptr = *mut c_void;
struct Library(NonNull<c_void>);
impl Library {
    fn open() -> AnyResult<Self> {
        let path = CString::new(std::env::var("SUBSTRATE_PAGED_LIBPQ")?)?;
        // SAFETY: the runner supplies the installed libpq path; the NUL-terminated
        // path lives through dlopen. The library stays loaded while symbols live.
        let ptr = unsafe { libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        Ok(Self(NonNull::new(ptr).ok_or("could not load libpq")?))
    }
    fn symbol<T: Copy>(&self, name: &CStr) -> AnyResult<T> {
        // SAFETY: names/signatures below are the public libpq C ABI. This helper
        // is private and only instantiated with function pointers of pointer size.
        let pointer = unsafe { libc::dlsym(self.0.as_ptr(), name.as_ptr()) };
        if pointer.is_null() || std::mem::size_of::<T>() != std::mem::size_of::<Ptr>() {
            return Err(format!("missing libpq symbol: {}", name.to_string_lossy()).into());
        }
        Ok(unsafe { std::mem::transmute_copy(&pointer) })
    }
}
impl Drop for Library {
    fn drop(&mut self) {
        // SAFETY: this handle is owned and all connection/result objects have
        // been cleared before the Api (and then library) is dropped.
        unsafe { libc::dlclose(self.0.as_ptr()) };
    }
}
struct Api {
    connect: unsafe extern "C" fn(*const c_char) -> Ptr,
    status: unsafe extern "C" fn(Ptr) -> c_int,
    error: unsafe extern "C" fn(Ptr) -> *const c_char,
    finish: unsafe extern "C" fn(Ptr),
    exec: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    prepare: unsafe extern "C" fn(Ptr, *const c_char, *const c_char, c_int, *const u32) -> Ptr,
    prepared: unsafe extern "C" fn(
        Ptr,
        *const c_char,
        c_int,
        *const *const c_char,
        *const c_int,
        *const c_int,
        c_int,
    ) -> Ptr,
    result_status: unsafe extern "C" fn(Ptr) -> c_int,
    result_error: unsafe extern "C" fn(Ptr) -> *const c_char,
    clear: unsafe extern "C" fn(Ptr),
    tuples: unsafe extern "C" fn(Ptr) -> c_int,
    fields: unsafe extern "C" fn(Ptr) -> c_int,
    field_type: unsafe extern "C" fn(Ptr, c_int) -> u32,
    value: unsafe extern "C" fn(Ptr, c_int, c_int) -> *const c_char,
    length: unsafe extern "C" fn(Ptr, c_int, c_int) -> c_int,
    is_null: unsafe extern "C" fn(Ptr, c_int, c_int) -> c_int,
    _library: Library,
}
impl Api {
    fn load() -> AnyResult<Self> {
        let library = Library::open()?;
        Ok(Self {
            connect: library.symbol(c"PQconnectdb")?,
            status: library.symbol(c"PQstatus")?,
            error: library.symbol(c"PQerrorMessage")?,
            finish: library.symbol(c"PQfinish")?,
            exec: library.symbol(c"PQexec")?,
            prepare: library.symbol(c"PQprepare")?,
            prepared: library.symbol(c"PQexecPrepared")?,
            result_status: library.symbol(c"PQresultStatus")?,
            result_error: library.symbol(c"PQresultErrorMessage")?,
            clear: library.symbol(c"PQclear")?,
            tuples: library.symbol(c"PQntuples")?,
            fields: library.symbol(c"PQnfields")?,
            field_type: library.symbol(c"PQftype")?,
            value: library.symbol(c"PQgetvalue")?,
            length: library.symbol(c"PQgetlength")?,
            is_null: library.symbol(c"PQgetisnull")?,
            _library: library,
        })
    }
}
struct Pg {
    api: Api,
    conn: NonNull<c_void>,
}
struct ResultGuard<'a> {
    api: &'a Api,
    ptr: NonNull<c_void>,
}
impl Drop for ResultGuard<'_> {
    fn drop(&mut self) {
        unsafe { (self.api.clear)(self.ptr.as_ptr()) };
    }
}
impl Drop for Pg {
    fn drop(&mut self) {
        unsafe { (self.api.finish)(self.conn.as_ptr()) };
    }
}
fn message(ptr: *const c_char) -> String {
    if ptr.is_null() {
        "libpq failed without a message".into()
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}
impl Pg {
    fn connect() -> AnyResult<Self> {
        let api = Api::load()?;
        let dsn = CString::new(std::env::var("SUBSTRATE_PAGED_PG_DSN")?)?;
        let conn = NonNull::new(unsafe { (api.connect)(dsn.as_ptr()) })
            .ok_or("libpq allocation failed")?;
        let pg = Self { api, conn };
        if unsafe { (pg.api.status)(pg.conn.as_ptr()) } != 0 {
            return Err(message(unsafe { (pg.api.error)(pg.conn.as_ptr()) }).into());
        }
        Ok(pg)
    }
    fn result(&self, ptr: Ptr) -> AnyResult<Vec<Vec<S>>> {
        let ptr = NonNull::new(ptr).ok_or("libpq result allocation failed")?;
        let result = ResultGuard {
            api: &self.api,
            ptr,
        };
        let p = result.ptr.as_ptr();
        if !matches!(unsafe { (self.api.result_status)(p) }, 1 | 2) {
            return Err(message(unsafe { (self.api.result_error)(p) }).into());
        }
        let rows = unsafe { (self.api.tuples)(p) };
        let fields = unsafe { (self.api.fields)(p) };
        if rows < 0 || fields < 0 {
            return Err("invalid libpq result shape".into());
        }
        let mut values = Vec::new();
        for row in 0..rows {
            let mut cells = Vec::new();
            for field in 0..fields {
                if unsafe { (self.api.is_null)(p, row, field) } != 0 {
                    cells.push(S::Null);
                    continue;
                }
                let value = unsafe { (self.api.value)(p, row, field) };
                let length = usize::try_from(unsafe { (self.api.length)(p, row, field) })?;
                if value.is_null() {
                    return Err("missing libpq result cell".into());
                }
                // SAFETY: libpq owns this length-delimited cell until ResultGuard
                // clears it; all query results explicitly request text format.
                let value = std::str::from_utf8(unsafe {
                    std::slice::from_raw_parts(value.cast::<u8>(), length)
                })?;
                cells.push(match unsafe { (self.api.field_type)(p, field) } {
                    20 | 21 | 23 | 1700 => S::Integer(value.parse()?),
                    _ => S::Text(value.to_owned()),
                });
            }
            values.push(cells);
        }
        Ok(values)
    }
    fn exec(&self, sql: &str) -> AnyResult<Vec<Vec<S>>> {
        let sql = CString::new(sql)?;
        self.result(unsafe { (self.api.exec)(self.conn.as_ptr(), sql.as_ptr()) })
    }
    fn prepare(&self, name: &CStr, sql: &str) -> AnyResult<()> {
        let sql = CString::new(sql)?;
        self.result(unsafe {
            (self.api.prepare)(
                self.conn.as_ptr(),
                name.as_ptr(),
                sql.as_ptr(),
                0,
                std::ptr::null(),
            )
        })?;
        Ok(())
    }
    fn query(&self, name: &CStr, params: &[S]) -> AnyResult<Vec<Vec<S>>> {
        let encoded = params
            .iter()
            .map(|v| match v {
                S::Text(s) => Ok(Some(CString::new(s.as_str())?)),
                S::Integer(n) => Ok(Some(CString::new(n.to_string())?)),
                S::Null => Ok(None),
                _ => Err("unsupported benchmark parameter".into()),
            })
            .collect::<AnyResult<Vec<_>>>()?;
        let pointers = encoded
            .iter()
            .map(|s| s.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()))
            .collect::<Vec<_>>();
        self.result(unsafe {
            (self.api.prepared)(
                self.conn.as_ptr(),
                name.as_ptr(),
                params.len().try_into()?,
                pointers.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                0,
            )
        })
    }
}

/// Explain the already executed named statement so PostgreSQL uses the same
/// retained prepared-plan state. These benchmark cases bind integers only;
/// accepting no arbitrary SQL/text values keeps construction unambiguous.
fn explain_prepared_sql(number: usize, params: &[i64]) -> String {
    let arguments = if params.is_empty() {
        String::new()
    } else {
        format!(
            "({})",
            params
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    format!("EXPLAIN (FORMAT JSON) EXECUTE query{number}{arguments}")
}

fn explain_json(result: Vec<Vec<S>>) -> AnyResult<serde_json::Value> {
    let [row] = result.as_slice() else {
        return Err("unexpected EXPLAIN result rows".into());
    };
    let [S::Text(encoded)] = row.as_slice() else {
        return Err("unexpected EXPLAIN result cell".into());
    };
    let plan: serde_json::Value = serde_json::from_str(encoded)?;
    if !plan.as_array().is_some_and(|items| {
        items.len() == 1
            && items[0]
                .get("Plan")
                .is_some_and(serde_json::Value::is_object)
    }) {
        return Err("unexpected EXPLAIN JSON plan shape".into());
    }
    Ok(plan)
}

pub(super) fn run(
    rows: usize,
    order: ReferenceOrder,
    suite: Suite,
    profile: HotProfile,
) -> AnyResult<()> {
    let pg = Pg::connect()?;
    let start = Instant::now();
    pg.exec("SET statement_timeout='120s'; CREATE SCHEMA core; CREATE SCHEMA app; CREATE TABLE core.\"Person\"(id uuid PRIMARY KEY,name text NOT NULL,number bigint NOT NULL UNIQUE); CREATE INDEX person_name ON core.\"Person\"(name); CREATE TABLE app.\"Score\"(id uuid PRIMARY KEY,person uuid NOT NULL UNIQUE REFERENCES core.\"Person\"(id),score bigint NOT NULL); CREATE INDEX score_value ON app.\"Score\"(score);")?;
    if suite != Suite::Reads {
        pg.exec("ALTER TABLE app.\"Score\" ADD COLUMN bucket bigint NOT NULL DEFAULT 0; CREATE INDEX score_bucket ON app.\"Score\"(bucket)")?;
    }
    pg.prepare(
        c"insert_person",
        "INSERT INTO core.\"Person\" VALUES($1,$2,$3)",
    )?;
    pg.prepare(
        c"insert_score",
        if suite != Suite::Reads {
            "INSERT INTO app.\"Score\" VALUES($1,$2,$3,$4)"
        } else {
            "INSERT INTO app.\"Score\" VALUES($1,$2,$3)"
        },
    )?;
    for start in (0..rows).step_by(128) {
        pg.exec("BEGIN")?;
        for n in start..(start + 128).min(rows) {
            let person = order.target_person(n, rows);
            pg.query(
                c"insert_person",
                &[
                    S::Text(id(person).to_string()),
                    S::Text(name(person)),
                    S::Integer(person as i64),
                ],
            )?;
            let mut values = vec![
                S::Text(id(rows + n).to_string()),
                S::Text(id(person).to_string()),
                S::Integer(n as i64 * 10),
            ];
            if suite != Suite::Reads {
                values.push(S::Integer((n % suite.buckets()) as i64));
            }
            pg.query(c"insert_score", &values)?;
        }
        pg.exec("COMMIT")?;
    }
    let load = start.elapsed().as_secs_f64();
    // Planner statistics are a separate, reported setup cost (not hidden in read
    // timings). All read results use the same retained, read-only snapshot.
    let analyze = Instant::now();
    pg.exec("ANALYZE core.\"Person\"; ANALYZE app.\"Score\"")?;
    let analyze = analyze.elapsed().as_secs_f64();
    let start = Instant::now();
    pg.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")?;
    pg.exec("SELECT 1 FROM core.\"Person\" LIMIT 1")?;
    let setup = start.elapsed().as_secs_f64();
    let mut results = Vec::new();
    for (number, (case, text, iterations)) in
        timed_cases(rows, suite, profile).into_iter().enumerate()
    {
        let aggregate_expected = case
            .starts_with("aggregate_")
            .then(|| expected(case, 0, rows, order, suite));
        let start = Instant::now();
        let statement = CString::new(format!("query{number}"))?;
        let text = text
            .replace("core.Person", "core.\"Person\"")
            .replace("app.Score", "app.\"Score\"")
            .replace("?1", "$1")
            .replace("?2", "$2");
        pg.prepare(&statement, &text)?;
        let prepare = start.elapsed().as_secs_f64();
        for _ in 0..profile.warmups(case) {
            assert_eq!(
                pg.query(&statement, &binding(case, 0, rows))?,
                expected(case, 0, rows, order, suite)
            );
        }
        let cpu = client_cpu_seconds();
        let t = Instant::now();
        for n in 0..iterations {
            let result = pg.query(&statement, &binding(case, n, rows))?;
            if let Some(expected) = &aggregate_expected {
                assert_eq!(&result, expected);
            } else {
                assert_eq!(result, expected(case, n, rows, order, suite));
            }
            black_box(result);
        }
        results.push(query_measurement(
            case,
            iterations,
            prepare,
            t.elapsed().as_secs_f64(),
            cpu,
        ));
    }
    profile.annotate(&mut results);
    // Diagnostics run only after every timed case, in the same retained
    // transaction/session. EXPLAIN has no ANALYZE and does not execute the query.
    let mut query_plans = Vec::new();
    for (number, (case, _, _)) in cases(rows, suite).into_iter().enumerate() {
        if !matches!(
            case,
            "aggregate_broad"
                | "aggregate_range"
                | "aggregate_grouped"
                | "aggregate_filtered_grouped"
        ) {
            continue;
        }
        let parameters = binding(case, 0, rows)
            .into_iter()
            .map(|value| match value {
                S::Integer(value) => Ok(value),
                _ => Err("EXPLAIN benchmark cases require integer parameters".into()),
            })
            .collect::<AnyResult<Vec<_>>>()?;
        let statement = explain_prepared_sql(number, &parameters);
        let plan = explain_json(pg.exec(&statement)?)?;
        query_plans
            .push(json!({"case":case,"parameters":parameters,"explain_sql":statement,"plan":plan}));
    }
    let settings = pg.exec("SELECT name,setting,unit FROM pg_settings WHERE name IN ('server_version','shared_buffers','work_mem','fsync','synchronous_commit','full_page_writes','wal_sync_method','transaction_isolation','max_parallel_workers_per_gather','jit','plan_cache_mode') ORDER BY name")?;
    // Serialize settings as strings without depending on rusqlite's Value serde.
    let settings = settings
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    S::Text(s) => s,
                    S::Null => String::new(),
                    S::Integer(n) => n.to_string(),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let scalar = |sql| -> AnyResult<i64> {
        let result = pg.exec(sql)?;
        match result.as_slice() {
            [row] => match row.as_slice() {
                [S::Integer(n)] => Ok(*n),
                _ => Err("unexpected metric cell".into()),
            },
            _ => Err("unexpected metric shape".into()),
        }
    };
    let database_bytes = scalar("SELECT pg_database_size(current_database())")?;
    let relation_bytes = scalar(
        "SELECT sum(pg_total_relation_size(t)) FROM unnest(ARRAY['core.\"Person\"'::regclass,'app.\"Score\"'::regclass]) t",
    )?;
    let backend_pid = scalar("SELECT pg_backend_pid()")?;
    pg.exec("ROLLBACK")?;
    println!(
        "{}",
        json!({"mode":"postgres","interface":"sql","storage":"postgres","timing_class":"headline","fixture_version":FIXTURE_VERSION,"schema_manifest":modules(suite),"hot_profile":profile.name(),"suite":suite.name(),"reference_order":order.name(),"people":rows,"total_rows":rows*2,"relations":2,"namespaces":2,"load_seconds":load,"analyze_seconds":analyze,"sql_setup_us":setup*1e6,"page_cache_bytes":CACHE,"decoded_cache_bytes":0,"queries":results,"verified":true,"postgres_query_plans":query_plans,"postgres_query_plan_method":"untimed EXPLAIN (FORMAT JSON), without ANALYZE, of the retained prepared statement after all timed queries","postgres_settings":settings,"backend_pid":backend_pid,"database_bytes":database_bytes,"relation_bytes":relation_bytes,"access":"persistent libpq prepared statements over private Unix socket; includes local IPC/protocol"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_uses_named_statement_and_exact_integer_bindings_without_analyze() {
        assert_eq!(
            explain_prepared_sql(9, &[]),
            "EXPLAIN (FORMAT JSON) EXECUTE query9"
        );
        assert_eq!(
            explain_prepared_sql(7, &[0, 50000]),
            "EXPLAIN (FORMAT JSON) EXECUTE query7(0,50000)"
        );
        assert_eq!(
            explain_prepared_sql(8, &[i64::MIN]),
            format!("EXPLAIN (FORMAT JSON) EXECUTE query8({})", i64::MIN)
        );
    }

    #[test]
    fn explain_response_requires_a_single_json_plan() {
        let expected = json!([{"Plan":{"Node Type":"Seq Scan"}}]);
        assert_eq!(
            explain_json(vec![vec![S::Text(expected.to_string())]]).unwrap(),
            expected
        );
        for rows in [
            vec![],
            vec![vec![S::Null]],
            vec![vec![S::Text("invalid".into())]],
            vec![vec![S::Text("[]".into())]],
            vec![vec![S::Text("[{\"Plan\":null}]".into())]],
        ] {
            assert!(explain_json(rows).is_err());
        }
    }
}
