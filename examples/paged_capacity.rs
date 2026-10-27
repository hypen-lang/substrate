//! Fixed-work current-state capacity comparison. Each invocation owns a fresh file.
use serde_json::json;
use std::{hint::black_box, path::Path, time::Instant};
use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};

type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const FAMILY: &str = "capacity.Item";
const CACHE: usize = 8 * 1024 * 1024;
const DECODED: usize = 1024 * 1024;
fn id(n: usize) -> Id {
    Id::from_u128(n as u128 + 1)
}
fn module() -> Module {
    Module::new(
        "capacity",
        1,
        vec![],
        vec![Relation {
            id: Id::from_u128(u128::MAX - 1),
            name: FAMILY.into(),
            roles: vec![
                Role::new("number", DataType::Int),
                Role::new("payload", DataType::Bytes),
            ],
            unique: vec![vec!["number".into()]],
            checks: vec![],
        }],
    )
}
fn values(n: usize, payload: usize) -> Vec<Value> {
    vec![
        Value::Int(n as i64),
        Value::Bytes(vec![(n % 251) as u8; payload]),
    ]
}
fn paged_options() -> PagedOptions {
    PagedOptions {
        cache_bytes: CACHE,
        decoded_cache_bytes: DECODED,
        ..Default::default()
    }
}
enum Store {
    Paged(PagedDatabase),
    Resident(Database),
}
impl Store {
    fn open(mode: &str, path: &Path) -> Result<Self> {
        match mode {
            "paged" => Ok(Self::Paged(PagedDatabase::open(path, paged_options())?)),
            "resident" => Ok(Self::Resident(Database::open_with_storage_options(
                path,
                DatabaseOptions::default(),
                GroupCommitOptions::default(),
                StorageOptions { cache_bytes: CACHE },
            )?)),
            _ => Err(Error::Constraint("mode must be paged or resident".into())),
        }
    }
    fn load(&self, rows: usize, payload: usize) -> Result<()> {
        match self {
            Self::Paged(db) => {
                let mut tx = db.admin().transaction()?;
                tx.install(module())?;
                tx.grant("reader", FAMILY, Grant::read_only())?;
                tx.commit()?;
                for start in (0..rows).step_by(256) {
                    let mut tx = db.admin().transaction()?;
                    for n in start..(start + 256).min(rows) {
                        tx.insert_with_id(FAMILY, id(n), values(n, payload))?;
                    }
                    tx.commit()?;
                }
            }
            Self::Resident(db) => {
                let mut tx = db.admin().transaction();
                tx.install(module())?;
                tx.grant("reader", FAMILY, Grant::read_only())?;
                tx.commit()?;
                for start in (0..rows).step_by(256) {
                    let mut tx = db.admin().transaction();
                    for n in start..(start + 256).min(rows) {
                        tx.insert_with_id(FAMILY, id(n), values(n, payload))?;
                    }
                    tx.commit()?;
                }
            }
        }
        Ok(())
    }
    fn read(&self, rows: usize, payload: usize) -> Result<serde_json::Value> {
        let iterations = 20_000;
        let hot = rows.min(256);
        let (point, lookup, scan) = match self {
            Self::Paged(db) => {
                let s = db.session("reader")?.snapshot()?;
                for n in 0..hot {
                    assert_eq!(
                        s.get(FAMILY, id(n))?.unwrap().values.as_ref(),
                        values(n, payload)
                    );
                }
                let t = Instant::now();
                for n in 0..iterations {
                    black_box(s.get(FAMILY, id(n % hot))?.ok_or(Error::NotFound)?);
                }
                let point = t.elapsed().as_secs_f64();
                let t = Instant::now();
                for n in 0..1000 {
                    let page =
                        s.lookup_page(FAMILY, "number", &Value::Int((n % rows) as i64), None, 2)?;
                    assert_eq!(page.rows.len(), 1);
                    assert_eq!(page.rows[0].id, id(n % rows));
                }
                let lookup = t.elapsed().as_secs_f64();
                let t = Instant::now();
                let mut after = None;
                let mut count = 0;
                loop {
                    let page = s.scan_page(FAMILY, after, 256)?;
                    for row in page.rows {
                        assert_eq!(row.id, id(count));
                        assert_eq!(row.values.as_ref(), values(count, payload));
                        count += 1;
                    }
                    after = page.next;
                    if after.is_none() {
                        break;
                    }
                }
                assert_eq!(count, rows);
                (point, lookup, t.elapsed().as_secs_f64())
            }
            Self::Resident(db) => {
                let s = db.session("reader").snapshot();
                for n in 0..hot {
                    assert_eq!(
                        s.get(FAMILY, id(n))?.unwrap().values.as_ref(),
                        values(n, payload)
                    );
                }
                let t = Instant::now();
                for n in 0..iterations {
                    black_box(s.get(FAMILY, id(n % hot))?.ok_or(Error::NotFound)?);
                }
                let point = t.elapsed().as_secs_f64();
                let t = Instant::now();
                for n in 0..1000 {
                    let page = s.page(
                        FAMILY,
                        Some(("number", &Value::Int((n % rows) as i64))),
                        None,
                        2,
                    )?;
                    assert_eq!(page.rows.len(), 1);
                    assert_eq!(page.rows[0].id, id(n % rows));
                }
                let lookup = t.elapsed().as_secs_f64();
                let t = Instant::now();
                let mut after = None;
                let mut count = 0;
                loop {
                    let page = s.page(FAMILY, None, after, 256)?;
                    for row in page.rows {
                        assert_eq!(row.id, id(count));
                        assert_eq!(row.values.as_ref(), values(count, payload));
                        count += 1;
                    }
                    after = page.next;
                    if after.is_none() {
                        break;
                    }
                }
                assert_eq!(count, rows);
                (point, lookup, t.elapsed().as_secs_f64())
            }
        };
        let (cached, entries) = match self {
            Self::Paged(db) => {
                let s = db.stats()?;
                assert!(s.decoded_cache_bytes <= DECODED);
                (s.decoded_cache_bytes, s.decoded_cache_entries)
            }
            _ => (0, 0),
        };
        Ok(
            json!({"hot_point_us":point*1e6/iterations as f64,"equality_page_us":lookup*1e6/1000.,"verified_scan_seconds":scan,"verified_scan_rows_per_second":rows as f64/scan,"decoded_cache_bytes":cached,"decoded_cache_entries":entries}),
        )
    }
}
fn main() -> AnyResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: paged_capacity paged|resident ROWS PAYLOAD_BYTES".into());
    }
    let rows: usize = args[1].parse()?;
    let payload: usize = args[2].parse()?;
    if !(100..=10_000_000).contains(&rows) || !(8..=4096).contains(&payload) {
        return Err("invalid fixture bounds".into());
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("capacity.redb");
    let t = Instant::now();
    let db = Store::open(&args[0], &path)?;
    db.load(rows, payload)?;
    let load = t.elapsed().as_secs_f64();
    drop(db);
    let t = Instant::now();
    let db = Store::open(&args[0], &path)?;
    let reopen = t.elapsed().as_secs_f64();
    let reads = db.read(rows, payload)?;
    println!(
        "{}",
        json!({"mode":args[0],"rows":rows,"payload_bytes":payload,"cache_target_bytes":CACHE,"decoded_cache_budget_bytes":if args[0]=="paged"{DECODED}else{0},"load_seconds":load,"load_rows_per_second":rows as f64/load,"reopen_seconds":reopen,"file_bytes":std::fs::metadata(&path)?.len(),"bytes_per_row":std::fs::metadata(&path)?.len() as f64/rows as f64,"reads":reads,"verified":true})
    );
    Ok(())
}
