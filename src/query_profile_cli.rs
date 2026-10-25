use rusqlite::types::Value;
use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};
use substrate_liquid::{
    profiling::{BenchmarkOptions, QueryMode, benchmark_query, sample_query},
    *,
};

const HELP: &str = "Usage: substrate-liquid bench-query DATABASE PRINCIPAL QUERY.sql [OPTIONS]

  --mode prepared|request  Reuse statement (default), or include request setup
  --params FILE            JSON array of SqlWireValue parameters
  --iterations N           Measured executions, default 200
  --warmup N               Additional warmups, default 10
  --output FILE            Write JSON report (must not already exist)
  --sample-seconds N       Repeat for a sampler instead of reporting benchmarks

Local trusted-host tooling: admin selects administrative access. Use an offline
database or consistent backup. Queries are read-only; database open can perform
format maintenance. Native results are fully materialized. No transport is timed.
For flamegraphs: node tools/profile-query.mjs --help";

type CliResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn parameters(path: Option<&str>) -> CliResult<Vec<Value>> {
    let Some(path) = path else {
        return Ok(vec![]);
    };
    let values: Vec<service::SqlWireValue> = serde_json::from_slice(&std::fs::read(path)?)?;
    values
        .into_iter()
        .map(|value| {
            Ok(match value {
                service::SqlWireValue::Null => Value::Null,
                service::SqlWireValue::Integer(n) => Value::Integer(n.parse()?),
                service::SqlWireValue::Real(n) if n.is_finite() => Value::Real(n),
                service::SqlWireValue::Text(s) => Value::Text(s),
                service::SqlWireValue::Blob(bytes) => Value::Blob(bytes),
                _ => return Err("nonfinite SQL parameter".into()),
            })
        })
        .collect()
}

pub fn run(args: &[String]) -> CliResult<()> {
    if args == ["--help"] || args == ["-h"] {
        println!("{HELP}");
        return Ok(());
    }
    if args.len() < 3 {
        return Err(HELP.into());
    }
    let mut options = BenchmarkOptions::default();
    let (mut params, mut output, mut sample_seconds) = (None, None, None);
    let mut seen = BTreeSet::new();
    let mut flags = args[3..].iter();
    while let Some(flag) = flags.next() {
        if !seen.insert(flag) {
            return Err(format!("duplicate option {flag}").into());
        }
        let value = flags
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--mode" => {
                options.mode = match value.as_str() {
                    "prepared" => QueryMode::Prepared,
                    "request" => QueryMode::Request,
                    _ => return Err("mode must be prepared or request".into()),
                }
            }
            "--params" => params = Some(value.as_str()),
            "--output" => output = Some(value),
            "--iterations" => options.iterations = value.parse()?,
            "--warmup" => options.warmup = value.parse()?,
            "--sample-seconds" => {
                let seconds: f64 = value.parse()?;
                if !seconds.is_finite() || seconds <= 0.0 || seconds > 3600.0 {
                    return Err("sample seconds must be >0 and <=3600".into());
                }
                sample_seconds = Some(Duration::from_secs_f64(seconds));
            }
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    if sample_seconds.is_some() && output.is_some() {
        return Err("--output is for benchmark reports; omit it with --sample-seconds".into());
    }
    if !Path::new(&args[0]).is_file() {
        return Err("database does not exist".into());
    }
    let sql = std::fs::read_to_string(&args[2])?;
    let params = parameters(params)?;
    let start = Instant::now();
    let db = Database::open_with_options(
        &args[0],
        DatabaseOptions {
            checkpoint_policy: None,
        },
    )?;
    let database_open_ms = start.elapsed().as_secs_f64() * 1000.0;
    let session = if args[1] == "admin" {
        db.admin()
    } else {
        db.session(&args[1])
    };
    if let Some(duration) = sample_seconds {
        let count = sample_query(&session, &sql, &params, options, duration)?;
        eprintln!(
            "Sampler workload completed: {count} executions ({:?}); do not use sampled runs as latency benchmarks.",
            options.mode
        );
    } else {
        let benchmark = benchmark_query(&session, &sql, &params, options)?;
        let report = serde_json::json!({
            "environment": { "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
                "sql_engine": "DataFusion", "datafusion": datafusion::DATAFUSION_VERSION, "sqlite_compatibility": rusqlite::version(), "debug_assertions": cfg!(debug_assertions),
                "package_version": env!("CARGO_PKG_VERSION") },
            "database_open_ms": database_open_ms,
            "benchmark": benchmark,
        });
        let mut bytes = serde_json::to_vec_pretty(&report)?;
        bytes.push(b'\n');
        if let Some(path) = output {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?
                .write_all(&bytes)?;
        } else {
            std::io::stdout().lock().write_all(&bytes)?;
        }
    }
    Ok(())
}
