use rusqlite::types::Value as S;
use std::{process::Command, time::Duration};
use substrate_liquid::{profiling::*, *};

fn fixture() -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    for name in ["Ada", "Bob"] {
        tx.insert("core.Person", vec![name.into()]).unwrap();
    }
    tx.grant(
        "reader",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "Ada".into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db
}

#[test]
fn both_modes_preserve_filters_parameters_and_report_plan() {
    let db = fixture();
    for mode in [QueryMode::Prepared, QueryMode::Request] {
        let report = benchmark_query(
            &db.session("reader"),
            "SELECT id FROM core.Person WHERE name=?1",
            &[S::Text("Ada".into())],
            BenchmarkOptions {
                mode,
                warmup: 1,
                iterations: 5,
            },
        )
        .unwrap();
        assert_eq!(report.samples_us.len(), 5);
        assert_eq!(
            (report.rows_per_iteration_min, report.rows_per_iteration_max),
            (1, 1)
        );
        assert!(
            report
                .plan
                .iter()
                .any(|p| p.detail.contains("LiquidScanExec"))
        );
        assert!(report.latencies.min_us <= report.latencies.p50_us);
        assert!(report.latencies.p99_us <= report.latencies.max_us);
        let hidden = benchmark_query(
            &db.session("reader"),
            "SELECT id FROM core.Person WHERE name=?1",
            &[S::Text("Bob".into())],
            BenchmarkOptions {
                mode,
                warmup: 0,
                iterations: 1,
            },
        )
        .unwrap();
        assert_eq!(hidden.rows_per_iteration_max, 0);
    }
}

#[test]
fn rejects_mutations_multiple_statements_denied_access_and_invalid_counts() {
    let db = fixture();
    for sql in ["DELETE FROM core.Person", "SELECT 1; SELECT 2"] {
        assert!(benchmark_query(&db.admin(), sql, &[], BenchmarkOptions::default()).is_err());
    }
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 2);
    assert!(
        benchmark_query(
            &db.session("stranger"),
            "SELECT * FROM core.Person",
            &[],
            BenchmarkOptions::default()
        )
        .is_err()
    );
    assert!(
        benchmark_query(
            &db.admin(),
            "SELECT 1",
            &[],
            BenchmarkOptions {
                iterations: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    for mode in [QueryMode::Prepared, QueryMode::Request] {
        assert!(
            sample_query(
                &db.admin(),
                "SELECT 1",
                &[],
                BenchmarkOptions {
                    mode,
                    warmup: 0,
                    ..Default::default()
                },
                Duration::from_millis(2)
            )
            .unwrap()
                > 0
        );
    }
}

#[test]
fn cli_supports_exact_parameters_and_never_overwrites_report() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("test.redb");
    drop(Database::open(&database).unwrap());
    let query = dir.path().join("query.sql");
    std::fs::write(&query, "SELECT ?1 WHERE ?1 = 9223372036854775807").unwrap();
    let params = dir.path().join("params.json");
    std::fs::write(&params, r#"[{"Integer":"9223372036854775807"}]"#).unwrap();
    let output = dir.path().join("report.json");
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_substrate-liquid"))
            .arg("bench-query")
            .arg(&database)
            .arg("admin")
            .arg(&query)
            .arg("--params")
            .arg(&params)
            .args(["--iterations", "3", "--warmup", "0", "--output"])
            .arg(&output)
            .output()
            .unwrap()
    };
    let result = run();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let original = std::fs::read(&output).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(report["benchmark"]["rows_per_iteration_min"], 1);
    assert_eq!(
        report["benchmark"]["samples_us"].as_array().unwrap().len(),
        3
    );
    assert!(!run().status.success());
    assert_eq!(std::fs::read(output).unwrap(), original);
}
