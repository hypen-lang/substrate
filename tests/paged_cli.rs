use std::process::Command;
fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_substrate-liquid"))
        .args(args)
        .output()
        .unwrap()
}
#[test]
fn cli_installs_queries_inspects_and_exports_the_paged_backend() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paged.redb");
    let path = path.to_str().unwrap();
    let result = run(&[
        "manifest-install",
        "--paged",
        path,
        "examples/dx/manifest.json",
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let result = run(&[
        "query",
        "--paged",
        path,
        "admin",
        "SELECT COUNT(*) AS n FROM core.Person",
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("Integer(0)"));
    let result = run(&["inspect", "--paged", path]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let export = dir.path().join("export.json");
    let result = run(&["manifest-export", "--paged", path, export.to_str().unwrap()]);
    assert!(result.status.success());
    substrate_liquid::SchemaManifest::from_json(&std::fs::read_to_string(export).unwrap()).unwrap();
    assert!(!run(&["query", path, "admin", "SELECT 1"]).status.success());
    assert!(!run(&["checkpoint", "--paged", path]).status.success());
    assert!(
        !run(&["inspect", "--paged", "--paged", path])
            .status
            .success()
    );
}
#[test]
fn paged_cli_query_does_not_create_missing_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.redb");
    assert!(
        !run(&[
            "query",
            "--paged",
            path.to_str().unwrap(),
            "admin",
            "SELECT 1"
        ])
        .status
        .success()
    );
    assert!(!path.exists());
}
