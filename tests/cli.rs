use std::process::{Command, Output};
fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_substrate-liquid"))
        .args(args)
        .output()
        .unwrap()
}
#[test]
fn durable_demo_query_and_inspect_work_from_cli() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("demo.redb");
    let path = path.to_str().unwrap();
    let output = cli(&["demo", path]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Equivalent SQLite query agrees"));
    let output = cli(&["query", path, "fitness", "SELECT name FROM core.Person"]);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Alice"));
    assert!(
        !cli(&["query", path, "fitness", "SELECT * FROM crm.Deal"])
            .status
            .success()
    );
    assert!(cli(&["checkpoint", path]).status.success());
    let inspect = cli(&["inspect", path]);
    assert!(inspect.status.success());
    assert!(String::from_utf8_lossy(&inspect.stdout).contains("replayed_commits: 0"));
    let result = cli(&["query", path, "fitness", "SELECT name FROM core.Person"]);
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("Alice"));
}
#[test]
fn schema_check_diff_and_install_are_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("schema.redb");
    let path = path.to_str().unwrap();
    assert!(
        cli(&["install", path, "examples/schemas/core.json"])
            .status
            .success()
    );
    for command in ["schema-check", "schema-diff"] {
        assert!(
            cli(&[command, path, "examples/schemas/fitness.json"])
                .status
                .success()
        );
    }
    assert!(
        !cli(&["query", path, "admin", "SELECT * FROM fitness.Workout"])
            .status
            .success()
    );
    assert!(
        cli(&["install", path, "examples/schemas/fitness.json"])
            .status
            .success()
    );
    assert!(
        cli(&[
            "query",
            path,
            "admin",
            "SELECT count(*) FROM fitness.Workout"
        ])
        .status
        .success()
    );
}
