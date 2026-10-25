#[test]
fn acknowledged_batches_survive_exit_without_destructors() {
    let mut backends = vec!["redb", "sqlite_wal_full"];
    if cfg!(feature = "rocks") {
        backends.push("rocks_sync_full");
    }
    for kind in backends {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database");
        let binary = env!("CARGO_BIN_EXE_substrate-storage-experiment");
        let crashed = std::process::Command::new(binary)
            .args(["crash-write", kind])
            .arg(&path)
            .arg("32")
            .status()
            .unwrap();
        assert_eq!(crashed.code(), Some(73));
        assert!(
            std::process::Command::new(binary)
                .args(["verify", kind])
                .arg(&path)
                .arg("32")
                .status()
                .unwrap()
                .success()
        );
    }
}
