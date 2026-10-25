//! Disposable development PKI. Requires openssl; never use these keys in production.
use std::{path::Path, process::Command};
pub fn certificates(dir: &Path) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    fn run(dir: &Path, args: &[&str]) {
        let output = Command::new("openssl")
            .current_dir(dir)
            .args(args)
            .output()
            .expect("openssl is required for TLS tests/demo");
        assert!(
            output.status.success(),
            "openssl: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    run(
        dir,
        &[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-subj",
            "/CN=Substrate Test CA",
            "-days",
            "1",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
    );
    run(
        dir,
        &[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
        ],
    );
    std::fs::write(dir.join("server.ext"),"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n").unwrap();
    run(
        dir,
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            "1",
            "-extfile",
            "server.ext",
        ],
    );
    (
        std::fs::read(dir.join("ca.pem")).unwrap(),
        std::fs::read(dir.join("server.pem")).unwrap(),
        std::fs::read(dir.join("server.key")).unwrap(),
    )
}
