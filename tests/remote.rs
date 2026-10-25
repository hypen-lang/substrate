use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use substrate_liquid::{
    capability::*,
    remote::*,
    service::{Command, Output, Response},
    *,
};
#[path = "support/tls.rs"]
mod tls;
struct Host {
    db: Database,
    server: Arc<RemoteServer>,
    addr: std::net::SocketAddr,
    ca: Vec<u8>,
    credential: auth::IssuedCredential,
    root: Id,
    shutdown: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Host {
    fn new() -> Self {
        Self::with_options(RemoteOptions::default())
    }
    fn with_options(options: RemoteOptions) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (ca, cert, key) = tls::certificates(dir.path());
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.insert("core.Person", vec!["Bob".into()]).unwrap();
        tx.grant("owner", "core.Person", Grant::read_write())
            .unwrap();
        tx.commit().unwrap();
        let end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let root = db
            .issue_capability_until(
                Id::new(),
                "owner",
                CapabilityScope {
                    relations: vec![RelationCapability {
                        relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                        grant: Grant {
                            filter: RowFilter::Equals {
                                role: "name".into(),
                                value: "Ada".into(),
                            },
                            ..Grant::read_only()
                        },
                    }],
                    schema_namespace: None,
                    delegations: 1,
                },
                Some(end),
            )
            .unwrap()
            .id;
        let credential = db.issue_credential("owner", root, end).unwrap();
        let server = Arc::new(
            RemoteServer::bind_with_options(
                db.clone(),
                "127.0.0.1:0".parse().unwrap(),
                &cert,
                &key,
                options,
            )
            .unwrap(),
        );
        let addr = server.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let stop = shutdown.clone();
        let running = server.clone();
        let worker = std::thread::spawn(move || running.run(stop).unwrap());
        Self {
            db,
            server,
            addr,
            ca,
            credential,
            root,
            shutdown,
            worker: Some(worker),
        }
    }
    fn client(&self, token: &str) -> RemoteClient {
        RemoteClient::new(self.addr, "localhost", &self.ca, token).unwrap()
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}
fn query() -> Command {
    Command::Query {
        sql: "SELECT name FROM core.Person".into(),
        params: vec![],
    }
}
#[test]
fn tls_authentication_rotation_and_revocation_preserve_scope() {
    let h = Host::new();
    let c = h.client(&h.credential.token);
    match c.request(query()).unwrap() {
        Response::Ok(Output::Query { rows, .. }) => assert_eq!(rows.len(), 1),
        r => panic!("{r:?}"),
    }
    assert!(matches!(
        h.client("invalid").request(query()).unwrap(),
        Response::Error(_)
    ));
    let next =
        h.db.rotate_credential(h.credential.id, h.credential.expires_at)
            .unwrap();
    assert!(matches!(c.request(query()).unwrap(), Response::Error(_)));
    assert!(matches!(
        h.client(&next.token).request(query()).unwrap(),
        Response::Ok(_)
    ));
    h.db.revoke_capability(h.root).unwrap();
    assert!(matches!(
        h.client(&next.token).request(query()).unwrap(),
        Response::Error(_)
    ));
}
#[test]
fn tls_rejects_wrong_hostname_and_untrusted_authority() {
    let h = Host::new();
    let wrong = RemoteClient::new(h.addr, "wrong.example", &h.ca, &h.credential.token).unwrap();
    assert!(wrong.request(query()).is_err());
    let dir = tempfile::tempdir().unwrap();
    let (ca, _, _) = tls::certificates(dir.path());
    let untrusted = RemoteClient::new(h.addr, "localhost", &ca, &h.credential.token).unwrap();
    assert!(untrusted.request(query()).is_err());
}
#[test]
fn no_plaintext_fallback_or_scope_switch() {
    use std::io::{Read, Write};
    let h = Host::new();
    let mut socket = std::net::TcpStream::connect(h.addr).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    socket
        .write_all(b"{\"version\":1,\"token\":\"secret\"}\n")
        .unwrap();
    let mut b = [0u8; 512];
    if let Ok(n) = socket.read(&mut b) {
        assert!(!String::from_utf8_lossy(&b[..n]).contains("Ada"));
    }
    assert!(matches!(
        h.client(&h.credential.token)
            .request(Command::UsingCapability {
                capability: Id::new(),
                command: Box::new(query())
            })
            .unwrap(),
        Response::Error(_)
    ));
}
#[test]
fn successful_self_revocation_is_acknowledged_then_denied() {
    let h = Host::new();
    let client = h.client(&h.credential.token);
    assert!(matches!(
        client
            .request(Command::RevokeCapability { id: h.root })
            .unwrap(),
        Response::Ok(Output::CapabilityRevoked)
    ));
    assert!(matches!(
        client.request(query()).unwrap(),
        Response::Error(_)
    ));
}

#[test]
fn invalid_credentials_trigger_cooldown_but_scoped_denials_do_not() {
    use std::time::Duration;
    let (audit, events) = std::sync::mpsc::sync_channel(64);
    let h = Host::with_options(RemoteOptions {
        limits: RemoteLimits {
            failure_limit: 1,
            block_for: Duration::from_millis(100),
            ..RemoteLimits::default()
        },
        audit: Some(audit),
    });
    let valid = h.client(&h.credential.token);
    assert!(matches!(
        valid
            .request(Command::UsingCapability {
                capability: Id::new(),
                command: Box::new(query())
            })
            .unwrap(),
        Response::Error(_)
    ));
    assert_eq!(h.server.admission_stats().authentication_failures, 0);
    assert!(matches!(valid.request(query()).unwrap(), Response::Ok(_)));
    assert!(matches!(
        h.client("wrong-secret").request(query()).unwrap(),
        Response::Error(_)
    ));
    assert_eq!(h.server.admission_stats().authentication_failures, 1);
    assert!(valid.request(query()).is_err());
    std::thread::sleep(Duration::from_millis(110));
    assert!(matches!(valid.request(query()).unwrap(), Response::Ok(_)));
    let records: Vec<_> = events.try_iter().collect();
    assert!(
        records
            .iter()
            .any(|e| e.kind == AuditKind::AuthenticationFailed)
    );
    assert!(
        records
            .iter()
            .any(|e| e.kind == AuditKind::Rejected(AdmissionRejection::PeerBlocked))
    );
    let audit = format!("{records:?}");
    assert!(!audit.contains("wrong-secret"));
    assert!(!audit.contains(&h.credential.token));
    assert!(!audit.contains("SELECT"));
    assert!(!audit.contains("Ada"));
}
#[test]
fn a_slow_handshake_cannot_exceed_peer_concurrency_or_io_deadline() {
    use std::time::{Duration, Instant};
    let h = Host::with_options(RemoteOptions {
        limits: RemoteLimits {
            max_connections_per_peer: 1,
            io_timeout: Duration::from_millis(80),
            ..RemoteLimits::default()
        },
        audit: None,
    });
    let raw = std::net::TcpStream::connect(h.addr).unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while h.server.admission_stats().active_connections == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(h.server.admission_stats().active_connections, 1);
    assert!(h.client(&h.credential.token).request(query()).is_err());
    while h.server.admission_stats().active_connections != 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(h.server.admission_stats().active_connections, 0);
    assert!(h.server.admission_stats().rejected >= 1);
    drop(raw);
    assert!(matches!(
        h.client(&h.credential.token).request(query()).unwrap(),
        Response::Ok(_)
    ));
}
#[test]
fn persistent_tls_authenticates_every_frame_across_rotation_and_revocation() {
    let h = Host::new();
    let mut session = h.client(&h.credential.token).connect().unwrap();
    for _ in 0..3 {
        assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    }
    assert_eq!(h.server.admission_stats().accepted, 1);
    let new =
        h.db.rotate_credential(h.credential.id, h.credential.expires_at)
            .unwrap();
    assert!(matches!(
        session.request(query()).unwrap(),
        Response::Error(_)
    ));
    assert!(session.request(query()).is_err());
    let mut session = h.client(&new.token).connect().unwrap();
    assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    h.db.revoke_capability(h.root).unwrap();
    assert!(matches!(
        session.request(query()).unwrap(),
        Response::Error(_)
    ));
}
#[test]
fn persistent_tls_observes_expiry_without_reconnecting() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let h = Host::with_options(RemoteOptions {
        limits: RemoteLimits {
            idle_timeout: Duration::from_secs(3),
            ..RemoteLimits::default()
        },
        audit: None,
    });
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1;
    let short = h.db.issue_credential("owner", h.root, end).unwrap();
    let mut session = h.client(&short.token).connect().unwrap();
    assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    let wait = Duration::from_secs(end)
        .saturating_sub(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
        + Duration::from_millis(20);
    std::thread::sleep(wait);
    assert!(matches!(
        session.request(query()).unwrap(),
        Response::Error(_)
    ));
}
#[test]
fn persistent_tls_request_idle_and_shutdown_bounds() {
    use std::time::{Duration, Instant};
    let h = Host::with_options(RemoteOptions {
        limits: RemoteLimits {
            max_requests_per_connection: 2,
            idle_timeout: Duration::from_millis(100),
            ..RemoteLimits::default()
        },
        audit: None,
    });
    let mut session = h.client(&h.credential.token).connect().unwrap();
    assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    assert!(session.request(query()).is_err());
    let mut idle = h.client(&h.credential.token).connect().unwrap();
    assert!(matches!(idle.request(query()).unwrap(), Response::Ok(_)));
    std::thread::sleep(Duration::from_millis(130));
    assert!(idle.request(query()).is_err());
    drop(h);
    let h = Host::new();
    let mut idle = h.client(&h.credential.token).connect().unwrap();
    assert!(matches!(idle.request(query()).unwrap(), Response::Ok(_)));
    let start = Instant::now();
    drop(h);
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(idle.request(query()).is_err());
}

#[test]
fn persistent_tls_connections_cannot_bypass_peer_cooldown() {
    use std::time::Duration;
    let h = Host::with_options(RemoteOptions {
        limits: RemoteLimits {
            failure_limit: 1,
            block_for: Duration::from_secs(1),
            ..RemoteLimits::default()
        },
        audit: None,
    });
    let mut session = h.client(&h.credential.token).connect().unwrap();
    assert!(matches!(
        session
            .request(Command::UsingCapability {
                capability: Id::new(),
                command: Box::new(query())
            })
            .unwrap(),
        Response::Error(_)
    ));
    assert_eq!(h.server.admission_stats().authentication_failures, 0);
    assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    assert!(matches!(
        h.client("incorrect-token").request(query()).unwrap(),
        Response::Error(_)
    ));
    assert!(session.request(query()).is_err());
}

fn raw_tls(h: &Host) -> rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream> {
    use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(&h.ca) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let socket = std::net::TcpStream::connect(h.addr).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    socket
        .set_write_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    rustls::StreamOwned::new(
        rustls::ClientConnection::new(Arc::new(config), ServerName::try_from("localhost").unwrap())
            .unwrap(),
        socket,
    )
}
#[test]
fn persistent_tls_preserves_pipelined_frames_and_rejects_oversize_or_partial_frames() {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::time::{Duration, Instant};
    let h = Host::with_options(RemoteOptions {
        limits: RemoteLimits {
            io_timeout: Duration::from_millis(200),
            ..RemoteLimits::default()
        },
        audit: None,
    });
    let mut stream = raw_tls(&h);
    let mut frame = serde_json::to_vec(&service::Request {
        version: 1,
        token: h.credential.token.clone(),
        command: query(),
    })
    .unwrap();
    frame.push(b'\n');
    let mut pipelined = frame.clone();
    pipelined.extend_from_slice(&frame);
    pipelined.extend_from_slice(&frame);
    stream.write_all(&pipelined).unwrap();
    stream.flush().unwrap();
    let mut reader = BufReader::new(stream);
    for _ in 0..3 {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(matches!(
            serde_json::from_str::<Response>(&line).unwrap(),
            Response::Ok(_)
        ));
    }
    drop(reader);
    let mut stream = raw_tls(&h);
    let oversized = vec![b'x'; 1024 * 1024 + 1];
    let _ = stream.write_all(&oversized);
    let _ = stream.flush();
    let mut byte = [0];
    assert!(!matches!(stream.read(&mut byte),Ok(n)if n>0));
    let mut stream = raw_tls(&h);
    stream.write_all(b"{").unwrap();
    stream.flush().unwrap();
    let start = Instant::now();
    assert!(!matches!(stream.read(&mut byte),Ok(n)if n>0));
    assert!(start.elapsed() < Duration::from_secs(1));
}
