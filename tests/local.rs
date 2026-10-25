#![cfg(unix)]
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use substrate_liquid::{local::*, service::*, *};
const TOKEN: &str = "local-fixture-token-0000000000000000";
struct Host {
    dir: tempfile::TempDir,
    db: Database,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Host {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
            .unwrap();
        tx.grant("app", "core.Person", Grant::read_write()).unwrap();
        tx.commit().unwrap();
        let server = LocalServer::bind(db.clone(), dir.path().join("socket"), config()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let worker = std::thread::spawn(move || server.run(signal).unwrap());
        Self {
            dir,
            db,
            stop,
            worker: Some(worker),
        }
    }
    fn client(&self) -> LocalClient {
        LocalClient::new(self.dir.path().join("socket"), TOKEN)
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}
fn config() -> ServerConfig {
    ServerConfig {
        version: 1,
        clients: vec![ClientCredential {
            principal: "app".into(),
            token: TOKEN.into(),
        }],
    }
}
#[test]
fn local_transport_roundtrips_concurrent_batches_and_enforces_authentication() {
    let host = Host::new();
    let schema = host.db.admin().snapshot().schema("core.Person").unwrap();
    std::thread::scope(|scope| {
        for i in 0..8 {
            let client = host.client();
            let schema = &schema;
            scope.spawn(move|| {
  let op=Mutation::Insert{relation:schema.name.clone(),schema_id:schema.id,fields:[("name".into(),WireValue::Text(format!("person {i}")))].into(),alias:None};
  assert!(matches!(client.request(Command::Batch{operations:vec![op]}).unwrap(),Response::Ok(Output::Batch{ids}) if ids.len()==1));
 });
        }
    });
    assert_eq!(
        host.db
            .admin()
            .snapshot()
            .scan("core.Person")
            .unwrap()
            .len(),
        8
    );
    assert!(
        matches!(LocalClient::new(host.dir.path().join("socket"),"wrong").request(Command::Capabilities).unwrap(),Response::Error(e) if e.code=="denied")
    );
    assert!(
        matches!(host.client().request(Command::Query{sql:"SELECT COUNT(*) FROM core.Person".into(),params:vec![]}).unwrap(),Response::Ok(Output::Query{rows,..}) if rows==vec![vec![SqlWireValue::Integer("8".into())]])
    );
    host.db.admin().snapshot().verify_indexes().unwrap();
}
#[test]
fn malformed_frames_and_stale_socket_paths_fail_closed() {
    let host = Host::new();
    let path = host.dir.path().join("socket");
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(LocalServer::bind(host.db.clone(), &path, config()).is_err());
    for frame in [b"not json\n".as_slice(), b"{}\n"] {
        let mut socket = UnixStream::connect(&path).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(30));
        socket.write_all(frame).unwrap();
        let mut line = String::new();
        BufReader::new(socket).read_line(&mut line).unwrap();
        assert!(
            matches!(serde_json::from_str::<Response>(&line).unwrap(),Response::Error(e) if e.code=="protocol")
        );
    }
    assert!(matches!(
        host.client().request(Command::Capabilities).unwrap(),
        Response::Ok(_)
    ));
}
#[test]
fn server_requires_private_socket_directory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(LocalServer::bind(Database::memory(), dir.path().join("socket"), config()).is_err());
    assert!(!dir.path().join("socket").exists());
}
#[test]
fn persistent_unix_preserves_coalesced_frames_and_refreshes_policy() {
    let host = Host::new();
    let mut socket = UnixStream::connect(host.dir.path().join("socket")).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    let request = Request {
        version: 1,
        token: TOKEN.into(),
        command: Command::Query {
            sql: "SELECT 1".into(),
            params: vec![],
        },
    };
    let frame = serde_json::to_string(&request).unwrap() + "\n";
    socket
        .write_all(format!("{frame}{frame}{frame}").as_bytes())
        .unwrap();
    let mut reader = BufReader::new(socket);
    for _ in 0..3 {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(matches!(
            serde_json::from_str::<Response>(&line).unwrap(),
            Response::Ok(_)
        ));
    }
    let mut session = host.client().connect().unwrap();
    let query = || Command::Query {
        sql: "SELECT * FROM core.Person".into(),
        params: vec![],
    };
    assert!(matches!(session.request(query()).unwrap(), Response::Ok(_)));
    let mut tx = host.db.admin().transaction();
    tx.revoke("app", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        session.request(query()).unwrap(),
        Response::Error(_)
    ));
}
#[test]
fn persistent_unix_rejects_oversized_or_partial_frames_and_shutdown_is_prompt() {
    use std::time::{Duration, Instant};
    let host = Host::new();
    let mut socket = UnixStream::connect(host.dir.path().join("socket")).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let _ = socket.write_all(&vec![b'x'; 1024 * 1024 + 1]);
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    assert!(reader.read_line(&mut line).is_err() || line.is_empty());
    let mut partial = UnixStream::connect(host.dir.path().join("socket")).unwrap();
    partial.write_all(b"{\"version\":").unwrap();
    let start = Instant::now();
    drop(host);
    assert!(start.elapsed() < Duration::from_secs(1));
}
