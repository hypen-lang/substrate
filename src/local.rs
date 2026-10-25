//! Local multi-process transport. One database owner, concurrent application
//! clients; durable commit publication is still serialized by the kernel/redb.
use crate::session_frame::Frames;
use crate::{service::*, *};
use std::{
    io::{Read, Write},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
const MAX_REQUEST: usize = 1024 * 1024;
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 32;
fn io(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}
pub struct LocalServer {
    listener: UnixListener,
    path: PathBuf,
    inode: u64,
    gateway: Gateway,
}
impl LocalServer {
    pub fn bind(db: Database, path: impl AsRef<Path>, config: ServerConfig) -> Result<Self> {
        let parent = path
            .as_ref()
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if std::fs::metadata(parent).map_err(io)?.permissions().mode() & 0o077 != 0 {
            return Err(Error::Storage(
                "socket parent must be private (chmod 700)".into(),
            ));
        }
        let gateway = Gateway::new(db, config)?;
        // bind refuses existing files/sockets. Never unlink another server's socket.
        let listener = UnixListener::bind(path.as_ref()).map_err(io)?;
        std::fs::set_permissions(path.as_ref(), std::fs::Permissions::from_mode(0o600))
            .map_err(io)?;
        listener.set_nonblocking(true).map_err(io)?;
        let inode = std::fs::metadata(path.as_ref()).map_err(io)?.ino();
        Ok(Self {
            listener,
            path: path.as_ref().to_path_buf(),
            inode,
            gateway,
        })
    }
    pub fn run(&self, shutdown: Arc<AtomicBool>) -> Result<()> {
        let mut readiness = mio::Poll::new().map_err(io)?;
        let mut source = mio::net::UnixListener::from_std(self.listener.try_clone().map_err(io)?);
        readiness
            .registry()
            .register(&mut source, mio::Token(0), mio::Interest::READABLE)
            .map_err(io)?;
        let mut events = mio::Events::with_capacity(8);
        let active = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        while !shutdown.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if active.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
                        drop(stream);
                        continue;
                    }
                    active.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    let gateway = self.gateway.clone();
                    let shutdown = shutdown.clone();
                    workers.push(std::thread::spawn(move || {
                        struct Count(Arc<AtomicUsize>);
                        impl Drop for Count {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                        let _count = Count(active);
                        let _ = serve(stream, &gateway, shutdown);
                    }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => readiness
                    .poll(&mut events, Some(Duration::from_millis(100)))
                    .map_err(io)?,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(io(e)),
            }
            let mut i = 0;
            while i < workers.len() {
                if workers[i].is_finished() {
                    let _ = workers.swap_remove(i).join();
                } else {
                    i += 1;
                }
            }
        }
        for worker in workers {
            let _ = worker.join();
        }
        Ok(())
    }
}
impl Drop for LocalServer {
    fn drop(&mut self) {
        if std::fs::metadata(&self.path).is_ok_and(|m| m.ino() == self.inode) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
struct SessionSocket {
    socket: UnixStream,
    deadline: Instant,
    lifetime: Option<Instant>,
    first_byte_budget: Option<Duration>,
    shutdown: Option<Arc<AtomicBool>>,
}
impl SessionSocket {
    fn new(socket: UnixStream) -> Result<Self> {
        socket.set_nonblocking(false).map_err(io)?;
        Ok(Self {
            socket,
            deadline: Instant::now() + Duration::from_secs(5),
            lifetime: None,
            first_byte_budget: None,
            shutdown: None,
        })
    }
    fn arm(&mut self, budget: Duration, first_byte_budget: Option<Duration>) {
        self.deadline = Instant::now() + budget;
        if let Some(end) = self.lifetime {
            self.deadline = self.deadline.min(end);
        }
        self.first_byte_budget = first_byte_budget;
    }
    fn remaining(&self) -> std::io::Result<Duration> {
        if self
            .shutdown
            .as_ref()
            .is_some_and(|s| s.load(Ordering::Relaxed))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "server shutdown",
            ));
        }
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "frame deadline"))
    }
}
impl Read for SessionSocket {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        loop {
            self.socket
                .set_read_timeout(Some(self.remaining()?.min(Duration::from_millis(100))))?;
            match self.socket.read(b) {
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Ok(n) => {
                    if n > 0
                        && let Some(budget) = self.first_byte_budget.take()
                    {
                        self.arm(budget, None);
                    }
                    return Ok(n);
                }
                other => return other,
            }
        }
    }
}
impl Write for SessionSocket {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.socket
            .set_write_timeout(Some(self.remaining()?.min(Duration::from_millis(100))))?;
        self.socket.write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.socket.flush()
    }
}
fn serve(stream: UnixStream, gateway: &Gateway, shutdown: Arc<AtomicBool>) -> Result<()> {
    let mut stream = SessionSocket::new(stream)?;
    stream.shutdown = Some(shutdown.clone());
    stream.lifetime = Some(Instant::now() + Duration::from_secs(60));
    let mut frames = Frames::default();
    for _ in 0..256 {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if frames.has_pending() {
            stream.arm(Duration::from_secs(5), None);
        } else {
            stream.arm(Duration::from_secs(2), Some(Duration::from_secs(5)));
        }
        let Some(bytes) = frames.next(&mut stream, MAX_REQUEST)? else {
            break;
        };
        stream.arm(Duration::from_secs(5), None);
        let response = match serde_json::from_slice::<Request>(&bytes) {
            Ok(request) => gateway.handle(request),
            Err(_) => Response::error("protocol", "invalid request"),
        };
        let mut output = serde_json::to_vec(&response).map_err(io)?;
        if output.len() > MAX_RESPONSE {
            output = serde_json::to_vec(&Response::error(
                "limit",
                "response too large; use smaller pages",
            ))
            .map_err(io)?;
        }
        output.push(b'\n');
        stream.write_all(&output).map_err(io)?;
        stream.flush().map_err(io)?;
    }
    Ok(())
}
pub struct LocalClient {
    path: PathBuf,
    token: String,
}
impl LocalClient {
    pub fn new(path: impl AsRef<Path>, token: impl Into<String>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            token: token.into(),
        }
    }
    /// No automatic transport retries: a disconnect after a mutation can have an
    /// uncertain commit outcome. Reconcile before retrying a non-idempotent batch.
    pub fn connect(&self) -> Result<LocalSession> {
        Ok(LocalSession {
            stream: SessionSocket::new(UnixStream::connect(&self.path).map_err(io)?)?,
            frames: Frames::default(),
            token: self.token.clone(),
            failed: false,
        })
    }
    pub fn request(&self, command: Command) -> Result<Response> {
        self.connect()?.request(command)
    }
    pub fn insert_record<R: Record>(&self, record: R) -> Result<TypedId<R>> {
        let command = Command::Batch {
            operations: vec![Mutation::Insert {
                relation: R::RELATION.into(),
                schema_id: R::TYPE_ID,
                fields: record
                    .fields()
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                alias: None,
            }],
        };
        let command = if let Some(fingerprint) = R::FINGERPRINT {
            Command::Checked {
                contracts: vec![Contract {
                    relation: R::RELATION.into(),
                    schema_id: R::TYPE_ID,
                    fingerprint: fingerprint.into(),
                }],
                command: Box::new(command),
            }
        } else {
            command
        };
        match self.request(command)? {
            Response::Ok(Output::Batch { ids }) if ids.len() == 1 => Ok(TypedId::from_raw(ids[0])),
            Response::Error(e) => Err(Error::Query(format!("{}: {}", e.code, e.message))),
            _ => Err(io("invalid batch response")),
        }
    }
}

/// Persistent sequential Unix connection. Explicit close or drop releases the socket.
pub struct LocalSession {
    stream: SessionSocket,
    frames: Frames,
    token: String,
    failed: bool,
}
impl LocalSession {
    pub fn request(&mut self, command: Command) -> Result<Response> {
        if self.failed {
            return Err(io(
                "closed/failed session; reconcile uncertain mutations before reconnecting",
            ));
        }
        let mut bytes = serde_json::to_vec(&Request {
            version: 1,
            token: self.token.clone(),
            command,
        })
        .map_err(io)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_REQUEST {
            return Err(Error::Constraint("request too large".into()));
        }
        let result = (|| {
            self.stream.arm(Duration::from_secs(10), None);
            self.stream.write_all(&bytes).map_err(io)?;
            self.stream.flush().map_err(io)?;
            let output = self
                .frames
                .next(&mut self.stream, MAX_RESPONSE)?
                .ok_or_else(|| io("closed session; mutation outcome may be uncertain"))?;
            serde_json::from_slice(&output).map_err(io)
        })();
        if result.is_err() {
            self.failed = true;
            let _ = self.stream.socket.shutdown(std::net::Shutdown::Both);
        }
        result
    }
    pub fn close(self) {
        let _ = self.stream.socket.shutdown(std::net::Shutdown::Both);
    }
}
