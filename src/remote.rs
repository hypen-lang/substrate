//! Bounded sequential JSON frames over TLS 1.3, sharing the local typed command protocol.
//! Server certificates and a trusted CA are required. No plaintext fallback.
#[path = "admission.rs"]
mod admission;
use crate::session_frame::Frames;
use crate::{
    service::{Command, Gateway, Request, Response},
    *,
};
use admission::Admission;
pub use admission::{
    AdmissionRejection, AdmissionStats, AuditEvent, AuditKind, RemoteLimits, RemoteOptions,
};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
const REQUEST_LIMIT: usize = 1024 * 1024;
const RESPONSE_LIMIT: usize = 4 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(10);
fn io(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}
fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(io)?;
    if certs.is_empty() {
        return Err(io("empty certificate chain"));
    }
    Ok(certs)
}
fn server_config(cert: &[u8], key: &[u8]) -> Result<Arc<ServerConfig>> {
    Ok(Arc::new(
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io)?
            .with_no_client_auth()
            .with_single_cert(
                certificates(cert)?,
                PrivateKeyDer::from_pem_slice(key).map_err(io)?,
            )
            .map_err(io)?,
    ))
}
fn client_config(ca: &[u8]) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    for cert in certificates(ca)? {
        roots.add(cert).map_err(io)?;
    }
    Ok(Arc::new(
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io)?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}
/// A fixed absolute connection budget also bounds TLS handshake and slow trickles.
/// Every underlying socket read/write gets only the remaining budget.
struct DeadlineSocket {
    socket: TcpStream,
    deadline: Instant,
    lifetime: Option<Instant>,
    first_byte_budget: Option<Duration>,
    shutdown: Option<Arc<AtomicBool>>,
}
impl DeadlineSocket {
    fn new(socket: TcpStream, budget: Duration) -> Result<Self> {
        socket.set_nonblocking(false).map_err(io)?;
        Ok(Self {
            socket,
            deadline: Instant::now() + budget,
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
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "connection deadline"))
    }
}
impl Read for DeadlineSocket {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let remaining = self.remaining()?;
            self.socket
                .set_read_timeout(Some(remaining.min(Duration::from_millis(100))))?;
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
impl Write for DeadlineSocket {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.socket
            .set_write_timeout(Some(self.remaining()?.min(Duration::from_millis(100))))?;
        self.socket.write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.socket.flush()
    }
}
// TLS may have application bytes buffered during handshake or a previous read.
// Start the request deadline on plaintext, not just on the next TCP packet.
struct RequestReader<'a> {
    stream: &'a mut StreamOwned<ServerConnection, DeadlineSocket>,
    budget: Duration,
    started: bool,
}
impl Read for RequestReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let n = self.stream.read(bytes)?;
        if n > 0 && !self.started {
            self.started = true;
            self.stream.sock.arm(self.budget, None);
        }
        Ok(n)
    }
}
fn encode_frame(value: &impl serde::Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(io)?;
    if bytes.len() > limit {
        return Err(io(
            "frame limit exceeded; mutation outcome may be uncertain",
        ));
    }
    bytes.push(b'\n');
    Ok(bytes)
}
pub struct RemoteServer {
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    gateway: Gateway,
    admission: Admission,
}
impl RemoteServer {
    pub fn bind(
        db: Database,
        address: SocketAddr,
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self> {
        Self::bind_with_options(
            db,
            address,
            certificate_pem,
            private_key_pem,
            RemoteOptions::default(),
        )
    }
    pub fn bind_with_options(
        db: Database,
        address: SocketAddr,
        certificate_pem: &[u8],
        private_key_pem: &[u8],
        options: RemoteOptions,
    ) -> Result<Self> {
        let admission = Admission::new(options)?;
        let tls = server_config(certificate_pem, private_key_pem)?;
        let listener = TcpListener::bind(address).map_err(io)?;
        listener.set_nonblocking(true).map_err(io)?;
        Ok(Self {
            listener,
            tls,
            gateway: Gateway::authenticated(db),
            admission,
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(io)
    }
    pub fn admission_stats(&self) -> AdmissionStats {
        self.admission.stats()
    }
    pub fn run(&self, shutdown: Arc<AtomicBool>) -> Result<()> {
        let mut readiness = mio::Poll::new().map_err(io)?;
        let mut source = mio::net::TcpListener::from_std(self.listener.try_clone().map_err(io)?);
        readiness
            .registry()
            .register(&mut source, mio::Token(0), mio::Interest::READABLE)
            .map_err(io)?;
        let mut events = mio::Events::with_capacity(8);
        let mut workers = Vec::<std::thread::JoinHandle<()>>::new();
        while !shutdown.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((socket, address)) => {
                    let peer = address.ip().to_canonical();
                    if let Ok(permit) = self.admission.enter(peer, Instant::now()) {
                        let admission = self.admission.clone();
                        let tls = self.tls.clone();
                        let gateway = self.gateway.clone();
                        let shutdown = shutdown.clone();
                        workers.push(std::thread::spawn(move || {
                            let _permit = permit;
                            let mut authenticated = false;
                            let mut failure_recorded = false;
                            let result = (|| -> Result<()> {
                                let mut stream = StreamOwned::new(
                                    ServerConnection::new(tls).map_err(io)?,
                                    DeadlineSocket::new(socket, admission.io_timeout())?,
                                );
                                stream.sock.shutdown = Some(shutdown.clone());
                                stream.sock.lifetime =
                                    Some(Instant::now() + admission.session_lifetime());
                                stream.sock.arm(admission.io_timeout(), None);
                                while stream.conn.is_handshaking() {
                                    stream.conn.complete_io(&mut stream.sock).map_err(io)?;
                                }
                                let mut frames = Frames::default();
                                for _ in 0..admission.max_requests() {
                                    if shutdown.load(Ordering::Relaxed)
                                        || admission.blocked(peer, Instant::now())
                                    {
                                        break;
                                    }
                                    let started = frames.has_pending();
                                    stream.sock.arm(
                                        if started {
                                            admission.io_timeout()
                                        } else {
                                            admission.idle_timeout()
                                        },
                                        None,
                                    );
                                    let mut reader = RequestReader {
                                        stream: &mut stream,
                                        budget: admission.io_timeout(),
                                        started,
                                    };
                                    let Some(bytes) = frames.next(&mut reader, REQUEST_LIMIT)?
                                    else {
                                        break;
                                    };
                                    if admission.blocked(peer, Instant::now()) {
                                        break;
                                    }
                                    stream.sock.arm(admission.io_timeout(), None);
                                    let mut anonymous = false;
                                    let (response, valid) =
                                        match serde_json::from_slice::<Request>(&bytes) {
                                            Ok(request) => {
                                                anonymous = request.token.is_empty();
                                                gateway.handle_authenticated_status(request)
                                            }
                                            Err(_) => (
                                                Response::error("protocol", "invalid request"),
                                                false,
                                            ),
                                        };
                                    authenticated = valid;
                                    if valid {
                                        admission.emit(
                                            peer,
                                            if anonymous {
                                                AuditKind::AnonymousRequest
                                            } else {
                                                AuditKind::Authenticated
                                            },
                                        );
                                    } else {
                                        admission.failure(peer, Instant::now());
                                        failure_recorded = true;
                                        admission.emit(peer, AuditKind::AuthenticationFailed);
                                    }
                                    admission.emit(
                                        peer,
                                        if matches!(response, Response::Ok(_)) {
                                            AuditKind::RequestCompleted
                                        } else {
                                            AuditKind::RequestRejected
                                        },
                                    );
                                    let mut output = serde_json::to_vec(&response).map_err(io)?;
                                    if output.len() > RESPONSE_LIMIT {
                                        output = serde_json::to_vec(&Response::error(
                                            "limit",
                                            "response too large; use smaller pages",
                                        ))
                                        .map_err(io)?;
                                    }
                                    output.push(b'\n');
                                    stream.write_all(&output).map_err(io)?;
                                    stream.flush().map_err(io)?;
                                    // Invalid credentials end the session. Every valid next frame authenticates again.
                                    if !valid {
                                        break;
                                    }
                                }
                                stream.conn.send_close_notify();
                                stream.flush().map_err(io)
                            })();
                            if result.is_err() {
                                if !authenticated && !failure_recorded {
                                    admission.failure(peer, Instant::now());
                                }
                                admission.emit(peer, AuditKind::TransportFailed);
                            }
                        }));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Readiness wakes immediately for a connection; timeout only checks shutdown.
                    match readiness.poll(&mut events, Some(Duration::from_millis(100))) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => return Err(io(e)),
                    }
                }
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
/// Uses an explicitly trusted CA and verifies the server name. Never retries mutations.
pub struct RemoteClient {
    address: SocketAddr,
    server_name: ServerName<'static>,
    tls: Arc<ClientConfig>,
    token: String,
}
impl RemoteClient {
    /// Read public schemas without a credential; TLS verification remains mandatory.
    pub fn anonymous(address: SocketAddr, server_name: &str, ca_pem: &[u8]) -> Result<Self> {
        Self::new(address, server_name, ca_pem, "")
    }
    pub fn new(
        address: SocketAddr,
        server_name: &str,
        ca_pem: &[u8],
        token: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            address,
            server_name: ServerName::try_from(server_name.to_owned()).map_err(io)?,
            tls: client_config(ca_pem)?,
            token: token.into(),
        })
    }
    pub fn connect(&self) -> Result<RemoteSession> {
        let socket = TcpStream::connect_timeout(&self.address, DEADLINE).map_err(io)?;
        socket.set_nodelay(true).map_err(io)?;
        let mut stream = StreamOwned::new(
            ClientConnection::new(self.tls.clone(), self.server_name.clone()).map_err(io)?,
            DeadlineSocket::new(socket, DEADLINE)?,
        );
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock).map_err(io)?;
        }
        Ok(RemoteSession {
            stream,
            frames: Frames::default(),
            token: self.token.clone(),
            failed: false,
        })
    }
    pub fn request(&self, command: Command) -> Result<Response> {
        self.connect()?.request(command)
    }
}
/// Sequential persistent session. A transport error poisons it; reconnect explicitly.
pub struct RemoteSession {
    stream: StreamOwned<ClientConnection, DeadlineSocket>,
    frames: Frames,
    token: String,
    failed: bool,
}
impl RemoteSession {
    pub fn request(&mut self, command: Command) -> Result<Response> {
        if self.failed {
            return Err(io(
                "closed/failed session; reconcile uncertain mutations before reconnecting",
            ));
        }
        let bytes = encode_frame(
            &Request {
                version: 1,
                token: self.token.clone(),
                command,
            },
            REQUEST_LIMIT,
        )?;
        let result = (|| {
            self.stream.sock.arm(DEADLINE, None);
            self.stream.write_all(&bytes).map_err(io)?;
            self.stream.flush().map_err(io)?;
            let frame = self
                .frames
                .next(&mut self.stream, RESPONSE_LIMIT)?
                .ok_or_else(|| io("closed session; mutation outcome may be uncertain"))?;
            serde_json::from_slice(&frame).map_err(io)
        })();
        if result.is_err() {
            self.failed = true;
            let _ = self.stream.sock.socket.shutdown(std::net::Shutdown::Both);
        }
        result
    }
    pub fn close(mut self) {
        self.stream.conn.send_close_notify();
        let _ = self.stream.flush();
        let _ = self.stream.sock.socket.shutdown(std::net::Shutdown::Both);
    }
}
