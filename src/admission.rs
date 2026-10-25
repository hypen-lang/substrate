//! Bounded host-side admission accounting. No caller-controlled payload is audited.
use crate::{Error, Result};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::SyncSender,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug)]
pub struct RemoteLimits {
    pub max_connections: usize,
    pub max_connections_per_peer: usize,
    pub max_tracked_peers: usize,
    pub connection_attempts_per_window: u32,
    pub failure_limit: u32,
    pub window: Duration,
    pub block_for: Duration,
    pub io_timeout: Duration,
    pub idle_timeout: Duration,
    pub session_lifetime: Duration,
    pub max_requests_per_connection: u32,
}
impl Default for RemoteLimits {
    fn default() -> Self {
        Self {
            max_connections: 32,
            max_connections_per_peer: 8,
            max_tracked_peers: 4096,
            connection_attempts_per_window: 1024,
            failure_limit: 16,
            window: Duration::from_secs(60),
            block_for: Duration::from_secs(60),
            io_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(2),
            session_lifetime: Duration::from_secs(60),
            max_requests_per_connection: 256,
        }
    }
}
impl RemoteLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_connections == 0
            || self.max_connections > 1024
            || self.max_connections_per_peer == 0
            || self.max_connections_per_peer > self.max_connections
            || self.max_tracked_peers == 0
            || self.max_tracked_peers > 65536
            || self.connection_attempts_per_window == 0
            || self.failure_limit == 0
            || self.window.is_zero()
            || self.window > Duration::from_secs(86400)
            || self.block_for.is_zero()
            || self.block_for > Duration::from_secs(86400)
            || self.idle_timeout < Duration::from_millis(10)
            || self.idle_timeout > Duration::from_secs(60)
            || self.session_lifetime < self.idle_timeout
            || self.session_lifetime > Duration::from_secs(3600)
            || self.max_requests_per_connection == 0
            || self.max_requests_per_connection > 4096
            || self.io_timeout < Duration::from_millis(10)
            || self.io_timeout > Duration::from_secs(300)
        {
            return Err(Error::Constraint("invalid remote admission limits".into()));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionRejection {
    GlobalConnections,
    PeerConnections,
    PeerRate,
    PeerBlocked,
    PeerTableFull,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditKind {
    Accepted,
    Rejected(AdmissionRejection),
    AuthenticationFailed,
    Authenticated,
    AnonymousRequest,
    RequestCompleted,
    RequestRejected,
    TransportFailed,
    Closed,
}
#[derive(Clone, Debug)]
pub struct AuditEvent {
    pub peer: IpAddr,
    pub at_unix_seconds: u64,
    pub kind: AuditKind,
}
/// try_send only: a stalled or full audit consumer never blocks database workers.
#[derive(Clone, Default)]
pub struct RemoteOptions {
    pub limits: RemoteLimits,
    pub audit: Option<SyncSender<AuditEvent>>,
}
#[derive(Clone, Debug, Default)]
pub struct AdmissionStats {
    pub active_connections: usize,
    pub tracked_peers: usize,
    pub accepted: u64,
    pub rejected: u64,
    pub authentication_failures: u64,
    pub dropped_audit_events: u64,
}
struct Peer {
    active: usize,
    window_start: Instant,
    attempts: u32,
    failures: u32,
    blocked_until: Option<Instant>,
}
impl Peer {
    fn refresh(&mut self, now: Instant, window: Duration) {
        if now.duration_since(self.window_start) >= window {
            self.window_start = now;
            self.attempts = 0;
            self.failures = 0;
        }
    }
}
#[derive(Default)]
struct State {
    peers: BTreeMap<IpAddr, Peer>,
    active: usize,
    accepted: u64,
    rejected: u64,
    failures: u64,
}
struct Inner {
    limits: RemoteLimits,
    audit: Option<SyncSender<AuditEvent>>,
    dropped: AtomicU64,
    state: Mutex<State>,
}
#[derive(Clone)]
pub(crate) struct Admission(Arc<Inner>);
pub(crate) struct Permit {
    admission: Admission,
    peer: IpAddr,
}
impl Drop for Permit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.admission.0.state.lock() {
            state.active = state.active.saturating_sub(1);
            if let Some(peer) = state.peers.get_mut(&self.peer) {
                peer.active = peer.active.saturating_sub(1);
            }
        }
        self.admission.emit(self.peer, AuditKind::Closed);
    }
}
impl Admission {
    pub(crate) fn new(options: RemoteOptions) -> Result<Self> {
        options.limits.validate()?;
        Ok(Self(Arc::new(Inner {
            limits: options.limits,
            audit: options.audit,
            dropped: AtomicU64::new(0),
            state: Mutex::new(State::default()),
        })))
    }
    pub(crate) fn idle_timeout(&self) -> Duration {
        self.0.limits.idle_timeout
    }
    pub(crate) fn session_lifetime(&self) -> Duration {
        self.0.limits.session_lifetime
    }
    pub(crate) fn max_requests(&self) -> u32 {
        self.0.limits.max_requests_per_connection
    }
    pub(crate) fn blocked(&self, ip: IpAddr, now: Instant) -> bool {
        self.0.state.lock().map_or(true, |state| {
            state
                .peers
                .get(&ip)
                .is_none_or(|p| p.blocked_until.is_some_and(|until| now < until))
        })
    }
    pub(crate) fn io_timeout(&self) -> Duration {
        self.0.limits.io_timeout
    }
    pub(crate) fn emit(&self, peer: IpAddr, kind: AuditKind) {
        if let Some(sink) = &self.0.audit {
            let event = AuditEvent {
                peer,
                kind,
                at_unix_seconds: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            };
            if sink.try_send(event).is_err() {
                self.0.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub(crate) fn enter(
        &self,
        ip: IpAddr,
        now: Instant,
    ) -> std::result::Result<Permit, AdmissionRejection> {
        let result = self.try_enter(ip, now);
        match result {
            Ok(()) => {
                self.emit(ip, AuditKind::Accepted);
                Ok(Permit {
                    admission: self.clone(),
                    peer: ip,
                })
            }
            Err(reason) => {
                if let Ok(mut state) = self.0.state.lock() {
                    state.rejected += 1;
                }
                self.emit(ip, AuditKind::Rejected(reason.clone()));
                Err(reason)
            }
        }
    }
    fn try_enter(&self, ip: IpAddr, now: Instant) -> std::result::Result<(), AdmissionRejection> {
        let limits = &self.0.limits;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| AdmissionRejection::Unavailable)?;
        if !state.peers.contains_key(&ip) {
            // Never evict an active or unexpired record: eviction must not reset limits.
            if state.peers.len() >= limits.max_tracked_peers {
                state.peers.retain(|_, p| {
                    p.active > 0
                        || now.duration_since(p.window_start) < limits.window
                        || p.blocked_until.is_some_and(|end| now < end)
                });
            }
            if state.peers.len() >= limits.max_tracked_peers {
                return Err(AdmissionRejection::PeerTableFull);
            }
            state.peers.insert(
                ip,
                Peer {
                    active: 0,
                    window_start: now,
                    attempts: 0,
                    failures: 0,
                    blocked_until: None,
                },
            );
        }
        let peer = state.peers.get_mut(&ip).expect("inserted peer");
        peer.refresh(now, limits.window);
        if peer.blocked_until.is_some_and(|end| now < end) {
            return Err(AdmissionRejection::PeerBlocked);
        }
        if peer.attempts >= limits.connection_attempts_per_window {
            return Err(AdmissionRejection::PeerRate);
        }
        peer.attempts += 1;
        if peer.active >= limits.max_connections_per_peer {
            return Err(AdmissionRejection::PeerConnections);
        }
        if state.active >= limits.max_connections {
            return Err(AdmissionRejection::GlobalConnections);
        }
        state.peers.get_mut(&ip).unwrap().active += 1;
        state.active += 1;
        state.accepted += 1;
        Ok(())
    }
    pub(crate) fn failure(&self, ip: IpAddr, now: Instant) {
        if let Ok(mut state) = self.0.state.lock() {
            state.failures += 1;
            if let Some(peer) = state.peers.get_mut(&ip) {
                peer.refresh(now, self.0.limits.window);
                peer.failures = peer.failures.saturating_add(1);
                if peer.failures >= self.0.limits.failure_limit {
                    peer.blocked_until = Some(now + self.0.limits.block_for);
                }
            }
        }
    }
    pub(crate) fn stats(&self) -> AdmissionStats {
        let Ok(state) = self.0.state.lock() else {
            return AdmissionStats::default();
        };
        AdmissionStats {
            active_connections: state.active,
            tracked_peers: state.peers.len(),
            accepted: state.accepted,
            rejected: state.rejected,
            authentication_failures: state.failures,
            dropped_audit_events: self.0.dropped.load(Ordering::Relaxed),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn options() -> RemoteOptions {
        RemoteOptions {
            limits: RemoteLimits {
                max_connections: 2,
                max_connections_per_peer: 1,
                max_tracked_peers: 2,
                connection_attempts_per_window: 3,
                failure_limit: 2,
                window: Duration::from_secs(10),
                block_for: Duration::from_secs(20),
                ..RemoteLimits::default()
            },
            audit: None,
        }
    }
    fn ip(n: u8) -> IpAddr {
        IpAddr::from([127, 0, 0, n])
    }
    #[test]
    fn concurrency_caps_release_on_drop_and_preserve_other_peers() {
        let a = Admission::new(options()).unwrap();
        let now = Instant::now();
        let one = a.enter(ip(1), now).unwrap();
        assert!(matches!(
            a.enter(ip(1), now),
            Err(AdmissionRejection::PeerConnections)
        ));
        let two = a.enter(ip(2), now).unwrap();
        assert_eq!(a.stats().active_connections, 2);
        drop(one);
        let three = a.enter(ip(1), now).unwrap();
        drop((two, three));
        assert_eq!(a.stats().active_connections, 0);
    }
    #[test]
    fn rate_window_is_not_reset_by_reconnect() {
        let a = Admission::new(options()).unwrap();
        let now = Instant::now();
        for _ in 0..3 {
            drop(a.enter(ip(1), now).unwrap());
        }
        assert!(matches!(
            a.enter(ip(1), now),
            Err(AdmissionRejection::PeerRate)
        ));
        assert!(a.enter(ip(1), now + Duration::from_secs(10)).is_ok());
    }
    #[test]
    fn failures_block_across_rate_windows() {
        let a = Admission::new(options()).unwrap();
        let now = Instant::now();
        drop(a.enter(ip(1), now).unwrap());
        a.failure(ip(1), now);
        a.failure(ip(1), now);
        assert!(matches!(
            a.enter(ip(1), now + Duration::from_secs(11)),
            Err(AdmissionRejection::PeerBlocked)
        ));
        assert!(a.enter(ip(1), now + Duration::from_secs(20)).is_ok());
    }
    #[test]
    fn peer_table_has_hard_bound_and_only_expired_entries_are_evicted() {
        let a = Admission::new(options()).unwrap();
        let now = Instant::now();
        let active = a.enter(ip(1), now).unwrap();
        drop(a.enter(ip(2), now).unwrap());
        assert!(matches!(
            a.enter(ip(3), now),
            Err(AdmissionRejection::PeerTableFull)
        ));
        drop(a.enter(ip(3), now + Duration::from_secs(10)).unwrap());
        assert_eq!(a.stats().tracked_peers, 2);
        assert!(a.0.state.lock().unwrap().peers.contains_key(&ip(1)));
        drop(active);
    }
    #[test]
    fn full_audit_channel_is_nonblocking_and_counted() {
        let (tx, _rx) = std::sync::mpsc::sync_channel(1);
        let a = Admission::new(RemoteOptions {
            audit: Some(tx),
            ..options()
        })
        .unwrap();
        drop(a.enter(ip(1), Instant::now()).unwrap());
        assert_eq!(a.stats().dropped_audit_events, 1);
    }
    #[test]
    fn invalid_limits_are_rejected() {
        let mut o = options();
        o.limits.io_timeout = Duration::ZERO;
        assert!(Admission::new(o).is_err());
    }
}
