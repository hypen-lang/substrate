# Bounded remote admission and security telemetry

The TLS host now has configurable per-peer concurrency, connection-rate and failed-authentication controls. These are an initial operational layer, not a complete production security program.

```rust,ignore
use substrate_liquid::remote::{RemoteLimits, RemoteOptions, RemoteServer};
let (events, receiver) = std::sync::mpsc::sync_channel(1024);
let server = RemoteServer::bind_with_options(db, address, &cert, &key,
    RemoteOptions {
        limits: RemoteLimits {
            max_connections: 32,
            max_connections_per_peer: 4,
            connection_attempts_per_window: 256,
            failure_limit: 8,
            ..RemoteLimits::default()
        },
        audit: Some(events),
    })?;
// A trusted host-owned consumer drains receiver and decides whether/how to persist events.
```

`bind` retains its existing API and uses defaults. Defaults are 32 global active connections, 8 active connections per canonical IP address, at most 4,096 tracked peer records, 1,024 connection attempts per peer per 60-second window, and a 60-second cooldown after 16 pre-authentication failures in a window. The absolute IO budget defaults to ten seconds per handshake/request/response phase and can be configured from 10 ms to 300 seconds. Persistent sessions additionally have idle, command-count and total-lifetime bounds. Limits are validated before binding the listener.

A connection permit is released on worker completion, early return or panic unwinding. Reconnecting does not reset rate/failure accounting. Active, unexpired or still-blocked peer entries are never evicted to make space. If the bounded peer table fills, new peers are refused until old entries expire; existing peers remain accounted. This is a deliberate fail-closed limit with an availability tradeoff. Configuration should reflect NAT/shared-proxy environments: these are IP-based limits, not per-principal fairness. Rejected peers are disconnected before TLS work, not given an application response. Distributed sources can still exhaust global resources.

Credential failures, malformed unauthenticated frames and TLS/pre-authentication transport failures count toward cooldown. A legitimate scoped operation denial does not: an authenticated application requesting forbidden data must not be treated as a guessed credential. The server still checks live credentials, capabilities, expiry and current permissions for every operation. It never caches an authentication decision across a later operation.

The optional bounded `SyncSender<AuditEvent>` is a trusted host-only sink. Events contain canonical peer IP, Unix-second observation time and a fixed enum outcome. They contain no credential/token, principal, SQL, schema names, rows, request payload or raw error messages. Events describe admission, authentication, operation completion/rejection, transport failure and closure. Operation completion means the engine returned a result, not that a client received network bytes. Events use `try_send`; a full/disconnected sink never blocks a worker. `admission_stats()` exposes active/tracked counts, cumulative accepted/rejected/pre-authentication-failure counts and dropped audit events. The audit stream is lossy, process-local and non-durable; it is not a transactional compliance log.

Both Unix and TLS listeners now use event-driven readiness instead of sleeping five milliseconds after an empty accept. Incoming connections wake the listener immediately; a 100 ms readiness timeout only checks shutdown and reaps finished workers. Accepted workers retain explicit IO deadlines. The platform implementation uses `mio`; validation and measurements were performed on arm64 macOS. Remote response bytes are encoded once; only an oversized response's replacement error needs another encoding. Rust client requests are also encoded once.

Run `npm run demo:remote` to exercise the configured host and bounded audit channel with real Node requests, rotation, revocation and expiry. `tests/remote.rs` adds real TLS tests for cooldown, scoped-denial distinction, redacted telemetry and slow-handshake concurrency/deadline enforcement. Six deterministic admission unit tests cover quota bounds, window reset, cooldown across windows, no unsafe peer eviction and audit overflow without sleeps.

## Loopback measurement

`cargo run --release --offline --example transport_bench` runs `SELECT 1` through a real restricted host, with five warmups and 100 sequential requests per transport. The historical runs below opened a new connection for each request; the Rust TLS client's shared configuration may resume TLS sessions. The harness now also measures persistent connections, documented separately. These measurements are warm, same-host latency, not production latency or sustained throughput. They include authentication and query setup. Development work was concurrent on the machine, so timings are indicative and raw reports are retained.

| Transport | Before median | After median | Before p95 | After p95 |
|---|---:|---:|---:|---:|
| TLS | 6,279.542 µs | 297.959 µs | 9,939.958 µs | 387.959 µs |
| Unix | 6,289.584 µs | 93.917 µs | 6,537.334 µs | 121.500 µs |

The before run used the already-built pre-change executable; the after run rebuilt the same harness. The large drop is consistent with removing the fixed accept-loop sleep. Other concurrent query changes can affect small portions of setup time; this is not an isolated attribution of serialization or admission cost. Raw reports: [before](benchmarks/2026-10-25-transport-before.json), [after](benchmarks/2026-10-25-transport-after.json).

## Still missing

Bounded persistent TLS/Unix sessions are now implemented; see [persistent transport](PERSISTENT-TRANSPORT.md) for API, limits, tests and comparative measurement. Connection pooling remains future work.

Also missing: enrollment/OIDC/mTLS identity mapping, certificate reload/rotation automation, durable audit storage and transactional audit guarantees, per-principal budgets, comprehensive CPU/memory isolation, secure time across restart, credential-ledger cleanup, remote cancellation and independent security review. Existing single-request frame/deadline controls do not preempt all kernel CPU work. Raw filesystem/database administrator access remains trusted.
