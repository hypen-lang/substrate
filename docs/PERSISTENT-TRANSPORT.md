# Bounded persistent request sessions

Rust and Node clients can reuse a TLS or Unix connection. Existing `request` calls on ordinary clients remain connection-per-request. The command/response format stays version 1: newline-delimited JSON, one response for each command in wire order. This does not merge commands into a transaction. Use the existing batch command when atomicity is required.

```rust,ignore
let mut session = client.connect()?; // RemoteClient or LocalClient
let first = session.request(command_one)?;
let second = session.request(command_two)?;
session.close(); // drop also releases the socket
```

```typescript
const session = new RemoteClient({host, port, servername, ca}, token).persistent();
try {
  const [first, second] = await Promise.all([
    session.query("SELECT name FROM core.Person"),
    session.query("SELECT name FROM core.Person"),
  ]);
} finally {
  session.close();
}
```

The Node client lazily opens one connection, waits for verified TLS before sending credentials, and serializes concurrent callers through a FIFO queue. At most 16 pending/in-flight operations and 4 MiB of serialized requests are retained. A single request is limited to 1 MiB. One command is in flight at a time, so responses cannot be misattributed. Responses are bounded to 4 MiB; malformed or unsolicited extra frames poison the session. Capability-selected client clones share the session and its close lifetime. A clone cannot switch a selected capability. Calling `close` rejects pending operations; closing an ordinary one-shot client has no effect.

Both Rust sessions require mutable access to send a command, enforcing sequential use. There is no transparent reconnection, replay or transport retry. A transport failure permanently closes that session. Open a new session explicitly after reconciling any uncertain mutation. A disconnect or timeout after sending a write does not prove rollback. Application errors such as a scoped denial preserve the connection when the credential remains valid. Credential authentication failure ends the TLS session after its denial response.

## Server bounds and authorization

TLS defaults are 256 commands, 60 seconds total connection lifetime, and 2 seconds idle waiting for another request. `RemoteLimits` exposes `max_requests_per_connection`, `session_lifetime`, and `idle_timeout`. The existing `io_timeout` gives separate fresh absolute budgets for handshake, request reception and response IO (10 seconds by default); total lifetime clamps them all. Request reception begins on the first plaintext bytes, including bytes buffered by TLS during handshake. Additional bytes do not renew a request budget. Unix uses 256 commands, 60 seconds lifetime, 2 seconds idle, and 5 seconds per request-receive/response-IO phase. Rust clients use a 10-second request deadline; Node uses distinct 10-second connection and active-request timers. Waiting Node requests are bounded by queue count/bytes, not individual queue-wait deadlines.

The server preserves bytes after a newline, including coalesced or pipelined frames, within a bounded parser buffer. Each command is executed and answered in order. A reused connection retains its active-connection permit; it cannot bypass global/per-peer connection limits. Peer cooldown is checked before and after receiving each request, so a connection opened before another connection's authentication failure cannot bypass the block. Scoped permission denial is not an authentication failure. Count/lifetime/idle bounds prevent an idle or indefinitely reused connection from retaining a worker forever. Shutdown interrupts idle socket reads in roughly 100 ms polling intervals without waiting for the session lifetime.

Every command goes through the gateway again using its wire credential and current engine policy. There is no cached authenticated engine session across commands. Rotation, expiry, grants and ancestor capability revocation therefore take effect on the next operation, including runtime descriptor operations and schema commands. Unix credentials retain their existing fixed host-config semantics; current grants and capabilities are still checked per operation.

These are transport bounds, not preemptive CPU/memory limits inside a running database command. A slow computation can delay shutdown and cannot be cancelled remotely yet. Connection pooling, async server execution, request cancellation, automatic idle reconnection, request-id deduplication, and per-principal request budgets remain future work. TLS session reuse does not change redb's single physical writer restriction.

## Verification and measurement

`npm run demo:remote` runs the TypeScript persistent client against a real TLS host and proves scoped reads/writes, credential rotation, revocation and expiry. `cargo test --offline --test remote --test local` covers real coalesced/pipelined sockets, partial/oversized frames, idle/count limits, shutdown, cooldown, and live authorization. `tools/persistent-client.test.mjs` covers FIFO queueing, queue bounds, shared capability selection, scope errors, unsolicited frames and no replay after an uncertain write disconnect.

`cargo run --release --offline --example transport_bench` compares the same authenticated `SELECT 1` workload and host with one-shot and persistent connections (5 warmups, 100 measured requests per case). The same-run [raw report](benchmarks/2026-10-25-transport-persistent.json) measured:

| Transport | One-shot median | Persistent median | One-shot p95 | Persistent p95 |
|---|---:|---:|---:|---:|
| TLS | 320.917 µs | 54.000 µs | 467.291 µs | 58.959 µs |
| Unix | 85.584 µs | 40.458 µs | 115.500 µs | 51.583 µs |

That is a 5.94× TLS and 2.12× Unix median improvement in this loopback microbenchmark. It measures connection overhead, not representative database throughput. The TLS one-shot client already shares a configuration that permits TLS resumption. Other development work was concurrent on this arm64 macOS machine; measurements are indicative rather than production guarantees.
