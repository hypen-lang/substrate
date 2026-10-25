# Effect integration

Substrate includes an **Effect 3** adapter at `sdk/typescript/effect.ts`, tested against the pinned `effect@3.20.0`. This is a repository SDK, not a published npm package. The core TypeScript client remains usable without Effect; applications using this adapter import Effect explicitly.

Use the same generated descriptors and query builders with a typed `Substrate` service:

```ts
import { Effect } from "effect";
import { Substrate, layer } from "./sdk/typescript/effect.js";
import { LocalClient } from "./sdk/typescript/client.mjs";
import { read, table } from "./sdk/typescript/query.js";
import { Person } from "./generated/schema.js";

const program = Effect.gen(function* () {
  const db = yield* Substrate;
  const id = yield* db.insert(Person, { name: "Ada" });
  yield* db.update(Person, id, { name: "Grace" });
  return yield* db.execute(read(table(Person, "p")));
});

const rows = await Effect.runPromise(
  program.pipe(Effect.provide(layer(new LocalClient(socket, token))))
);
```

`layer` accepts a `LocalClient`, `RemoteClient`, or any transport implementing `request(command): Promise<unknown>`. It retains the selected client's credential and capability restrictions. It does not create additional database permissions or a separate authorization cache.

`execute` accepts both typed SQL selections and nested relationship reads, preserving their inferred result types. Nested loading continues to use the existing batched server query; the adapter adds no per-row requests. `insert`, `update`, and `delete` use the descriptor's validation and schema contract. `batch` sends all operations in one server transaction. `use(client => ...)` bridges existing asynchronous helpers such as `introspect`, `describe`, `delegateCapability`, and schema proposals.

All operations are lazy. Both local validation failures and rejected promises become `SubstrateFailure` values in the Effect error channel, carrying `code` and `message`. For example:

```ts
const result = program.pipe(
  Effect.catchTag("SubstrateFailure", error =>
    error.code === "denied"
      ? Effect.succeed([])
      : Effect.fail(error)
  )
);
```

## Transactions, retries, and lifetime

- A sequence inside `Effect.gen` is **not** a database transaction. Use `db.batch` for an atomic group of writes. The SDK does not expose a transaction callback across network requests.
- No retry policy is installed, including for mutations. A disconnect, timeout, malformed response, or interruption can leave a mutation's commit outcome uncertain. Reconcile the outcome before resubmitting; wrapping arbitrary writes in `Effect.retry` can duplicate them.
- Fiber interruption stops the caller waiting. The current transport does not provide a server cancellation protocol, and an already sent mutation may still commit. Effect interruption is not a rollback or revocation operation.
- The layer manages access to the service: after its scope closes, new operations through an escaped service fail with code `closed`. Clients open and close a connection for each request; there is no connection pool for the layer to dispose. Closing a scope does not revoke a credential or cancel requests already issued.
- Use Effect's ordinary composition, concurrency controls, timeout, tracing, and dependency injection around the service. Choose bounded concurrency appropriate for the host's connection limit. Server authorization, transaction validation, and live revocation remain authoritative.

## Run the example

```sh
npm run demo:effect
```

This creates a temporary durable database, starts a Unix host, runs a separate Effect application, and cleans up. The application writes a typed workout and relationship, performs a nested read, and catches a cross-application access denial through the typed error channel. Socket creation requires normal local network/socket permissions.

`npm run test:ts` includes ten adapter runtime tests plus compile-time inference checks. The tests cover laziness, typed query decoding, contract-bound writes, validation before transport, no implicit retry, scoped service lifetime, malformed responses, and interruption without claiming mutation rollback.

The integration uses Effect 3's [Context and Layer](https://effect.website/docs/v3/requirements-management/managing-layers/) and [fallible promise effects](https://effect.website/docs/v3/getting-started/creating-effects/). Effect 4 is not currently a supported target.
