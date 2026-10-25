/** Effect 3 integration. Every method is lazy and failures stay in the typed channel.
 * A sequence of effects is NOT a database transaction: use batch for atomic writes.
 * No retries are installed. Interruption stops waiting, not server-side execution.
 */
import { Context, Data, Effect, Layer } from "effect";
import { insertOp, SubstrateError, type Mutation } from "./client.mjs";
import type { Descriptor, EntityId } from "./generated-preamble.js";
import { ValidationError, uuidPattern } from "./validation.js";
import type { QueryClient } from "./query.js";

export class SubstrateFailure extends Data.TaggedError("SubstrateFailure")<{
  readonly code: string;
  readonly message: string;
}> {}

function failure(error: unknown): SubstrateFailure {
  if (error instanceof SubstrateFailure) return error;
  return new SubstrateFailure({
    code: error instanceof SubstrateError || error instanceof ValidationError ? error.code : "client",
    message: error instanceof Error ? error.message : "Substrate operation failed",
  });
}
export interface Executable<A> { execute(client: QueryClient): Promise<A> }
export interface SubstrateApi {
  /** Escape hatch for catalog, runtime descriptors and capability helpers. */
  readonly use: <A>(operation: (client: QueryClient) => Promise<A>) => Effect.Effect<A, SubstrateFailure>;
  readonly request: (command: unknown) => Effect.Effect<unknown, SubstrateFailure>;
  /** Accepts both typed SQL selects and batched nested relationship reads. */
  readonly execute: <A>(query: Executable<A>) => Effect.Effect<A, SubstrateFailure>;
  readonly batch: (operations: readonly Mutation[]) => Effect.Effect<string[], SubstrateFailure>;
  readonly insert: <T, R extends string>(descriptor: Descriptor<T, R>, value: NoInfer<T>) => Effect.Effect<EntityId<R>, SubstrateFailure>;
  readonly update: <T, R extends string>(descriptor: Descriptor<T, R>, id: EntityId<NoInfer<R>>, value: Partial<NoInfer<T>>) => Effect.Effect<void, SubstrateFailure>;
  readonly delete: <T, R extends string>(descriptor: Descriptor<T, R>, id: EntityId<NoInfer<R>>) => Effect.Effect<void, SubstrateFailure>;
}
export class Substrate extends Context.Tag("substrate/Client")<Substrate, SubstrateApi>() {}

function managed(client: QueryClient): { api: SubstrateApi; close: () => void } {
  let closed = false;
  const use = <A>(operation: (client: QueryClient) => Promise<A>): Effect.Effect<A, SubstrateFailure> => Effect.tryPromise({
    try: async () => {
      if (closed) throw new SubstrateFailure({ code: "closed", message: "Substrate layer scope has closed" });
      return operation(client);
    },
    catch: failure,
  });
  const batch = (operations: readonly Mutation[]): Effect.Effect<string[], SubstrateFailure> => use(async client => {
    const contracts = operations.map(op => ({ relation: op.relation, schema_id: op.schema_id, fingerprint: op.fingerprint }));
    const wire = operations.map(({ fingerprint, ...op }) => op);
    const output = await client.request({ op: "checked", contracts, command: { op: "batch", operations: wire } }) as { kind?: unknown; ids?: unknown };
    if (output?.kind !== "batch" || !Array.isArray(output.ids) || output.ids.length !== operations.filter(op => op.op === "insert").length || !output.ids.every(id => typeof id === "string" && uuidPattern.test(id))) {
      throw new SubstrateError("protocol", "invalid batch response; mutation outcome may be uncertain");
    }
    return output.ids;
  });
  // Encoding is deferred too, so local validation errors remain in the Effect error channel.
  const api: SubstrateApi = {
    use,
    request: command => use(client => client.request(command)),
    execute: query => use(client => query.execute(client)),
    batch,
    insert: (descriptor, value) => Effect.flatMap(use(async () => insertOp(descriptor, value)), operation =>
      Effect.map(batch([operation]), ids => ids[0] as EntityId<typeof descriptor.relation>)),
    update: (descriptor, id, value) => Effect.flatMap(use(async () => ({ op: "update" as const, relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint, id, fields: descriptor.encodePatch(value) })), operation => Effect.asVoid(batch([operation]))),
    delete: (descriptor, id) => Effect.asVoid(batch([{ op: "delete", relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint, id }])),
  };
  return { api: Object.freeze(api), close: () => { closed = true; } };
}

/** Scoped service for an existing LocalClient, RemoteClient or compatible transport.
 * Clients use one connection per request, so this layer owns no persistent socket.
 * Scope closure disables new operations through this service. Already issued requests
 * may complete on the server, including mutations. It does not revoke credentials.
 */
export function layer(client: QueryClient): Layer.Layer<Substrate> {
  return Layer.scoped(Substrate, Effect.map(
    Effect.acquireRelease(Effect.sync(() => managed(client)), resource => Effect.sync(resource.close)),
    resource => resource.api,
  ));
}
