import type { Descriptor, EntityId, WireValue } from "./generated-preamble.js";
export type Mutation = { op: "insert"; relation: string; schema_id: string; fingerprint: string; fields: Record<string, WireValue>; alias: string | null } | { op: "update"; relation: string; schema_id: string; fingerprint: string; id: string; fields: Record<string, WireValue> } | { op: "delete"; relation: string; schema_id: string; fingerprint: string; id: string };
export type SqlParameter = "Null" | { Integer: string } | { Real: number } | { Text: string } | { Blob: number[] };
export class SubstrateError extends Error { readonly code: string; constructor(code: string, message: string); }
export function link<R extends string>(alias: string): EntityId<R>;
export function insertOp<T, R extends string>(descriptor: Descriptor<T,R>, value: NoInfer<T>, alias?: string): Mutation;
export class LocalClient {
  constructor(socket: string, token: string);
  withCapability(id: string): LocalClient;
  persistent(): LocalClient;
  close(): void;
  request(command: unknown): Promise<unknown>;
  capabilities(): Promise<unknown>;
  batch(operations: Mutation[]): Promise<string[]>;
  insert<T, R extends string>(descriptor: Descriptor<T,R>, value: NoInfer<T>): Promise<EntityId<R>>;
  update<T, R extends string>(descriptor: Descriptor<T,R>, id: EntityId<NoInfer<R>>, value: Partial<NoInfer<T>>): Promise<void>;
  delete<T, R extends string>(descriptor: Descriptor<T,R>, id: EntityId<NoInfer<R>>): Promise<void>;
  page<T, R extends string>(descriptor: Descriptor<T,R>, options?: { after?: EntityId<R>; limit?: number; expectedRevision?: string; filter?: [string,WireValue] }): Promise<{ revision: string; next: EntityId<R> | null; rows: Array<{ id: EntityId<R>; data: T }> }>;
  queryChecked<T>(sql: string, params: SqlParameter[], resultSchema: { readonly "~standard": { readonly version: 1; readonly vendor: string; readonly validate: (input: unknown) => { value: T; issues?: undefined } | { issues: readonly { message: string }[] } | Promise<{ value: T; issues?: undefined } | { issues: readonly { message: string }[] }> } }): Promise<T[]>;
  querySelected(sql: string, params: SqlParameter[], descriptors: ReadonlyArray<{relation: string; schemaId: string; fingerprint: string}>): Promise<{ columns: string[]; rows: Array<Array<null | bigint | number | string | Uint8Array>> }>;
  query(sql: string, params?: SqlParameter[]): Promise<{ columns: string[]; rows: Array<Array<null | bigint | number | string | Uint8Array>> }>;
}

export class RemoteClient extends LocalClient {
  constructor(options: { host: string; port: number; servername: string; ca: string | Buffer }, token: string);
}
