/** Authoring syntax only. The Rust compiler validates imports, references and installation. */
import { objectValidator, type Validator, type FieldSpec, type RoleCheck } from "./validation.js";
export type EntityId<R extends string> = string & { readonly __relation: R };
export type DataType = "Bool" | "Int" | "Text" | "Timestamp" | "Bytes" | { Ref: string } | { Decimal: { scale: number } };
export interface Scalar<T> extends FieldSpec { readonly dataType: DataType; readonly _type?: T }
function scalar<T>(kind: Scalar<T>["kind"], dataType: DataType): Scalar<T> { return Object.freeze({ kind, dataType }); }
export const s = {
  bool: () => scalar<boolean>("Bool", "Bool"),
  string: () => scalar<string>("Text", "Text"),
  int64: () => scalar<bigint>("Int", "Int"),
  timestamp: () => scalar<bigint>("Timestamp", "Timestamp"),
  bytes: () => scalar<Uint8Array>("Bytes", "Bytes"),
  decimal(scale: number) { if (!Number.isInteger(scale) || scale < 0 || scale > 18) throw new Error("decimal scale must be 0..18"); return scalar<bigint>("Decimal", { Decimal: { scale } }); }
};
export function ref<R extends string>(name: R): Scalar<EntityId<R>> { return scalar("Ref", { Ref: name }); }
type Shape = Readonly<Record<string, Scalar<unknown>>>;
type Data<F extends Shape> = { [K in keyof F]: F[K] extends Scalar<infer T> ? T : never };
export interface Definition<T> {
  readonly fields: Shape;
  readonly uniqueKeys: readonly (readonly string[])[];
  readonly checks: readonly RoleCheck[];
  readonly insertSchema: Validator<T>;
  readonly updateSchema: Validator<Partial<T>>;
  unique(...roles: (keyof T & string)[]): Definition<T>;
  intRange(role: keyof T & string, min: bigint, max: bigint): Definition<T>;
  /** Text counts UTF-8 bytes, not characters. Bounds are inclusive u32. */
  byteLength(role: keyof T & string, min: number, max: number): Definition<T>;
  /** All roles already have equality indexes; validates the requested access path. */
  index(role: keyof T & string): Definition<T>;
}
function definition<T>(fields: Shape, keys: readonly (readonly string[])[] = [], checks: readonly RoleCheck[] = []): Definition<T> {
  function check(role: string) { if (!Object.hasOwn(fields, role)) throw new Error(`unknown role ${role}`); }
  const checkedFields = Object.fromEntries(Object.entries(fields).map(([name, field]) => [name, {...field, checks: checks.filter(c => c.role === name)}]));
  return Object.freeze({ fields: Object.freeze({ ...fields }), uniqueKeys: keys, checks,
    insertSchema: objectValidator<T>(checkedFields), updateSchema: objectValidator<Partial<T>>(checkedFields, true),
    unique(...roles: string[]) { if (!roles.length) throw new Error("empty unique key"); roles.forEach(check); return definition<T>(fields, [...keys, roles], checks); },
    intRange(role: string, min: bigint, max: bigint) {
      check(role); if (fields[role].kind !== "Int" || typeof min !== "bigint" || typeof max !== "bigint" || min > max || min < -(1n << 63n) || max >= 1n << 63n) throw new Error("invalid IntRange");
      return definition<T>(fields, keys, [...checks, {kind:"IntRange", role, min, max}]);
    },
    byteLength(role: string, min: number, max: number) {
      check(role); if (!["Text", "Bytes"].includes(fields[role].kind) || !Number.isInteger(min) || !Number.isInteger(max) || min < 0 || max > 4294967295 || min > max) throw new Error("invalid ByteLength");
      return definition<T>(fields, keys, [...checks, {kind:"ByteLength", role, min, max}]);
    },
    index(role: string) { check(role); return definition<T>(fields, keys, checks); }
  });
}
export function entity<F extends Shape>(fields: F): Definition<Data<F>> { return definition(fields); }
export const relation = entity;
/** Absence is no occurrence. The target family's layout is never changed. */
export function optionalProperty<R extends string, T>(target: R, value: Scalar<T>): Definition<{ person: EntityId<R>; value: T }> {
  return relation({ person: ref(target), value }).unique("person") as Definition<{ person: EntityId<R>; value: T }>;
}
export type InferEntity<D> = D extends Definition<infer T> ? T : never;
export type InferInsert<D> = InferEntity<D>;
export type InferUpdate<D> = Partial<InferEntity<D>>;
type Definitions = Readonly<Record<string, Definition<any>>>;
export interface Module<E extends Definitions = Definitions, R extends Definitions = Definitions> {
  readonly namespace: string; readonly version: number; readonly imports: Readonly<Record<string, number>>;
  readonly entities: E; readonly relations: R;
}
export function defineModule<E extends Definitions = {}, R extends Definitions = {}>(input: {
  namespace: string; version: number; imports?: Readonly<Record<string, number>>; entities?: E; relations?: R;
}): Module<E, R> {
  for (const key of Object.keys(input)) if (!["namespace", "version", "imports", "entities", "relations"].includes(key)) throw new Error(`unsupported module option ${key}`);
  if (!Number.isSafeInteger(input.version) || input.version < 1) throw new Error("expected positive integer module version");
  return Object.freeze({ namespace: input.namespace, version: input.version, imports: input.imports ?? {}, entities: input.entities ?? {} as E, relations: input.relations ?? {} as R });
}
/** JSON object order is irrelevant. Role arrays are an explicit immutable ABI:
 * TS authors get alphabetical slots; Rust/JSON authors specify that same order. */
export function compileModules(modules: readonly Module[]) {
  const constrained = modules.some(m => [...Object.values(m.entities), ...Object.values(m.relations)].some(d => d.checks.length > 0));
  return { format: constrained ? 2 : 1, modules: [...modules].sort((a,b) => a.namespace < b.namespace ? -1 : a.namespace > b.namespace ? 1 : a.version-b.version).map(m => {
    const names = new Set<string>();
    const definitions = [...Object.entries(m.entities), ...Object.entries(m.relations)];
    return { name: m.namespace, version: m.version, imports: Object.entries(m.imports).sort(([a],[b]) => a < b ? -1 : a > b ? 1 : 0),
      relations: definitions.sort(([a],[b]) => a < b ? -1 : a > b ? 1 : 0).map(([local, def]) => {
        if (names.has(local)) throw new Error(`duplicate relation ${local}`); names.add(local);
        const roles = Object.keys(def.fields).sort().map(name => {
          const field = def.fields[name];
          if (!field || !["Bool", "Int", "Text", "Ref", "Decimal", "Timestamp", "Bytes"].includes(field.kind) || Object.keys(field).some(k => !["kind", "dataType"].includes(k))) throw new Error(`unsupported field declaration ${local}.${name}`);
          return { name, data_type: field.dataType };
        });
        const checks = def.checks.map(c => c.kind === "IntRange" ? {IntRange:{role:c.role,min:c.min.toString(),max:c.max.toString()}} : {ByteLength:{role:c.role,min:c.min,max:c.max}});
        return { name: `${m.namespace}.${local}`, roles, unique: def.uniqueKeys, ...(checks.length ? {checks} : {}) };
      }) };
  }) };
}
