/** Dependency-free validation, structurally compatible with Standard Schema V1. */
export interface ValidationIssue {
  readonly code: "missing" | "unknown_field" | "type" | "constraint";
  readonly path: readonly (string | number)[];
  readonly expected: string;
  readonly message: string;
}
export type ParseResult<T> = { success: true; data: T } | { success: false; issues: readonly ValidationIssue[] };
export interface StandardValidator<T> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly types?: { readonly input: unknown; readonly output: T };
    readonly validate: (input: unknown) => { value: T; issues?: undefined } | { issues: readonly ValidationIssue[] };
  };
}
export interface Validator<T> extends StandardValidator<T> {
  safeParse(input: unknown): ParseResult<T>;
  parse(input: unknown): T;
}
export class ValidationError extends Error {
  readonly code = "validation";
  readonly phase = "validation";
  readonly retryable = false;
  constructor(readonly issues: readonly ValidationIssue[]) {
    super(issues.map(i => `${i.path.join(".") || "input"}: ${i.message}`).join("; "));
    this.name = "ValidationError";
  }
}
export function validator<T>(check: (input: unknown) => ParseResult<T>): Validator<T> {
  return Object.freeze({
    safeParse: check,
    parse(input: unknown): T { const result = check(input); if (result.success === false) throw new ValidationError(result.issues); return result.data; },
    "~standard": { version: 1 as const, vendor: "substrate", validate(input: unknown) { const r = check(input); return r.success === true ? { value: r.data } : { issues: r.issues }; } }
  });
}
export type ScalarKind = "Bool" | "Int" | "Text" | "Ref" | "Decimal" | "Timestamp" | "Bytes";
export type FieldCheck = { readonly kind: "IntRange"; readonly min: bigint; readonly max: bigint } | { readonly kind: "ByteLength"; readonly min: number; readonly max: number };
export type RoleCheck = FieldCheck & { readonly role: string };
export interface FieldSpec { readonly kind: ScalarKind; readonly target?: string; readonly scale?: number; readonly checks?: readonly FieldCheck[] }
export function checkAccepts(check: FieldCheck, value: unknown): boolean {
  switch (check.kind) {
    case "IntRange": return typeof value === "bigint" && value >= check.min && value <= check.max;
    case "ByteLength": { const size = typeof value === "string" ? new TextEncoder().encode(value).length : value instanceof Uint8Array ? value.byteLength : -1; return size >= check.min && size <= check.max; }
    default: return false;
  }
}
export const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function scalarAccepts(kind: ScalarKind, v: unknown, aliases: boolean): boolean {
  switch (kind) {
    case "Bool": return typeof v === "boolean";
    case "Text": return typeof v === "string" && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(v);
    case "Bytes": return v instanceof Uint8Array;
    case "Ref": return typeof v === "string" && (uuidPattern.test(v) || (aliases && /^\$[A-Za-z_][A-Za-z0-9_]*$/.test(v)));
    default: return typeof v === "bigint" && v >= -(1n << 63n) && v < (1n << 63n);
  }
}
/** Object validators are strict, own-property-only and reject explicit undefined.
 * Aliases are enabled only at the batch wire encoder, never for stored reads. */
export function objectValidator<T>(fields: Readonly<Record<string, FieldSpec>>, partial = false, aliases = false): Validator<T> {
  const entries = Object.entries(fields);
  return validator(input => {
    const issues: ValidationIssue[] = [];
    if (input === null || typeof input !== "object" || Array.isArray(input)) return { success: false, issues: [{ code: "type", path: [], expected: "object", message: "expected object" }] };
    const obj = input as Record<string, unknown>;
    for (const key of Object.keys(obj)) if (!Object.hasOwn(fields, key)) issues.push({ code: "unknown_field", path: [key], expected: "declared field", message: "unknown field" });
    for (const [key, spec] of entries) {
      if (!Object.hasOwn(obj, key)) { if (!partial) issues.push({ code: "missing", path: [key], expected: spec.kind, message: "required field is missing" }); }
      else if (!scalarAccepts(spec.kind, obj[key], aliases)) issues.push({ code: "type", path: [key], expected: spec.kind, message: `expected ${spec.kind}${["Int", "Decimal", "Timestamp"].includes(spec.kind) ? " signed 64-bit bigint" : ""}` });
      else for (const check of spec.checks ?? []) if (!checkAccepts(check, obj[key])) issues.push({ code: "constraint", path: [key], expected: check.kind, message: `failed ${check.kind} check` });
    }
    return issues.length ? { success: false, issues } : { success: true, data: input as T };
  });
}
