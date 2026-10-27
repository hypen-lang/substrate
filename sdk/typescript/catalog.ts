import { uuidPattern, type RoleCheck } from "./validation.js";
import type { QueryClient } from "./query.js";
import type { Visibility } from "./schema-control.js";

export interface CatalogOptions { after?: string; expectedRevision?: string; relation?: string; limit?: number }
export interface RoleId { relation: string; slot: number }
export type CatalogType = {kind:"bool"|"int"|"text"|"timestamp"|"bytes"|"float"} | {kind:"decimal";scale:number} | {kind:"ref";target:{id:string;name:string}|null};
export interface CatalogRelation {
  id:string; name:string; fingerprint:string|null; module:{name:string;version:string};
  roles:Array<{id:RoleId;name:string;data_type:CatalogType;required:boolean;equality_index:boolean;on_delete?:"Restrict"|"Cascade";deferrable?:boolean}>;
  unique:RoleId[][];
  checks?:readonly RoleCheck[];
  operations:{read:boolean;insert:boolean;update:boolean;delete:boolean;reference:boolean};
  row_restricted:boolean;
  /** null/omitted denotes a legacy schema with explicit grants only. */
  visibility?:Visibility|null;
  lifecycle:"active"|"deprecated"|"retired"; aliases:string[];
}
export interface CatalogPage { format:1; revision:string; relations:CatalogRelation[]; next:string|null }
const fail=():never=>{throw new Error("invalid catalog response");};
function object(value:unknown):Record<string,unknown> {return value!==null&&typeof value==="object"&&!Array.isArray(value)?value as Record<string,unknown>:fail();}
function string(value:unknown):string{return typeof value==="string"?value:fail();}
function id(value:unknown):string {const s=string(value);return uuidPattern.test(s)?s:fail();}
function bool(value:unknown):boolean{return typeof value==="boolean"?value:fail();}
function list(value:unknown):unknown[]{return Array.isArray(value)?value:fail();}
function uint(value:unknown,max:number):number{return typeof value==="number"&&Number.isSafeInteger(value)&&value>=0&&value<=max?value:fail();}
function revision(value:unknown):string {const s=string(value);return /^(0|[1-9][0-9]*)$/.test(s)&&s.length<=20&&BigInt(s)<=18446744073709551615n?s:fail();}
function roleId(value:unknown):RoleId {const v=object(value);return {relation:id(v.relation),slot:uint(v.slot,4294967295)};}
function lifecycle(value:unknown):"active"|"deprecated"|"retired" {if(value===undefined)return "active";return value==="active"||value==="deprecated"||value==="retired"?value:fail();}
function visibility(value:unknown):Visibility|null {return value===undefined||value===null?null:value==="public"||value==="authenticated"||value==="private"?value:fail();}
function dataType(value:unknown):CatalogType {
  const v=object(value);
  switch(v.kind) {
    case "bool":case "int":case "text":case "timestamp":case "bytes":case "float":return {kind:v.kind};
    case "decimal":return {kind:"decimal",scale:uint(v.scale,18)};
    case "ref":{const t=v.target===null?null:object(v.target);return {kind:"ref",target:t===null?null:{id:id(t.id),name:string(t.name)}};}
    default:return fail();
  }
}
function schemaChecks(value: unknown): RoleCheck[] {
  if (value === undefined) return [];
  const checks = list(value); if (checks.length > 64) fail();
  return checks.map(raw => {
    const entry = object(raw); if (Object.keys(entry).length !== 1) fail();
    const kind = Object.keys(entry)[0]; const body = object(entry[kind]);
    if (Object.keys(body).some(k => !["role", "min", "max"].includes(k))) fail();
    const role = string(body.role);
    if (kind === "IntRange") {
      const parse = (value: unknown) => { const raw = string(value); if (!/^(0|-[1-9][0-9]*|[1-9][0-9]*)$/.test(raw) || raw.length > 20) fail(); const n = BigInt(raw); if (n < -(1n << 63n) || n >= 1n << 63n) fail(); return n; };
      const min = parse(body.min), max = parse(body.max); if (min > max) fail();
      return { kind, role, min, max };
    }
    if (kind === "ByteLength") { const min = uint(body.min, 4294967295), max = uint(body.max, 4294967295); if (min > max) fail(); return { kind, role, min, max }; }
    return fail();
  });
}
/** One fresh policy/data snapshot per call. Pass expectedRevision on subsequent
 * pages to fail on catalog, data or policy changes; restart enumeration on conflict.
 * Operation flags describe grants, not a promise that a particular row is allowed.
 */
export async function introspect(client:QueryClient,options:CatalogOptions={}):Promise<CatalogPage> {
  const limit=options.limit??50;
  if(!Number.isInteger(limit)||limit<1||limit>100)throw new Error("catalog limit must be 1..100");
  if(options.after!==undefined)id(options.after);
  if(options.expectedRevision!==undefined)revision(options.expectedRevision);
  if(options.relation!==undefined)string(options.relation);
  if(options.after!==undefined&&options.relation!==undefined)throw new Error("catalog relation cannot be combined with after");
  const response=object(await client.request({op:"catalog",options:{after:options.after??null,expected_revision:options.expectedRevision??null,relation:options.relation??null,limit}}));
  if(response.kind!=="catalog")fail();
  const page=object(response.page);if(page.format!==1)fail();
  const relations=list(page.relations).map(value=>{
    const r=object(value),m=object(r.module),ops=object(r.operations);
    const result:CatalogRelation={id:id(r.id),name:string(r.name),fingerprint:r.fingerprint===null?null:string(r.fingerprint),module:{name:string(m.name),version:revision(m.version)},
      roles:list(r.roles).map(value=>{
        const v=object(value),kind=dataType(v.data_type);
        const action=v.on_delete===undefined?"Restrict":v.on_delete;
        if (action!=="Restrict" && action!=="Cascade") fail();
        const deferred=v.deferrable===undefined?false:bool(v.deferrable);
        if (kind.kind!=="ref" && (action!=="Restrict" || deferred)) fail();
        return {id:roleId(v.id),name:string(v.name),data_type:kind,required:bool(v.required),equality_index:bool(v.equality_index),...(v.on_delete===undefined?{}:{on_delete:action as "Restrict"|"Cascade"}),...(v.deferrable===undefined?{}:{deferrable:deferred})};
      }),
      ...(r.checks === undefined ? {} : {checks:schemaChecks(r.checks)}),
      ...(r.visibility === undefined ? {} : {visibility:visibility(r.visibility)}),
      unique:list(r.unique).map(key=>list(key).map(roleId)),operations:{read:bool(ops.read),insert:bool(ops.insert),update:bool(ops.update),delete:bool(ops.delete),reference:bool(ops.reference)},row_restricted:bool(r.row_restricted),lifecycle:lifecycle(r.lifecycle),aliases:list(r.aliases??[]).map(string)};
    if((result.fingerprint!==null&&!/^[0-9a-f]{64}$/.test(result.fingerprint))||result.module.version==="0")fail();
    const opaque=result.roles.some(role=>role.data_type.kind==="ref"&&role.data_type.target===null);
    if(opaque!==(result.fingerprint===null))fail();
    if(new Set(result.roles.map(role=>role.name)).size!==result.roles.length)fail();
    result.roles.forEach((role,slot)=>{if(role.id.relation!==result.id||role.id.slot!==slot)fail();});
    for(const key of result.unique)if(!key.length||new Set(key.map(role=>role.slot)).size!==key.length||key.some(role=>role.relation!==result.id||role.slot>=result.roles.length))fail();
    for (const check of result.checks ?? []) {
      const role = result.roles.find(r => r.name === check.role) ?? fail();
      if (check.kind === "IntRange" ? role.data_type.kind !== "int" : !["text", "bytes"].includes(role.data_type.kind)) fail();
    }
    return result;
  });
  const next=page.next===null?null:id(page.next);
  if(relations.length>limit||new Set(relations.map(r=>r.id)).size!==relations.length||(next!==null&&(relations.length!==limit||relations.at(-1)?.id!==next)))fail();
  return {format:1,revision:revision(page.revision),relations,next};
}
