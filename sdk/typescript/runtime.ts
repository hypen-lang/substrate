import {defineRecord,type Descriptor} from "./generated-preamble.js";
import {introspect,type CatalogRelation} from "./catalog.js";
import type {QueryClient} from "./query.js";
import type {FieldSpec} from "./validation.js";
export type RuntimeScalar=boolean|bigint|string|Uint8Array;
export type RuntimeRecord=Record<string,RuntimeScalar>;
export type RuntimeDescriptor=Descriptor<RuntimeRecord,string>;

/** Runtime validation, not invented compile-time types for an unknown schema.
 * Cached descriptors remain contract-checked and subject to fresh server policy.
 */
export function runtimeDescriptor(relation:CatalogRelation):RuntimeDescriptor {
  if(relation.fingerprint===null)throw new Error("opaque reference domain: cannot construct runtime descriptor");
  const fields:Record<string,FieldSpec>=Object.fromEntries(relation.roles.map(role=>{
    const t=role.data_type;let spec:FieldSpec;
    switch(t.kind) {
      case "ref":if(t.target===null)throw new Error("opaque reference domain");spec={kind:"Ref",target:t.target.name};break;
      case "decimal":spec={kind:"Decimal",scale:t.scale};break;
      case "bool":spec={kind:"Bool"};break;
      case "int":spec={kind:"Int"};break;
      case "text":spec={kind:"Text"};break;
      case "bytes":spec={kind:"Bytes"};break;
      case "timestamp":spec={kind:"Timestamp"};break;
    }
    const checks = (relation.checks ?? []).filter(c => c.role === role.name);
    if (checks.length) spec = {...spec, checks};
    return [role.name,Object.freeze(spec)];
  }));
  return defineRecord<RuntimeRecord,string>(relation.name,relation.id,relation.fingerprint,Object.freeze(fields));
}
export async function describe(client:QueryClient,nameOrAlias:string):Promise<RuntimeDescriptor> {
  const page=await introspect(client,{relation:nameOrAlias});
  if(page.relations.length!==1)throw new Error("expected one schema descriptor");
  return runtimeDescriptor(page.relations[0]);
}

import {Column,type Table} from "./query.js";
import type {EntityId} from "./generated-preamble.js";
/** A runtime-checked reference column for nested plans discovered dynamically. */
export function referenceColumn(table:Table<RuntimeRecord,string>,name:string):Column<EntityId<string>,string> {
  const column=table.c[name];
  if(!column||column.spec.kind!=="Ref"||name==="id")throw new Error("expected a reference role");
  return column as Column<EntityId<string>,string>;
}
