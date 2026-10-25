import type {QueryClient} from "./query.js";
import type {DataType} from "./schema.js";
import {uuidPattern} from "./validation.js";
export interface AdditiveModule {
  name:string;version:number;imports:[string,number][];
  relations:Array<{id:string;name:string;roles:Array<{name:string;data_type:DataType}>;unique:string[][];checks?:Array<{IntRange:{role:string;min:string;max:string}}|{ByteLength:{role:string;min:number;max:number}}>} >;
}
export type ProposalStatus="pending"|"approved"|"installed"|"rejected"|"cancelled";
export interface SchemaUsage {relations:number;roles:number;versions:number;schema_bytes:number;proposals:number}
export interface SchemaProposal {
  id:string;owner:string;status:ProposalStatus;
  plan:{format:1;digest:string;policy_generation:string;module:AdditiveModule;usage_after:SchemaUsage;
    costs:Array<{relation:string;role_index_entries_per_row:number;reference_index_entries_per_row:number;unique_index_entries_per_row:number;minimum_packed_put_bytes:number}>;
    references:Array<{relation:string;schema_id:string;fingerprint:string|null}>;
    existing_record_rewrites:0;grants_to_owner:string[]};
}
export interface NamespaceInfo {policy:{namespace:string;owner:string;auto_approve:boolean;enabled:boolean;quotas:SchemaUsage};generation:string;latest_version:string;usage:SchemaUsage}
function identity(id:string){if(!uuidPattern.test(id))throw new Error("expected stable proposal UUID");}
function moduleNumbers(module:AdditiveModule) {
  if(!Number.isSafeInteger(module.version)||module.version<1||module.imports.some(([,v])=>!Number.isSafeInteger(v)||v<1))throw new Error("module versions must be positive safe integers");
  module.relations.forEach(r=>identity(r.id));
}
async function receipt(client:QueryClient,command:unknown):Promise<SchemaProposal> {
  const r=await client.request(command) as {kind:string;proposal:SchemaProposal};
  const p=r?.proposal;
  if(r?.kind!=="schema_proposal"||!p||!uuidPattern.test(p.id)||!["pending","approved","installed","rejected","cancelled"].includes(p.status)||p.plan?.format!==1||! /^[0-9a-f]{64}$/.test(p.plan.digest)||p.plan.existing_record_rewrites!==0)throw new Error("invalid schema proposal response");
  moduleNumbers(p.plan.module);return p;
}
/** Retain the same proposal ID and exact module when reconciling a lost reply.
 * These helpers never silently retry a mutation. The server ledger is idempotent.
 */
export function proposeSchema(client:QueryClient,id:string,module:AdditiveModule):Promise<SchemaProposal> {identity(id);moduleNumbers(module);return receipt(client,{op:"schema_propose",id,module});}
export function schemaProposal(client:QueryClient,id:string):Promise<SchemaProposal> {identity(id);return receipt(client,{op:"schema_proposal",id});}
export function applySchema(client:QueryClient,proposal:SchemaProposal):Promise<SchemaProposal> {identity(proposal.id);return receipt(client,{op:"schema_apply",id:proposal.id,digest:proposal.plan.digest});}
export function cancelSchema(client:QueryClient,id:string):Promise<SchemaProposal> {identity(id);return receipt(client,{op:"schema_cancel",id});}
export async function schemaNamespaces(client:QueryClient):Promise<NamespaceInfo[]> {
  const r=await client.request({op:"schema_namespaces"}) as {kind:string;namespaces:NamespaceInfo[]};
  if(r?.kind!=="schema_namespaces"||!Array.isArray(r.namespaces))throw new Error("invalid namespace response");return r.namespaces;
}
async function changed(client:QueryClient,command:unknown):Promise<void> {const r=await client.request(command) as {kind:string};if(r?.kind!=="schema_changed")throw new Error("invalid schema change response");}
export function createSchemaAlias(client:QueryClient,alias:string,target:string):Promise<void> {return changed(client,{op:"schema_alias",alias,target});}
export function setSchemaLifecycle(client:QueryClient,relation:string,state:"active"|"deprecated"|"retired"):Promise<void> {return changed(client,{op:"schema_lifecycle",relation,state});}
