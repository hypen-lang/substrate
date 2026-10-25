import type {QueryClient} from "./query.js";
import type {WireValue} from "./generated-preamble.js";
import {uuidPattern} from "./validation.js";
export interface CapabilityGrant {
  read:boolean;write:boolean;reference:boolean;
  filter:"All"|{Equals:{role:string;value:WireValue}};
}
export interface CapabilityScope {
  relations:Array<{relation:string;grant:CapabilityGrant}>;
  schema_namespace:string|null;
  delegations:number;
}
export interface DelegatedCapability {
  format:1|2;expires_at?:number;id:string;subject:string;issuer:string;authority:string;parent:string|null;
  scope:CapabilityScope;namespace_generation:string|null;
}
function uuid(id:string) { if(typeof id!=="string"||!uuidPattern.test(id))throw new Error("expected capability UUID"); }
function scopeCheck(scope:CapabilityScope) {
  if(!scope||!Array.isArray(scope.relations)||scope.relations.length>64||!Number.isInteger(scope.delegations)||scope.delegations<0||scope.delegations>8)throw new Error("invalid capability bounds");
  const ids=new Set<string>();
  for(const r of scope.relations) {
    uuid(r.relation);if(ids.has(r.relation))throw new Error("duplicate relation");ids.add(r.relation);
    if(!r.grant||[r.grant.read,r.grant.write,r.grant.reference].some(v=>typeof v!=="boolean"))throw new Error("invalid rights");
    if(r.grant.filter!=="All") {
      const f=r.grant.filter?.Equals;
      if(!f||typeof f.role!=="string"||!f.value||Object.keys(f.value).length!==1)throw new Error("invalid row filter");
      const [kind,value]=Object.entries(f.value)[0];
      if(["Int","Timestamp","Decimal"].includes(kind)) {
        if(typeof value!=="string"||! /^-?(0|[1-9][0-9]*)$/.test(value)||value.length>20||BigInt(value)<-(1n<<63n)||BigInt(value)>=(1n<<63n))throw new Error("expected exact i64 policy string");
      }
    }
  }
}
/** Principal credentials authenticate the recipient. IDs alone are not bearer tokens.
 * No transport retries: keep returned IDs; reconcile uncertain delivery explicitly. */
export async function delegateCapability(client:QueryClient,parent:string,id:string,subject:string,scope:CapabilityScope,expiresAt?:number):Promise<DelegatedCapability> {
  uuid(parent);uuid(id);scopeCheck(scope);
  if(!subject||subject.length>128)throw new Error("invalid capability subject");
  if(expiresAt!==undefined&&(!Number.isSafeInteger(expiresAt)||expiresAt<=0||expiresAt>253402300799))throw new Error("invalid expiry: expected Unix seconds");
  const result=await client.request({op:expiresAt===undefined?"delegate_capability":"delegate_capability_until",parent,id,subject,scope,...(expiresAt===undefined?{}:{expires_at:expiresAt})}) as {kind:string;capability:DelegatedCapability};
  const cap=result?.capability;
  if(result?.kind!=="delegated_capability"||![1,2].includes(cap?.format)||cap.id!==id||cap.parent!==parent||cap.subject!==subject)throw new Error("invalid delegated capability response");
  if(cap.expires_at!==undefined&&(!Number.isSafeInteger(cap.expires_at)||cap.expires_at<=0||cap.expires_at>253402300799||cap.format===1))throw new Error("invalid capability expiry response");
  if(expiresAt!==undefined&&cap.expires_at!==expiresAt)throw new Error("capability expiry response mismatch");
  scopeCheck(cap.scope);return cap;
}
export async function revokeCapability(client:QueryClient,id:string):Promise<void> {
  uuid(id);const result=await client.request({op:"revoke_capability",id}) as {kind:string};
  if(result?.kind!=="capability_revoked")throw new Error("invalid revocation response");
}
