import test from "node:test";
import assert from "node:assert/strict";
import {delegateCapability,revokeCapability} from "../target/dx-ts/sdk/typescript/capability.js";
import {LocalClient} from "../sdk/typescript/client.mjs";
const parent="00000000-0000-4000-8000-000000000001",id="00000000-0000-4000-8000-000000000002";
const scope={relations:[{relation:parent,grant:{read:true,write:false,reference:false,filter:{Equals:{role:"value",value:{Int:"9007199254740993"}}}}}],schema_namespace:null,delegations:0};
const capability={format:1,id,parent,subject:"worker",issuer:"owner",authority:"owner",scope,namespace_generation:null};
test("delegation preserves exact filter values and explicit identity without retries",async()=>{
 const commands=[];const client={request:async command=>{commands.push(command);return {kind:"delegated_capability",capability};}};
 assert.deepEqual(await delegateCapability(client,parent,id,"worker",scope),capability);
 assert.equal(commands.length,1);assert.equal(commands[0].scope.relations[0].grant.filter.Equals.value.Int,"9007199254740993");
 let calls=0;await assert.rejects(delegateCapability({request:async()=>{calls++;throw new Error("disconnect");}},parent,id,"worker",scope));assert.equal(calls,1);
});
test("capability bounds unsafe integer filters and altered response identity are rejected",async()=>{
 const client={request:async()=>({kind:"delegated_capability",capability})};
 await assert.rejects(delegateCapability(client,parent,id,"worker",{...scope,delegations:9}));
 await assert.rejects(delegateCapability(client,"invalid",id,"worker",scope));
 const unsafe=structuredClone(scope);unsafe.relations[0].grant.filter.Equals.value.Int=9007199254740993;
 await assert.rejects(delegateCapability(client,parent,id,"worker",unsafe));
 const tooLarge=structuredClone(scope);tooLarge.relations[0].grant.filter.Equals.value.Int="9223372036854775808";
 await assert.rejects(delegateCapability(client,parent,id,"worker",tooLarge));
 await assert.rejects(delegateCapability({request:async()=>({kind:"delegated_capability",capability:{...capability,subject:"other"}})},parent,id,"worker",scope));
});
test("revocation sends only the capability ID and validates acknowledgment",async()=>{
 await revokeCapability({request:async command=>{assert.deepEqual(command,{op:"revoke_capability",id});return {kind:"capability_revoked"};}},id);
 await assert.rejects(revokeCapability({request:async()=>({kind:"schema_changed"})},id));
});
test("selected client retains typed operations and cannot switch to another chain",()=>{
 const client=new LocalClient("/unused","token"),selected=client.withCapability(id);
 assert.notEqual(client,selected);assert.equal(typeof selected.insert,"function");assert.equal(typeof selected.page,"function");
 assert.throws(()=>selected.withCapability(parent));assert.throws(()=>client.withCapability("invalid"));assert.doesNotThrow(()=>selected.withCapability(id));
});
test("v2 delegation sends exact seconds and validates deadline bounds",async()=>{
 const end=1900000000;let sent;
 const client={request:async command=>{sent=command;return {kind:"delegated_capability",capability:{...capability,format:2,expires_at:end}};}};
 assert.equal((await delegateCapability(client,parent,id,"worker",scope,end)).expires_at,end);
 assert.equal(sent.op,"delegate_capability_until");assert.equal(sent.expires_at,end);
 for(const invalid of [0,-1,1.5,253402300800,Number.MAX_SAFE_INTEGER+1])await assert.rejects(delegateCapability(client,parent,id,"worker",scope,invalid));
});
test("deadline response cannot silently extend or omit an explicit expiry",async()=>{
 for(const expires_at of [undefined,1900000001,1.5,0]){
  const client={request:async()=>({kind:"delegated_capability",capability:{...capability,format:2,expires_at}})};
  await assert.rejects(delegateCapability(client,parent,id,"worker",scope,1900000000));
 }
});
