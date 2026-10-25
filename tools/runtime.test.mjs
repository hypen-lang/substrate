import test from "node:test";
import assert from "node:assert/strict";
import {runtimeDescriptor,describe,referenceColumn} from "../target/dx-ts/sdk/typescript/runtime.js";
import {table,read,many} from "../target/dx-ts/sdk/typescript/query.js";
import {proposeSchema,applySchema,cancelSchema,schemaProposal,schemaNamespaces} from "../target/dx-ts/sdk/typescript/schema-control.js";
const id="00000000-0000-4000-8000-000000000001";
const owner="00000000-0000-4000-8000-000000000002";
const role=(name,data_type,slot)=>({name,data_type,id:{relation:id,slot},required:true,equality_index:true});
const catalog={id,name:"agent.Score",fingerprint:"a".repeat(64),module:{name:"agent",version:"1"},roles:[role("owner",{kind:"ref",target:{id:owner,name:"core.Person"}},0),role("n",{kind:"int"},1),role("decimal",{kind:"decimal",scale:2},2),role("time",{kind:"timestamp"},3),role("bytes",{kind:"bytes"},4),role("active",{kind:"bool"},5),role("text",{kind:"text"},6)],unique:[],operations:{read:true,insert:true,update:true,delete:true,reference:true},row_restricted:false,lifecycle:"active",aliases:[]};
test("runtime descriptors validate exact scalar domains without generated source",()=>{
 const d=runtimeDescriptor(catalog),value={owner,n:9007199254740993n,decimal:-42n,time:-99n,bytes:Uint8Array.of(0,255),active:true,text:"hello"};
 assert.deepEqual(d.decode({id,fields:d.encode(value)}).data,value);assert.deepEqual(d.encodePatch({n:2n}),{n:{Int:"2"}});
 assert.throws(()=>d.encode({...value,n:42}));assert.throws(()=>d.encode({...value,unknown:1n}));assert.throws(()=>d.encodePatch({owner:"wrong"}));assert.throws(()=>d.encodePatch({n:1n<<63n}));
 assert.equal(d.fields.decimal.scale,2);assert.equal(d.fields.owner.target,"core.Person");
});
test("opaque descriptors cannot create clients and dynamic reference roles are checked",()=>{
 assert.throws(()=>runtimeDescriptor({...catalog,fingerprint:null}),/opaque/);
 assert.throws(()=>runtimeDescriptor({...catalog,roles:[role("owner",{kind:"ref",target:null},0)]}),/opaque/);
 const t=table(runtimeDescriptor(catalog),"s");assert.throws(()=>referenceColumn(t,"n"));assert.throws(()=>referenceColumn(t,"id"));assert.throws(()=>referenceColumn(t,"missing"));
 const parent=table(runtimeDescriptor({...catalog,id:owner,name:"core.Person",roles:[]}),"p");
 const plan=read(parent).include({scores:many(t,referenceColumn(t,"owner"))}).plan();assert.equal(plan.include.scores.role,"owner");
});
test("discovery resolves canonical descriptors with lifecycle and aliases",async()=>{
 const d=await describe({request:async command=>{assert.equal(command.options.relation,"agent.Rating");return {kind:"catalog",page:{format:1,revision:"1",relations:[{...catalog,lifecycle:"retired",aliases:["agent.Rating"]}],next:null}};}},"agent.Rating");
 assert.equal(d.relation,"agent.Score");assert.equal(d.schemaId,id);
 await assert.rejects(describe({request:async()=>({kind:"catalog",page:{format:1,revision:"1",relations:[],next:null}})},"missing"));
});
const module={name:"agent",version:1,imports:[],relations:[{id,name:"agent.Score",roles:[],unique:[]}]};
const proposal={id,owner:"agent",status:"approved",plan:{format:1,digest:"a".repeat(64),policy_generation:"1",module,usage_after:{relations:1,roles:0,versions:1,schema_bytes:100,proposals:1},costs:[],references:[],existing_record_rewrites:0,grants_to_owner:["agent.Score"]}};
test("schema helpers preserve stable request IDs and exact approval digest",async()=>{
 const commands=[];const client={request:async command=>{commands.push(command);return {kind:"schema_proposal",proposal};}};
 await proposeSchema(client,id,module);await applySchema(client,proposal);await schemaProposal(client,id);await cancelSchema(client,id);
 assert.deepEqual(commands,[{op:"schema_propose",id,module},{op:"schema_apply",id,digest:proposal.plan.digest},{op:"schema_proposal",id},{op:"schema_cancel",id}]);
 let calls=0;await assert.rejects(applySchema({request:async()=>{calls++;throw new Error("disconnect");}},proposal));assert.equal(calls,1);
});
test("schema helpers reject invalid identity versions and server responses",async()=>{
 const client={request:async()=>({kind:"schema_proposal",proposal})};
 assert.throws(()=>proposeSchema(client,"bad",module));assert.throws(()=>proposeSchema(client,id,{...module,version:2**54}));assert.throws(()=>proposeSchema(client,id,{...module,relations:[{...module.relations[0],id:"bad"}]}));
 await assert.rejects(applySchema({request:async()=>({kind:"schema_proposal",proposal:{...proposal,plan:{...proposal.plan,format:2}}})},proposal));
 await assert.rejects(schemaNamespaces({request:async()=>({kind:"query",rows:[]})}));
});
