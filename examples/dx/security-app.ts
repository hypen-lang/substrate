import assert from "node:assert/strict";
import {randomUUID} from "node:crypto";
import {LocalClient} from "../../sdk/typescript/client.mjs";
import {delegateCapability,revokeCapability,type CapabilityScope} from "../../sdk/typescript/capability.js";
import {describe,referenceColumn} from "../../sdk/typescript/runtime.js";
import {proposeSchema,applySchema,schemaProposal,type AdditiveModule} from "../../sdk/typescript/schema-control.js";
import {table,read,many} from "../../sdk/typescript/query.js";
const socket=process.env.SUBSTRATE_SOCKET!;
const owner=new LocalClient(socket,process.env.OWNER_TOKEN!);
const worker=new LocalClient(socket,process.env.WORKER_TOKEN!);
const reader=new LocalClient(socket,process.env.READER_TOKEN!);
const rights:CapabilityScope={relations:[
  {relation:process.env.PERSON_TYPE!,grant:{read:true,write:false,reference:true,filter:{Equals:{role:"name",value:{Text:"Ada"}}}}},
  {relation:process.env.ENTRY_TYPE!,grant:{read:true,write:true,reference:false,filter:{Equals:{role:"person",value:{Ref:process.env.ADA!}}}}}
],schema_namespace:"app",delegations:1};
const cap=await delegateCapability(owner,process.env.ROOT_CAPABILITY!,randomUUID(),"worker",rights);
const scoped=worker.withCapability(cap.id);
await assert.rejects(describe(reader.withCapability(cap.id),"core.Person"));
await assert.rejects(describe(worker,"core.Person")); // selection does not mint ambient grants
const people=await describe(scoped,"core.Person"),entry=await describe(scoped,"app.Entry");
assert.equal((await scoped.page(people)).rows.length,1);
const id=await scoped.insert(entry,{person:process.env.ADA!,value:9007199254740993n});
await scoped.update(entry,id,{value:9007199254740994n});
await assert.rejects(scoped.insert(entry,{person:process.env.BOB!,value:1n}));
const p=table(people,"p"),e=table(entry,"e");
const query=read(p).include({entries:many(e,referenceColumn(e,"person"))});
assert.equal((await query.execute(scoped)).rows[0].related.entries[0].data.value,9007199254740994n);
const childRights:CapabilityScope={...rights,schema_namespace:null,delegations:0,relations:rights.relations.map(r=>({...r,grant:{...r.grant,write:false}}))};
const leaf=await delegateCapability(scoped,cap.id,randomUUID(),"reader",childRights);
const viewer=reader.withCapability(leaf.id);
assert.equal((await query.execute(viewer)).rows.length,1);
await assert.rejects(viewer.update(entry,id,{value:0n}));
const module:AdditiveModule={name:"app",version:2,imports:[["core",1]],relations:[{id:randomUUID(),name:"app.NewNote",roles:[{name:"person",data_type:{Ref:"core.Person"}},{name:"text",data_type:"Text"}],unique:[]}]};
const proposal=await proposeSchema(scoped,randomUUID(),module);assert.equal(proposal.owner,"owner");
await assert.rejects(applySchema(scoped,proposal));
console.log(JSON.stringify({review:proposal.id,digest:proposal.plan.digest}));
const deadline=Date.now()+5000;
while((await schemaProposal(scoped,proposal.id)).status!=="approved") {if(Date.now()>deadline)throw new Error("approval timeout");await new Promise(r=>setTimeout(r,10));}
await applySchema(scoped,proposal);
await assert.rejects(describe(scoped,"app.NewNote")); // new grants belong to owner, outside the fixed scope
assert.equal((await describe(owner,"app.NewNote")).relation,"app.NewNote");
await revokeCapability(owner,cap.id);
await assert.rejects(scoped.page(entry));await assert.rejects(query.execute(viewer));await assert.rejects(scoped.update(entry,id,{value:0n}));
await assert.rejects(applySchema(scoped,proposal));
assert.equal((await owner.page(entry)).rows[0].data.value,9007199254740994n);
console.log("PASS: subject-bound delegation, attenuated row filters, exact runtime writes, nested reads, approved schema installation and cascading live revocation");
