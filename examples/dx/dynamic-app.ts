import assert from "node:assert/strict";
import {randomUUID} from "node:crypto";
import {LocalClient} from "../../sdk/typescript/client.mjs";
import {describe,referenceColumn} from "../../sdk/typescript/runtime.js";
import {schemaNamespaces,proposeSchema,applySchema,schemaProposal,createSchemaAlias,setSchemaLifecycle,type AdditiveModule} from "../../sdk/typescript/schema-control.js";
import {table,read,many,eq} from "../../sdk/typescript/query.js";
import {introspect} from "../../sdk/typescript/catalog.js";
const client=new LocalClient(process.env.SUBSTRATE_SOCKET!,process.env.SUBSTRATE_TOKEN!);
const stranger=new LocalClient(process.env.SUBSTRATE_SOCKET!,process.env.SUBSTRATE_OTHER_TOKEN!);
const namespace=(await schemaNamespaces(client)).find(n=>n.policy.namespace==="agent")!;
assert.equal(namespace.policy.auto_approve,true);
const module:AdditiveModule={name:"agent",version:1,imports:[["core",1]],relations:[{id:randomUUID(),name:"agent.Score",roles:[{name:"person",data_type:{Ref:"core.Person"}},{name:"value",data_type:"Int"}],unique:[["person"]]}]};
const proposal=await proposeSchema(client,randomUUID(),module);
assert.equal(proposal.status,"approved");assert.equal(proposal.plan.existing_record_rewrites,0);
await applySchema(client,proposal);await applySchema(client,proposal);
const score=await describe(client,"agent.Score");
const people=await describe(client,"core.Person");
const person=(await client.page(people)).rows[0].id;
const id=await client.insert(score,{person,value:9007199254740993n});
await client.update(score,id,{value:9007199254740994n});
await assert.rejects(client.insert(score,{person,value:"wrong type"}));
await assert.rejects(describe(stranger,"agent.Score"));
const p=table(people,"p"),s=table(score,"s");
const nested=await read(p).include({scores:many(s,referenceColumn(s,"person")).where(eq(s.c.value,9007199254740994n))}).execute(client);
assert.equal(nested.rows[0].related.scores[0].data.value,9007199254740994n);
assert.equal(nested.stats.sql_statements,2);
await createSchemaAlias(client,"agent.Rating","agent.Score");
assert.equal((await describe(client,"agent.Rating")).schemaId,score.schemaId);
await setSchemaLifecycle(client,"agent.Score","deprecated");
await setSchemaLifecycle(client,"agent.Score","retired");
assert.equal((await introspect(client,{relation:"agent.Rating"})).relations[0].lifecycle,"retired");
assert.equal((await client.page(score)).rows[0].data.value,9007199254740994n);
const pending=await proposeSchema(client,randomUUID(),{name:"reviewed",version:1,imports:[],relations:[{id:randomUUID(),name:"reviewed.Note",roles:[{name:"text",data_type:"Text"}],unique:[]}]});
assert.equal(pending.status,"pending");await assert.rejects(applySchema(client,pending));
// The demo host receives this request over stdout, reviews the exact stored plan,
// and approves through its trusted Database handle. No public approval RPC exists.
console.log(JSON.stringify({review:pending.id,digest:pending.plan.digest}));
const deadline=Date.now()+5000;
while((await schemaProposal(client,pending.id)).status!=="approved") {
  if(Date.now()>deadline)throw new Error("host approval timeout");
  await new Promise(resolve=>setTimeout(resolve,10));
}
await applySchema(client,pending);
await client.insert(await describe(client,"reviewed.Note"),{text:"installed without restart or generated source"});
console.log("PASS: automatic and host-approved install, runtime writes, exact bigint, nested reads, isolation, aliases and lifecycle");
