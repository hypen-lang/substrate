import test from "node:test";
import assert from "node:assert/strict";
import {table,select,eq,gt,and,not,read,many,one,asc,desc} from "../target/dx-ts/sdk/typescript/query.js";
import {core_Person,fitness_Performed,fitness_Workout,crm_Deal,crm_CustomerDeal,defineRecord} from "../target/dx-ts/examples/dx/generated/schema.js";
const p=table(core_Person,"p"),f=table(fitness_Performed,"f"),w=table(fitness_Workout,"w");
const id="00000000-0000-4000-8000-000000000001";
test("query builder binds values, quotes identifiers, and preserves join parameter order",()=>{
 const q=select({'name"alias':p.c.name,minutes:w.c.durationMinutes}).from(p).innerJoin(f,eq(f.c.athlete,p.c.id)).innerJoin(w,and(eq(f.c.workout,w.c.id),gt(w.c.durationMinutes,10n))).where(eq(p.c.name,"' OR 1=1 --")).orderBy(desc(w.c.durationMinutes)).limit(2);
 const {sql,params,contracts}=q.compile();assert.ok(!sql.includes("OR 1=1"));assert.match(sql,/AS "name""alias"/);assert.match(sql,/INNER JOIN "fitness"."Performed"/);
 assert.deepEqual(params,[{Integer:"10"},{Text:"' OR 1=1 --"},{Integer:"2"}]);assert.equal(contracts.length,3);
});
test("queries validate binding scope, domains, aliases and scalar ranges at runtime",()=>{
 assert.throws(()=>select({n:w.c.durationMinutes}).from(p).compile(),/unbound/);
 assert.throws(()=>select({name:p.c.name}).from(p).innerJoin(table(core_Person,"p"),eq(p.c.id,id)).compile(),/duplicate/);
 assert.throws(()=>select({name:p.c.name}).from(p).innerJoin(f,eq(f.c.workout,w.c.id)).innerJoin(w,eq(w.c.id,f.c.workout)).compile(),/unbound/);
 assert.throws(()=>eq(f.c.athlete,w.c.id),/domains/);assert.throws(()=>eq(w.c.durationMinutes,1),/bigint/);
 assert.throws(()=>gt(f.c.athlete,id));assert.throws(()=>eq(w.c.durationMinutes,1n<<63n));
 assert.throws(()=>select({name:p.c.name}).from(p).limit(0));
 const d=table(crm_Deal,"d");assert.throws(()=>eq(d.c.amount,w.c.durationMinutes),/domains/);
});
test("selected results decode exact scalar domains and reject malformed output",async()=>{
 const query=select({name:p.c.name,minutes:w.c.durationMinutes}).from(p).innerJoin(w,gt(w.c.durationMinutes,0n));
 let calls=0;const client={request:async command=>{calls++;assert.equal(command.op,"checked");return {kind:"query",columns:["name","minutes"],rows:[[{Text:"Ada"},{Integer:"9007199254740993"}]]};}};
 assert.deepEqual(await query.execute(client),[{name:"Ada",minutes:9007199254740993n}]);assert.equal(calls,1);
 await assert.rejects(query.execute({request:async()=>({kind:"query",columns:["name","minutes"],rows:[[{Text:"Ada"},{Integer:"9223372036854775808"}]]})}));
 await assert.rejects(query.execute({request:async()=>({kind:"query",columns:["different"],rows:[]})}));
});
test("nested plan is one request, explicitly scoped, with per-parent options",async()=>{
 const q=read(p).where(not(eq(p.c.name,"excluded"))).orderBy(asc(p.c.id)).limit(20).include({performed:many(f,f.c.athlete).limit(2).include({workout:one(w,f.c.workout).where(gt(w.c.durationMinutes,30n))})});
 const plan=q.plan();assert.equal(plan.include.performed.plan.limit,2);assert.equal(plan.include.performed.plan.include.workout.direction,"one");
 let calls=0;const response=await q.execute({request:async command=>{calls++;assert.equal(command.op,"read");return {kind:"read",result:{revision:"1",rows:[{row:{id,fields:{name:{Text:"Ada"}}},related:{performed:{kind:"many",value:[]}}}],stats:{sql_statements:2,fetched_rows:1,returned_nodes:1}}};}});
 assert.equal(calls,1);assert.deepEqual(response.rows[0],{id,data:{name:"Ada"},related:{performed:[]}});
 assert.throws(()=>read(w).include({performed:many(f,f.c.athlete)}).plan(),/different parent/);
 assert.throws(()=>read(p).where(gt(w.c.durationMinutes,3n)).plan(),/own table/);
 assert.throws(()=>read(p).where(eq(p.c.name,p.c.name)).plan(),/field to a value/);
 assert.throws(()=>one(w,f.c.workout).limit(2));
});
test("nested result decoders reject foreign fields and branch/cardinality drift",async()=>{
 const q=read(p).include({performed:many(f,f.c.athlete)});
 const client=row=>({request:async()=>({kind:"read",result:{revision:"1",rows:[row],stats:{sql_statements:1,fetched_rows:1,returned_nodes:1}}})});
 await assert.rejects(q.execute(client({row:{id,fields:{name:{Text:"Ada"},extra:{Int:"2"}}},related:{performed:{kind:"many",value:[]}}})));
 await assert.rejects(q.execute(client({row:{id,fields:{name:{Text:"Ada"}}},related:{performed:{kind:"one",value:null}}})));
});

test("projection decoders preserve bool, bytes, timestamp and decimal domains",async()=>{
 const descriptor=defineRecord("test.All",id,"fixture",{active:{kind:"Bool"},payload:{kind:"Bytes"},time:{kind:"Timestamp"},amount:{kind:"Decimal",scale:2},owner:{kind:"Ref",target:"core.Person"}});
 const t=table(descriptor,"t"),q=select({active:t.c.active,payload:t.c.payload,time:t.c.time,amount:t.c.amount,owner:t.c.owner}).from(t);
 const result={kind:"query",columns:["active","payload","time","amount","owner"],rows:[[{Integer:"1"},{Blob:[0,255]},{Integer:"-9007199254740993"},{Integer:"9007199254740993"},{Text:id}]]};
 assert.deepEqual(await q.execute({request:async()=>result}),[{active:true,payload:Uint8Array.from([0,255]),time:-9007199254740993n,amount:9007199254740993n,owner:id}]);
 result.rows[0][0]={Integer:"2"};await assert.rejects(q.execute({request:async()=>result}));
 const uppercase="AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA";
 assert.deepEqual(select({id:p.c.id}).from(p).where(eq(p.c.id,uppercase)).compile().params[0],{Text:uppercase.toLowerCase()});
});
test("explain submits parameterized plan introspection in one checked request",async()=>{
 let seen;const q=select({id:p.c.id}).from(p).where(eq(p.c.name,"Ada"));
 const result=await q.explain({request:async c=>{seen=c;return {kind:"query",columns:[],rows:[]};}});
 assert.ok(seen.command.sql.startsWith("EXPLAIN SELECT"));assert.equal(seen.contracts[0].schema_id,core_Person.schemaId);assert.deepEqual(result.params,[{Text:"Ada"},{Integer:"100"}]);
});
