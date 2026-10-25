import test from "node:test";
import assert from "node:assert/strict";
import { core_Person, crm_Deal, fitness_Workout, fitness_Performed } from "../target/dx-ts/examples/dx/generated/schema.js";
const id = "00000000-0000-4000-8000-000000000001";
test("generated integer encoders preserve i64 extremes", () => {
  for (const n of [-(1n << 63n), (1n << 63n) - 1n, 9007199254740993n]) {
    const fields = crm_Deal.encode({ amount: n });
    const wire = JSON.parse(JSON.stringify({ id, fields }));
    assert.equal(crm_Deal.decode(wire).data.amount, n);
  }
  assert.throws(() => crm_Deal.encode({ amount: 1n << 63n }));
  assert.throws(() => fitness_Workout.encode({ durationMinutes: 45 }));
});
test("decoders reject malformed rows, unsafe numbers, and identities", () => {
  assert.throws(() => core_Person.decode({ id, fields: {} }));
  assert.throws(() => core_Person.decode({ id: "not-a-uuid", fields: { name: { Text: "Ada" } } }));
  assert.throws(() => crm_Deal.decode({ id, fields: { amount: { Decimal: 9007199254740993 } } }));
  assert.throws(() => crm_Deal.decode({ id, fields: { amount: { Decimal: "9223372036854775808" } } }));
  assert.throws(() => fitness_Performed.decode({ id, fields: { athlete: { Ref: "bad" }, workout: { Ref: id } } }));
});

import { defineModule, entity, relation, optionalProperty, ref, s, compileModules } from "../target/dx-ts/sdk/typescript/schema.js";
import { core, fitness, declarations } from "../target/dx-ts/examples/dx/schema.js";
import { defineRecord } from "../target/dx-ts/examples/dx/generated/schema.js";
import { LocalClient } from "../target/dx-ts/sdk/typescript/client.mjs";
test("strict validators return paths, support partial patches and Standard Schema V1", () => {
  const bad = core_Person.insertSchema.safeParse({ name: 4, extra: true });
  assert.equal(bad.success, false);
  assert.deepEqual(bad.issues.map(i => i.path), [["extra"],["name"]]);
  assert.equal(core_Person.insertSchema.safeParse({}).issues[0].code, "missing");
  assert.equal(core_Person.updateSchema.safeParse({}).success, true);
  assert.equal(core_Person.updateSchema.safeParse({ name: undefined }).success, false);
  assert.equal(core_Person.insertSchema.safeParse(Object.create({name:"inherited"})).success, false);
  assert.throws(() => core_Person.encode({ name: "Ada", extra: true }));
  assert.deepEqual(core_Person.encodePatch({}), {});
  assert.equal(core_Person.insertSchema["~standard"].version, 1);
  assert.deepEqual(core_Person.insertSchema["~standard"].validate({name:"Ada"}), {value:{name:"Ada"}});
  assert.equal(core_Person.insertSchema["~standard"].validate({}).issues[0].code,"missing");
  assert.equal(core_Person.readSchema.safeParse({id,data:{name:"Ada"}}).success,true);
  assert.equal(core_Person.readSchema.safeParse({id,data:{name:4}}).issues[0].path.join("."),"data.name");
});
test("TypeScript authoring inference shares runtime scalar semantics", () => {
  assert.equal(fitness.entities.Workout.insertSchema.safeParse({durationMinutes:12n}).success,true);
  assert.equal(fitness.entities.Workout.insertSchema.safeParse({durationMinutes:12}).success,false);
  const defs = compileModules([defineModule({namespace:"a",version:1,entities:{Thing:entity({z:s.string(),a:s.int64()})}})]);
  const reordered = compileModules([defineModule({namespace:"a",version:1,entities:{Thing:entity({a:s.int64(),z:s.string()})}})]);
  assert.deepEqual(defs,reordered);
  assert.throws(() => s.decimal(19));
  assert.throws(() => defineModule({namespace:"bad",version:1,views:{}}));
  assert.throws(() => entity({a:s.string()}).index("bad"));
  assert.throws(() => compileModules([defineModule({namespace:"a",version:1,entities:{X:entity({x:{...s.string(),minLength:3}})}})]));
});
test("optional properties compile to separate owned relation families", () => {
  const ext = compileModules([defineModule({namespace:"other",version:1,imports:{core:1},relations:{Goal:optionalProperty("core.Person",s.string())}})]);
  assert.equal(ext.modules[0].relations[0].name,"other.Goal");
  assert.deepEqual(ext.modules[0].relations[0].unique,[["person"]]);
  assert.deepEqual(ext.modules[0].relations[0].roles[0].data_type,{Ref:"core.Person"});
  assert.deepEqual(compileModules([core]).modules[0].relations[0].roles,[{name:"name",data_type:"Text"}]);
});
test("all scalar codecs roundtrip exactly and reject invalid external values", () => {
  const d=defineRecord("test.All",id,"test",{bool:{kind:"Bool"},text:{kind:"Text"},bytes:{kind:"Bytes"},time:{kind:"Timestamp"},ref:{kind:"Ref"}});
  const v={bool:true,text:"🙂",bytes:Uint8Array.from([0,255]),time:-9007199254740993n,ref:id};
  assert.deepEqual(d.decode(JSON.parse(JSON.stringify({id,fields:d.encode(v)}))).data,v);
  for (const [key,value] of [["bool",1],["text","\uD800"],["bytes",[1]],["ref","bad"],["time",new Date()]]) assert.throws(()=>d.encode({...v,[key]:value}));
  assert.throws(()=>d.decode({id,fields:{...d.encode(v),unknown:{Text:"bad"}}}));
  assert.throws(()=>d.decode({id,fields:{...d.encode(v),bytes:{Bytes:[256]}}}));
  assert.equal(d.insertSchema.safeParse({...v,ref:"$batch"}).success,false);
  assert.deepEqual(d.encode({...v,ref:"$batch"}).ref,{Ref:"$batch"});
});
test("SQL result contracts validate actual data and reject duplicate aliases", async()=>{
  const client=new LocalClient("unused","unused");
  client.query=async()=>({columns:["name"],rows:[["Ada"]]});
  assert.deepEqual(await client.queryChecked("ignored",[],core_Person.insertSchema),[{name:"Ada"}]);
  client.query=async()=>({columns:["name"],rows:[[42n]]});
  await assert.rejects(client.queryChecked("ignored",[],core_Person.insertSchema),e=>e.code==="validation");
  client.query=async()=>({columns:["name","name"],rows:[["a","b"]]});
  await assert.rejects(client.queryChecked("ignored",[],core_Person.insertSchema),e=>e.code==="query");
});


import { Person, Workout } from "../target/dx-ts/examples/dx/generated/schema.js";
test("clean names alias the same descriptors and preserve canonical identities", () => {
  assert.equal(Person, core_Person);
  assert.equal(Workout, fitness_Workout);
  assert.equal(Person.relation, "core.Person");
  assert.deepEqual(Person.encode({name:"Ada"}), core_Person.encode({name:"Ada"}));
});
