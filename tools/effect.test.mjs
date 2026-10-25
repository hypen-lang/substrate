import test from "node:test";
import assert from "node:assert/strict";
import { Effect, Fiber } from "effect";
import { Substrate, SubstrateFailure, layer } from "../target/dx-ts/sdk/typescript/effect.js";
import { SubstrateError } from "../target/dx-ts/sdk/typescript/client.mjs";
import { defineRecord } from "../target/dx-ts/sdk/typescript/generated-preamble.js";
import { table, select } from "../target/dx-ts/sdk/typescript/query.js";
const id = "00000000-0000-4000-8000-000000000001";
const Person = defineRecord("core.Person", id, "test-fingerprint", {name: {kind:"Text"}});
const run = (client, program) => Effect.runPromise(program.pipe(Effect.provide(layer(client))));

test("Effect requests are lazy and execute once per evaluation", async () => {
  let calls = 0;
  const client = {request: async command => {calls++;return command;}};
  const program = Effect.gen(function*() { const db = yield* Substrate; return yield* db.request({op:"catalog"}); });
  assert.equal(calls, 0);
  assert.deepEqual(await run(client, program), {op:"catalog"});
  assert.equal(calls,1);
});
test("typed SQL query executes without losing its decoder or parameterization", async () => {
  const person = table(Person,"p");
  const query = select({name:person.c.name}).from(person);
  const commands=[];
  const client={request:async command=>{commands.push(command);return {kind:"query",columns:["name"],rows:[[{Text:"Ada"}]]};}};
  const result=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* db.execute(query);}));
  assert.deepEqual(result,[{name:"Ada"}]);assert.equal(commands.length,1);
  assert.equal(commands[0].op,"checked");
});
test("insert preserves descriptor checks and returns a stable identity", async()=>{
  const calls=[];const client={request:async command=>{calls.push(command);return {kind:"batch",ids:[id]};}};
  const result=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* db.insert(Person,{name:"Ada"});}));
  assert.equal(result,id);assert.equal(calls.length,1);
  assert.deepEqual(calls[0].contracts,[{relation:"core.Person",schema_id:id,fingerprint:"test-fingerprint"}]);
  assert.deepEqual(calls[0].command.operations[0].fields,{name:{Text:"Ada"}});
});
test("update and delete accept the empty insertion-ID list and send one batch each", async()=>{
  const commands=[];const client={request:async command=>{commands.push(command);return {kind:"batch",ids:[]};}};
  await run(client,Effect.gen(function*(){const db=yield* Substrate;yield* db.update(Person,id,{name:"Grace"});yield* db.delete(Person,id);}));
  assert.deepEqual(commands.map(c=>c.command.operations[0].op),["update","delete"]);
});
test("invalid local values fail through the typed channel before transport", async()=>{
  let calls=0;const client={request:async()=>{calls++;}};
  const failure=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* Effect.flip(db.insert(Person,{name:42}));}));
  assert.equal(failure._tag,"SubstrateFailure");assert.equal(failure.code,"validation");assert.equal(calls,0);
});
test("server denial and unknown transport outcomes never trigger implicit retries", async()=>{
  for(const code of ["denied","conflict","transport"]){
    let calls=0;const client={request:async()=>{calls++;throw new SubstrateError(code,"failure");}};
    const failure=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* Effect.flip(db.insert(Person,{name:"Ada"}));}));
    assert.equal(failure._tag,"SubstrateFailure");assert.equal(failure.code,code);assert.equal(calls,1);
  }
});
test("catalog/capability helper escape hatch preserves results and catches thrown errors",async()=>{
  const client={request:async()=>({ok:true})};
  const result=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* db.use(async transport=>transport.request({op:"catalog"}));}));
  assert.deepEqual(result,{ok:true});
  const failure=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* Effect.flip(db.use(()=>{throw new Error("invalid descriptor");}));}));
  assert.equal(failure.code,"client");
});
test("closing the layer rejects use of an escaped service",async()=>{
  let calls=0;
  const api=await run({request:async()=>{calls++;}},Substrate);
  const error=await Effect.runPromise(Effect.flip(api.request({op:"catalog"})));
  assert.equal(error.code,"closed");assert.equal(calls,0);
});
test("fiber interruption stops waiting but cannot retract an already issued mutation",async()=>{
  let complete, started;
  const ready=new Promise(resolve=>{started=resolve;});
  let committed=false,calls=0;
  const client={request:async()=>{calls++;started();await new Promise(resolve=>{complete=resolve;});committed=true;return {kind:"batch",ids:[id]};}};
  const program=Effect.gen(function*(){const db=yield* Substrate;return yield* db.insert(Person,{name:"Ada"});}).pipe(Effect.provide(layer(client)));
  const fiber=Effect.runFork(program);
  await ready;
  await Effect.runPromise(Fiber.interrupt(fiber));
  assert.equal(committed,false);
  complete();await new Promise(resolve=>setImmediate(resolve));
  assert.equal(committed,true);assert.equal(calls,1);
});
test("malformed success responses are typed protocol failures without replay",async()=>{
  let calls=0;const client={request:async()=>{calls++;return {kind:"batch",ids:["not-a-uuid"]};}};
  const error=await run(client,Effect.gen(function*(){const db=yield* Substrate;return yield* Effect.flip(db.insert(Person,{name:"Ada"}));}));
  assert.equal(error.code,"protocol");assert.equal(calls,1);
});
