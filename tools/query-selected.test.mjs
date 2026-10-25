import test from "node:test";
import assert from "node:assert/strict";
import {LocalClient} from "../sdk/typescript/client.mjs";
test("querySelected binds explicit contracts and preserves exact SQL decoding",async()=>{
 const client=new LocalClient("/unused","unused");let command;
 client.request=async value=>{command=value;return {kind:"query",columns:["n"],rows:[[{Integer:"9007199254740993"}]]};};
 const descriptor={relation:"core.Person",schemaId:"00000000-0000-4000-8000-000000000001",fingerprint:"contract"};
 assert.deepEqual(await client.querySelected("SELECT 9007199254740993 AS n",[],[descriptor]),{columns:["n"],rows:[[9007199254740993n]]});
 assert.deepEqual(command,{op:"checked",contracts:[{relation:descriptor.relation,schema_id:descriptor.schemaId,fingerprint:descriptor.fingerprint}],command:{op:"query",sql:"SELECT 9007199254740993 AS n",params:[]}});
});
