import test from "node:test";
import assert from "node:assert/strict";
import {defineRecord} from "../target/dx-ts/sdk/typescript/generated-preamble.js";
import {entity,s,ref,defineModule,compileModules} from "../target/dx-ts/sdk/typescript/schema.js";
import {table,select,eq} from "../target/dx-ts/sdk/typescript/query.js";
import {introspect} from "../target/dx-ts/sdk/typescript/catalog.js";
import {runtimeDescriptor} from "../target/dx-ts/sdk/typescript/runtime.js";
import {LocalClient} from "../target/dx-ts/sdk/typescript/client.mjs";
const id="00000000-0000-4000-8000-000000000001";
test("finite floats and nullable values validate and round-trip in generated descriptors",()=>{
 const item=defineRecord("app.Item",id,"a".repeat(64),{score:{kind:"Float",nullable:true},name:{kind:"Text"}});
 for(const score of [null,0,-0,1.5,Number.MAX_VALUE]){
   const fields=item.encode({score,name:"test"});
   assert.deepEqual(item.decode({id,fields}).data,{score,name:"test"});
 }
 for(const score of [NaN,Infinity,-Infinity,"3",undefined]) assert.throws(()=>item.encode({score,name:"test"}));
 assert.throws(()=>item.decode({id,fields:{score:{Float:Infinity},name:{Text:"test"}}}));
 assert.throws(()=>item.decode({id,fields:{score:"Null",name:"Null"}}));
 assert.throws(()=>item.encode({name:"test"}));
 const draft=compileModules([defineModule({namespace:"app",version:1,entities:{Item:entity({score:s.nullable(s.float())})}})]);
 assert.deepEqual(draft.modules[0].relations[0].roles[0].data_type,{Nullable:"Float"});
 const t=table(item,"i"),q=select({score:t.c.score}).from(t).where(eq(t.c.score,null)).compile();
 assert.match(q.sql,/IS NULL/);assert.deepEqual(q.params,[{Integer:"100"}]);
});
test("execute uses the explicit write command and validates affected count",async()=>{
 const client=new LocalClient("unused","unused");let command;
 client.request=async value=>{command=value;return {kind:"execute",affected_rows:1,ids:[id]};};
 assert.deepEqual(await client.execute("DELETE FROM app.Item WHERE id=$1",[{Text:id}]),{affectedRows:1,ids:[id]});
 assert.equal(command.op,"execute");assert.deepEqual(command.params,[{Text:id}]);
 for(const invalid of [null,{kind:"execute",affected_rows:1,ids:["bad-id"]},{kind:"execute",affected_rows:2,ids:[id,id]},{kind:"execute",affected_rows:2,ids:[id]},{kind:"execute",affected_rows:-1,ids:[]},{kind:"query",affected_rows:0,ids:[]}]){
   client.request=async()=>invalid;await assert.rejects(client.execute("unused"));
 }
});

test("reference options preserve declaration and authorized runtime metadata",async()=>{
 const parent=s.nullable(ref("tree.Node",{onDelete:"cascade",deferrable:true}));
 const draft=compileModules([defineModule({namespace:"tree",version:1,entities:{Node:entity({parent,score:s.float()})}})]);
 assert.deepEqual(draft.modules[0].relations[0].roles[0],{name:"parent",data_type:{Nullable:{Ref:"tree.Node"}},on_delete:"Cascade",deferrable:true});
 for (const options of [{onDelete:"execute"},{deferrable:"yes"},{unknown:true}]) assert.throws(()=>ref("tree.Node",options));
 assert.throws(()=>compileModules([defineModule({namespace:"tree",version:1,entities:{Bad:entity({score:{...s.float(),onDelete:"cascade"}})}})]));
 const base={id,name:"tree.Node",fingerprint:"a".repeat(64),module:{name:"tree",version:"1"},roles:[{id:{relation:id,slot:0},name:"parent",data_type:{kind:"ref",target:{id,name:"tree.Node"}},required:false,equality_index:true,on_delete:"Cascade",deferrable:true}],unique:[],operations:{read:true,insert:true,update:true,delete:true,reference:true},row_restricted:false,lifecycle:"active",aliases:[]};
 const client=relation=>({request:async()=>({kind:"catalog",page:{format:1,revision:"1",relations:[relation],next:null}})});
 const page=await introspect(client(base));const descriptor=runtimeDescriptor(page.relations[0]);
 assert.equal(descriptor.fields.parent.onDelete,"cascade");assert.equal(descriptor.fields.parent.deferrable,true);assert.equal(descriptor.fields.parent.nullable,true);
 assert.deepEqual(descriptor.encode({parent:null}),{parent:"Null"});
 for(const changes of [{on_delete:"Execute"},{deferrable:"yes"},{data_type:{kind:"float"}}]){
   const bad={...base,roles:[{...base.roles[0],...changes}]};await assert.rejects(introspect(client(bad)),/invalid catalog/);
 }
 const legacy={...base,roles:[{...base.roles[0]}]};delete legacy.roles[0].on_delete;delete legacy.roles[0].deferrable;
 const old=runtimeDescriptor((await introspect(client(legacy))).relations[0]);assert.equal(old.fields.parent.onDelete,"restrict");assert.equal(old.fields.parent.deferrable,false);
});
