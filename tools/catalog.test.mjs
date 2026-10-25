import test from "node:test";
import assert from "node:assert/strict";
import {introspect} from "../target/dx-ts/sdk/typescript/catalog.js";
const id="00000000-0000-4000-8000-000000000001";
const relation={id,name:"app.Link",fingerprint:null,module:{name:"app",version:"1"},roles:[{id:{relation:id,slot:0},name:"owner",data_type:{kind:"ref",target:null},required:true,equality_index:true}],unique:[[{relation:id,slot:0}]],operations:{read:true,insert:false,update:false,delete:false,reference:false},row_restricted:true,lifecycle:"active",aliases:[]};
const page={format:1,revision:"9007199254740993",relations:[relation],next:id};
test("catalog sends bounded revision-guarded options and preserves opaque domains",async()=>{
 let sent;const client={request:async command=>{sent=command;return {kind:"catalog",page};}};
 const got=await introspect(client,{limit:1,expectedRevision:"9007199254740993"});
 assert.deepEqual(got,page);assert.equal(got.relations[0].roles[0].data_type.target,null);
 assert.deepEqual(sent,{op:"catalog",options:{limit:1,expected_revision:"9007199254740993",after:null,relation:null}});
});
test("catalog validates pages and metadata before returning descriptors",async()=>{
 const run=page=>introspect({request:async()=>({kind:"catalog",page})},{limit:1});
 for(const malformed of [{...page,format:2},{...page,revision:"18446744073709551616"},{...page,next:"bad"},{...page,relations:[{...relation,fingerprint:"bad"}]},{...page,relations:[{...relation,roles:[{...relation.roles[0],id:{relation:id,slot:7}}]}]},{...page,relations:[{...relation,unique:[[{relation:id,slot:2}]]}]},{...page,relations:[{...relation,roles:[{...relation.roles[0],data_type:{kind:"ref",target:{id:"bad",name:"x"}}}]}]}])await assert.rejects(run(malformed));
});
test("catalog rejects invalid options before invoking transport",async()=>{
 const client={request:async()=>{assert.fail("must not send");}};
 for(const options of [{limit:0},{limit:101},{after:"bad"},{expectedRevision:"-1"},{relation:"app.Link",after:id}])await assert.rejects(introspect(client,options));
});
