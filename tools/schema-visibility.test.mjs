import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:net';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LocalClient,RemoteClient} from '../target/dx-ts/sdk/typescript/client.mjs';
import {proposeSchema,setSchemaVisibility} from '../target/dx-ts/sdk/typescript/schema-control.js';
import {introspect as catalog} from '../target/dx-ts/sdk/typescript/catalog.js';

const id='00000000-0000-4000-8000-000000000001';
const module={name:'gaming',version:1,imports:[],relations:[{id,name:'gaming.GamerProfile',roles:[],unique:[]}]};
const plan={format:2,digest:'a'.repeat(64),policy_generation:'1',module,visibility:{'gaming.GamerProfile':'authenticated'},usage_after:{},costs:[],references:[],existing_record_rewrites:0,grants_to_owner:['gaming.GamerProfile']};
const proposal={id,owner:'gaming',status:'approved',plan};

test('visibility proposals preserve policy separately from immutable modules',async()=>{
 const sent=[];const client={request:async command=>{sent.push(command);return command.op==='schema_visibility'?{kind:'schema_changed'}:{kind:'schema_proposal',proposal};}};
 await proposeSchema(client,id,module);
 await proposeSchema(client,id,module,{'gaming.GamerProfile':'private'});
 await setSchemaVisibility(client,'gaming.GamerProfile','public');
 assert.deepEqual(sent,[{op:'schema_propose',id,module},{op:'schema_propose_with_visibility',id,module,visibility:{'gaming.GamerProfile':'private'}},{op:'schema_visibility',relation:'gaming.GamerProfile',visibility:'public'}]);
 assert.equal(Object.hasOwn(module.relations[0],'visibility'),false);
 assert.throws(()=>proposeSchema(client,id,module,{'gaming.Unknown':'public'}));
 assert.throws(()=>proposeSchema(client,id,module,{'gaming.GamerProfile':'everybody'}));
 assert.throws(()=>setSchemaVisibility(client,'gaming.GamerProfile','everybody'));
 for(const visibility of [null,{},[],{'gaming.GamerProfile':'wrong'},{'gaming.GamerProfile':'private','gaming.Extra':'public'}]) {
  await assert.rejects(proposeSchema({request:async()=>({kind:'schema_proposal',proposal:{...proposal,plan:{...plan,visibility}}})},id,module));
 }
});

test('catalog visibility accepts legacy absence and rejects unknown policy values',async()=>{
 const relation={id,name:'gaming.GamerProfile',fingerprint:'a'.repeat(64),module:{name:'gaming',version:'1'},roles:[],unique:[],operations:{read:true,insert:false,update:false,delete:false,reference:false},row_restricted:false,lifecycle:'active',aliases:[]};
 for(const visibility of [undefined,null,'public','authenticated','private']) {
  const item={...relation,...(visibility===undefined?{}:{visibility})};
  const result=await catalog({request:async()=>({kind:'catalog',page:{format:1,revision:'1',relations:[item],next:null}})});
  assert.equal(result.relations[0].visibility,visibility);
 }
 await assert.rejects(catalog({request:async()=>({kind:'catalog',page:{format:1,revision:'1',relations:[{...relation,visibility:'wrong'}],next:null}})}));
});

test('anonymous local clients explicitly send no credential on persistent reads',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'substrate-public-'));const path=join(dir,'socket');const requests=[];
 const server=createServer(socket=>{let bytes='';socket.on('data',chunk=>{bytes+=chunk;let end;while((end=bytes.indexOf('\n'))>=0){requests.push(JSON.parse(bytes.slice(0,end)));bytes=bytes.slice(end+1);socket.write(JSON.stringify({status:'ok',value:{kind:'capabilities',relations:[]}})+'\n');}});});
 const client=LocalClient.anonymous(path).persistent();
 try {
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(path,resolve);});
  await client.capabilities();await client.capabilities();
  assert.equal(requests.length,2);assert.ok(requests.every(r=>r.token===''&&r.version===1));
  assert.ok(RemoteClient.anonymous({host:'localhost',port:443,servername:'localhost',ca:'CA'}) instanceof RemoteClient);
 } finally {client.close();await new Promise(resolve=>server.close(resolve));await rm(dir,{recursive:true,force:true});}
});
