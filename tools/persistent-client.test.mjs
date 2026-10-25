import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {LocalClient,SubstrateError} from "../sdk/typescript/client.mjs";
async function host(t,onRequest){
 const dir=await mkdtemp(join(tmpdir(),"substrate-persistent-"));const path=join(dir,"socket");const sockets=new Set();let accepted=0;
 const server=net.createServer(socket=>{accepted++;sockets.add(socket);socket.on("error",()=>{});socket.on("close",()=>sockets.delete(socket));let pending="";socket.on("data",bytes=>{pending+=bytes;let end;while((end=pending.indexOf("\n"))>=0){const frame=pending.slice(0,end);pending=pending.slice(end+1);onRequest(JSON.parse(frame),socket);}});});
 await new Promise((resolve,reject)=>{server.once("error",reject);server.listen(path,resolve);});
 t.after(async()=>{for(const s of sockets)s.destroy();await new Promise(resolve=>server.close(resolve));await rm(dir,{recursive:true,force:true});});
 return {path,accepted:()=>accepted};
}
function respond(socket,value){socket.write(JSON.stringify({status:"ok",value})+"\n");}
test("persistent client serializes queued operations on one connection and retains capability",async t=>{
 const id="00000000-0000-4000-8000-000000000001";const commands=[];
 const server=await host(t,(request,socket)=>{commands.push(request);respond(socket,{sequence:commands.length});});
 const client=new LocalClient(server.path,"secret").withCapability(id).persistent();t.after(()=>client.close());
 assert.deepEqual(await Promise.all([client.request({op:"one"}),client.request({op:"two"}),client.request({op:"three"})]),[{sequence:1},{sequence:2},{sequence:3}]);
 assert.equal(server.accepted(),1);assert(commands.every(r=>r.token==="secret"&&r.command.capability===id));
 assert.throws(()=>client.withCapability("00000000-0000-4000-8000-000000000002"));
 client.close();await assert.rejects(client.request({op:"four"}),e=>e.code==="closed");
});
test("persistent queue is bounded and closing rejects pending requests",async t=>{
 const server=await host(t,()=>{});const client=new LocalClient(server.path,"secret").persistent();
 const requests=Array.from({length:16},()=>client.request({op:"wait"}).then(()=>"resolved",e=>e.code));
 await assert.rejects(client.request({op:"overflow"}),e=>e.code==="limit");
 client.close();assert.deepEqual(await Promise.all(requests),Array(16).fill("closed"));
});
test("transport failure is terminal and a mutation is never replayed",async t=>{
 let requests=0;const server=await host(t,(_request,socket)=>{requests++;socket.destroy();});const client=new LocalClient(server.path,"secret").persistent();
 await assert.rejects(client.request({op:"batch",operations:[]}),e=>e instanceof SubstrateError&&e.code==="transport");
 await assert.rejects(client.request({op:"batch",operations:[]}),e=>e.code==="closed");
 assert.equal(requests,1);assert.equal(server.accepted(),1);
});
test("scope denial keeps the session usable, malformed or extra frames close it",async t=>{
 let requests=0;const server=await host(t,(_request,socket)=>{requests++;if(requests===1)socket.write(JSON.stringify({status:"error",value:{code:"denied",message:"access denied"}})+"\n");else respond(socket,{kind:"ok"});});
 const client=new LocalClient(server.path,"secret").persistent();t.after(()=>client.close());
 await assert.rejects(client.request({op:"denied"}),e=>e.code==="denied");assert.deepEqual(await client.request({op:"allowed"}),{kind:"ok"});assert.equal(server.accepted(),1);
 const bad=await host(t,(_request,socket)=>socket.write('{"status":"ok","value":1}\n{"status":"ok","value":2}\n'));
 const broken=new LocalClient(bad.path,"secret").persistent();await assert.rejects(broken.request({op:"bad"}),e=>e.code==="protocol");await assert.rejects(broken.request({op:"again"}),e=>e.code==="closed");
});
