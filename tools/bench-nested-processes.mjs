// Real Node -> Unix socket -> Rust host measurement; no injected network delay.
import {mkdtemp,readFile,rm,writeFile,mkdir} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
import {spawn} from "node:child_process";
import {once} from "node:events";
import {performance} from "node:perf_hooks";
import assert from "node:assert/strict";
import {LocalClient} from "../target/dx-ts/sdk/typescript/client.mjs";
import {table,read,many,one,asc} from "../target/dx-ts/sdk/typescript/query.js";
import {core_Person,fitness_Performed,fitness_Workout} from "../target/dx-ts/examples/dx/generated/schema.js";
function run(command,args) {return new Promise((resolve,reject)=>{const child=spawn(command,args,{stdio:["ignore","ignore","inherit"]});child.once("error",reject);child.once("exit",code=>code===0?resolve():reject(new Error(`${command} exited ${code}`)));});}
await run("cargo",["build","--release","--offline","--bin","substrate-liquid","--example","dx_host"]);
const p=table(core_Person,"p"),f=table(fitness_Performed,"f"),w=table(fitness_Workout,"w");
const measurements=[];
for(const n of [10,100]) {
 const parent=await mkdtemp(join(tmpdir(),"liquid-nested-")),dir=join(parent,"host"),socket=join(dir,"socket");let server;
 try {
  await run(resolve("target/release/examples/dx_host"),[dir,resolve("examples/dx/manifest.json"),String(n)]);
  const config=JSON.parse(await readFile(join(dir,"credentials.json"),"utf8"));
  server=spawn(resolve("target/release/substrate-liquid"),["serve",join(dir,"database.redb"),socket,join(dir,"credentials.json")],{stdio:["ignore","pipe","inherit"]});
  await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error("startup timeout")),10000);server.once("error",e=>{clearTimeout(timer);reject(e);});server.once("exit",c=>{clearTimeout(timer);reject(new Error(`server exited ${c}`));});server.stdout.on("data",b=>{if(b.toString().includes("Listening on")){clearTimeout(timer);resolve();}});});
  const client=new LocalClient(socket,config.clients.find(c=>c.principal==="fitness").token);
  let requests=0;const send=client.request.bind(client);client.request=c=>{requests++;return send(c);};
  const query=read(p).orderBy(asc(p.c.id)).limit(n).include({performed:many(f,f.c.athlete).orderBy(asc(f.c.id)).limit(2).include({workout:one(w,f.c.workout)})});
  async function naive() {
   const root=await client.query("SELECT id,name FROM core.Person ORDER BY id LIMIT ?1",[{Integer:String(n)}]);const out=[];
   for(const [id,name] of root.rows) {
    const links=await client.query("SELECT id,athlete,workout FROM fitness.Performed WHERE athlete=?1 ORDER BY id LIMIT 2",[{Text:id}]);const children=[];
    for(const [linkId,athlete,workout] of links.rows) {
     const target=await client.query("SELECT id,durationMinutes FROM fitness.Workout WHERE id=?1",[{Text:workout}]);
     children.push({id:linkId,data:{athlete,workout},related:{workout:{id:target.rows[0][0],data:{durationMinutes:target.rows[0][1]},related:{}}}});
    }
    out.push({id,data:{name},related:{performed:children}});
   }return out;
  }
  const nested=await query.execute(client);assert.equal(nested.stats.sql_statements,3);assert.deepEqual(nested.rows,await naive());
  async function measure(fn,expectedRequests) {const times=[];for(let i=0;i<3;i++){requests=0;const start=performance.now();await fn();times.push(performance.now()-start);assert.equal(requests,expectedRequests);}times.sort((a,b)=>a-b);return {median_ms:times[1],min_ms:times[0],max_ms:times[2],requests:expectedRequests};}
  const batched=await measure(()=>query.execute(client),1),perParent=await measure(naive,1+3*n);
  const result={parents:n,batched,per_parent:perParent,sql_statements:3,speedup:perParent.median_ms/batched.median_ms};measurements.push(result);console.log(JSON.stringify(result));
 }finally{
  if(server&&server.exitCode===null&&server.signalCode===null){const exited=once(server,"exit");server.kill();await exited;}
  await rm(parent,{recursive:true,force:true});
 }
}
await mkdir("target/nested-bench",{recursive:true});await writeFile("target/nested-bench/processes.json",JSON.stringify({samples:3,transport:"Unix socket",profile:"release",idle_accept_poll_ms:5,measurements},null,2)+"\n");
