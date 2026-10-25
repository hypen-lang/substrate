// A fresh real database + Unix host, with an Effect application in a separate process.
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
function run(command,args,env=process.env) {
  return new Promise((resolve,reject)=>{
    const child=spawn(command,args,{env,stdio:"inherit"});
    child.once("error",reject);
    child.once("exit",(code,signal)=>code===0?resolve():reject(new Error(`${command} exited ${code??signal}`)));
  });
}
const temporary=await mkdtemp(join(tmpdir(),"substrate-effect-"));
const directory=join(temporary,"database");
let server;
try {
  await run("cargo",["build","--offline","--bin","substrate-liquid","--example","dx_host"]);
  await run(resolve("target/debug/examples/dx_host"),[directory]);
  const socket=join(directory,"db.sock");
  const config=JSON.parse(await readFile(join(directory,"credentials.json"),"utf8"));
  server=spawn(resolve("target/debug/substrate-liquid"),["serve",join(directory,"database.redb"),socket,join(directory,"credentials.json")],{stdio:["ignore","pipe","inherit"]});
  await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>reject(new Error("host startup timeout")),10000);
    const fail=e=>{clearTimeout(timer);reject(e);};
    server.once("error",fail);
    server.once("exit",code=>fail(new Error(`host exited ${code}`)));
    server.stdout.on("data",data=>{if(data.toString().includes("Listening on")){clearTimeout(timer);resolve();}});
  });
  await run(process.execPath,[resolve("target/dx-ts/examples/dx/effect-app.js")],{
    ...process.env,SUBSTRATE_SOCKET:socket,SUBSTRATE_TOKEN:config.clients.find(c=>c.principal==="fitness").token,
  });
} finally {
  if(server && server.exitCode===null) {const exited=once(server,"exit");server.kill();await exited;}
  await rm(temporary,{recursive:true,force:true});
}
