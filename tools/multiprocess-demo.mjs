import { mkdtemp, readFile, rm, writeFile, copyFile, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import assert from "node:assert/strict";
import {declarations} from "../target/dx-ts/examples/dx/schema.js";
function run(command, args, env = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => code === 0 ? resolve() : reject(new Error(`${command} exited ${code ?? signal}`)));
  });
}
const parent = await mkdtemp(join(tmpdir(), "liquid-dx-"));
const dir = join(parent, "demo");
let server;
try {
  await run("cargo", ["build", "--offline", "--bin", "substrate-liquid", "--example", "dx_host"]);
  const source=join(parent,"declarations.json"), manifest=join(parent,"manifest.json"), catalog=join(parent,"catalog.json");
  await writeFile(source,JSON.stringify(declarations));await copyFile("examples/dx/manifest.json",manifest);
  const cli=(...args)=>run(resolve("target/debug/substrate-liquid"),args);
  await cli("schema-lock",source,manifest);
  await run(resolve("target/debug/examples/dx_host"), [dir,manifest]);
  await cli("manifest-export",join(dir,"database.redb"),catalog);
  await cli("generate",catalog,join(parent,"generated"));
  // The app is statically checked against exactly this regenerated contract.
  assert.equal(await readFile(join(parent,"generated/schema.ts"),"utf8"),await readFile("examples/dx/generated/schema.ts","utf8"));
  await run(resolve("node_modules/.bin/tsc"),[join(parent,"generated/schema.ts"),"--strict","--target","ES2022","--module","ES2022","--skipLibCheck","--outDir",join(parent,"compiled")]);
  await copyFile(join(parent,"compiled/schema.js"),"target/dx-ts/examples/dx/generated/schema.js");
  const socket = join(dir, "liquid.sock");
  const config = JSON.parse(await readFile(join(dir, "credentials.json"), "utf8"));
  async function start() {
  server = spawn(resolve("target/debug/substrate-liquid"), ["serve", join(dir, "database.redb"), socket, join(dir, "credentials.json")], { stdio: ["ignore", "pipe", "inherit"] });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("server startup timeout")), 10000);
    server.once("error", e => { clearTimeout(timer); reject(e); });
    server.once("exit", code => { clearTimeout(timer); reject(new Error(`server exited ${code}`)); });
    server.stdout.on("data", data => { if (data.toString().includes("Listening on")) { clearTimeout(timer); resolve(); } });
  });
  }
  await start();
  const app = principal => run(process.execPath, [resolve("target/dx-ts/examples/dx/app.js"), principal], { ...process.env, SUBSTRATE_SOCKET: socket, SUBSTRATE_TOKEN: config.clients.find(c => c.principal === principal).token });
  await Promise.all([app("fitness"), app("crm")]);
  await app("reporter");
  // Crash the host after acknowledged commits, reopen durable state, query again.
  const exited=once(server,"exit");server.kill("SIGKILL");await exited;
  await unlink(socket); // Own temporary socket from the now-dead fixture process.
  await start();await app("reporter");
  console.log("PASS: separate application processes, shared identity, exact integers, atomic batches, SQL join, isolation, crash/reopen");
} finally {
  if (server && server.exitCode === null) { const exit = once(server, "exit"); server.kill(); await exit; }
  await rm(parent, { recursive: true, force: true });
}
