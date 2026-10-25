import assert from "node:assert/strict";
import {createInterface} from "node:readline";
import {RemoteClient, SubstrateError} from "../../sdk/typescript/client.mjs";
import {describe} from "../../sdk/typescript/runtime.js";
const lines=createInterface({input:process.stdin})[Symbol.asyncIterator]();
async function next(){const line=await lines.next();if(line.done)throw new Error("host disconnected");return JSON.parse(line.value);}
function done(){process.stdout.write("ok\n");}
const config=await next();
const client=(token:string)=>new RemoteClient(config,token).persistent();
const old=client(config.token);
const person=await describe(old,"core.Person");
assert.equal((await old.page(person)).rows.length,1);
assert.deepEqual((await old.query("SELECT name FROM core.Person")).rows,[["Ada"]]);
await assert.rejects(old.insert(person,{name:"Bob"}),error=>error instanceof SubstrateError&&error.code==="denied");
done();
const rotated=client((await next()).token);
await assert.rejects(old.query("SELECT name FROM core.Person"),error=>error instanceof SubstrateError&&error.code==="denied");
assert.equal((await rotated.page(person)).rows.length,1);done();
await next();
await assert.rejects(rotated.page(person),error=>error instanceof SubstrateError&&error.code==="denied");done();
const expiring=await next();const short=client(expiring.token);
assert.equal((await short.page(person)).rows.length,1);
await new Promise(resolve=>setTimeout(resolve,Math.max(0,expiring.expires_at*1000-Date.now())+50));
await assert.rejects(short.page(person),error=>error instanceof SubstrateError&&error.code==="denied");done();
old.close();rotated.close();short.close();
process.exit(0);
