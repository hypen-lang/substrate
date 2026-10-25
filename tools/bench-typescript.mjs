// Cold and incremental compiler measurements, not a claim about IDE latency.
import {mkdirSync,writeFileSync,readFileSync,rmSync,statSync} from "node:fs";
import {resolve} from "node:path";
import {spawnSync} from "node:child_process";
import {performance} from "node:perf_hooks";
import {defineModule,entity,relation,s,ref,compileModules} from "../target/dx-ts/sdk/typescript/schema.js";
const dir=resolve("target/dx-type-bench");mkdirSync(dir,{recursive:true});
const modules=[defineModule({namespace:"core",version:1,entities:{Person:entity({name:s.string()})}})];
for(let m=0;m<10;m++) {
 const relations={};for(let r=0;r<50;r++) relations[`Fact${r}`]=relation({owner:ref("core.Person"),amount:s.decimal(2),count:s.int64(),label:s.string(),timestamp:s.timestamp(),active:s.bool()});
 modules.push(defineModule({namespace:`app${m}`,version:1,imports:{core:1},relations}));
}
const draft=compileModules(modules);let i=0;
for(const module of draft.modules)for(const def of module.relations)def.id=`00000000-0000-4000-8000-${(++i).toString(16).padStart(12,"0")}`;
writeFileSync(`${dir}/declarations.json`,JSON.stringify(draft));
function run(command,args) {const start=performance.now();const result=spawnSync(command,args,{encoding:"utf8"});if(result.status!==0)throw new Error(result.stderr+result.stdout);return {ms:performance.now()-start,output:result.stdout};}
run("cargo",["build","--offline","--bin","substrate-liquid"]);
run("target/debug/substrate-liquid",["schema-lock",`${dir}/declarations.json`,`${dir}/manifest.json`]);
const generation=run("target/debug/substrate-liquid",["generate",`${dir}/manifest.json`,dir]);
let use='import * as schema from "./schema.js";\nimport {table,select,eq,read,many} from "../../sdk/typescript/query.js";\nconst person=table(schema.core_Person,"p");\n';
for(let m=0;m<10;m++)for(let r=0;r<50;r++)use+=`const row${m}_${r}: schema.app${m}_Fact${r}Insert = {owner: "00000000-0000-4000-8000-000000000001" as schema.EntityId<"core.Person">,amount:1n,count:1n,label:"x",timestamp:0n,active:true}; schema.app${m}_Fact${r}.insertSchema.parse(row${m}_${r});\n`;
for(let m=0;m<10;m++)for(let r=0;r<50;r++) {
 use+=`const t${m}_${r}=table(schema.app${m}_Fact${r},"f"); select({amount:t${m}_${r}.c.amount}).from(t${m}_${r}).where(eq(t${m}_${r}.c.count,1n)).compile();\n`;
 if(r===0)use+=`read(person).include({facts:many(t${m}_${r},t${m}_${r}.c.owner)}).plan();\n`;
}
writeFileSync(`${dir}/usage.ts`,use);
writeFileSync(`${dir}/tsconfig.json`,JSON.stringify({compilerOptions:{strict:true,target:"ES2022",module:"ES2022",moduleResolution:"bundler",noEmit:true,incremental:true,tsBuildInfoFile:"build.tsbuildinfo",types:[],skipLibCheck:false},files:["schema.ts","usage.ts"]}));
rmSync(`${dir}/build.tsbuildinfo`,{force:true});
const args=["-p",`${dir}/tsconfig.json`,"--extendedDiagnostics"];
const cold=run("node_modules/.bin/tsc",args),incremental=run("node_modules/.bin/tsc",args);
const memory=Number(cold.output.match(/Memory used:\s+(\d+)K/)[1]);
const size=statSync(`${dir}/schema.ts`).size;
const stats={relations:501,modules:11,generation_ms:generation.ms,cold_ms:cold.ms,incremental_ms:incremental.ms,compiler_memory_kib:memory,generated_ts_bytes:size,sdk_runtime_source_bytes:statSync("sdk/typescript/client.mjs").size,query_builder_source_bytes:statSync("sdk/typescript/query.ts").size,editor_latency:"not measured",bundle_bytes:"not measured"};
writeFileSync(`${dir}/results.json`,JSON.stringify(stats,null,2)+"\n");writeFileSync(`${dir}/compiler.txt`,cold.output+"\nIncremental:\n"+incremental.output);
console.log(JSON.stringify(stats,null,2));
// Regression ceilings, deliberately loose across development machines.
if(cold.ms>15000||incremental.ms>10000||memory>350000||size>2000000)throw new Error("TypeScript regression budget exceeded");
