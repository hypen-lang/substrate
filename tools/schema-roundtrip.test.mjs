import test from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,readFileSync,writeFileSync,rmSync,copyFileSync,statSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
import {spawnSync} from "node:child_process";
import {declarations} from "../target/dx-ts/examples/dx/schema.js";
const build=spawnSync("cargo",["build","--offline","--bin","substrate-liquid"],{encoding:"utf8"});
assert.equal(build.status,0,build.stderr);
function cli(...args) {const r=spawnSync(resolve("target/debug/substrate-liquid"),args,{encoding:"utf8"});assert.equal(r.status,0,r.stderr);return r.stdout;}
test("TS declaration -> Rust validation -> install -> introspection -> generated clients",()=>{
 const dir=mkdtempSync(join(tmpdir(),"liquid-contract-"));
 try {
  const source=join(dir,"declarations.json"),lock=join(dir,"manifest.json"),db=join(dir,"database.redb"),pulled=join(dir,"catalog.json");
  writeFileSync(source,JSON.stringify(declarations));copyFileSync("examples/dx/manifest.json",lock);
  cli("schema-lock",source,lock);
  assert.equal(readFileSync(lock,"utf8"),readFileSync("examples/dx/manifest.json","utf8"));
  cli("manifest-install",db,lock);cli("manifest-export",db,pulled);
  assert.equal(readFileSync(pulled,"utf8"),readFileSync(lock,"utf8"));
  cli("generate",pulled,join(dir,"generated"));
  for (const name of ["schema.ts","schema.rs"]) assert.equal(readFileSync(join(dir,"generated",name),"utf8"),readFileSync(join("examples/dx/generated",name),"utf8"));
  const time=statSync(join(dir,"generated/schema.ts")).mtimeMs;
  cli("generate",pulled,join(dir,"generated"));assert.equal(statSync(join(dir,"generated/schema.ts")).mtimeMs,time);
  cli("generate",pulled,join(dir,"fitness"),"fitness@1");
  assert.ok(!readFileSync(join(dir,"fitness/schema.ts"),"utf8").includes("crm_Deal"));
  const invalid=structuredClone(declarations);invalid.modules.find(m=>m.name==="fitness").imports=[];
  writeFileSync(source,JSON.stringify(invalid));
  const r=spawnSync(resolve("target/debug/substrate-liquid"),["schema-lock",source,join(dir,"invalid.json")],{encoding:"utf8"});
  assert.notEqual(r.status,0);assert.match(r.stderr,/import/i);
  // Editing an immutable layout cannot overwrite the existing lock.
  const changed=structuredClone(declarations);changed.modules.find(m=>m.name==="core").relations[0].roles[0].data_type="Int";
  writeFileSync(source,JSON.stringify(changed));
  assert.notEqual(spawnSync(resolve("target/debug/substrate-liquid"),["schema-lock",source,lock]).status,0);
  assert.equal(readFileSync(lock,"utf8"),readFileSync(pulled,"utf8"));
 }finally{rmSync(dir,{recursive:true,force:true});}
});

test("generated clean imports typecheck while ambiguous and reserved names stay qualified",()=>{
 const dir=mkdtempSync(join(tmpdir(),"liquid-clean-names-"));
 try {
  const modules=[['core',['Person','Address','Descriptor','class','Uint8Array']],['other',['Person','AddressRead','Project','core_Person']]];
  const source=join(dir,"source.json"),lock=join(dir,"manifest.json");
  writeFileSync(source,JSON.stringify({format:1,modules:modules.map(([name,names])=>({name,version:1,imports:[],relations:names.map(local=>({name:`${name}.${local}`,roles:[{name:'label',data_type:'Text'}]}))}))}));
  cli("schema-lock",source,lock);cli("generate",lock,dir);
  writeFileSync(join(dir,"use.ts"),`
import { Project, core_Person, other_Person, core_Descriptor, core_class, core_Uint8Array, type ProjectInsert, type ProjectRead, type ProjectUpdate } from "./schema.js";
// @ts-expect-error Two Person definitions have no shared unqualified alias.
import { Person } from "./schema.js";
// @ts-expect-error Address's companion type would collide with AddressRead.
import { Address } from "./schema.js";
const row: Project = {label:"ok"};
const insert: ProjectInsert = row;
const update: ProjectUpdate = {};
const read: ProjectRead = Project.decode({id:"9d9adf54-56a8-466b-9b0c-c5c64d9792c0",fields:Project.encode(insert)});
void [update,read,core_Person,other_Person,core_Descriptor,core_class,core_Uint8Array];
// @ts-expect-error Clean aliases preserve scalar typing.
Project.encode({label:1});
`);
  const check=spawnSync(resolve("node_modules/.bin/tsc"),[join(dir,"use.ts"),"--strict","--target","ES2022","--module","ES2022","--skipLibCheck","--noEmit"],{encoding:"utf8"});
  assert.equal(check.status,0,check.stdout+check.stderr);
 }finally{rmSync(dir,{recursive:true,force:true});}
});
