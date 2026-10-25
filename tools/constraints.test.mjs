import test from "node:test";
import assert from "node:assert/strict";
import {entity, s, defineModule, compileModules} from "../target/dx-ts/sdk/typescript/schema.js";
import {defineRecord} from "../target/dx-ts/sdk/typescript/generated-preamble.js";
import {describe} from "../target/dx-ts/sdk/typescript/runtime.js";
import {mkdtempSync,writeFileSync,readFileSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
import {pathToFileURL} from "node:url";
import {spawnSync} from "node:child_process";

test("TS checks compile exact bounds to manifest v2 and validate UTF-8 byte lengths", () => {
  const Profile = entity({age:s.int64(),name:s.string(),photo:s.bytes()})
    .intRange("age", 0n, 150n).byteLength("name", 1, 8).byteLength("photo", 0, 4);
  assert.equal(Profile.insertSchema.safeParse({age:150n,name:"🦀🦀",photo:new Uint8Array(4)}).success,true);
  for (const value of [{age:151n,name:"Ada",photo:new Uint8Array()}, {age:1n,name:"🦀🦀x",photo:new Uint8Array()}, {age:1n,name:"Ada",photo:new Uint8Array(5)}])
    assert.equal(Profile.insertSchema.safeParse(value).success,false);
  assert.equal(Profile.updateSchema.safeParse({name:""}).success,false);
  assert.equal(Profile.updateSchema.safeParse({age:0n}).success,true);
  const declaration = compileModules([defineModule({namespace:"app",version:1,entities:{Profile}})]);
  assert.equal(declaration.format,2);
  assert.deepEqual(declaration.modules[0].relations[0].checks[0],{IntRange:{role:"age",min:"0",max:"150"}});
  const Exact = entity({n:s.int64()}).intRange("n",-(1n<<63n),(1n<<63n)-1n);
  const exact = compileModules([defineModule({namespace:"all",version:1,entities:{Exact}})]);
  assert.match(JSON.stringify(exact),/9223372036854775807/);
  assert.equal(compileModules([defineModule({namespace:"plain",version:1,entities:{Plain:entity({x:s.string()})}})]).format,1);
  assert.throws(()=>entity({x:s.string()}).intRange("x",0n,1n));
  assert.throws(()=>entity({x:s.int64()}).byteLength("x",0,1));
  assert.throws(()=>entity({x:s.int64()}).intRange("x",2n,1n));
});

test("generated descriptor checks cover inserts patches encoded batches and stored reads", () => {
  const id="00000000-0000-4000-8000-000000000001";
  const Profile=defineRecord("app.Profile",id,"a".repeat(64),{
    age:{kind:"Int",checks:[{kind:"IntRange",min:0n,max:150n}]},
    name:{kind:"Text",checks:[{kind:"ByteLength",min:1,max:8}]}
  });
  assert.throws(()=>Profile.encode({age:151n,name:"Ada"}),/IntRange/);
  assert.throws(()=>Profile.encodePatch({name:"🦀🦀x"}),/ByteLength/);
  assert.throws(()=>Profile.decode({id,fields:{age:{Int:"-1"},name:{Text:"Ada"}}}),/IntRange/);
  assert.deepEqual(Profile.decode({id,fields:Profile.encode({age:150n,name:"🦀🦀"})}).data,{age:150n,name:"🦀🦀"});
});

test("runtime discovery parses and applies checks, rejecting unknown or incompatible metadata", async () => {
  const id="00000000-0000-4000-8000-000000000001";
  const base={id,name:"app.Age",fingerprint:"a".repeat(64),module:{name:"app",version:"1"},roles:[{id:{relation:id,slot:0},name:"age",data_type:{kind:"int"},required:true,equality_index:true}],unique:[],operations:{read:true,insert:true,update:true,delete:true,reference:false},row_restricted:false,lifecycle:"active",aliases:[]};
  const client=checks=>({request:async()=>({kind:"catalog",page:{format:1,revision:"1",relations:[{...base,checks}],next:null}})});
  const descriptor=await describe(client([{IntRange:{role:"age",min:"0",max:"150"}}]),"app.Age");
  assert.throws(()=>descriptor.encode({age:151n}),/IntRange/);
  assert.deepEqual(descriptor.encode({age:150n}),{age:{Int:"150"}});
  for(const checks of [[{Execute:{role:"age",min:0,max:1}}],[{IntRange:{role:"age",min:0,max:"150"}}],[{ByteLength:{role:"age",min:0,max:1}}],[{IntRange:{role:"hidden",min:"0",max:"1"}}]])
    await assert.rejects(describe(client(checks),"app.Age"),/invalid catalog/);
});

test("constrained TS declaration locks, persists and generates an executable checked client", async () => {
  const build=spawnSync("cargo",["build","--offline","--bin","substrate-liquid"],{encoding:"utf8"});
  assert.equal(build.status,0,build.stderr);
  const dir=mkdtempSync(join(tmpdir(),"substrate-checks-"));
  const cli=(...args)=>{const result=spawnSync(resolve("target/debug/substrate-liquid"),args,{encoding:"utf8"});assert.equal(result.status,0,result.stderr);};
  try {
    const Profile=entity({age:s.int64(),name:s.string()}).intRange("age",0n,150n).byteLength("name",1,8);
    writeFileSync(join(dir,"source.json"),JSON.stringify(compileModules([defineModule({namespace:"app",version:1,entities:{Profile}})])));
    cli("schema-lock",join(dir,"source.json"),join(dir,"locked.json"));
    cli("manifest-install",join(dir,"db.redb"),join(dir,"locked.json"));
    cli("manifest-export",join(dir,"db.redb"),join(dir,"export.json"));
    assert.equal(readFileSync(join(dir,"locked.json"),"utf8"),readFileSync(join(dir,"export.json"),"utf8"));
    cli("generate",join(dir,"export.json"),dir);
    writeFileSync(join(dir,"package.json"),'{"type":"module"}');
    const tsc=spawnSync(resolve("node_modules/.bin/tsc"),[join(dir,"schema.ts"),"--target","ES2022","--module","ES2022","--strict","--skipLibCheck"],{encoding:"utf8"});
    assert.equal(tsc.status,0,tsc.stdout+tsc.stderr);
    const generated=await import(pathToFileURL(join(dir,"schema.js")).href);
    assert.throws(()=>generated.Profile.encode({age:151n,name:"Ada"}),/IntRange/);
    assert.throws(()=>generated.Profile.encodePatch({name:"🦀🦀x"}),/ByteLength/);
    assert.deepEqual(generated.Profile.encode({age:150n,name:"🦀🦀"}),{age:{Int:"150"},name:{Text:"🦀🦀"}});
  } finally {rmSync(dir,{recursive:true,force:true});}
});
