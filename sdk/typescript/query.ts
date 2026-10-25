/** Typed, parameterized server queries. No data joins or filtering run in JS. */
import type {Descriptor,EntityId,WireValue,WireRow} from "./generated-preamble.js";
import {objectValidator,type FieldSpec} from "./validation.js";
import type {SqlParameter} from "./client.mjs";
export interface QueryClient {request(command: unknown): Promise<unknown>}
export class Column<T,R extends string=string> {
  declare readonly __type: T;
  declare readonly __owner: R;
  constructor(readonly table:Table<any,R>,readonly name:string,readonly spec:FieldSpec) {Object.freeze(this);}
}
type Columns<T,R extends string> = {[K in keyof T]:Column<T[K],R>} & {readonly id:Column<EntityId<R>,R>};
export class Table<T,R extends string> {
  readonly c:Columns<T,R>;
  constructor(readonly descriptor:Descriptor<T,R>,readonly alias:string) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) throw new Error("invalid table alias");
    this.c=Object.freeze(Object.fromEntries([["id",new Column(this,"id",{kind:"Ref",target:descriptor.relation})],...Object.entries(descriptor.fields).map(([name,spec])=>[name,new Column(this,name,spec as FieldSpec)])])) as Columns<T,R>;
    Object.freeze(this);
  }
}
export function table<T,R extends string>(descriptor:Descriptor<T,R>,alias:string):Table<T,R> {return new Table(descriptor,alias);}
type AnyColumn=Column<unknown,string>;
type Compare="eq"|"ne"|"gt"|"ge"|"lt"|"le";
type Expr={op:"compare";left:AnyColumn;comparison:Compare;right:unknown}|{op:"and"|"or";terms:Predicate[]}|{op:"not";term:Predicate};
export class Predicate {constructor(readonly expression:Expr) {Object.freeze(this);}}
function compare<T>(left:Column<T>,comparison:Compare,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {
  if (["gt","ge","lt","le"].includes(comparison)&&!["Int","Decimal","Timestamp","Text"].includes(left.spec.kind)) throw new Error("ordered comparison requires text or numeric scalar");
  if (right instanceof Column) {
    if (left.spec.kind!==right.spec.kind || left.spec.scale!==right.spec.scale || left.spec.target!==right.spec.target) throw new Error("incompatible column domains");
  } else scalarWire(left.spec,right);
  return new Predicate({op:"compare",left,comparison,right});
}
export function eq<T>(left:Column<T>,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {return compare(left,"eq",right);}
export function ne<T>(left:Column<T>,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {return compare(left,"ne",right);}
export function gt<T extends bigint|string>(left:Column<T>,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {return compare(left,"gt",right);}
export function ge<T extends bigint|string>(left:Column<T>,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {return compare(left,"ge",right);}
export function lt<T extends bigint|string>(left:Column<T>,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {return compare(left,"lt",right);}
export function le<T extends bigint|string>(left:Column<T>,right:NoInfer<T>|Column<NoInfer<T>>):Predicate {return compare(left,"le",right);}
export function and(...terms:Predicate[]):Predicate {if(!terms.length)throw new Error("empty predicate group");return new Predicate({op:"and",terms});}
export function or(...terms:Predicate[]):Predicate {if(!terms.length)throw new Error("empty predicate group");return new Predicate({op:"or",terms});}
export function not(term:Predicate):Predicate {return new Predicate({op:"not",term});}
export interface Ordering {readonly column:AnyColumn;readonly descending:boolean}
export function asc(column:AnyColumn):Ordering {return {column,descending:false};}
export function desc(column:AnyColumn):Ordering {return {column,descending:true};}
function scalarWire(spec:FieldSpec,input:unknown):WireValue {
  objectValidator<{value:unknown}>({value:spec}).parse({value:input});
  return {[spec.kind]:["Int","Decimal","Timestamp"].includes(spec.kind)?(input as bigint).toString():spec.kind==="Bytes"?Array.from(input as Uint8Array):input} as WireValue;
}
function parameter(spec:FieldSpec,input:unknown):SqlParameter {
  scalarWire(spec,input);
  switch(spec.kind) {
    case "Bool":return {Integer:input?"1":"0"};
    case "Int":case "Decimal":case "Timestamp":return {Integer:(input as bigint).toString()};
    case "Bytes":return {Blob:Array.from(input as Uint8Array)};
    case "Ref":return {Text:(input as string).toLowerCase()};
    default:return {Text:input as string};
  }
}
function decode(spec:FieldSpec,input:unknown):unknown {
  if(!input||typeof input!=="object"||Object.keys(input).length!==1)throw new Error("invalid SQL result value");
  const v=input as Record<string,unknown>;let value:unknown;
  switch(spec.kind) {
    case "Bool":if(v.Integer!=="0"&&v.Integer!=="1")throw new Error("invalid SQL boolean");value=v.Integer==="1";break;
    case "Int":case "Decimal":case "Timestamp":if(typeof v.Integer!=="string"||v.Integer.length>20||! /^-?(0|[1-9][0-9]*)$/.test(v.Integer))throw new Error("invalid SQL integer");value=BigInt(v.Integer);break;
    case "Bytes":if(!Array.isArray(v.Blob)||!v.Blob.every(n=>Number.isInteger(n)&&n>=0&&n<=255))throw new Error("invalid SQL bytes");value=Uint8Array.from(v.Blob);break;
    default:value=v.Text;
  }
  objectValidator({value:spec}).parse({value});return value;
}
function quote(s:string):string {return `"${s.replaceAll('"','""')}"`;}
function contract(t:Table<any,string>) {return {relation:t.descriptor.relation,schema_id:t.descriptor.schemaId,fingerprint:t.descriptor.fingerprint};}
function qualified(t:Table<any,string>) {return t.descriptor.relation.split(".").map(quote).join(".")+" AS "+quote(t.alias);}
function checkLimit(n:number):number {if(!Number.isInteger(n)||n<1||n>1000)throw new Error("limit must be 1..1000");return n;}
function expression(p:Predicate,leaf:(e:Extract<Expr,{op:"compare"}>)=>any,group:(op:"and"|"or",terms:any[])=>any,negate:(term:any)=>any,budget={remaining:256},depth=0):any {
  if(depth>16||--budget.remaining<0)throw new Error("predicate complexity limit exceeded");
  const e=p.expression;
  if(e.op==="compare")return leaf(e);
  if(e.op==="not")return negate(expression(e.term,leaf,group,negate,budget,depth+1));
  return group(e.op,e.terms.map(t=>expression(t,leaf,group,negate,budget,depth+1)));
}
type Projection=Record<string,AnyColumn>;
export type Selected<P extends Projection>={[K in keyof P]:P[K] extends Column<infer T>?T:never};
interface Join {table:Table<any,string>;on:Predicate}
/** Inner joins only. SQL-nullable outer joins need a separate typed surface. */
export class SelectQuery<P extends Projection> {
  constructor(private readonly source:Table<any,string>,private readonly projection:P,private readonly joins:Join[]=[],private readonly filter?:Predicate,private readonly ordering:Ordering[]=[],private readonly rowLimit=100) {}
  innerJoin<T,R extends string>(joined:Table<T,R>,on:Predicate):SelectQuery<P> {return new SelectQuery(this.source,this.projection,[...this.joins,{table:joined,on}],this.filter,this.ordering,this.rowLimit);}
  where(filter:Predicate):SelectQuery<P> {return new SelectQuery(this.source,this.projection,this.joins,filter,this.ordering,this.rowLimit);}
  orderBy(...ordering:Ordering[]):SelectQuery<P> {return new SelectQuery(this.source,this.projection,this.joins,this.filter,ordering,this.rowLimit);}
  limit(n:number):SelectQuery<P> {return new SelectQuery(this.source,this.projection,this.joins,this.filter,this.ordering,checkLimit(n));}
  compile() {
    const tables=[this.source,...this.joins.map(j=>j.table)];if(tables.length>16||new Set(tables.map(t=>t.alias)).size!==tables.length)throw new Error("duplicate alias or too many joins");
    const params:SqlParameter[]=[];
    const col=(c:AnyColumn,available=tables)=>{if(!available.includes(c.table))throw new Error("column belongs to an unbound table");return `${quote(c.table.alias)}.${quote(c.name)}`;};
    const expr=(p:Predicate,available=tables):string=>expression(p,e=>{
      const right=e.right instanceof Column?col(e.right,available):(params.push(parameter(e.left.spec,e.right)),"?");
      return `${col(e.left,available)} ${{eq:"=",ne:"!=",gt:">",ge:">=",lt:"<",le:"<="}[e.comparison]} ${right}`;
    },(op,terms)=>`(${terms.join(op==="and"?" AND ":" OR ")})`,term=>`(NOT ${term})`);
    const entries=Object.entries(this.projection);if(!entries.length||entries.length>128||this.ordering.length>8)throw new Error("invalid projection/order size");
    let sql=`SELECT ${entries.map(([name,c])=>`${col(c)} AS ${quote(name)}`).join(",")} FROM ${qualified(this.source)}`;
    this.joins.forEach((j,i)=>{sql+=` INNER JOIN ${qualified(j.table)} ON ${expr(j.on,tables.slice(0,i+2))}`;});
    if(this.filter)sql+=` WHERE ${expr(this.filter)}`;
    if(this.ordering.length)sql+=` ORDER BY ${this.ordering.map(o=>`${col(o.column)} ${o.descending?"DESC":"ASC"}`).join(",")}`;
    params.push({Integer:this.rowLimit.toString()});sql+=" LIMIT ?";
    return {sql,params,contracts:tables.map(contract)};
  }
  async execute(client:QueryClient):Promise<Array<Selected<P>>> {
    const q=this.compile();const result=await client.request({op:"checked",contracts:q.contracts,command:{op:"query",sql:q.sql,params:q.params}}) as {kind:string;columns:string[];rows:unknown[][]};
    const entries=Object.entries(this.projection);
    if(result.kind!=="query"||JSON.stringify(result.columns)!==JSON.stringify(entries.map(([k])=>k)))throw new Error("invalid query response");
    return result.rows.map(row=>{if(row.length!==entries.length)throw new Error("invalid result arity");return Object.fromEntries(entries.map(([k,c],i)=>[k,decode(c.spec,row[i])])) as Selected<P>;});
  }
  async explain(client:QueryClient):Promise<{sql:string;params:SqlParameter[];plan:unknown}> {
    const q=this.compile();const plan=await client.request({op:"checked",contracts:q.contracts,command:{op:"query",sql:"EXPLAIN QUERY PLAN "+q.sql,params:q.params}});return {sql:q.sql,params:q.params,plan};
  }
}
export function select<P extends Projection>(projection:P) {return {from<T,R extends string>(source:Table<T,R>):SelectQuery<P>{return new SelectQuery(source,projection);}};}

export interface Node<T,R extends string,N={}> {id:EntityId<R>;data:T;related:N}
interface Options {filter?:Predicate;order:Ordering[];limit:number;include:Record<string,Branch<any,any>>}
export interface NestedStats {sql_statements:number;fetched_rows:number;returned_nodes:number}
export interface NestedResponse<T> {revision:string;rows:T[];stats:NestedStats}
type Included<C>={[K in keyof C]:C[K] extends Branch<any,infer V>?V:never};
const initial=():Options=>({order:[],limit:100,include:{}});
export class ReadQuery<T,R extends string,N={}> {
  constructor(readonly source:Table<T,R>,readonly options:Options=initial()) {}
  where(filter:Predicate):ReadQuery<T,R,N> {return new ReadQuery(this.source,{...this.options,filter});}
  orderBy(...order:Ordering[]):ReadQuery<T,R,N> {return new ReadQuery(this.source,{...this.options,order});}
  limit(limit:number):ReadQuery<T,R,N> {return new ReadQuery(this.source,{...this.options,limit:checkLimit(limit)});}
  include<C extends Record<string,Branch<R,any>>>(children:C):ReadQuery<T,R,Included<C>> {return new ReadQuery(this.source,{...this.options,include:children});}
  plan():unknown {return planNode(this.source,this.options,0,{nodes:0});}
  async execute(client:QueryClient):Promise<NestedResponse<Node<T,R,N>>> {
    const output=await client.request({op:"read",plan:this.plan()}) as {kind:string;result:{revision:string;rows:WireNode[];stats:NestedStats}};
    if(output.kind!=="read"||!output.result||!Array.isArray(output.result.rows)||typeof output.result.revision!=="string"||! /^[0-9]{1,20}$/.test(output.result.revision)||BigInt(output.result.revision)>(1n<<64n)-1n)throw new Error("invalid nested response");
    const stats=output.result.stats;
    if(!stats||![stats.sql_statements,stats.fetched_rows,stats.returned_nodes].every(n=>Number.isInteger(n)&&n>=0)||stats.sql_statements<1||stats.sql_statements>17||stats.fetched_rows>5000||stats.returned_nodes>5000)throw new Error("invalid nested statistics");
    return {...output.result,rows:output.result.rows.map(row=>decodeNode(this.source,this.options,row) as Node<T,R,N>)};
  }
}
/** P is the parent family; V is this branch's inferred result. */
export class Branch<P extends string,V> {
  declare readonly __parent:P;
  declare readonly __value:V;
  constructor(readonly source:Table<any,string>,readonly direction:"many"|"one",readonly role:Column<any,string>,readonly options:Options) {}
  where(filter:Predicate):Branch<P,V> {return new Branch(this.source,this.direction,this.role,{...this.options,filter});}
  orderBy(...order:Ordering[]):Branch<P,V> {return new Branch(this.source,this.direction,this.role,{...this.options,order});}
  limit(limit:number):Branch<P,V> {if(this.direction==="one"&&limit!==1)throw new Error("forward reference limit must be one");return new Branch(this.source,this.direction,this.role,{...this.options,limit:checkLimit(limit)});}
  include<C extends Record<string,Branch<ChildRelation<V>,any>>>(children:C):Branch<P,WithIncluded<V,Included<C>>> {return new Branch(this.source,this.direction,this.role,{...this.options,include:children});}
}
type ChildRelation<V>=NonNullable<V> extends Node<any,infer R,any>[]?R:NonNullable<V> extends Node<any,infer R,any>?R:never;
type WithIncluded<V,N>=NonNullable<V> extends Node<infer T,infer R,any>[]?Node<T,R,N>[]:NonNullable<V> extends Node<infer T,infer R,any>?Node<T,R,N>|null:never;
export function read<T,R extends string>(source:Table<T,R>):ReadQuery<T,R> {return new ReadQuery(source);}
export function many<T,R extends string,P extends string>(child:Table<T,R>,childReference:Column<EntityId<P>,NoInfer<R>>):Branch<P,Node<T,R>[]> {
  if(childReference.table!==child||childReference.spec.kind!=="Ref"||childReference.name==="id")throw new Error("many requires a reference role on the child table");
  return new Branch(child,"many",childReference,initial());
}
export function one<T,R extends string,P extends string>(target:Table<T,R>,parentReference:Column<EntityId<NoInfer<R>>,P>):Branch<P,Node<T,R>|null> {
  if(parentReference.spec.kind!=="Ref"||parentReference.spec.target!==target.descriptor.relation||parentReference.name==="id")throw new Error("one requires a matching parent reference role");
  return new Branch(target,"one",parentReference,{...initial(),limit:1});
}
function planNode(t:Table<any,string>,o:Options,depth:number,budget:{nodes:number}):unknown {
  if(depth>4||++budget.nodes>17)throw new Error("nested plan size/depth limit exceeded");
  const own=(c:AnyColumn)=>{if(c.table!==t)throw new Error("nested filter/order must use its own table");return c.name;};
  const filter=o.filter?expression(o.filter,e=>{if(e.right instanceof Column)throw new Error("nested filters compare a field to a value");return {op:"compare",field:own(e.left),comparison:e.comparison,value:scalarWire(e.left.spec,e.right)};},(op,terms)=>({op,terms}),term=>({op:"not",term})):null;
  const include=Object.fromEntries(Object.entries(o.include).map(([name,b])=>{
    if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))throw new Error("invalid include name");
    if(b.direction==="many"?b.role.spec.target!==t.descriptor.relation:b.role.table!==t)throw new Error("include belongs to a different parent");
    return [name,{direction:b.direction,role:b.role.name,plan:planNode(b.source,b.options,depth+1,budget)}];
  }));
  return {contract:contract(t),filter,order:o.order.map(v=>({field:own(v.column),descending:v.descending})),limit:checkLimit(o.limit),include};
}
interface WireNode {row:WireRow;related:Record<string,{kind:"many";value:WireNode[]}|{kind:"one";value:WireNode|null}>}
function decodeNode(t:Table<any,string>,o:Options,node:WireNode):unknown {
  const row=t.descriptor.decode(node.row);
  if(Object.keys(node.related).length!==Object.keys(o.include).length)throw new Error("nested result branches mismatch");
  const related=Object.fromEntries(Object.entries(o.include).map(([name,b])=>{
    const result=node.related[name];if(!result||result.kind!==b.direction)throw new Error("nested result cardinality mismatch");
    const value=result.kind==="many"?result.value.map(n=>decodeNode(b.source,b.options,n)):result.value===null?null:decodeNode(b.source,b.options,result.value);
    return [name,value];
  }));return {...row,related};
}
