import {introspect} from "../../sdk/typescript/catalog.js";
import { LocalClient, insertOp, link, SubstrateError } from "../../sdk/typescript/client.mjs";
import { Person, Workout, Performed, Deal, CustomerDeal } from "./generated/schema.js";
import assert from "node:assert/strict";
import {table,select,eq,gt,read,many,one,asc,type QueryClient} from "../../sdk/typescript/query.js";
const p=table(Person,"p"),f=table(Performed,"f"),w=table(Workout,"w"),c=table(CustomerDeal,"c"),d=table(Deal,"d");
const socket = process.env.SUBSTRATE_SOCKET!;
const client = new LocalClient(socket, process.env.SUBSTRATE_TOKEN!);
const app = process.argv[2];
if (app === "fitness" || app === "crm") {
  const catalog = await introspect(client);
  assert.deepEqual(catalog.relations.map(r => r.name).sort(), (app === "fitness"
    ? ["core.Person", "fitness.Performed", "fitness.TrainingGoal", "fitness.Workout"]
    : ["core.Person", "crm.CustomerDeal", "crm.Deal"]).sort());
  assert.ok(catalog.relations.every(r => r.module.name === "core" || r.module.name === app));
  const personSchema = catalog.relations.find(r => r.name === "core.Person")!;
  assert.equal(personSchema.id, Person.schemaId);
  assert.equal(personSchema.operations.insert, false);
  const people = await client.page(Person);
  assert.equal(people.rows.length, 1);
  const person = people.rows[0].id;
  if (app === "fitness") {
    await client.batch([
      insertOp(Workout, { durationMinutes: 45n }, "workout"),
      insertOp(Performed, { athlete: person, workout: link<"fitness.Workout">("workout") })
    ]);
    const own=await read(p).include({performed:many(f,f.c.athlete).include({workout:one(w,f.c.workout)})}).execute(client);
    assert.equal(own.rows[0].related.performed[0].related.workout?.data.durationMinutes,45n);
    assert.equal(own.stats.sql_statements,3);
    await assert.rejects(read(p).include({deals:many(c,c.c.customer)}).execute(client), (e: unknown) => e instanceof SubstrateError && e.code === "denied");
    await assert.rejects(client.page(Deal), (e: unknown) => e instanceof SubstrateError && e.code === "denied");
  } else {
    await client.batch([
      insertOp(Deal, { amount: 9007199254740993n }, "deal"),
      insertOp(CustomerDeal, { customer: person, deal: link<"crm.Deal">("deal") })
    ]);
    await assert.rejects(client.page(Workout), (e: unknown) => e instanceof SubstrateError && e.code === "denied");
  }
  console.log(`${app}: committed independent extension for Person ${person}; cross-app access denied`);
} else if (app === "reporter") {
  const result = await client.query("SELECT p.name,w.durationMinutes,d.amount FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout JOIN crm.CustomerDeal c ON c.customer=p.id JOIN crm.Deal d ON d.id=c.deal");
  assert.deepEqual(result.rows, [["Ada", 45n, 9007199254740993n]]);
  const joined=await select({name:p.c.name,minutes:w.c.durationMinutes,amount:d.c.amount}).from(p)
    .innerJoin(f,eq(f.c.athlete,p.c.id)).innerJoin(w,eq(f.c.workout,w.c.id))
    .innerJoin(c,eq(c.c.customer,p.c.id)).innerJoin(d,eq(c.c.deal,d.c.id))
    .where(gt(w.c.durationMinutes,30n)).orderBy(asc(p.c.id)).limit(10).execute(client);
  assert.deepEqual(joined,[{name:"Ada",minutes:45n,amount:9007199254740993n}]);
  let requests=0;const counted:QueryClient={request(command){requests++;return client.request(command);}};
  const nested=await read(p).limit(10).include({
    performed:many(f,f.c.athlete).limit(5).include({workout:one(w,f.c.workout)}),
    deals:many(c,c.c.customer).limit(5).include({deal:one(d,c.c.deal)})
  }).execute(counted);
  assert.equal(requests,1);assert.equal(nested.stats.sql_statements,5);
  assert.equal(nested.rows[0].related.performed[0].related.workout?.data.durationMinutes,45n);
  assert.equal(nested.rows[0].related.deals[0].related.deal?.data.amount,9007199254740993n);
  console.log("reporter: typed join and nested reads agree; one request, five statements");
  console.log("reporter: SQL join agrees; amount 9007199254740993 round-tripped exactly");
} else { throw new Error("expected fitness, crm, or reporter"); }
