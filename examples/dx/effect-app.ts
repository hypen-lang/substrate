import assert from "node:assert/strict";
import { Effect } from "effect";
import { LocalClient, insertOp } from "../../sdk/typescript/client.mjs";
import { Substrate, layer } from "../../sdk/typescript/effect.js";
import { introspect } from "../../sdk/typescript/catalog.js";
import { table, read, many, one } from "../../sdk/typescript/query.js";
import { Person, Workout, Performed, Deal } from "./generated/schema.js";

const socket = process.env.SUBSTRATE_SOCKET;
const token = process.env.SUBSTRATE_TOKEN;
if (!socket || !token) throw new Error("Set SUBSTRATE_SOCKET and SUBSTRATE_TOKEN for a fitness principal");
const people = table(Person, "p"), workouts = table(Workout, "w"), performed = table(Performed, "f");
const program = Effect.gen(function* () {
  const db = yield* Substrate;
  const catalog = yield* db.use(introspect);
  assert.ok(catalog.relations.some(relation => relation.name === "core.Person"));
  const before = yield* db.execute(read(people));
  const person = before.rows[0].id;
  const workout = yield* db.insert(Workout, { durationMinutes: 37n });
  yield* db.batch([insertOp(Performed, { athlete: person, workout })]);
  const result = yield* db.execute(read(people).include({
    performed: many(performed, performed.c.athlete).include({ workout: one(workouts, performed.c.workout) }),
  }));
  assert.equal(result.rows[0].related.performed[0].related.workout?.data.durationMinutes, 37n);
  const denied = yield* db.insert(Deal, { amount: 100n }).pipe(Effect.catchTag("SubstrateFailure", error => Effect.succeed(error.code)));
  assert.equal(denied, "denied");
  return { people: result.rows.length, statements: result.stats.sql_statements, crossApp: denied };
});
const result = await Effect.runPromise(program.pipe(Effect.provide(layer(new LocalClient(socket, token)))));
console.log("Effect: typed writes and nested reads, scoped service, cross-app denial", result);
