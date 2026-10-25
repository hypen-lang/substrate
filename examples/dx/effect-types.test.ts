import { Effect } from "effect";
import { Substrate, SubstrateFailure } from "../../sdk/typescript/effect.js";
import { Person, Workout } from "./generated/schema.js";
import type { EntityId } from "../../sdk/typescript/generated-preamble.js";
import { select, table } from "../../sdk/typescript/query.js";
// Compile-only contracts; no network work is executed.
const program = Effect.gen(function* () {
  const db = yield* Substrate;
  const id: EntityId<"core.Person"> = yield* db.insert(Person, { name: "Ada" });
  yield* db.update(Person, id, { name: "Grace" });
  // @ts-expect-error another relation's ID cannot identify a Person
  yield* db.update(Person, "x" as EntityId<"fitness.Workout">, { name: "Grace" });
  // @ts-expect-error scalar types remain checked
  yield* db.insert(Workout, { durationMinutes: 45 });
  // @ts-expect-error unknown fields remain checked
  yield* db.insert(Person, { name: "Ada", surprise: true });
  const p = table(Person,"p");
  const result: Array<{name:string}> = yield* db.execute(select({name:p.c.name}).from(p));
  return result;
});
const typed: Effect.Effect<Array<{name:string}>,SubstrateFailure,Substrate> = program;
void typed;
