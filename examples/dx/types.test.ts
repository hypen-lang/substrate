import { core_Person, fitness_Workout, fitness_Performed, type EntityId } from "./generated/schema.js";
import { LocalClient } from "../../sdk/typescript/client.mjs";
// Compiled but never invoked. Unused expect-error directives fail typechecking.
function typeErrors(client: LocalClient, person: EntityId<"core.Person">, workout: EntityId<"fitness.Workout">) {
  // @ts-expect-error wrong scalar type
  core_Person.encode({ name: 3 });
  // @ts-expect-error numbers would lose 64-bit precision
  fitness_Workout.encode({ durationMinutes: 45 });
  // @ts-expect-error swapped relation identities
  fitness_Performed.encode({ athlete: workout, workout: person });
  // @ts-expect-error required name missing
  client.insert(core_Person, {});
}
void typeErrors;

import { fitness, type NewWorkout, type WorkoutUpdate } from "./schema.js";
function authoringTypeErrors(client: LocalClient, person: EntityId<"core.Person">) {
  const insert: NewWorkout = {durationMinutes: 1n};
  const patch: WorkoutUpdate = {};
  fitness.entities.Workout.insertSchema.parse(insert);
  // @ts-expect-error TS authoring requires bigint
  const wrong: NewWorkout = {durationMinutes: 1};
  // @ts-expect-error required insert field
  const missing: NewWorkout = {};
  // @ts-expect-error unknown update field
  const unknown: WorkoutUpdate = {missing: true};
  client.update(fitness_Workout, "id" as EntityId<"fitness.Workout">, patch);
  // @ts-expect-error identity from another relation
  client.delete(fitness_Workout, person);
  // @ts-expect-error patch has wrong scalar
  client.update(fitness_Workout, "id" as EntityId<"fitness.Workout">, {durationMinutes: "x"});
}
void authoringTypeErrors;

// Short names preserve both the descriptor and its record type, without changing
// canonical relation branding or the qualified compatibility exports.
import { Person, type PersonInsert, type PersonRead, type PersonUpdate } from "./generated/schema.js";
const cleanPerson: Person = { name: "Ada" };
const cleanInsert: PersonInsert = cleanPerson;
const cleanUpdate: PersonUpdate = {};
const cleanRead: PersonRead = { id: "person" as EntityId<"core.Person">, data: cleanInsert };
void [cleanUpdate, cleanRead, Person.encode(cleanPerson)];
// @ts-expect-error Short names keep the same field types.
Person.encode({name: 42});
