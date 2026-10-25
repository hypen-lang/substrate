import { defineModule, entity, relation, ref, s, optionalProperty, compileModules, type InferInsert, type InferUpdate } from "../../sdk/typescript/schema.js";
export const core = defineModule({ namespace: "core", version: 1, entities: { Person: entity({ name: s.string() }) } });
export const fitness = defineModule({ namespace: "fitness", version: 1, imports: { core: 1 }, entities: { Workout: entity({ durationMinutes: s.int64() }) }, relations: {
  Performed: relation({ athlete: ref("core.Person"), workout: ref("fitness.Workout") }).index("athlete"),
  TrainingGoal: optionalProperty("core.Person", s.string())
} });
export const crm = defineModule({ namespace: "crm", version: 1, imports: { core: 1 }, entities: { Deal: entity({ amount: s.decimal(2) }) }, relations: {
  CustomerDeal: relation({ customer: ref("core.Person"), deal: ref("crm.Deal") })
} });
export const declarations = compileModules([core, fitness, crm]);
export type NewWorkout = InferInsert<typeof fitness.entities.Workout>;
export type WorkoutUpdate = InferUpdate<typeof fitness.entities.Workout>;
