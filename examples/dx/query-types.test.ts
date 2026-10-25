import {table,select,eq,gt,read,many,one,asc,type QueryClient} from "../../sdk/typescript/query.js";
import {core_Person,fitness_Performed,fitness_Workout,crm_CustomerDeal,crm_Deal,type EntityId} from "./generated/schema.js";
const p=table(core_Person,"p"),f=table(fitness_Performed,"f"),w=table(fitness_Workout,"w"),c=table(crm_CustomerDeal,"c"),d=table(crm_Deal,"d");
async function compileTypes(client:QueryClient) {
 const q=select({person:p.c.name,minutes:w.c.durationMinutes}).from(p).innerJoin(f,eq(f.c.athlete,p.c.id)).innerJoin(w,eq(f.c.workout,w.c.id)).where(gt(w.c.durationMinutes,30n)).orderBy(asc(p.c.id));
 const rows=await q.execute(client);const name:string=rows[0].person;const minutes:bigint=rows[0].minutes;
 // @ts-expect-error selected value is bigint
 const bad:number=rows[0].minutes;
 // @ts-expect-error unknown field
 p.c.unknown;
 // @ts-expect-error invalid scalar
 eq(w.c.durationMinutes,"30");
 // @ts-expect-error mismatched reference domains
 eq(f.c.athlete,w.c.id);
 // @ts-expect-error wrong reference target
 one(p,f.c.workout);
 // @ts-expect-error wrong owner of child reference role
 many(f,c.c.customer);
 const result=await read(p).include({performed:many(f,f.c.athlete).include({workout:one(w,f.c.workout)})}).execute(client);
 const related:bigint|undefined=result.rows[0].related.performed[0].related.workout?.data.durationMinutes;
 const id:EntityId<"fitness.Performed">=result.rows[0].related.performed[0].id;
 // @ts-expect-error wrong parent relation
 read(w).include({performed:many(f,f.c.athlete)});
 // @ts-expect-error wrong nested parent relation
 many(f,f.c.athlete).include({deals:many(c,c.c.customer)});
 // @ts-expect-error forward reference can be filtered out, so it is nullable
 result.rows[0].related.performed[0].related.workout.data.durationMinutes;
 // @ts-expect-error include does not merge global cross-app properties
 result.rows[0].data.workouts;
 void [name,minutes,bad,related,id];
}
void compileTypes;
