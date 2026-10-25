mod common;
use common::*;
use proptest::prelude::*;
use rusqlite::types::Value as S;
use std::collections::BTreeMap;
use substrate_liquid::*;

// Deliberately simple oracle: ordered maps and nested loops, no production indexes
// or production query operators. Generated IDs are opaque symbols to the model.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn operation_sequences_match_reference(ops in prop::collection::vec((0u8..6,0usize..100, -20i64..100),1..70)) {
        let (db,person)=fixture();
        let mut reference=BTreeMap::<Id,(Id,i64)>::new(); // workout -> (performed, duration)
        let original=db.admin().snapshot().scan("core.Person").unwrap();
        let mut extension=0;
        for (op,choice,minutes) in ops {
            let old=db.admin().snapshot();
            let old_rows=old.scan("fitness.Workout").unwrap();
            match op {
                0 | 1 => {
                    let mut tx=db.session("fitness").transaction(); let (id,link)=workout(&mut tx,person,minutes); tx.commit().unwrap();
                    reference.insert(id,(link,minutes));
                }
                2 | 3 if !reference.is_empty() => {
                    let id=*reference.keys().nth(choice%reference.len()).unwrap();
                    let mut tx=db.session("fitness").transaction();
                    if op==2 { tx.update("fitness.Workout",id,vec![minutes.into()]).unwrap(); reference.get_mut(&id).unwrap().1=minutes; }
                    else { tx.delete("fitness.Performed",reference[&id].0).unwrap(); tx.delete("fitness.Workout",id).unwrap(); reference.remove(&id); }
                    tx.commit().unwrap();
                }
                4 => {
                    extension+=1; let mut tx=db.admin().transaction();
                    // New optional facts live in a new relation family, never a new Person column.
                    tx.install(Module::new("extensions",extension,vec![("core".into(),1)],vec![Relation::new(&format!("extensions.F{extension}"),vec![Role::new("person",DataType::Ref("core.Person".into())),Role::new("value",DataType::Int)])])).unwrap();
                    tx.commit().unwrap();
                    prop_assert_eq!(db.admin().snapshot().scan("core.Person").unwrap(),original.clone());
                }
                _ => {
                    let mut tx=db.session("fitness").transaction(); tx.insert("fitness.Workout",vec![minutes.into()]).unwrap();
                    prop_assert!(tx.insert("fitness.Performed",vec![Id::new().into(),Id::new().into()]).is_err());
                    prop_assert!(matches!(tx.commit(),Err(Error::Aborted)));
                }
            }
            let snapshot=db.admin().snapshot(); snapshot.verify_indexes().unwrap();
            prop_assert_eq!(old.scan("fitness.Workout").unwrap(),old_rows);
            let actual=snapshot.scan("fitness.Workout").unwrap().into_iter().map(|r|(r.id,r.values[0].clone())).collect::<BTreeMap<_,_>>();
            let expected=reference.iter().map(|(id,(_,v))|(*id,Value::Int(*v))).collect::<BTreeMap<_,_>>();
            prop_assert_eq!(actual,expected);
            let indexed=snapshot.lookup("fitness.Workout","durationMinutes",&minutes.into()).unwrap().into_iter().map(|r|r.id).collect::<Vec<_>>();
            let expected=reference.iter().filter(|(_,(_,v))|*v==minutes).map(|(id,_)|*id).collect::<Vec<_>>();
            prop_assert_eq!(indexed,expected);
        }
        let snapshot=db.admin().snapshot();
        let actual=snapshot.table("fitness.Performed","f").unwrap().join(&snapshot.table("fitness.Workout","w").unwrap(),"f.workout","w.id").unwrap().project(&["f.athlete","w.durationMinutes"]).unwrap();
        let mut expected=reference.values().map(|(_,minutes)|vec![Value::Ref(person),Value::Int(*minutes)]).collect::<Vec<_>>();
        let mut actual=actual.rows().to_vec(); actual.sort(); expected.sort(); prop_assert_eq!(actual,expected);
        let sql=snapshot.sql().unwrap();
        let mut actual=sql.query("SELECT w.durationMinutes FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout",&[]).unwrap().rows.into_iter().map(|r|match r[0] { S::Integer(v)=>v,_=>panic!("integer required") }).collect::<Vec<_>>();
        let mut expected=reference.values().map(|(_,v)|*v).collect::<Vec<_>>(); actual.sort(); expected.sort(); prop_assert_eq!(actual,expected);
    }
    #[test]
    fn randomized_visible_aggregate_matches_input_filter(values in prop::collection::vec(-100i64..100,0..60),allowed in -100i64..100) {
        let (db,p)=fixture(); let mut tx=db.admin().transaction();
        for value in &values { workout(&mut tx,p,*value); }
        tx.grant("viewer","core.Person",Grant::read_only()).unwrap();
        tx.grant("viewer","fitness.Performed",Grant::read_only()).unwrap();
        tx.grant("viewer","fitness.Workout",Grant { filter:RowFilter::Equals { role:"durationMinutes".into(),value:allowed.into() },..Grant::read_only() }).unwrap();
        tx.commit().unwrap();
        let sql=db.session("viewer").snapshot().sql().unwrap();
        let expected=values.iter().filter(|v|**v==allowed).count() as i64;
        let rows=sql.query("SELECT count(*),coalesce(sum(w.durationMinutes),0) FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout",&[]).unwrap().rows;
        prop_assert_eq!(rows,vec![vec![S::Integer(expected),S::Integer(expected*allowed)]]);
    }
}
