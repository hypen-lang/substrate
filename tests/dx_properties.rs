//! Differential coverage for the new write adapter and paged query path.
use proptest::prelude::*;
use rusqlite::types::Value as S;
use std::collections::BTreeMap;
use substrate_liquid::*;
proptest! {
 #![proptest_config(ProptestConfig::with_cases(96))]
 #[test]
 fn sql_mutation_histories_match_native_rows_and_rebuilt_indexes(ops in prop::collection::vec((0u8..3,0u8..12,any::<i64>()),1..70)) {
  let db=Database::memory();let mut tx=db.admin().transaction();tx.install(Module::new("app",1,vec![],vec![Relation::new("app.Item",vec![Role::new("value",DataType::Int)])])).unwrap();tx.commit().unwrap();
  let mut expected=BTreeMap::new();
  for (op,key,value) in ops {
   let id=Id::from_u128(u128::from(key)+1);
   if op==0 && !expected.contains_key(&id) {
    // Once allocated, deleted IDs cannot be reused. Skip those insert attempts.
    let mut tx=db.admin().sql_transaction().unwrap();
    match tx.execute("INSERT INTO app.Item(id,value) VALUES (?1,?2)",&[S::Text(id.to_string()),S::Integer(value)]) {
     Ok(_)=>{tx.commit().unwrap();expected.insert(id,value);},Err(_)=>{prop_assert!(tx.commit().is_err());}
    }
   } else if op==1 && expected.contains_key(&id) {
    let mut tx=db.admin().sql_transaction().unwrap();tx.execute("UPDATE app.Item SET value=?1 WHERE id=?2",&[S::Integer(value),S::Text(id.to_string())]).unwrap();tx.commit().unwrap();expected.insert(id,value);
   } else if op==2 && expected.contains_key(&id) {
    let mut tx=db.admin().sql_transaction().unwrap();tx.execute("DELETE FROM app.Item WHERE id=?1",&[S::Text(id.to_string())]).unwrap();tx.commit().unwrap();expected.remove(&id);
   }
   let snapshot=db.admin().snapshot();snapshot.verify_indexes().unwrap();
   let rows=snapshot.scan("app.Item").unwrap().into_iter().map(|r|(r.id,if let Value::Int(v)=r.values[0] {v} else {panic!()})).collect::<BTreeMap<_,_>>();
   prop_assert_eq!(&rows,&expected);
   let indexed=snapshot.lookup("app.Item","value",&Value::Int(value)).unwrap().into_iter().map(|r|r.id).collect::<Vec<_>>();
   prop_assert_eq!(indexed,expected.iter().filter_map(|(id,v)|(*v==value).then_some(*id)).collect::<Vec<_>>());
  }
 }
 #[test]
 fn keyset_pages_match_reference_under_duplicate_values(values in prop::collection::vec(-8i64..8,0..160),limit in 1usize..20) {
  let db=Database::memory();let mut tx=db.admin().transaction();tx.install(Module::new("app",1,vec![],vec![Relation::new("app.Item",vec![Role::new("value",DataType::Int)])])).unwrap();
  for (i,v) in values.iter().enumerate(){tx.insert_with_id("app.Item",Id::from_u128(i as u128+1),vec![Value::Int(*v)]).unwrap();}tx.commit().unwrap();
  let snapshot=db.admin().snapshot();
  for predicate in [None,Some(("value",Value::Int(2)))] {
   let mut actual=vec![];let mut after=None;
   loop {let page=snapshot.page("app.Item",predicate.as_ref().map(|(r,v)|(*r,v)),after,limit).unwrap();actual.extend(page.rows);after=page.next;if after.is_none(){break;}}
   let expected=snapshot.scan("app.Item").unwrap().into_iter().filter(|r|predicate.is_none()||r.values[0]==Value::Int(2)).collect::<Vec<_>>();prop_assert_eq!(actual,expected);
  }
 }
}
