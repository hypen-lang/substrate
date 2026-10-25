use proptest::prelude::*;
use std::collections::BTreeMap;
use substrate_liquid::*;

// Independent oracle: plain ordered maps, no engine indexes or replay routines.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn histories_survive_checkpoints_restarts_and_policy_changes(
        ops in prop::collection::vec((0u8..9,0usize..100,-10i64..10),10..55),
        budget in 2u64..9,
    ) {
        let dir=tempfile::tempdir().unwrap(); let path=dir.path().join("db");
        let options=DatabaseOptions { checkpoint_policy:Some(CheckpointPolicy {max_log_commits:budget,max_log_bytes:4096}) };
        let mut db=Database::open_with_options(&path,options).unwrap();
        let mut tx=db.admin().transaction();
        let def=Relation::new("core.Entry",vec![Role::new("value",DataType::Int)]);
        let role_id=def.role_id("value").unwrap();
        tx.install(Module::new("core",1,vec![],vec![def])).unwrap();tx.commit().unwrap();
        let mut expected=BTreeMap::<Id,i64>::new();let mut permitted=None;let mut extensions=0;
        for (op,choice,value) in ops {
            let old=db.admin().snapshot(); let old_rows=old.scan("core.Entry").unwrap();
            match op {
                0|1 => {let mut tx=db.admin().transaction();let id=tx.insert("core.Entry",vec![value.into()]).unwrap();tx.commit().unwrap();expected.insert(id,value);}
                2|3 if !expected.is_empty() => {
                    let id=*expected.keys().nth(choice%expected.len()).unwrap();let mut tx=db.admin().transaction();
                    if op==2 {tx.update("core.Entry",id,vec![value.into()]).unwrap();expected.insert(id,value);} else {tx.delete("core.Entry",id).unwrap();expected.remove(&id);}
                    tx.commit().unwrap();
                }
                4 => {db.checkpoint().unwrap();}
                5 => {drop(db);db=Database::open_with_options(&path,options).unwrap();prop_assert!(db.recovery_info().unwrap().replayed_commits<budget);}
                6 => {let mut tx=db.admin().transaction();tx.grant("reader","core.Entry",Grant {filter:RowFilter::Equals {role:"value".into(),value:value.into()},..Grant::read_only()}).unwrap();tx.commit().unwrap();permitted=Some(value);}
                7 => {let mut tx=db.admin().transaction();tx.revoke("reader","core.Entry").unwrap();tx.commit().unwrap();permitted=None;}
                _ => {
                    extensions+=1;let mut tx=db.admin().transaction();
                    let name=format!("extra.Tag{extensions}");
                    tx.install(Module::new("extra",extensions,vec![],vec![Relation::new(&name,vec![])])).unwrap();tx.commit().unwrap();
                }
            }
            let snapshot=db.admin().snapshot();snapshot.verify_indexes().unwrap();
            prop_assert_eq!(snapshot.schema("core.Entry").unwrap().role_id("value").unwrap(),role_id);
            prop_assert_eq!(old.scan("core.Entry").unwrap(),old_rows);
            let actual=snapshot.scan("core.Entry").unwrap().into_iter().map(|r|(r.id,r.values[0].clone())).collect::<BTreeMap<_,_>>();
            prop_assert_eq!(actual,expected.iter().map(|(id,v)|(*id,Value::Int(*v))).collect::<BTreeMap<_,_>>());
            let indexed=snapshot.lookup("core.Entry","value",&value.into()).unwrap().into_iter().map(|r|r.id).collect::<Vec<_>>();
            prop_assert_eq!(indexed,expected.iter().filter(|(_,v)|**v==value).map(|(id,_)|*id).collect::<Vec<_>>());
            let visible=db.session("reader").snapshot().scan("core.Entry");
            if let Some(allowed)=permitted {
                prop_assert_eq!(visible.unwrap().iter().map(|r|r.id).collect::<Vec<_>>(),expected.iter().filter(|(_,v)|**v==allowed).map(|(id,_)|*id).collect::<Vec<_>>());
            } else {prop_assert!(matches!(visible,Err(Error::Denied)));}
            let storage=db.storage_info().unwrap();prop_assert!(storage.log_commits<budget);prop_assert!(storage.log_bytes<4096);
        }
        db.checkpoint().unwrap();drop(db);let db=Database::open_with_options(&path,options).unwrap();
        prop_assert_eq!(db.recovery_info().unwrap().replayed_commits,0);
        let mut actual=db.admin().snapshot().sql().unwrap().query("SELECT value FROM core.Entry",&[]).unwrap().rows.into_iter().map(|r| match r[0] {rusqlite::types::Value::Integer(n)=>n,_=>panic!("expected integer")}).collect::<Vec<_>>();
        let mut values=expected.values().copied().collect::<Vec<_>>();actual.sort();values.sort();prop_assert_eq!(actual,values);
    }
}
