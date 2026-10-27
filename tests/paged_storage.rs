//! Public API regression tests for current data served from disk.
use substrate_liquid::{
    paged::{PagedDatabase, PagedOptions},
    *,
};
fn id(n: u128) -> Id {
    Id::from_u128(n)
}
fn people() -> Module {
    Module::new(
        "people",
        1,
        vec![],
        vec![Relation {
            id: id(1000),
            name: "people.Person".into(),
            roles: vec![Role::new("name", DataType::Text)],
            unique: vec![vec!["name".into()]],
            checks: vec![Check::byte_length("name", 1, 64)],
        }],
    )
}
fn options() -> PagedOptions {
    PagedOptions {
        cache_bytes: 1024 * 1024,
        decoded_cache_bytes: 512,
        max_page_rows: 8,
        ..Default::default()
    }
}
fn fixture(path: &std::path::Path) -> PagedDatabase {
    let db = PagedDatabase::open(path, options()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install(people()).unwrap();
    tx.grant("reader", "people.Person", Grant::read_only())
        .unwrap();
    for n in 1..=25 {
        tx.insert_with_id(
            "people.Person",
            id(n),
            vec![format!("Person{n:02}").as_str().into()],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    db
}
#[test]
fn eviction_pagination_retained_snapshot_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("paged.redb");
    let db = fixture(&path);
    let old = db.session("reader").unwrap().snapshot().unwrap();
    let mut cursor = None;
    let mut all = Vec::new();
    loop {
        let page = old.scan_page("people.Person", cursor, 8).unwrap();
        all.extend(page.rows.into_iter().map(|r| r.id));
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(all, (1..=25).map(id).collect::<Vec<_>>());
    assert!(db.stats().unwrap().decoded_cache_bytes <= 512);
    let mut tx = db.admin().transaction().unwrap();
    tx.update("people.Person", id(1), vec!["Changed".into()])
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        old.get("people.Person", id(1)).unwrap().unwrap().values[0],
        Value::Text("Person01".into())
    );
    assert_eq!(
        db.admin()
            .snapshot()
            .unwrap()
            .lookup_page("people.Person", "name", &"Changed".into(), None, 8)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        db.admin()
            .snapshot()
            .unwrap()
            .lookup_page("people.Person", "name", &"Person01".into(), None, 8)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(old);
    drop(db);
    let db = PagedDatabase::open(&path, options()).unwrap();
    assert_eq!(db.stats().unwrap().decoded_cache_entries, 0);
    assert_eq!(
        db.admin()
            .get("people.Person", id(1))
            .unwrap()
            .unwrap()
            .values[0],
        Value::Text("Changed".into())
    );
}
#[test]
fn unique_conflict_and_independent_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let db = fixture(&tmp.path().join("db"));
    let mut a = db.admin().transaction().unwrap();
    let mut b = db.admin().transaction().unwrap();
    a.update("people.Person", id(1), vec!["A".into()]).unwrap();
    b.update("people.Person", id(2), vec!["B".into()]).unwrap();
    a.commit().unwrap();
    b.commit().unwrap();
    let mut a = db.admin().transaction().unwrap();
    let mut b = db.admin().transaction().unwrap();
    a.insert_with_id("people.Person", id(101), vec!["Same".into()])
        .unwrap();
    b.insert_with_id("people.Person", id(102), vec!["Same".into()])
        .unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(Error::Conflict)));
    assert!(db.admin().get("people.Person", id(102)).unwrap().is_none());
}
#[test]
fn failed_write_poison_is_atomic() {
    let tmp = tempfile::tempdir().unwrap();
    let db = fixture(&tmp.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.update("people.Person", id(1), vec!["Pending".into()])
        .unwrap();
    assert!(
        tx.insert_with_id("people.Person", id(99), vec![Value::Int(1)])
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
    assert_eq!(
        db.admin()
            .get("people.Person", id(1))
            .unwrap()
            .unwrap()
            .values[0],
        Value::Text("Person01".into())
    );
}
#[test]
fn wrong_openers_fail_without_modifying_other_formats() {
    let tmp = tempfile::tempdir().unwrap();
    let paged = tmp.path().join("paged");
    let resident = tmp.path().join("resident");
    drop(fixture(&paged));
    assert!(Database::open(&paged).is_err());
    assert_eq!(
        PagedDatabase::open(&paged, options())
            .unwrap()
            .admin()
            .get("people.Person", id(1))
            .unwrap()
            .unwrap()
            .id,
        id(1)
    );
    {
        let db = Database::open(&resident).unwrap();
        let mut tx = db.admin().transaction();
        tx.install(people()).unwrap();
        tx.insert_with_id("people.Person", id(1), vec!["Resident".into()])
            .unwrap();
        tx.commit().unwrap();
    }
    assert!(PagedDatabase::open(&resident, options()).is_err());
    assert_eq!(
        Database::open(&resident)
            .unwrap()
            .admin()
            .snapshot()
            .get("people.Person", id(1))
            .unwrap()
            .unwrap()
            .values[0],
        Value::Text("Resident".into())
    );
}
#[test]
fn private_visibility_live_revocation_and_separate_write_permission() {
    let tmp = tempfile::tempdir().unwrap();
    let db = fixture(&tmp.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.set_visibility("people.Person", Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    let reader = db.session("reader").unwrap();
    let old = reader.snapshot().unwrap();
    old.get("people.Person", id(1)).unwrap();
    assert!(matches!(
        db.anonymous().get("people.Person", id(1)),
        Err(Error::Denied)
    ));
    assert!(matches!(
        db.session("stranger").unwrap().get("people.Person", id(1)),
        Err(Error::Denied)
    ));
    let mut tx = reader.transaction().unwrap();
    assert!(matches!(
        tx.update("people.Person", id(1), vec!["No".into()]),
        Err(Error::Denied)
    ));
    let mut tx = db.admin().transaction().unwrap();
    tx.revoke("reader", "people.Person").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        old.get("people.Person", id(1)),
        Err(Error::Denied)
    ));
    let mut tx = db.admin().transaction().unwrap();
    tx.set_visibility("people.Person", Visibility::Public)
        .unwrap();
    tx.commit().unwrap();
    assert!(
        db.anonymous()
            .get("people.Person", id(1))
            .unwrap()
            .is_some()
    );
}
#[test]
fn limits_and_bounds_are_explicit() {
    let tmp = tempfile::tempdir().unwrap();
    let db = fixture(&tmp.path().join("db"));
    let s = db.admin().snapshot().unwrap();
    assert!(s.scan_page("people.Person", None, 0).is_err());
    assert!(s.scan_page("people.Person", None, 9).is_err());
    assert!(
        s.lookup_page("people.Person", "name", &Value::Int(1), None, 1)
            .is_err()
    );
    assert!(db.session("").is_err());
    assert!(db.session("\0admin").is_err());
}
#[test]
fn acknowledged_data_survives_process_exit_without_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("crash");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "crash_child", "--nocapture"])
        .env("SUBSTRATE_PAGED_CHILD_PATH", &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let db = PagedDatabase::open(&path, options()).unwrap();
    assert_eq!(
        db.admin()
            .get("people.Person", id(1))
            .unwrap()
            .unwrap()
            .values[0],
        Value::Text("Person01".into())
    );
    assert!(db.admin().get("people.Person", id(99)).unwrap().is_none());
}
#[test]
#[ignore]
fn crash_child() {
    let Some(path) = std::env::var_os("SUBSTRATE_PAGED_CHILD_PATH") else {
        return;
    };
    let db = fixture(std::path::Path::new(&path));
    let mut tx = db.admin().transaction().unwrap();
    tx.insert_with_id("people.Person", id(99), vec!["Uncommitted".into()])
        .unwrap();
    std::process::exit(73);
}
#[test]
fn independent_schema_extensions_preserve_shared_identities_and_old_readers() {
    let tmp = tempfile::tempdir().unwrap();
    let db = fixture(&tmp.path().join("db"));
    let mut grant = db.admin().transaction().unwrap();
    grant
        .grant(
            "game",
            "people.Person",
            Grant {
                read: true,
                write: false,
                reference: true,
                filter: RowFilter::All,
            },
        )
        .unwrap();
    grant.commit().unwrap();
    let old = db.session("reader").unwrap().snapshot().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(
        Module::new(
            "game",
            1,
            vec![("people".into(), 1)],
            vec![Relation {
                id: id(1001),
                name: "game.Profile".into(),
                roles: vec![Role::new("handle", DataType::Text)],
                unique: vec![],
                checks: vec![],
            }],
        ),
        "game",
        Visibility::Private,
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(old.get("people.Person", id(1)).unwrap().is_some());
    let mut tx = db.session("game").unwrap().transaction().unwrap();
    tx.extend(
        "people.Person",
        "game.Profile",
        id(1),
        vec!["player1".into()],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.session("game")
            .unwrap()
            .get("game.Profile", id(1))
            .unwrap()
            .unwrap()
            .id,
        id(1)
    );
    assert!(matches!(
        db.session("reader").unwrap().get("game.Profile", id(1)),
        Err(Error::Denied)
    ));
    assert_eq!(
        old.get("people.Person", id(1)).unwrap().unwrap().values[0],
        Value::Text("Person01".into())
    );
}
#[test]
fn references_and_concurrent_delete_remain_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let db = fixture(&tmp.path().join("db"));
    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new(
        "notes",
        1,
        vec![("people".into(), 1)],
        vec![Relation {
            id: id(1001),
            name: "notes.Note".into(),
            roles: vec![
                Role::new("person", DataType::Ref("people.Person".into())),
                Role::new("text", DataType::Text),
            ],
            unique: vec![],
            checks: vec![],
        }],
    ))
    .unwrap();
    tx.commit().unwrap();
    let mut delete = db.admin().transaction().unwrap();
    delete.delete("people.Person", id(1)).unwrap();
    let mut insert = db.admin().transaction().unwrap();
    insert
        .insert_with_id("notes.Note", id(100), vec![id(1).into(), "hi".into()])
        .unwrap();
    insert.commit().unwrap();
    assert!(matches!(delete.commit(), Err(Error::Conflict)));
    let mut tx = db.admin().transaction().unwrap();
    assert!(matches!(
        tx.delete("people.Person", id(1)),
        Err(Error::Constraint(_))
    ));
    let mut tx = db.admin().transaction().unwrap();
    tx.set_visibility("notes.Note", Visibility::Public).unwrap();
    tx.set_visibility("people.Person", Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    assert!(db.anonymous().get("notes.Note", id(100)).unwrap().is_none());
    assert!(
        db.anonymous()
            .snapshot()
            .unwrap()
            .schemas()
            .unwrap()
            .is_empty()
    );
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn disk_indexes_match_reference_model_after_mixed_writes(ops in proptest::collection::vec((0u8..3,0u8..5),1..45)) {
        use std::collections::BTreeMap;
        let tmp=tempfile::tempdir().unwrap();let path=tmp.path().join("property");
        let mut db=PagedDatabase::open(&path,options()).unwrap();
        let mut tx=db.admin().transaction().unwrap();tx.install(Module::new("model",1,vec![],vec![Relation{id:id(1000),name:"model.Item".into(),roles:vec![Role::new("value",DataType::Int)],unique:vec![],checks:vec![]}])).unwrap();tx.commit().unwrap();
        let mut expected=BTreeMap::<Id,i64>::new();let mut next=1;
        for (step,(operation,value)) in ops.iter().copied().enumerate() {
            let selected=expected.keys().nth(value as usize % expected.len().max(1)).copied();
            let mut tx=db.admin().transaction().unwrap();
            match (operation,selected) {
                (1,Some(key))=>{tx.update("model.Item",key,vec![Value::Int(value.into())]).unwrap();expected.insert(key,value.into());},
                (2,Some(key))=>{tx.delete("model.Item",key).unwrap();expected.remove(&key);},
                _=>{let key=id(next);next+=1;tx.insert_with_id("model.Item",key,vec![Value::Int(value.into())]).unwrap();expected.insert(key,value.into());}
            }
            tx.commit().unwrap();
            if step%11==0 {drop(db);db=PagedDatabase::open(&path,options()).unwrap();}
            let s=db.admin().snapshot().unwrap();
            let mut observed=BTreeMap::new();let mut after=None;
            loop {let p=s.scan_page("model.Item",after,7).unwrap();for row in p.rows {let Value::Int(v)=row.values[0] else {panic!("wrong type")};observed.insert(row.id,v);}after=p.next;if after.is_none(){break}}
            proptest::prop_assert_eq!(&observed,&expected);
            for value in 0..5 {
                let mut found=Vec::new();let mut after=None;
                loop {let p=s.lookup_page("model.Item","value",&Value::Int(value),after,3).unwrap();found.extend(p.rows.into_iter().map(|r|r.id));after=p.next;if after.is_none(){break}}
                proptest::prop_assert_eq!(found,expected.iter().filter(|(_,v)|**v==value).map(|(k,_)|*k).collect::<Vec<_>>());
            }
            proptest::prop_assert!(db.stats().unwrap().decoded_cache_bytes<=512);
        }
    }
}
