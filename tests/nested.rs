use std::collections::BTreeMap;
use substrate_liquid::{nested::*, service::*, *};
fn plan(snapshot: &Snapshot, name: &str, limit: usize) -> ReadPlan {
    let def = snapshot.schema(name).unwrap();
    ReadPlan {
        contract: Contract {
            relation: name.into(),
            schema_id: def.id,
            fingerprint: def.fingerprint(),
        },
        filter: None,
        order: vec![],
        limit,
        include: BTreeMap::new(),
    }
}
fn fixture(n: usize) -> (Database, ReadPlan, Vec<Id>) {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.install(demo::fitness()).unwrap();
    tx.install(demo::crm()).unwrap();
    let mut people = vec![];
    for i in 0..n {
        let person = tx
            .insert_with_id(
                "core.Person",
                Id::from_u128(i as u128 + 1),
                vec![format!("person{i}").as_str().into()],
            )
            .unwrap();
        people.push(person);
        for minutes in [10, 20, 30] {
            let w = tx.insert("fitness.Workout", vec![minutes.into()]).unwrap();
            tx.insert("fitness.Performed", vec![person.into(), w.into()])
                .unwrap();
        }
    }
    tx.grant("fitness", "core.Person", Grant::read_only())
        .unwrap();
    tx.grant("fitness", "fitness.Performed", Grant::read_only())
        .unwrap();
    tx.grant("fitness", "fitness.Workout", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let s = db.admin().snapshot();
    let mut root = plan(&s, "core.Person", 1000);
    let mut performed = plan(&s, "fitness.Performed", 2);
    let mut workout = plan(&s, "fitness.Workout", 1);
    workout.filter = Some(Predicate::Compare {
        field: "durationMinutes".into(),
        comparison: Comparison::Ge,
        value: WireValue::Int("20".into()),
    });
    performed.include.insert(
        "workout".into(),
        Include {
            direction: Direction::One,
            role: "workout".into(),
            plan: workout,
        },
    );
    root.include.insert(
        "performed".into(),
        Include {
            direction: Direction::Many,
            role: "athlete".into(),
            plan: performed,
        },
    );
    (db, root, people)
}
fn many(row: &NestedRow) -> &Vec<NestedRow> {
    let NestedValue::Many(rows) = &row.related["performed"] else {
        panic!()
    };
    rows
}
#[test]
fn multiple_parents_use_three_statements_and_per_parent_limits() {
    let (db, p, people) = fixture(40);
    let result = db.session("fitness").snapshot().read_nested(&p).unwrap();
    assert_eq!(result.stats.sql_statements, 3);
    assert_eq!(result.rows.len(), 40);
    assert_eq!(
        result.stats.fetched_rows,
        40 + 80
            + result
                .rows
                .iter()
                .flat_map(many)
                .filter(|r| matches!(r.related["workout"], NestedValue::One(Some(_))))
                .count()
    );
    for (row, id) in result.rows.iter().zip(people) {
        assert_eq!(row.row.id, id);
        assert_eq!(many(row).len(), 2);
        for link in many(row) {
            assert_eq!(link.row.fields["athlete"], WireValue::Ref(id.to_string()));
        }
    }
}
#[test]
fn authorization_precedes_limits_and_reference_closure_hides_participants() {
    let (db, mut p, _) = fixture(3);
    p.include.get_mut("performed").unwrap().plan.limit = 1;
    let mut tx = db.admin().transaction();
    tx.grant(
        "fitness",
        "fitness.Workout",
        Grant {
            filter: RowFilter::Equals {
                role: "durationMinutes".into(),
                value: 20.into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let r = db.session("fitness").snapshot().read_nested(&p).unwrap();
    for person in &r.rows {
        assert_eq!(many(person).len(), 1);
        let NestedValue::One(Some(workout)) = &many(person)[0].related["workout"] else {
            panic!()
        };
        assert_eq!(
            workout.row.fields["durationMinutes"],
            WireValue::Int("20".into())
        );
    }
}
#[test]
fn forbidden_or_invalid_branches_are_rejected_even_when_root_is_empty() {
    let (db, mut p, _) = fixture(0);
    p.include
        .get_mut("performed")
        .unwrap()
        .plan
        .contract
        .relation = "crm.CustomerDeal".into();
    assert!(matches!(
        db.session("fitness").snapshot().read_nested(&p),
        Err(Error::Denied)
    ));
    let (_, mut p, _) = fixture(0);
    p.contract.fingerprint = "stale".into();
    assert!(matches!(
        db.admin().snapshot().read_nested(&p),
        Err(Error::Schema(_))
    ));
}
#[test]
fn nested_snapshot_pins_data_but_fails_after_live_revocation() {
    let (db, p, people) = fixture(2);
    let old = db.session("fitness").snapshot();
    let before = serde_json::to_value(old.read_nested(&p).unwrap()).unwrap();
    let mut tx = db.admin().transaction();
    tx.update_named("core.Person", people[0], [("name", "changed".into())])
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        serde_json::to_value(old.read_nested(&p).unwrap()).unwrap(),
        before
    );
    let mut tx = db.admin().transaction();
    tx.revoke("fitness", "fitness.Workout").unwrap();
    tx.commit().unwrap();
    assert!(matches!(old.read_nested(&p), Err(Error::Denied)));
    assert!(matches!(
        db.session("fitness").snapshot().read_nested(&p),
        Err(Error::Denied)
    ));
}
#[test]
fn invalid_roles_predicates_limits_and_depth_fail_before_execution() {
    let (db, p, _) = fixture(1);
    let s = db.admin().snapshot();
    let mut bad = p.clone();
    bad.limit = 0;
    assert!(s.read_nested(&bad).is_err());
    let mut bad = p.clone();
    bad.include.get_mut("performed").unwrap().role = "workout".into();
    assert!(s.read_nested(&bad).is_err());
    let mut bad = p.clone();
    bad.filter = Some(Predicate::Compare {
        field: "name".into(),
        comparison: Comparison::Eq,
        value: WireValue::Int("1".into()),
    });
    assert!(s.read_nested(&bad).is_err());
    let mut bad = p.clone();
    bad.order = vec![
        Order {
            field: "name".into(),
            descending: false
        };
        9
    ];
    assert!(s.read_nested(&bad).is_err());
    let mut bad = p.clone();
    let parent = bad.include["performed"].clone();
    for i in 0..18 {
        bad.include.insert(format!("branch{i}"), parent.clone());
    }
    assert!(s.read_nested(&bad).is_err());
}
#[test]
fn filters_are_parameters_empty_collections_and_nullable_forward_filters_are_explicit() {
    let (db, mut p, _) = fixture(2);
    p.filter = Some(Predicate::Compare {
        field: "name".into(),
        comparison: Comparison::Eq,
        value: WireValue::Text("' OR 1=1 --".into()),
    });
    let r = db.session("fitness").snapshot().read_nested(&p).unwrap();
    assert!(r.rows.is_empty());
    assert_eq!(r.stats.sql_statements, 1);
    p.filter = None;
    p.include.get_mut("performed").unwrap().plan.filter = Some(Predicate::Compare {
        field: "id".into(),
        comparison: Comparison::Eq,
        value: WireValue::Ref(Id::new().to_string()),
    });
    let r = db.session("fitness").snapshot().read_nested(&p).unwrap();
    assert_eq!(r.stats.sql_statements, 2);
    assert!(r.rows.iter().all(|r| many(r).is_empty()));
}
#[test]
fn shared_targets_and_duplicate_relationship_occurrences_preserve_multiplicity() {
    let (db, mut p, people) = fixture(2);
    let mut tx = db.admin().transaction();
    let w = tx.insert("fitness.Workout", vec![99.into()]).unwrap();
    for person in people {
        for _ in 0..2 {
            tx.insert("fitness.Performed", vec![person.into(), w.into()])
                .unwrap();
        }
    }
    tx.commit().unwrap();
    let child = &mut p.include.get_mut("performed").unwrap().plan;
    child.limit = 10;
    child.filter = Some(Predicate::Compare {
        field: "workout".into(),
        comparison: Comparison::Eq,
        value: WireValue::Ref(w.to_string()),
    });
    let r = db.session("fitness").snapshot().read_nested(&p).unwrap();
    assert_eq!(r.stats.sql_statements, 3);
    assert_eq!(r.stats.fetched_rows, 2 + 4 + 1);
    assert_eq!(r.stats.returned_nodes, 2 + 4 + 4);
    for parent in &r.rows {
        assert_eq!(many(parent).len(), 2);
        assert_ne!(many(parent)[0].row.id, many(parent)[1].row.id);
    }
}
#[test]
fn gateway_returns_nested_result_under_bound_principal() {
    let (db, p, _) = fixture(2);
    let token = "nested-test-token-000000000000000000";
    let g = Gateway::new(
        db,
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: token.into(),
                principal: "fitness".into(),
            }],
        },
    )
    .unwrap();
    assert!(
        matches!(g.handle(Request{version:1,token:token.into(),command:Command::Read{plan:p}}),Response::Ok(Output::Read{result}) if result.rows.len()==2&&result.stats.sql_statements==3)
    );
}

#[test]
fn unrelated_readable_modules_do_not_expand_the_selected_sql_catalog() {
    let (db, p, _) = fixture(1);
    let mut tx = db.admin().transaction();
    for n in 0..12 {
        let module = format!("extra{n}");
        let name = format!("{module}.Item");
        tx.install(Module::new(
            &module,
            1,
            vec![],
            vec![Relation::new(&name, vec![])],
        ))
        .unwrap();
        tx.grant("fitness", &name, Grant::read_only()).unwrap();
    }
    tx.commit().unwrap();
    let snapshot = db.session("fitness").snapshot();
    assert_eq!(snapshot.read_nested(&p).unwrap().rows.len(), 1);
    let g = Gateway::new(
        db,
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: "scoped-catalog-test-0000000000000000".into(),
                principal: "fitness".into(),
            }],
        },
    )
    .unwrap();
    let command = Command::Checked {
        contracts: vec![p.contract],
        command: Box::new(Command::Query {
            sql: "SELECT name FROM core.Person".into(),
            params: vec![],
        }),
    };
    assert!(
        matches!(g.handle(Request{version:1,token:"scoped-catalog-test-0000000000000000".into(),command}),Response::Ok(Output::Query{rows,..}) if rows.len()==1)
    );
}
#[test]
fn ranking_helper_cannot_collide_with_application_role_names() {
    let (db, mut p, people) = fixture(1);
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "extra",
        1,
        vec![("core".into(), 1)],
        vec![Relation::new(
            "extra.Item",
            vec![
                Role::new("person", DataType::Ref("core.Person".into())),
                Role::new("__liquid_rank", DataType::Int),
            ],
        )],
    ))
    .unwrap();
    for value in [500, 300, 100] {
        tx.insert("extra.Item", vec![people[0].into(), value.into()])
            .unwrap();
    }
    tx.commit().unwrap();
    let s = db.admin().snapshot();
    let mut child = plan(&s, "extra.Item", 1);
    child.order.push(Order {
        field: "__liquid_rank".into(),
        descending: false,
    });
    p.include.clear();
    p.include.insert(
        "items".into(),
        Include {
            direction: Direction::Many,
            role: "person".into(),
            plan: child,
        },
    );
    let r = s.read_nested(&p).unwrap();
    let NestedValue::Many(rows) = &r.rows[0].related["items"] else {
        panic!()
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].row.fields["__liquid_rank"],
        WireValue::Int("100".into())
    );
}
#[test]
fn output_and_depth_budgets_fail_explicitly_without_partial_results() {
    let (db, mut p, _) = fixture(300);
    let branch = p.include["performed"].clone();
    for i in 1..8 {
        p.include.insert(format!("extra{i}"), branch.clone());
    }
    assert!(db.admin().snapshot().read_nested(&p).is_err());
    let s = db.admin().snapshot();
    let mut node = plan(&s, "core.Person", 1);
    for _ in 0..3 {
        let mut link = plan(&s, "fitness.Performed", 1);
        link.include.insert(
            "person".into(),
            Include {
                direction: Direction::One,
                role: "athlete".into(),
                plan: node,
            },
        );
        let mut root = plan(&s, "core.Person", 1);
        root.include.insert(
            "links".into(),
            Include {
                direction: Direction::Many,
                role: "athlete".into(),
                plan: link,
            },
        );
        node = root;
    }
    assert!(s.read_nested(&node).is_err());
}

use proptest::prelude::*;
proptest! {
 #![proptest_config(ProptestConfig::with_cases(96))]
 #[test]
 fn randomized_nested_reads_match_authorized_enumeration(
  input in prop::collection::vec((0usize..8,0i64..5),0..80),
  root_limit in 1usize..9,per_parent in 1usize..6,allowed in 0i64..5,threshold in 0i64..5,restricted in any::<bool>(),descending in any::<bool>()
 ) {
  let (db,mut p,people)=fixture(0);let mut tx=db.admin().transaction();
  let _=people;
  for n in 0..8 {tx.insert_with_id("core.Person",Id::from_u128(n+1),vec![format!("person{n}").as_str().into()]).unwrap();}
  for (i,(person,value)) in input.iter().enumerate() {let w=Id::from_u128(i as u128+1000);tx.insert_with_id("fitness.Workout",w,vec![(*value).into()]).unwrap();tx.insert_with_id("fitness.Performed",Id::from_u128(i as u128+100),vec![Id::from_u128(*person as u128+1).into(),w.into()]).unwrap();}
  if restricted {tx.grant("fitness","fitness.Workout",Grant{filter:RowFilter::Equals{role:"durationMinutes".into(),value:allowed.into()},..Grant::read_only()}).unwrap();}tx.commit().unwrap();
  p.limit=root_limit;let child=&mut p.include.get_mut("performed").unwrap().plan;child.limit=per_parent;child.order=vec![Order{field:"id".into(),descending}];
  child.include.get_mut("workout").unwrap().plan.filter=Some(Predicate::Compare{field:"durationMinutes".into(),comparison:Comparison::Gt,value:WireValue::Int(threshold.to_string())});
  let r=db.session("fitness").snapshot().read_nested(&p).unwrap();prop_assert_eq!(r.rows.len(),root_limit);let mut count=0;
  for (person,row) in r.rows.iter().enumerate() {
   let mut expected=input.iter().enumerate().filter(|(_, (owner,value))|*owner==person&&(!restricted||*value==allowed)).collect::<Vec<_>>();if descending {expected.reverse();}expected.truncate(per_parent);
   prop_assert_eq!(many(row).len(),expected.len());count+=expected.len();
   for (actual,(i,(_,value))) in many(row).iter().zip(expected) {
    prop_assert_eq!(actual.row.id,Id::from_u128(i as u128+100));
    let NestedValue::One(target)=&actual.related["workout"] else {panic!()};prop_assert_eq!(target.is_some(),*value>threshold);
    if let Some(target)=target {prop_assert_eq!(&target.row.fields["durationMinutes"],&WireValue::Int(value.to_string()));}
   }
  }
  prop_assert_eq!(r.stats.sql_statements,if count>0 {3} else {2});
 }
}
