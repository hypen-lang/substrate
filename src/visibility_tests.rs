use super::*;
use crate::{
    capability::{CapabilityScope, RelationCapability},
    storage::Op,
};
fn graph(edges: &[(usize, usize)], allowed: &[bool]) -> (Database, Vec<Id>, Id) {
    let mut state = State::default();
    let relation = Relation::new(
        "graph.Node",
        vec![
            Role::new("allowed", DataType::Bool),
            Role::new("left", DataType::Ref("graph.Node".into())),
            Role::new("right", DataType::Ref("graph.Node".into())),
        ],
    );
    let tid = relation.id;
    state
        .apply_recovered(
            &Op::Install(Module::new("graph", 1, vec![], vec![relation])),
            1,
        )
        .unwrap();
    let ids: Vec<_> = (0..edges.len()).map(|_| Id::new()).collect();
    for id in &ids {
        state.apply_recovered(&Op::Identity(*id), 1).unwrap();
    }
    for (i, (left, right)) in edges.iter().enumerate() {
        state
            .apply_recovered(
                &Op::Put(
                    tid,
                    Row {
                        id: ids[i],
                        values: vec![
                            Value::Bool(allowed[i]),
                            ids[*left].into(),
                            ids[*right].into(),
                        ]
                        .into_boxed_slice(),
                    },
                ),
                1,
            )
            .unwrap();
    }
    state
        .apply_recovered(
            &Op::Grant(
                "reader".into(),
                tid,
                Some(Grant {
                    filter: RowFilter::Equals {
                        role: "allowed".into(),
                        value: Value::Bool(true),
                    },
                    ..Grant::read_only()
                }),
            ),
            1,
        )
        .unwrap();
    state.revision = 1;
    state.validate_recovered().unwrap();
    (Database::from_state(state, None), ids, tid)
}
#[test]
fn completed_cycle_results_do_not_hide_a_denied_exit_or_poison_unrelated_branch() {
    // 0->1->0 is a cycle; 1 also reaches denied2. Node3 is an independent good cycle.
    let (db, ids, _) = graph(
        &[(1, 3), (0, 2), (2, 2), (3, 3)],
        &[true, true, false, true],
    );
    let snap = db.session("reader").snapshot();
    assert!(snap.get("graph.Node", ids[0]).unwrap().is_none());
    assert!(snap.get("graph.Node", ids[1]).unwrap().is_none());
    assert!(snap.get("graph.Node", ids[3]).unwrap().is_some());
    assert_eq!(snap.visibility.lock().unwrap().expanded, 4);
    for _ in 0..5 {
        assert!(snap.clone().get("graph.Node", ids[0]).unwrap().is_none());
    }
    assert_eq!(snap.visibility.lock().unwrap().expanded, 4);
}
#[test]
fn shared_chain_expands_each_node_once_per_snapshot_including_negative_results() {
    for hidden in [false, true] {
        let n = 1000;
        let mut edges = vec![(0, 0)];
        for i in 1..n {
            edges.push((i - 1, i - 1));
        }
        for _ in 0..n {
            edges.push((n - 1, n - 1));
        }
        let mut allowed = vec![true; n * 2];
        allowed[0] = !hidden;
        let (db, ids, _) = graph(&edges, &allowed);
        let snap = db.session("reader").snapshot();
        for id in &ids[n..] {
            assert_eq!(snap.get("graph.Node", *id).unwrap().is_some(), !hidden);
        }
        assert_eq!(snap.visibility.lock().unwrap().expanded, 2 * n);
    }
}
#[test]
fn cache_is_bound_to_effective_state_and_principal_even_when_snapshot_is_rebound() {
    let (db, ids, tid) = graph(&[(0, 0)], &[true]);
    let mut tx = db.admin().transaction();
    tx.grant(
        "other",
        "graph.Node",
        Grant {
            filter: RowFilter::Equals {
                role: "allowed".into(),
                value: Value::Bool(false),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let snap = db.session("reader").snapshot();
    assert!(snap.get("graph.Node", ids[0]).unwrap().is_some());
    let mut different = snap.clone();
    different.principal = Some("other".into());
    assert!(different.get("graph.Node", ids[0]).unwrap().is_none());
    assert!(snap.get("graph.Node", ids[0]).unwrap().is_some());
    let mut altered = (*snap.state).clone();
    altered.apply(
        &Op::Grant(
            "reader".into(),
            tid,
            Some(Grant {
                filter: RowFilter::Equals {
                    role: "allowed".into(),
                    value: Value::Bool(false),
                },
                ..Grant::read_only()
            }),
        ),
        2,
    );
    let mut rebound = snap.clone();
    rebound.state = Arc::new(altered);
    assert!(rebound.get("graph.Node", ids[0]).unwrap().is_none());
    assert!(snap.get("graph.Node", ids[0]).unwrap().is_some());
}
#[test]
fn memo_does_not_replace_live_revocation_or_expand_selected_capability() {
    let (db, ids, tid) = graph(&[(0, 0)], &[true]);
    let ambient = db.session("reader").snapshot();
    assert!(ambient.get("graph.Node", ids[0]).unwrap().is_some());
    let root = db
        .issue_capability(
            Id::new(),
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: tid,
                    grant: Grant {
                        filter: RowFilter::Equals {
                            role: "allowed".into(),
                            value: Value::Bool(true),
                        },
                        ..Grant::read_only()
                    },
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let scoped = db
        .session("reader")
        .with_capability(root.id)
        .unwrap()
        .snapshot();
    assert!(scoped.get("graph.Node", ids[0]).unwrap().is_some());
    db.revoke_capability(root.id).unwrap();
    assert!(scoped.get("graph.Node", ids[0]).is_err());
    assert!(ambient.get("graph.Node", ids[0]).unwrap().is_some());
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "graph.Node").unwrap();
    tx.commit().unwrap();
    assert!(ambient.get("graph.Node", ids[0]).is_err());
}
#[test]
fn transaction_reads_still_track_reachable_rows_after_snapshot_cache_warmup() {
    let (db, ids, _) = graph(&[(0, 0), (0, 0)], &[true, true]);
    let session = db.session("reader");
    let snapshot = session.snapshot();
    assert!(snapshot.get("graph.Node", ids[1]).unwrap().is_some());
    let mut tx = session.transaction();
    assert!(tx.get("graph.Node", ids[1]).unwrap().is_some());
    let mut writer = db.admin().transaction();
    writer
        .update(
            "graph.Node",
            ids[0],
            vec![Value::Bool(false), ids[0].into(), ids[0].into()],
        )
        .unwrap();
    writer.commit().unwrap();
    assert!(matches!(tx.commit(), Err(Error::Conflict)));
    assert!(snapshot.get("graph.Node", ids[1]).unwrap().is_some()); // pinned old data
    assert!(
        session
            .snapshot()
            .get("graph.Node", ids[1])
            .unwrap()
            .is_none()
    );
}
#[test]
fn quota_counts_preserve_family_phantom_dependency_without_copying_rows() {
    let (db, ids, _) = graph(&[(0, 0)], &[true]);
    let mut tx = db.admin().transaction();
    assert_eq!(tx.count_internal_family("graph.Node").unwrap(), 1);
    let mut writer = db.admin().transaction();
    writer
        .insert(
            "graph.Node",
            vec![Value::Bool(true), ids[0].into(), ids[0].into()],
        )
        .unwrap();
    writer.commit().unwrap();
    assert!(matches!(tx.commit(), Err(Error::Conflict)));
    assert!(
        db.session("reader")
            .transaction()
            .count_internal_family("graph.Node")
            .is_err()
    );
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn memo_matches_fresh_reference_traversal_for_arbitrary_cyclic_graphs(nodes in proptest::collection::vec((0usize..64,0usize..64,proptest::bool::ANY),1..64)) {
        let n=nodes.len();let edges=nodes.iter().map(|(a,b,_)|(a%n,b%n)).collect::<Vec<_>>();let allowed=nodes.iter().map(|(_,_,c)|*c).collect::<Vec<_>>();
        let(db,ids,tid)=graph(&edges,&allowed);let snapshot=db.session("reader").snapshot();
        for id in ids.iter().rev() {
            let reference=snapshot.state.visible(Some("reader"),tid,*id,&mut BTreeSet::new(),&mut BTreeSet::new());
            proptest::prop_assert_eq!(snapshot.get("graph.Node",*id).unwrap().is_some(),reference);
        }
        proptest::prop_assert_eq!(snapshot.visibility.lock().unwrap().expanded,n);
    }
}
