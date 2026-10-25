use rusqlite::types::Value as S;
use substrate_liquid::*;

fn fixture() -> Database {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "probes",
            1,
            vec![],
            vec![
                Relation::new(
                    "probes.Left",
                    vec![
                        Role::new("bucket", DataType::Int),
                        Role::new("key", DataType::Text),
                    ],
                ),
                Relation::new(
                    "probes.Right",
                    vec![
                        Role::new("key", DataType::Text),
                        Role::new("n", DataType::Int),
                        Role::new("visible", DataType::Bool),
                    ],
                ),
            ],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    for key in ["a", "a", "b", "missing"] {
        tx.insert("probes.Left", vec![1.into(), key.into()])
            .unwrap();
    }
    for i in 0..1000 {
        let key = match i {
            0 | 1 => "a".into(),
            2 => "b".into(),
            _ => format!("k{i}"),
        };
        tx.insert_with_id(
            "probes.Right",
            Id::from_u128(0xab00 + i as u128),
            vec![Value::Text(key), Value::Int(i), Value::Bool(i != 1)],
        )
        .unwrap();
    }
    for i in 0..200 {
        tx.insert("probes.Left", vec![2.into(), Value::Text(format!("k{i}"))])
            .unwrap();
    }
    tx.grant("reader", "probes.Left", Grant::read_only())
        .unwrap();
    tx.grant(
        "reader",
        "probes.Right",
        Grant {
            filter: RowFilter::Equals {
                role: "visible".into(),
                value: Value::Bool(true),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    db
}

fn plan(sql: &SqlSnapshot, query: &str, params: &[S], analyze: bool) -> String {
    format!(
        "{:?}",
        sql.query(
            &format!("EXPLAIN {}{query}", if analyze { "ANALYZE " } else { "" }),
            params
        )
        .unwrap()
    )
}

#[test]
fn selective_join_probes_deduplicate_keys_without_deduplicating_matches() {
    let db = fixture();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let query = "SELECT l.key,r.n FROM probes.Left l JOIN probes.Right r ON l.key=r.key WHERE l.bucket=?1 ORDER BY l.key,r.n";
    let params = [S::Integer(1)];
    let explanation = plan(&sql, query, &params, false);
    assert!(explanation.contains("LiquidIndexJoinExec"), "{explanation}");
    let explanation = plan(&sql, query, &params, true);
    assert!(explanation.contains("probe_keys=3"), "{explanation}");
    assert!(explanation.contains("probe_rows=3"), "{explanation}");
    let expected = sqlite.query(query, &params).unwrap().rows;
    assert_eq!(expected.len(), 5);
    let mut prepared = sql.prepare(query).unwrap();
    for value in [1, 1, 2, 1] {
        let params = [S::Integer(value)];
        assert_eq!(
            prepared.query(&params).unwrap().rows,
            sqlite.query(query, &params).unwrap().rows
        );
    }
    assert!(!plan(&sql, query, &[S::Integer(2)], true).contains("LiquidIndexJoinExec"));
}

#[test]
fn probe_filters_limits_nulls_and_unsupported_join_types_match_sqlite() {
    let db = fixture();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let nullable = "SELECT l.n,r.n FROM (SELECT CASE WHEN key='b' THEN NULL ELSE bucket END AS n FROM probes.Left WHERE bucket=1) l JOIN probes.Right r ON l.n=r.n ORDER BY l.n,r.n";
    let nullable_plan = plan(&sql, nullable, &[], true);
    assert!(
        nullable_plan.contains("LiquidIndexJoinExec"),
        "{nullable_plan}"
    );
    let nullable_plan = plan(&sql, nullable, &[], false);
    assert!(
        nullable_plan.contains("LiquidIndexJoinExec"),
        "{nullable_plan}"
    );
    assert_eq!(
        sql.query(nullable, &[]).unwrap().rows,
        sqlite.query(nullable, &[]).unwrap().rows
    );
    for query in [
        "SELECT l.key,r.n FROM probes.Left l JOIN probes.Right r ON l.key=r.key AND r.n>0 WHERE l.bucket=1 ORDER BY l.key,r.n LIMIT 2 OFFSET 1",
        "SELECT l.key,r.n FROM probes.Left l JOIN probes.Right r ON l.key=r.key AND l.bucket<r.n WHERE l.bucket=1 ORDER BY l.key,r.n",
        "SELECT l.key,r.n FROM probes.Left l LEFT JOIN probes.Right r ON l.key=r.key WHERE l.bucket=1 ORDER BY l.key,r.n",
        "SELECT l.key FROM probes.Left l WHERE l.bucket=1 AND EXISTS (SELECT 1 FROM probes.Right r WHERE r.key=l.key) ORDER BY l.key",
        "SELECT l.key,r.n FROM (SELECT CASE WHEN key='b' THEN NULL ELSE key END AS key FROM probes.Left WHERE bucket=1) l JOIN probes.Right r ON l.key=r.key ORDER BY l.key,r.n",
    ] {
        for _ in 0..2 {
            assert_eq!(
                sql.query(query, &[]).unwrap().rows,
                sqlite.query(query, &[]).unwrap().rows,
                "{query}"
            );
        }
    }
    let outer = plan(
        &sql,
        "SELECT l.key,r.n FROM probes.Left l LEFT JOIN probes.Right r ON l.key=r.key WHERE l.bucket=1",
        &[],
        true,
    );
    assert!(!outer.contains("LiquidIndexJoinExec"), "{outer}");
}

#[test]
fn identity_probes_reject_noncanonical_keys_and_keep_row_policies_live() {
    let db = fixture();
    let canonical = Id::from_u128(0xab00).to_string();
    let mut tx = db.admin().transaction();
    for key in [
        canonical.clone(),
        canonical.to_uppercase(),
        "invalid".into(),
    ] {
        tx.insert("probes.Left", vec![3.into(), Value::Text(key)])
            .unwrap();
    }
    tx.commit().unwrap();
    let snapshot = db.session("reader").snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let query = "SELECT l.key,r.n FROM probes.Left l JOIN probes.Right r ON l.key=r.id WHERE l.bucket=3 ORDER BY l.key,r.n";
    assert_eq!(
        sql.query(query, &[]).unwrap().rows,
        sqlite.query(query, &[]).unwrap().rows
    );
    assert_eq!(sql.query(query, &[]).unwrap().rows.len(), 1);
    let filtered = "SELECT l.key,r.n FROM probes.Left l JOIN probes.Right r ON l.key=r.key WHERE l.bucket=1 ORDER BY l.key,r.n";
    let mut prepared = sql.prepare(filtered).unwrap();
    for _ in 0..2 {
        assert_eq!(
            prepared.query(&[]).unwrap().rows,
            sqlite.query(filtered, &[]).unwrap().rows
        );
        assert_eq!(prepared.query(&[]).unwrap().rows.len(), 3);
    }
    let mut tx = db.admin().transaction();
    tx.revoke("reader", "probes.Right").unwrap();
    tx.commit().unwrap();
    assert!(matches!(prepared.query(&[]), Err(Error::Denied)));
}

#[test]
fn reference_probe_closure_hides_inaccessible_targets() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), substrate_liquid::Visibility::Private)
        .unwrap();
    tx.install_with_visibility(demo::fitness(), substrate_liquid::Visibility::Private)
        .unwrap();
    let ada = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    for minutes in [10, 20, 30] {
        let workout = tx.insert("fitness.Workout", vec![minutes.into()]).unwrap();
        tx.insert("fitness.Performed", vec![ada.into(), workout.into()])
            .unwrap();
    }
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.grant("reader", "fitness.Performed", Grant::read_only())
        .unwrap();
    tx.grant(
        "reader",
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
    let snapshot = db.session("reader").snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let query = "SELECT p.name,w.durationMinutes FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout WHERE p.id=?1 ORDER BY w.durationMinutes";
    let params = [S::Text(ada.to_string())];
    assert_eq!(
        sql.query(query, &params).unwrap().rows,
        sqlite.query(query, &params).unwrap().rows
    );
    assert_eq!(
        sql.query(query, &params).unwrap().rows,
        vec![vec![S::Text("Ada".into()), S::Integer(20)]]
    );
}

#[test]
fn relationship_and_five_way_probes_restore_ordered_join_contracts() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    for module in [demo::core(), demo::fitness(), demo::crm()] {
        tx.install_with_visibility(module, substrate_liquid::Visibility::Private)
            .unwrap();
    }
    let person = Id::from_u128(1);
    tx.insert_with_id("core.Person", person, vec!["Ada".into()])
        .unwrap();
    for n in 0..64u128 {
        let workout = Id::from_u128(1000 + n);
        let deal = Id::from_u128(2000 + n);
        tx.insert_with_id("fitness.Workout", workout, vec![Value::Int(n as i64)])
            .unwrap();
        tx.insert_with_id("crm.Deal", deal, vec![Value::Decimal(n as i64)])
            .unwrap();
        if n < 2 {
            // Link identity order is the reverse of the referenced target order.
            // Probe order therefore cannot stand in for the original HashJoin
            // ordering inherited from its right-hand identity scan.
            tx.insert_with_id(
                "fitness.Performed",
                Id::from_u128(3000 - n),
                vec![person.into(), workout.into()],
            )
            .unwrap();
            tx.insert_with_id(
                "crm.CustomerDeal",
                Id::from_u128(4000 - n),
                vec![person.into(), deal.into()],
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    let params = [S::Text(person.to_string())];
    let relationship = "SELECT f.id,w.id,w.durationMinutes FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout WHERE f.athlete=?1 ORDER BY f.id";
    let five_way = "SELECT p.id,f.id,w.durationMinutes,c.id,d.amount FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout JOIN crm.CustomerDeal c ON c.customer=p.id JOIN crm.Deal d ON d.id=c.deal WHERE p.id=?1 ORDER BY f.id,c.id";
    for (query, joins) in [(relationship, 1), (five_way, 4)] {
        let explanation = plan(&sql, query, &params, false);
        assert_eq!(
            explanation.matches("LiquidIndexJoinExec").count(),
            joins,
            "{explanation}"
        );
        assert!(!explanation.contains("HashJoinExec"), "{explanation}");
        for _ in 0..2 {
            assert_eq!(
                sql.query(query, &params).unwrap().rows,
                sqlite.query(query, &params).unwrap().rows
            );
        }
    }
    for query in [
        "SELECT f.id,w.id FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout WHERE f.athlete=?1 ORDER BY w.id",
        "SELECT f.id,w.id FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout WHERE f.athlete=?1 ORDER BY w.id LIMIT 1",
        "SELECT f.id,w.id FROM fitness.Performed f JOIN fitness.Workout w ON w.id=f.workout WHERE f.athlete=?1 ORDER BY f.workout LIMIT 1 OFFSET 1",
    ] {
        if !query.contains("LIMIT") {
            let explanation = plan(&sql, query, &params, false);
            assert!(explanation.contains("LiquidIndexJoinExec"), "{explanation}");
        }
        assert_eq!(
            sql.query(query, &params).unwrap().rows,
            sqlite.query(query, &params).unwrap().rows,
            "{query}"
        );
    }
}
