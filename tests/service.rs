use std::collections::BTreeMap;
use substrate_liquid::{service::*, *};
const TOKEN: &str = "test-only-fitness-token-000000000000";
fn fixture() -> (Database, Gateway, SchemaManifest, Id) {
    let db = Database::memory();
    let manifest = SchemaManifest::from_json(include_str!("../examples/dx/manifest.json")).unwrap();
    let mut tx = db.admin().transaction();
    tx.install_manifest_with_visibility(&manifest, Visibility::Private)
        .unwrap();
    let person = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.grant(
        "fitness",
        "core.Person",
        Grant {
            reference: true,
            ..Grant::read_only()
        },
    )
    .unwrap();
    for name in ["fitness.Workout", "fitness.Performed"] {
        tx.grant("fitness", name, Grant::read_write()).unwrap();
    }
    tx.commit().unwrap();
    let gateway = Gateway::new(
        db.clone(),
        ServerConfig {
            version: 1,
            clients: vec![
                ClientCredential {
                    token: TOKEN.into(),
                    principal: "fitness".into(),
                },
                ClientCredential {
                    token: "test-admin-is-not-root-000000000000".into(),
                    principal: "admin".into(),
                },
            ],
        },
    )
    .unwrap();
    (db, gateway, manifest, person)
}
fn call(g: &Gateway, command: Command) -> Response {
    g.handle(Request {
        version: 1,
        token: TOKEN.into(),
        command,
    })
}
fn id(m: &SchemaManifest, name: &str) -> Id {
    m.modules
        .iter()
        .flat_map(|m| &m.relations)
        .find(|r| r.name == name)
        .unwrap()
        .id
}
fn insert(
    m: &SchemaManifest,
    name: &str,
    fields: impl IntoIterator<Item = (&'static str, WireValue)>,
    alias: Option<&str>,
) -> Mutation {
    Mutation::Insert {
        relation: name.into(),
        schema_id: id(m, name),
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        alias: alias.map(str::to_owned),
    }
}
#[test]
fn remote_batches_are_atomic_and_preserve_exact_integers() {
    let (db, g, m, p) = fixture();
    let operations = vec![
        insert(
            &m,
            "fitness.Workout",
            [("durationMinutes", WireValue::Int(i64::MAX.to_string()))],
            Some("workout"),
        ),
        insert(
            &m,
            "fitness.Performed",
            [
                ("athlete", WireValue::Ref(p.to_string())),
                ("workout", WireValue::Ref("$workout".into())),
            ],
            None,
        ),
    ];
    assert!(
        matches!(call(&g,Command::Batch{operations}),Response::Ok(Output::Batch{ids}) if ids.len()==2)
    );
    assert!(
        matches!(call(&g,Command::Query{sql:"SELECT durationMinutes FROM fitness.Workout".into(),params:vec![]}),Response::Ok(Output::Query{rows,..}) if rows==vec![vec![SqlWireValue::Integer(i64::MAX.to_string())]])
    );
    let before = db.admin().snapshot().stats().unwrap();
    let operations = vec![
        insert(
            &m,
            "fitness.Workout",
            [("durationMinutes", WireValue::Int("1".into()))],
            Some("workout"),
        ),
        insert(
            &m,
            "fitness.Performed",
            [
                ("athlete", WireValue::Ref(Id::new().to_string())),
                ("workout", WireValue::Ref("$workout".into())),
            ],
            None,
        ),
    ];
    assert!(matches!(
        call(&g, Command::Batch { operations }),
        Response::Error(_)
    ));
    assert_eq!(db.admin().snapshot().stats().unwrap(), before);
}
#[test]
fn authentication_admin_names_and_cross_app_access_fail_closed() {
    let (_, g, m, _) = fixture();
    assert!(
        matches!(g.handle(Request{version:1,token:"wrong".into(),command:Command::Capabilities}),Response::Error(e) if e.code=="denied")
    );
    assert!(
        matches!(g.handle(Request{version:1,token:"test-admin-is-not-root-000000000000".into(),command:Command::Capabilities}),Response::Ok(Output::Capabilities{relations,..}) if relations.is_empty())
    );
    let command = Command::Page {
        relation: "crm.Deal".into(),
        schema_id: id(&m, "crm.Deal"),
        after: None,
        limit: 10,
        expected_revision: None,
        filter: None,
    };
    assert!(matches!(call(&g,command),Response::Error(e) if e.code=="denied"));
    let operations = vec![insert(
        &m,
        "crm.Deal",
        [("amount", WireValue::Decimal("100".into()))],
        None,
    )];
    assert!(matches!(call(&g,Command::Batch{operations}),Response::Error(e) if e.code=="denied"));
    assert!(serde_json::from_str::<Request>(&format!(r#"{{"version":1,"token":"{TOKEN}","principal":"admin","command":{{"op":"capabilities"}}}}"#)).is_err());
}
#[test]
fn schema_guards_revocation_and_pagination_revision_guards_are_enforced() {
    let (db, g, m, _) = fixture();
    let command = |schema_id, expected_revision| Command::Page {
        relation: "core.Person".into(),
        schema_id,
        after: None,
        limit: 1,
        expected_revision,
        filter: None,
    };
    assert!(matches!(call(&g,command(Id::new(),None)),Response::Error(e) if e.code=="schema"));
    let revision = match call(&g, command(id(&m, "core.Person"), None)) {
        Response::Ok(Output::Page { revision, .. }) => revision,
        _ => panic!(),
    };
    let mut tx = db.admin().transaction();
    tx.insert("core.Person", vec!["Grace".into()]).unwrap();
    tx.commit().unwrap();
    assert!(
        matches!(call(&g,command(id(&m,"core.Person"),Some(revision))),Response::Error(e) if e.code=="conflict")
    );
    let mut tx = db.admin().transaction();
    tx.revoke("fitness", "core.Person").unwrap();
    tx.commit().unwrap();
    assert!(
        matches!(call(&g,command(id(&m,"core.Person"),None)),Response::Error(e) if e.code=="denied")
    );
}
#[test]
fn invalid_aliases_numeric_payloads_versions_and_batch_sizes_do_not_write() {
    let (db, g, m, _) = fixture();
    let before = db.admin().snapshot().stats().unwrap();
    for n in ["9223372036854775808", "not-an-integer"] {
        let op = insert(
            &m,
            "fitness.Workout",
            [("durationMinutes", WireValue::Int(n.into()))],
            None,
        );
        assert!(matches!(
            call(
                &g,
                Command::Batch {
                    operations: vec![op]
                }
            ),
            Response::Error(_)
        ));
    }
    let op = insert(
        &m,
        "fitness.Workout",
        [("durationMinutes", WireValue::Int("1".into()))],
        Some("duplicate"),
    );
    for operations in [vec![], vec![op.clone(), op.clone()], vec![op; 101]] {
        assert!(matches!(
            call(&g, Command::Batch { operations }),
            Response::Error(_)
        ));
    }
    assert!(
        matches!(g.handle(Request{version:2,token:TOKEN.into(),command:Command::Capabilities}),Response::Error(e) if e.code=="protocol")
    );
    assert_eq!(db.admin().snapshot().stats().unwrap(), before);
    assert!(
        Gateway::new(
            db,
            ServerConfig {
                version: 1,
                clients: vec![ClientCredential {
                    token: "short".into(),
                    principal: "x".into()
                }]
            }
        )
        .is_err()
    );
}
#[test]
fn paged_index_and_family_scans_match_reference_without_skipping_or_duplicating() {
    let (db, _, _, _) = fixture();
    let mut tx = db.admin().transaction();
    for i in 0..220 {
        tx.insert("fitness.Workout", vec![(i % 4).into()]).unwrap();
    }
    tx.commit().unwrap();
    let snapshot = db.admin().snapshot();
    for predicate in [None, Some(("durationMinutes", Value::Int(2)))] {
        let mut after = None;
        let mut rows = Vec::new();
        loop {
            let page = snapshot
                .page(
                    "fitness.Workout",
                    predicate.as_ref().map(|(r, v)| (*r, v)),
                    after,
                    7,
                )
                .unwrap();
            rows.extend(page.rows);
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        let expected = if let Some((r, v)) = &predicate {
            snapshot.lookup("fitness.Workout", r, v).unwrap()
        } else {
            snapshot.scan("fitness.Workout").unwrap()
        };
        assert_eq!(rows, expected);
    }
    let result = snapshot
        .sql()
        .unwrap()
        .query(
            "SELECT durationMinutes FROM fitness.Workout ORDER BY durationMinutes",
            &[],
        )
        .unwrap();
    assert_eq!(result.rows.len(), 220);
}
#[test]
fn sql_budgets_stop_work_and_reset_for_the_next_query() {
    let (db, _, _, _) = fixture();
    let sql = db.admin().snapshot().sql().unwrap();
    let limits = QueryLimits {
        max_rows: 2,
        max_bytes: 64,
        vm_steps: 1000,
        timeout_ms: 5_000,
    };
    assert!(
        sql.query_with_limits(
            "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
            &[],
            limits
        )
        .is_err()
    );
    assert!(
        sql.query_with_limits("SELECT hex(zeroblob(100))", &[], limits)
            .is_err()
    );
    assert!(
        sql.query_with_limits(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n",
            &[],
            limits
        )
        .is_err()
    );
    assert_eq!(
        sql.query_with_limits("SELECT 42", &[], limits)
            .unwrap()
            .rows,
        vec![vec![rusqlite::types::Value::Integer(42)]]
    );
}
#[test]
fn capability_discovery_exposes_only_explicitly_granted_families() {
    let (_, g, _, _) = fixture();
    let Response::Ok(Output::Capabilities { relations, .. }) = call(&g, Command::Capabilities)
    else {
        panic!()
    };
    let names = relations
        .into_iter()
        .map(|c| (c.relation.name, (c.read, c.write, c.reference)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(names.len(), 3);
    assert_eq!(names["core.Person"], (true, false, true));
    assert!(!names.contains_key("crm.Deal"));
}

#[test]
fn generated_contract_checks_detect_shape_drift_and_allow_write_only_clients() {
    let (db, g, m, _) = fixture();
    let relation = m
        .modules
        .iter()
        .flat_map(|m| &m.relations)
        .find(|r| r.name == "fitness.Workout")
        .unwrap();
    let wrap = |fingerprint: String| Command::Checked {
        contracts: vec![Contract {
            relation: relation.name.clone(),
            schema_id: relation.id,
            fingerprint,
        }],
        command: Box::new(Command::Batch {
            operations: vec![insert(
                &m,
                "fitness.Workout",
                [("durationMinutes", WireValue::Int("42".into()))],
                None,
            )],
        }),
    };
    assert!(matches!(call(&g,wrap("wrong".into())),Response::Error(e) if e.code=="schema"));
    assert!(
        db.admin()
            .snapshot()
            .scan("fitness.Workout")
            .unwrap()
            .is_empty()
    );
    let mut tx = db.admin().transaction();
    tx.grant(
        "fitness",
        "fitness.Workout",
        Grant {
            read: false,
            ..Grant::read_write()
        },
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        call(&g, wrap(relation.fingerprint())),
        Response::Ok(Output::Batch { .. })
    ));
    let mut tx = db.admin().transaction();
    tx.revoke("fitness", "fitness.Workout").unwrap();
    tx.commit().unwrap();
    assert!(matches!(call(&g,wrap("wrong".into())),Response::Error(e) if e.code=="denied"));
    let nested = Command::Checked {
        contracts: vec![],
        command: Box::new(Command::Capabilities),
    };
    assert!(matches!(call(&g, nested), Response::Error(_)));
}
