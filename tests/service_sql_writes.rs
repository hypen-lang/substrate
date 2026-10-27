//! The same SQL mutation contract is exercised against both storage backends.
use substrate_liquid::{paged::*, service::*, *};
const TOKEN: &str = "sql-write-test-token-at-least-thirty-two-bytes";
fn module() -> Module {
    Module::new(
        "app",
        1,
        vec![],
        vec![
            Relation::new(
                "app.Item",
                vec![
                    Role::new("name", DataType::Text),
                    Role::new("number", DataType::Int),
                    Role::new("score", DataType::Float.nullable()),
                ],
            )
            .unique(&["name"]),
            Relation::new(
                "app.Link",
                vec![Role::new("item", DataType::Ref("app.Item".into()))],
            ),
        ],
    )
}
fn config() -> ServerConfig {
    ServerConfig {
        version: 1,
        clients: vec![ClientCredential {
            token: TOKEN.into(),
            principal: "writer".into(),
        }],
    }
}
fn resident() -> Gateway {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(module(), Visibility::Private)
        .unwrap();
    tx.grant("writer", "app.Item", Grant::read_write()).unwrap();
    tx.grant("writer", "app.Link", Grant::read_write()).unwrap();
    tx.commit().unwrap();
    Gateway::new(db, config()).unwrap()
}
fn paged() -> (tempfile::TempDir, Gateway) {
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(module(), "writer", Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    (dir, Gateway::new(db, config()).unwrap())
}
fn call(g: &Gateway, command: Command) -> Response {
    g.handle(Request {
        version: 1,
        token: TOKEN.into(),
        command,
    })
}
fn execute(g: &Gateway, sql: &str, params: Vec<SqlWireValue>) -> Response {
    call(
        g,
        Command::Execute {
            sql: sql.into(),
            params,
        },
    )
}
fn good(response: Response) -> Vec<Id> {
    match response {
        Response::Ok(Output::Execute { affected_rows, ids }) => {
            assert_eq!(affected_rows, ids.len());
            ids
        }
        r => panic!("{r:?}"),
    }
}
fn query(g: &Gateway) -> Vec<Vec<SqlWireValue>> {
    match call(
        g,
        Command::Query {
            sql: "SELECT name, number FROM app.Item ORDER BY name".into(),
            params: vec![],
        },
    ) {
        Response::Ok(Output::Query { rows, .. }) => rows,
        r => panic!("{r:?}"),
    }
}
fn integer(n: i64) -> SqlWireValue {
    SqlWireValue::Integer(n.to_string())
}
fn contract(g: &Gateway) {
    assert!(
        matches!(call(g,Command::Capabilities),Response::Ok(Output::Capabilities{sql_writes:true,sql_write_features,..}) if sql_write_features.contains(&"insert_values".to_owned()))
    );
    let hostile = "x'); DELETE FROM app.Item; --";
    assert!(matches!(
        g.handle(Request {
            version: 1,
            token: String::new(),
            command: Command::Execute {
                sql: "INSERT INTO app.Item (name,number,score) VALUES ('anonymous',1,NULL)".into(),
                params: vec![]
            }
        }),
        Response::Error(_)
    ));
    let ids = good(execute(
        g,
        "INSERT INTO \"app\".\"Item\" (name, number, score) VALUES ($1,$2,NULL), ('second',-9223372036854775808,1.25)",
        vec![SqlWireValue::Text(hostile.into()), integer(i64::MAX)],
    ));
    assert_eq!(ids.len(), 2);
    assert_eq!(query(g).len(), 2);
    let first = SqlWireValue::Text(ids[0].to_string());
    assert_eq!(
        good(execute(
            g,
            "UPDATE app.Item SET number=?1,score=?2 WHERE id IN (?3,?3)",
            vec![integer(7), SqlWireValue::Real(2.5), first.clone()]
        )),
        vec![ids[0]]
    );
    assert_eq!(
        good(execute(
            g,
            "UPDATE app.Item SET number=8 WHERE id=$1",
            vec![SqlWireValue::Text(Id::new().to_string())]
        ))
        .len(),
        0
    );
    let before = query(g);
    assert!(matches!(
        execute(
            g,
            "INSERT INTO app.Link (item) VALUES ($1),($2)",
            vec![first.clone(), SqlWireValue::Text(Id::new().to_string())]
        ),
        Response::Error(_)
    ));
    assert!(
        matches!(call(g,Command::Query{sql:"SELECT COUNT(*) FROM app.Link".into(),params:vec![]}),Response::Ok(Output::Query{rows,..}) if rows==vec![vec![integer(0)]])
    );
    let too_many = format!(
        "INSERT INTO app.Link (item) VALUES {}",
        vec!["($1)"; 101].join(",")
    );
    assert!(matches!(
        execute(g, &too_many, vec![first.clone()]),
        Response::Error(_)
    ));
    let expanded = "INSERT INTO app.Item (name,number,score) VALUES ($1,1,NULL),($1,2,NULL),($1,3,NULL),($1,4,NULL),($1,5,NULL)";
    assert!(matches!(
        execute(
            g,
            expanded,
            vec![SqlWireValue::Text("x".repeat(1024 * 1024))]
        ),
        Response::Error(error) if error.message.contains("materialization budget")
    ));
    for (sql, params) in [
        (
            "INSERT INTO app.Item (name,number,score) VALUES ('third',3,NULL),('second',4,NULL)",
            vec![],
        ),
        (
            "UPDATE app.Item SET name='second' WHERE id=$1",
            vec![first.clone()],
        ),
        (
            "UPDATE app.Item SET number='wrong' WHERE id=$1",
            vec![first.clone()],
        ),
        (
            "UPDATE app.Item SET number=NULL WHERE id=$1",
            vec![first.clone()],
        ),
        (
            "UPDATE app.Item SET score=$1 WHERE id=$2",
            vec![SqlWireValue::Real(f64::NAN), first.clone()],
        ),
        (
            "UPDATE app.Item SET score=$1 WHERE id=$2",
            vec![integer(i64::MAX), first.clone()],
        ),
        ("UPDATE app.Item SET number=3", vec![]),
        ("DELETE FROM app.Item", vec![]),
        (
            "DELETE FROM app.Item WHERE id=$1; DELETE FROM app.Item WHERE id=$1",
            vec![first.clone()],
        ),
        (
            "DELETE FROM app.Item WHERE id=$1 RETURNING id",
            vec![first.clone()],
        ),
        ("DELETE FROM app.Item WHERE id=$1", vec![]),
        (
            "DELETE FROM app.Item WHERE id=$1",
            vec![first.clone(), first.clone()],
        ),
        ("UPDATE app.Item SET id=$1 WHERE id=$1", vec![first.clone()]),
        (
            "INSERT INTO app.Item (name,number,score) SELECT name,number,score FROM app.Item",
            vec![],
        ),
    ] {
        assert!(
            matches!(execute(g, sql, params), Response::Error(_)),
            "accepted {sql}"
        );
        assert_eq!(query(g), before, "partial write {sql}");
    }
    assert!(matches!(
        call(
            g,
            Command::Query {
                sql: "DELETE FROM app.Item".into(),
                params: vec![]
            }
        ),
        Response::Error(_)
    ));
    assert_eq!(
        good(execute(
            g,
            "DELETE FROM app.Item WHERE id IN ($1,$1,$2)",
            vec![first, SqlWireValue::Text(Id::new().to_string())]
        )),
        vec![ids[0]]
    );
    assert_eq!(query(g).len(), 1);
}
#[test]
fn resident_sql_write_contract() {
    contract(&resident());
}
#[test]
fn paged_sql_write_contract() {
    let (_dir, g) = paged();
    contract(&g);
}
#[test]
fn write_only_grants_old_and_new_row_filters_and_live_revocation() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(module(), Visibility::Private)
        .unwrap();
    tx.grant(
        "writer",
        "app.Item",
        Grant {
            read: false,
            write: true,
            reference: false,
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "allowed".into(),
            },
        },
    )
    .unwrap();
    let hidden = tx
        .insert(
            "app.Item",
            vec!["hidden".into(), Value::Int(9), Value::Null],
        )
        .unwrap();
    tx.commit().unwrap();
    let g = Gateway::new(db.clone(), config()).unwrap();
    for target in [hidden, Id::new()] {
        assert!(
            good(execute(
                &g,
                "DELETE FROM app.Item WHERE id=$1",
                vec![SqlWireValue::Text(target.to_string())]
            ))
            .is_empty()
        );
        assert!(
            good(execute(
                &g,
                "UPDATE app.Item SET number=5 WHERE id=$1",
                vec![SqlWireValue::Text(target.to_string())]
            ))
            .is_empty()
        );
    }
    let id = good(execute(
        &g,
        "INSERT INTO app.Item (name,number,score) VALUES ('allowed',1,NULL)",
        vec![],
    ))[0];
    let parameter = SqlWireValue::Text(id.to_string());
    good(execute(
        &g,
        "UPDATE app.Item SET number=2 WHERE id=$1",
        vec![parameter.clone()],
    ));
    assert!(matches!(
        execute(
            &g,
            "UPDATE app.Item SET name='forbidden' WHERE id=$1",
            vec![parameter.clone()]
        ),
        Response::Error(_)
    ));
    let mut tx = db.admin().transaction();
    tx.revoke("writer", "app.Item").unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        execute(&g, "DELETE FROM app.Item WHERE id=$1", vec![parameter]),
        Response::Error(_)
    ));
    assert_eq!(db.admin().snapshot().scan("app.Item").unwrap().len(), 2);
}
#[path = "support/tls.rs"]
mod tls;
#[test]
fn paged_tls_execute_preserves_capability_scope_rotation_and_live_revocation() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use substrate_liquid::{capability::*, remote::*};
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
    let mut tx = db.admin().transaction().unwrap();
    tx.install_owned(module(), "writer", Visibility::Private)
        .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction().unwrap();
    let hidden = tx
        .insert(
            "app.Item",
            vec!["hidden".into(), Value::Int(9), Value::Null],
        )
        .unwrap();
    tx.commit().unwrap();
    let relation = db
        .admin()
        .snapshot()
        .unwrap()
        .schema("app.Item")
        .unwrap()
        .id;
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let scope = |grant| CapabilityScope {
        relations: vec![RelationCapability { relation, grant }],
        schema_namespace: None,
        delegations: 0,
    };
    let root = db
        .issue_capability_until(
            Id::new(),
            "writer",
            scope(Grant {
                filter: RowFilter::Equals {
                    role: "name".into(),
                    value: "allowed".into(),
                },
                ..Grant::read_write()
            }),
            Some(end),
        )
        .unwrap();
    let issued = db.issue_credential("writer", root.id, end).unwrap();
    let read_root = db
        .issue_capability_until(Id::new(), "writer", scope(Grant::read_only()), Some(end))
        .unwrap();
    let read_issued = db.issue_credential("writer", read_root.id, end).unwrap();
    let (ca, cert, key) = tls::certificates(dir.path());
    let server =
        RemoteServer::bind(db.clone(), "127.0.0.1:0".parse().unwrap(), &cert, &key).unwrap();
    let address = server.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let shutdown = stop.clone();
    let worker = std::thread::spawn(move || server.run(shutdown).unwrap());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let client = |token: &str| RemoteClient::new(address, "localhost", &ca, token).unwrap();
        let c = client(&issued.token);
        for target in [hidden, Id::new()] {
            assert_eq!(
                c.execute(
                    "UPDATE app.Item SET number=4 WHERE id=$1",
                    vec![SqlWireValue::Text(target.to_string())]
                )
                .unwrap()
                .affected_rows,
                0
            );
            assert_eq!(
                c.execute(
                    "DELETE FROM app.Item WHERE id=$1",
                    vec![SqlWireValue::Text(target.to_string())]
                )
                .unwrap()
                .affected_rows,
                0
            );
        }

        let sql = "INSERT INTO app.Item (name,number,score) VALUES ($1,1,NULL)";
        assert!(
            client(&read_issued.token)
                .execute(sql, vec![SqlWireValue::Text("allowed".into())])
                .is_err()
        );
        assert!(
            c.execute(sql, vec![SqlWireValue::Text("forbidden".into())])
                .is_err()
        );
        let result = c
            .execute(sql, vec![SqlWireValue::Text("allowed".into())])
            .unwrap();
        assert_eq!(result.affected_rows, 1);
        let id = SqlWireValue::Text(result.ids[0].to_string());
        let next = db.rotate_credential(issued.id, end).unwrap();
        assert!(
            c.execute("UPDATE app.Item SET number=2 WHERE id=$1", vec![id.clone()])
                .is_err()
        );
        let next_client = client(&next.token);
        assert_eq!(
            next_client
                .execute("UPDATE app.Item SET number=2 WHERE id=$1", vec![id.clone()])
                .unwrap()
                .affected_rows,
            1
        );
        db.revoke_capability(root.id).unwrap();
        assert!(
            next_client
                .execute("DELETE FROM app.Item WHERE id=$1", vec![id])
                .is_err()
        );
    }));
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    result.unwrap();
}
#[test]
fn multirow_delete_checks_the_whole_statement_and_counts_explicit_cascade_roots() {
    for action in [OnDelete::Restrict, OnDelete::Cascade] {
        let schema = || {
            Module::new(
                "tree",
                1,
                vec![],
                vec![Relation::new(
                    "tree.Node",
                    vec![
                        Role::new("parent", DataType::Ref("tree.Node".into()).nullable())
                            .on_delete(action),
                    ],
                )],
            )
        };
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install_with_visibility(schema(), Visibility::Private)
            .unwrap();
        tx.grant("writer", "tree.Node", Grant::read_write())
            .unwrap();
        let parent = tx.insert("tree.Node", vec![Value::Null]).unwrap();
        let child = tx.insert("tree.Node", vec![parent.into()]).unwrap();
        tx.commit().unwrap();
        let gateway = Gateway::new(db, config()).unwrap();
        assert_eq!(
            good(execute(
                &gateway,
                "DELETE FROM tree.Node WHERE id IN ($1,$2)",
                vec![
                    SqlWireValue::Text(parent.to_string()),
                    SqlWireValue::Text(child.to_string())
                ]
            )),
            vec![parent, child]
        );
        let dir = tempfile::tempdir().unwrap();
        let db = PagedDatabase::open(dir.path().join("db"), PagedOptions::default()).unwrap();
        let mut tx = db.admin().transaction().unwrap();
        tx.install_owned(schema(), "writer", Visibility::Private)
            .unwrap();
        let parent = tx.insert("tree.Node", vec![Value::Null]).unwrap();
        let child = tx.insert("tree.Node", vec![parent.into()]).unwrap();
        tx.commit().unwrap();
        let gateway = Gateway::new(db, config()).unwrap();
        assert_eq!(
            good(execute(
                &gateway,
                "DELETE FROM tree.Node WHERE id IN ($1,$2)",
                vec![
                    SqlWireValue::Text(parent.to_string()),
                    SqlWireValue::Text(child.to_string())
                ]
            )),
            vec![parent, child]
        );
    }
}
