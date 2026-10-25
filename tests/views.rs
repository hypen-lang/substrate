use rusqlite::types::Value as SqlValue;
use substrate_liquid::{capability::*, *};
const LEDGER: &str = "liquid_view_catalog.Definition";
fn fixture(db: &Database) -> (Id, Id) {
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.insert("core.Person", vec!["Ada".into()]).unwrap();
    tx.insert("core.Person", vec!["Bob".into()]).unwrap();
    tx.grant(
        "reader",
        "core.Person",
        Grant {
            filter: RowFilter::Equals {
                role: "name".into(),
                value: "Ada".into(),
            },
            ..Grant::read_only()
        },
    )
    .unwrap();
    tx.grant("owner", "core.Person", Grant::read_write())
        .unwrap();
    tx.commit().unwrap();
    let id = Id::new();
    db.define_view(
        id,
        "People",
        "SELECT id, name FROM core.Person ORDER BY name",
        &["core.Person"],
    )
    .unwrap();
    db.grant_view(id, "reader").unwrap();
    db.grant_view(id, "owner").unwrap();
    (id, db.admin().snapshot().schema("core.Person").unwrap().id)
}
#[test]
fn invoker_rows_and_private_definition_metadata() {
    let db = Database::memory();
    let (id, _) = fixture(&db);
    let reader = db.session("reader").snapshot();
    let result = reader.query_view(id).unwrap();
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], SqlValue::Text("Ada".into()));
    assert_eq!(
        reader.view_definition(id).unwrap().name,
        "liquid_views.People"
    );
    assert!(reader.scan("liquid_views.People").unwrap().is_empty());
    assert!(reader.scan(LEDGER).is_err());
    assert!(
        reader
            .sql()
            .unwrap()
            .query("SELECT record FROM liquid_view_catalog.Definition", &[])
            .is_err()
    );
    assert!(
        db.session("stranger")
            .snapshot()
            .view_definition(id)
            .is_err()
    );
    assert!(
        db.session("stranger")
            .snapshot()
            .view_definitions()
            .unwrap()
            .is_empty()
    );
}
#[test]
fn marker_permission_alone_never_discloses_sql_or_source_contracts() {
    let db = Database::memory();
    let (id, _) = fixture(&db);
    db.grant_view(id, "marker_only").unwrap();
    let snapshot = db.session("marker_only").snapshot();
    assert!(snapshot.scan("liquid_views.People").unwrap().is_empty());
    assert!(
        snapshot
            .sql()
            .unwrap()
            .query("SELECT * FROM liquid_views.People", &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(snapshot.view_definition(id).is_err());
    assert!(snapshot.query_view(id).is_err());
    assert!(snapshot.scan(LEDGER).is_err());
    assert!(snapshot.view_definitions().unwrap().is_empty());
}
#[test]
fn existing_source_access_does_not_grant_view_access() {
    let db = Database::memory();
    let (id, _) = fixture(&db);
    let mut tx = db.admin().transaction();
    tx.grant("source_only", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    assert!(db.session("source_only").snapshot().query_view(id).is_err());
}
#[test]
fn views_reject_mutations_multiple_statements_parameters_and_undeclared_sources() {
    let db = Database::memory();
    fixture(&db);
    for sql in [
        "DELETE FROM core.Person",
        "SELECT name FROM core.Person; SELECT 2",
        "SELECT ?1",
        "PRAGMA user_version",
        "ATTACH ':memory:' AS extra",
    ] {
        assert!(
            db.define_view(Id::new(), "Invalid", sql, &["core.Person"])
                .is_err(),
            "{sql}"
        );
    }
    assert!(
        db.define_view(Id::new(), "Missing", "SELECT * FROM core.Person", &[])
            .is_err()
    );
    assert!(
        db.admin()
            .snapshot()
            .schema("liquid_views.Invalid")
            .is_err()
    );
    let mut tx = db.session("reader").transaction();
    assert!(tx.insert("liquid_views.People", vec![]).is_err());
    let before = db.admin().snapshot().scan("core.Person").unwrap();
    let reader = db
        .session("reader")
        .snapshot()
        .open_view(db.admin().snapshot().view_definitions().unwrap()[0].id)
        .unwrap();
    assert_eq!(reader.query().unwrap().rows.len(), 1);
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap(), before);
}
#[test]
fn source_and_view_revocation_fence_retained_handles() {
    for source in [false, true] {
        let db = Database::memory();
        let (id, _) = fixture(&db);
        let snapshot = db.session("reader").snapshot();
        let handle = snapshot.open_view(id).unwrap();
        if source {
            let mut tx = db.admin().transaction();
            tx.revoke("reader", "core.Person").unwrap();
            tx.commit().unwrap();
        } else {
            db.revoke_view(id, "reader").unwrap();
        }
        assert!(handle.query().is_err());
        assert!(handle.definition().is_err());
        assert!(snapshot.view_definition(id).is_err());
        assert!(db.session("reader").snapshot().query_view(id).is_err());
    }
}
#[test]
fn credentials_and_capabilities_keep_view_scopes_and_live_rotation() {
    let db = Database::memory();
    let (id, person) = fixture(&db);
    let scope = CapabilityScope {
        relations: vec![
            RelationCapability {
                relation: id,
                grant: Grant::read_only(),
            },
            RelationCapability {
                relation: person,
                grant: Grant {
                    filter: RowFilter::Equals {
                        role: "name".into(),
                        value: "Ada".into(),
                    },
                    ..Grant::read_only()
                },
            },
        ],
        schema_namespace: None,
        delegations: 0,
    };
    let root = db.issue_capability(Id::new(), "owner", scope).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let credential = db.issue_credential("owner", root.id, now + 300).unwrap();
    let selected = db.authenticate(&credential.token).unwrap();
    let view = selected.snapshot().open_view(id).unwrap();
    assert_eq!(view.query().unwrap().rows.len(), 1);
    db.rotate_credential(credential.id, now + 300).unwrap();
    assert!(view.query().is_err());
    let ledger = db.admin().snapshot().schema(LEDGER).unwrap().id;
    let mut tx = db.admin().transaction();
    tx.grant("owner", LEDGER, Grant::read_only()).unwrap();
    tx.commit().unwrap();
    assert!(
        db.issue_capability(
            Id::new(),
            "owner",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: ledger,
                    grant: Grant::read_only()
                }],
                schema_namespace: None,
                delegations: 0
            }
        )
        .is_err()
    );
}
#[test]
fn definitions_survive_redo_and_checkpoint_without_changing_old_rows() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Database::open(&path).unwrap();
        let (id, _) = fixture(&db);
        let rows = db.admin().snapshot().scan("core.Person").unwrap();
        let original = db.session("reader").snapshot().view_definition(id).unwrap();
        let second = db
            .define_view(
                Id::new(),
                "Count",
                "SELECT count(*) AS count FROM core.Person",
                &["core.Person"],
            )
            .unwrap();
        assert_ne!(id, second.id);
        assert_eq!(db.admin().snapshot().scan("core.Person").unwrap(), rows);
        if checkpoint {
            db.checkpoint().unwrap();
        }
        drop(db);
        let db = Database::open(&path).unwrap();
        let reader = db.session("reader").snapshot();
        assert_eq!(reader.view_definition(id).unwrap(), original);
        assert_eq!(reader.query_view(id).unwrap().rows.len(), 1);
        assert_eq!(db.admin().snapshot().scan("core.Person").unwrap(), rows);
    }
}
#[test]
fn immutable_ids_names_and_protected_namespaces() {
    let db = Database::memory();
    let (id, _) = fixture(&db);
    assert!(db.define_view(id, "Again", "SELECT 1", &[]).is_err());
    assert!(
        db.define_view(Id::new(), "People", "SELECT 1", &[])
            .is_err()
    );
    let mut tx = db.admin().transaction();
    tx.grant_schema("app", "liquid_views").unwrap();
    tx.grant_schema("app", "liquid_view_catalog").unwrap();
    tx.commit().unwrap();
    for name in ["liquid_views", "liquid_view_catalog"] {
        let mut tx = db.session("app").transaction();
        assert!(
            tx.install(Module::new(
                name,
                2,
                vec![],
                vec![Relation::new(&format!("{name}.Spoof"), vec![])]
            ))
            .is_err()
        );
    }
}
#[test]
fn stored_sql_is_revalidated_before_execution_even_after_host_tampering() {
    let db = Database::memory();
    let (id, _) = fixture(&db);
    let row = db.admin().snapshot().scan(LEDGER).unwrap().remove(0);
    let Value::Text(json) = &row.values[1] else {
        panic!()
    };
    let mut definition: views::ViewDefinition = serde_json::from_str(json).unwrap();
    definition.select = "DELETE FROM core.Person".into();
    let mut tx = db.admin().transaction();
    tx.update(
        LEDGER,
        row.id,
        vec![
            row.values[0].clone(),
            Value::Text(serde_json::to_string(&definition).unwrap()),
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(db.session("reader").snapshot().query_view(id).is_err());
    assert_eq!(db.admin().snapshot().scan("core.Person").unwrap().len(), 2);
}
#[test]
fn constant_views_are_explicitly_supported_without_data_authority() {
    let db = Database::memory();
    let id = Id::new();
    db.define_view(id, "Greeting", "SELECT 'hello' AS greeting", &[])
        .unwrap();
    db.grant_view(id, "reader").unwrap();
    assert_eq!(
        db.session("reader").snapshot().query_view(id).unwrap().rows,
        vec![vec![SqlValue::Text("hello".into())]]
    );
}

#[test]
fn source_reference_metadata_must_be_visible_to_inspect_a_definition() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "vault",
        1,
        vec![],
        vec![
            Relation::new("vault.Secret", vec![]),
            Relation::new(
                "vault.Link",
                vec![Role::new("target", DataType::Ref("vault.Secret".into()))],
            ),
        ],
    ))
    .unwrap();
    tx.grant("reader", "vault.Link", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    let id = Id::new();
    db.define_view(id, "Links", "SELECT * FROM vault.Link", &["vault.Link"])
        .unwrap();
    db.grant_view(id, "reader").unwrap();
    let reader = db.session("reader").snapshot();
    assert!(reader.view_definition(id).is_err());
    assert!(reader.query_view(id).is_err());
    assert!(reader.view_definitions().unwrap().is_empty());
}

#[test]
fn malformed_versioned_definitions_fail_closed_on_recovery() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Database::open(&path).unwrap();
        let (id, _) = fixture(&db);
        let row = db.admin().snapshot().scan(LEDGER).unwrap().remove(0);
        let Value::Text(json) = &row.values[1] else {
            panic!()
        };
        let mut definition: views::ViewDefinition = serde_json::from_str(json).unwrap();
        definition.format = 99;
        let mut tx = db.admin().transaction();
        tx.update(
            LEDGER,
            row.id,
            vec![
                row.values[0].clone(),
                Value::Text(serde_json::to_string(&definition).unwrap()),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        assert!(db.session("reader").snapshot().query_view(id).is_err());
        if checkpoint {
            db.checkpoint().unwrap();
        }
        drop(db);
        assert!(Database::open(path).is_err());
    }
}
