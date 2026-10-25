use substrate_liquid::*;
#[allow(dead_code)]
#[path = "../examples/dx/generated/schema.rs"]
mod generated;
use generated::{
    core::Person,
    fitness::{Performed, Workout},
};
fn manifest() -> SchemaManifest {
    SchemaManifest::from_json(include_str!("../examples/dx/manifest.json")).unwrap()
}
fn fixture() -> Database {
    let db = Database::memory();
    db.admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.install_manifest(&manifest())
        })
        .unwrap();
    db
}
#[test]
fn relocking_and_generation_are_reproducible() {
    let old = manifest();
    let relocked =
        SchemaManifest::lock(include_str!("../examples/dx/schema.json"), Some(&old)).unwrap();
    assert_eq!(old, relocked);
    assert_eq!(old.to_json().unwrap(), relocked.to_json().unwrap());
    let code = old.generate().unwrap();
    assert_eq!(
        code.rust,
        include_str!("../examples/dx/generated/schema.rs")
    );
    assert_eq!(
        code.typescript,
        include_str!("../examples/dx/generated/schema.ts")
    );
}
#[test]
fn locked_manifests_require_ids_and_immutable_history() {
    assert!(SchemaManifest::from_json(include_str!("../examples/dx/schema.json")).is_err());
    let old = manifest();
    let mut draft: serde_json::Value =
        serde_json::from_str(include_str!("../examples/dx/schema.json")).unwrap();
    draft["modules"][0]["relations"][0]["roles"][0]["data_type"] = serde_json::json!("Int");
    assert!(SchemaManifest::lock(&draft.to_string(), Some(&old)).is_err());
    draft["modules"].as_array_mut().unwrap().remove(0);
    assert!(SchemaManifest::lock(&draft.to_string(), Some(&old)).is_err());
}
#[test]
fn additive_versions_preserve_generated_ids_and_existing_results() {
    let old = manifest();
    let mut draft: serde_json::Value =
        serde_json::from_str(include_str!("../examples/dx/schema.json")).unwrap();
    draft["modules"].as_array_mut().unwrap().push(serde_json::json!({"name":"fitness","version":2,"imports":[["core",1]],"relations":[{"name":"fitness.Notes","roles":[{"name":"person","data_type":{"Ref":"core.Person"}},{"name":"text","data_type":"Text"}]}]}));
    let next = SchemaManifest::lock(&draft.to_string(), Some(&old)).unwrap();
    let db = fixture();
    let id = db
        .admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_record(Person {
                name: "Alice".into(),
            })
        })
        .unwrap();
    let before = db.admin().snapshot().get_record(id).unwrap();
    let plan = db.admin().snapshot().plan_manifest(&next).unwrap();
    assert_eq!(plan.additions, vec!["fitness@2"]);
    db.admin()
        .transact(RetryPolicy::default(), |tx| tx.install_manifest(&next))
        .unwrap();
    assert_eq!(db.admin().snapshot().get_record(id).unwrap(), before);
}
#[test]
fn install_manifest_is_idempotent_and_detects_identity_drift() {
    let db = fixture();
    let revision = db.admin().snapshot().revision();
    db.admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.install_manifest(&manifest())
        })
        .unwrap();
    assert_eq!(db.admin().snapshot().revision(), revision);
    let other = SchemaManifest::lock(include_str!("../examples/dx/schema.json"), None).unwrap();
    assert!(db.admin().snapshot().plan_manifest(&other).is_err());
    let mut tx = db.session("app").transaction();
    assert!(matches!(
        tx.install_manifest(&manifest()),
        Err(Error::Denied)
    ));
}
#[test]
fn generated_records_roundtrip_through_named_writes_and_sql() {
    let db = fixture();
    let (person, workout) = db
        .admin()
        .transact(RetryPolicy::default(), |tx| {
            let person = tx.insert_record(Person { name: "Ada".into() })?;
            let workout = tx.insert_record(Workout {
                durationMinutes: 45,
            })?;
            tx.insert_record(Performed {
                athlete: person,
                workout,
            })?;
            Ok((person, workout))
        })
        .unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .get_record(person)
            .unwrap()
            .unwrap()
            .name,
        "Ada"
    );
    db.admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.replace_record(
                workout,
                Workout {
                    durationMinutes: 60,
                },
            )
        })
        .unwrap();
    let result=db.admin().snapshot().sql().unwrap().query("SELECT p.name,w.durationMinutes FROM core.Person p JOIN fitness.Performed f ON f.athlete=p.id JOIN fitness.Workout w ON w.id=f.workout",&[]).unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            rusqlite::types::Value::Text("Ada".into()),
            rusqlite::types::Value::Integer(60)
        ]]
    );
    let other = Database::memory();
    let mut tx = other.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.commit().unwrap();
    let mut tx = other.admin().transaction();
    assert!(
        tx.insert_record(Person {
            name: "wrong package".into()
        })
        .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Aborted)));
}
#[test]
fn named_fields_reject_unknown_duplicate_missing_and_bad_types_atomically() {
    for fields in [
        vec![],
        vec![("unknown", Value::from("x"))],
        vec![("name", "x".into()), ("name", "y".into())],
        vec![("name", 1.into())],
    ] {
        let db = fixture();
        let mut tx = db.admin().transaction();
        tx.insert_record(Person {
            name: "rolled back".into(),
        })
        .unwrap();
        assert!(tx.insert_named("core.Person", fields).is_err());
        assert!(matches!(tx.commit(), Err(Error::Aborted)));
        assert!(
            db.admin()
                .snapshot()
                .scan("core.Person")
                .unwrap()
                .is_empty()
        );
    }
}
#[test]
fn named_patches_preserve_unspecified_fields_and_apply_policy_to_old_and_new_rows() {
    let db = fixture();
    let p = db
        .admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_record(Person {
                name: "allowed".into(),
            })
        })
        .unwrap();
    db.admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.grant(
                "app",
                "core.Person",
                Grant {
                    filter: RowFilter::Equals {
                        role: "name".into(),
                        value: "allowed".into(),
                    },
                    ..Grant::read_write()
                },
            )
        })
        .unwrap();
    let mut tx = db.session("app").transaction();
    assert!(matches!(
        tx.update_named("core.Person", p.raw(), [("name", "denied".into())]),
        Err(Error::Denied)
    ));
    assert!(tx.commit().is_err());
    db.session("app")
        .transact(RetryPolicy::default(), |tx| {
            tx.update_named("core.Person", p.raw(), Vec::<(String, Value)>::new())
        })
        .unwrap();
    assert_eq!(
        db.admin().snapshot().get_record(p).unwrap().unwrap().name,
        "allowed"
    );
}
#[test]
fn retry_conflicts_use_fresh_state_without_duplicating_attempts() {
    let db = fixture();
    let p = db
        .admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_record(Person {
                name: "before".into(),
            })
        })
        .unwrap();
    let mut attempts = 0;
    let result = db
        .admin()
        .transact(
            RetryPolicy {
                max_attempts: 3,
                delay: std::time::Duration::ZERO,
            },
            |tx| {
                attempts += 1;
                let name = tx.get("core.Person", p.raw())?.unwrap().values[0].clone();
                if attempts == 1 {
                    let mut other = db.admin().transaction();
                    other.update_named("core.Person", p.raw(), [("name", "after".into())])?;
                    other.commit()?;
                }
                tx.insert_record(Workout {
                    durationMinutes: attempts,
                })?;
                Ok(name)
            },
        )
        .unwrap();
    assert_eq!(result, Value::from("after"));
    assert_eq!(attempts, 2);
    assert_eq!(
        db.admin().snapshot().scan("fitness.Workout").unwrap().len(),
        1
    );
}
#[test]
fn retry_exhaustion_nonconflict_errors_and_zero_attempts_are_explicit() {
    let db = fixture();
    let mut calls = 0;
    let result: Result<()> = db.admin().transact(
        RetryPolicy {
            max_attempts: 2,
            delay: std::time::Duration::ZERO,
        },
        |_| {
            calls += 1;
            Err(Error::Conflict)
        },
    );
    assert!(matches!(result, Err(Error::Conflict)));
    assert_eq!(calls, 2);
    for error in [Error::Denied, Error::Storage("uncertain".into())] {
        let mut called = 0;
        let mut error = Some(error);
        let result: Result<()> = db.admin().transact(RetryPolicy::default(), |_| {
            called += 1;
            Err(error.take().unwrap())
        });
        assert!(result.is_err());
        assert_eq!(called, 1);
    }
    let result: Result<()> = db.admin().transact(
        RetryPolicy {
            max_attempts: 0,
            delay: std::time::Duration::ZERO,
        },
        |_| panic!("must not run"),
    );
    assert!(result.is_err());
}

#[test]
fn canonical_fingerprints_and_selected_contracts_ignore_unrelated_modules() {
    let full = manifest();
    let selected = full.select(&[("fitness".into(), 1)]).unwrap();
    assert_eq!(
        selected
            .modules
            .iter()
            .map(|m| m.name.as_str())
            .collect::<Vec<_>>(),
        vec!["core", "fitness"]
    );
    assert!(!selected.generate().unwrap().typescript.contains("crm_Deal"));
    assert_eq!(
        selected.fingerprint().unwrap(),
        selected
            .select(&[("fitness".into(), 1)])
            .unwrap()
            .fingerprint()
            .unwrap()
    );
    let mut shuffled = full.clone();
    shuffled.modules.reverse();
    for m in &mut shuffled.modules {
        m.relations.reverse();
    }
    assert_eq!(full.fingerprint().unwrap(), shuffled.fingerprint().unwrap());
    assert_eq!(full.to_json().unwrap(), shuffled.to_json().unwrap());
    assert_ne!(full.fingerprint().unwrap(), selected.fingerprint().unwrap());
    assert!(full.select(&[("fitness".into(), 2)]).is_err());
}
#[test]
fn rust_builders_install_export_and_regenerate_the_same_contract() {
    let locked = manifest();
    let mut core = demo::core();
    // IDs are loaded from the checked-in contract, never derived from Rust names.
    core.relations[0].id = Person::TYPE_ID;
    let rust = SchemaManifest {
        format: 1,
        modules: vec![core],
    };
    rust.validate().unwrap();
    let expected = locked.select(&[("core".into(), 1)]).unwrap();
    assert_eq!(rust.fingerprint().unwrap(), expected.fingerprint().unwrap());
    let db = Database::memory();
    db.admin()
        .transact(RetryPolicy::default(), |tx| tx.install_manifest(&rust))
        .unwrap();
    let catalog = db.admin().snapshot().manifest().unwrap();
    assert_eq!(catalog.to_json().unwrap(), expected.to_json().unwrap());
    assert_eq!(
        catalog.generate().unwrap().rust,
        expected.generate().unwrap().rust
    );
    let person = db
        .admin()
        .transact(RetryPolicy::default(), |tx| {
            tx.insert_record(Person {
                name: "Rust authored".into(),
            })
        })
        .unwrap();
    assert_eq!(
        db.admin()
            .snapshot()
            .get_record(person)
            .unwrap()
            .unwrap()
            .name,
        "Rust authored"
    );
}
#[test]
fn generated_contract_detects_same_id_with_different_layout() {
    let mut contract = manifest().select(&[("core".into(), 1)]).unwrap();
    contract.modules[0].relations[0].roles[0].name = "renamed".into();
    let db = Database::memory();
    db.admin()
        .transact(RetryPolicy::default(), |tx| tx.install_manifest(&contract))
        .unwrap();
    assert!(
        db.admin()
            .transact(RetryPolicy::default(), |tx| tx.insert_record(Person {
                name: "wrong layout".into()
            }))
            .is_err()
    );
    assert!(
        db.admin()
            .snapshot()
            .get_record(TypedId::<Person>::from_raw(Id::new()))
            .is_err()
    );
}

fn naming_manifest(definitions: &[(&str, &[&str])]) -> SchemaManifest {
    SchemaManifest {
        format: 1,
        modules: definitions
            .iter()
            .map(|(namespace, names)| {
                Module::new(
                    namespace,
                    1,
                    vec![],
                    names
                        .iter()
                        .map(|name| Relation::new(&format!("{namespace}.{name}"), vec![]))
                        .collect(),
                )
            })
            .collect(),
    }
}
#[test]
fn clean_typescript_names_keep_qualified_exports_and_identity() {
    let code = manifest().generate().unwrap().typescript;
    assert!(code.contains("export { core_Person as Person };"));
    assert!(code.contains("export type PersonInsert = core_PersonInsert;"));
    assert!(code.contains("export type PersonUpdate = core_PersonUpdate;"));
    assert!(code.contains("export type PersonRead = core_PersonRead;"));
    assert!(code.contains("export const core_Person: Descriptor<core_Person, \"core.Person\">"));
}
#[test]
fn ambiguous_short_names_require_qualified_imports() {
    let code = naming_manifest(&[("core", &["Person"]), ("other", &["Person", "Address"])])
        .generate()
        .unwrap()
        .typescript;
    assert!(!code.contains(" as Person }"));
    assert!(code.contains("export interface core_Person"));
    assert!(code.contains("export interface other_Person"));
    assert!(code.contains("export { other_Address as Address };"));
}
#[test]
fn companion_and_preamble_collisions_suppress_short_alias_groups() {
    let code = naming_manifest(&[
        (
            "core",
            &[
                "Person",
                "Descriptor",
                "defineRecord",
                "class",
                "Uint8Array",
            ],
        ),
        ("other", &["PersonRead"]),
    ])
    .generate()
    .unwrap()
    .typescript;
    for name in [
        "Person",
        "PersonRead",
        "Descriptor",
        "defineRecord",
        "class",
        "Uint8Array",
    ] {
        assert!(
            !code.contains(&format!(" as {name} }}")),
            "unexpected short alias {name}"
        );
    }
    assert!(!code.contains("export type PersonInsert ="));
}
#[test]
fn short_names_do_not_shadow_qualified_compatibility_exports() {
    let code = naming_manifest(&[("core", &["Person"]), ("other", &["core_Person"])])
        .generate()
        .unwrap()
        .typescript;
    assert!(!code.contains("export { other_core_Person as core_Person };"));
    assert!(code.contains("export { core_Person as Person };"));
}
#[test]
fn qualified_companion_collisions_fail_instead_of_emitting_invalid_typescript() {
    assert!(
        naming_manifest(&[("core", &["Person", "PersonInsert"])])
            .generate()
            .is_err()
    );
    assert!(
        naming_manifest(&[("a_b", &["C"]), ("a", &["b_C"])])
            .generate()
            .is_err()
    );
}
#[test]
fn clean_name_selection_is_independent_of_manifest_order() {
    let mut manifest = naming_manifest(&[
        ("core", &["Person", "Address"]),
        ("other", &["Person", "Project"]),
    ]);
    let original = manifest.generate().unwrap().typescript;
    manifest.modules.reverse();
    for module in &mut manifest.modules {
        module.relations.reverse();
    }
    assert_eq!(original, manifest.generate().unwrap().typescript);
}
