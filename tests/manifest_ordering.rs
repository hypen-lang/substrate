use std::collections::BTreeSet;
use substrate_liquid::*;

// Frozen pre-change ordering algorithm: an independent compatibility oracle.
fn previous(manifest: &SchemaManifest) -> Result<SchemaManifest> {
    if manifest.format != 1 {
        return Err(Error::Schema("unsupported schema manifest format".into()));
    }
    let mut pending = manifest.modules.clone();
    pending.sort_by(|a, b| (&a.name, a.version).cmp(&(&b.name, b.version)));
    let mut keys = BTreeSet::new();
    for m in &pending {
        if !keys.insert((m.name.clone(), m.version)) {
            return Err(Error::Schema("duplicate module version".into()));
        }
    }
    let mut available = BTreeSet::new();
    let mut ordered = vec![];
    while !pending.is_empty() {
        let index = pending
            .iter()
            .position(|m| {
                m.version > 0
                    && (m.version == 1 || available.contains(&(m.name.clone(), m.version - 1)))
                    && m.imports.iter().all(|i| available.contains(i))
            })
            .ok_or_else(|| {
                Error::Schema("missing/cyclic imports or nonconsecutive module versions".into())
            })?;
        let mut m = pending.remove(index);
        m.imports.sort();
        m.relations.sort_by(|a, b| a.name.cmp(&b.name));
        available.insert((m.name.clone(), m.version));
        ordered.push(m);
    }
    Ok(SchemaManifest {
        format: 1,
        modules: ordered,
    })
}
fn module(name: &str, version: u64, imports: &[(&str, u64)]) -> Module {
    Module::new(
        name,
        version,
        imports.iter().map(|(n, v)| (n.to_string(), *v)).collect(),
        vec![],
    )
}
fn compare(m: &SchemaManifest) {
    assert_eq!(
        m.canonical().map_err(|e| e.to_string()),
        previous(m).map_err(|e| e.to_string())
    );
}
#[test]
fn smallest_ready_order_matches_original_and_keeps_immutable_definitions() {
    let mut m = SchemaManifest {
        format: 1,
        modules: vec![
            module("z", 1, &[]),
            module("b", 2, &[("z", 1)]),
            module("c", 1, &[]),
            module("a", 1, &[("z", 1)]),
            module("b", 1, &[]),
        ],
    };
    let relation = Relation {
        id: Id::from_u128(42),
        name: "a.Item".into(),
        roles: vec![Role::new("value", DataType::Int)],
        unique: vec![],
        checks: vec![],
    };
    m.modules[3].relations.push(relation.clone());
    compare(&m);
    let ordered = m.canonical().unwrap();
    assert_eq!(
        ordered
            .modules
            .iter()
            .map(|m| (m.name.as_str(), m.version))
            .collect::<Vec<_>>(),
        vec![("b", 1), ("c", 1), ("z", 1), ("a", 1), ("b", 2)]
    );
    assert_eq!(ordered.modules[3].relations, vec![relation]);
    let json = m.to_json().unwrap();
    m.modules.reverse();
    assert_eq!(m.to_json().unwrap(), json);
}
#[test]
fn dependency_errors_and_duplicate_import_semantics_match_original() {
    for modules in [
        vec![module("a", 0, &[])],
        vec![module("a", 2, &[])],
        vec![module("a", 1, &[("missing", 1)])],
        vec![module("a", 1, &[("b", 1)]), module("b", 1, &[("a", 1)])],
        vec![module("a", 1, &[]), module("a", 1, &[])],
        vec![module("a", 1, &[]), module("b", 1, &[("a", 1), ("a", 1)])],
        vec![module("a", 1, &[]), module("a", 2, &[("a", 1)])],
        vec![module("a", 1, &[]), module("a", u64::MAX, &[])],
    ] {
        compare(&SchemaManifest { format: 1, modules });
    }
}
#[test]
fn fixture_manifest_fingerprint_is_byte_compatible() {
    use sha2::{Digest, Sha256};
    let m = SchemaManifest::from_json(include_str!("../examples/dx/manifest.json")).unwrap();
    assert_eq!(
        m.fingerprint().unwrap(),
        "ea476cecdf7df000e5006654915b8508112b958ceead485073bc09d51994bd55"
    );
    let mut hash = Sha256::new();
    hash.update(b"LiquidManifest/1\0");
    hash.update(serde_json::to_vec(&previous(&m).unwrap()).unwrap());
    assert_eq!(m.fingerprint().unwrap(), format!("{:x}", hash.finalize()));
}
#[test]
fn local_reference_validation_keeps_case_sensitive_targets_and_forward_cycles() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(
        Module::new(
            "cycle",
            1,
            vec![],
            vec![
                Relation::new(
                    "cycle.A",
                    vec![Role::new("b", DataType::Ref("cycle.B".into()))],
                ),
                Relation::new(
                    "cycle.B",
                    vec![Role::new("a", DataType::Ref("cycle.A".into()))],
                ),
            ],
        ),
        substrate_liquid::Visibility::Private,
    )
    .unwrap();
    tx.commit().unwrap();
    let mut tx = db.admin().transaction();
    assert!(
        tx.install_with_visibility(
            Module::new(
                "bad",
                1,
                vec![],
                vec![
                    Relation::new("bad.A", vec![Role::new("b", DataType::Ref("bad.b".into()))]),
                    Relation::new("bad.B", vec![])
                ]
            ),
            substrate_liquid::Visibility::Private
        )
        .is_err()
    );
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn random_graph_order_and_errors_match_frozen_algorithm(edges in proptest::collection::vec(proptest::collection::vec(0usize..32,0..8),1..32),acyclic in proptest::bool::ANY,reverse in proptest::bool::ANY) {
        let len=edges.len();
        let mut modules=edges.into_iter().enumerate().map(|(i,edges)| {
            let imports=edges.into_iter().map(|v|v%len).filter(|j|!acyclic||*j<i).map(|j|(format!("n{j:03}"),1)).collect();
            Module::new(&format!("n{i:03}"),1,imports,vec![])
        }).collect::<Vec<_>>();
        if reverse {modules.reverse();}
        compare(&SchemaManifest{format:1,modules});
    }
}
