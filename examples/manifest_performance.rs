//! Isolate canonical dependency ordering. Includes the frozen old algorithm as a
//! same-process reference; does not claim full schema installation is linear.
use std::{collections::BTreeSet, hint::black_box, time::Instant};
use substrate_liquid::*;
fn previous(m: &SchemaManifest) -> SchemaManifest {
    let mut pending = m.modules.clone();
    pending.sort_by(|a, b| (&a.name, a.version).cmp(&(&b.name, b.version)));
    let mut keys = BTreeSet::new();
    for module in &pending {
        assert!(keys.insert((module.name.clone(), module.version)));
    }
    let mut available = BTreeSet::new();
    let mut ordered = vec![];
    while !pending.is_empty() {
        let i = pending
            .iter()
            .position(|m| {
                m.version > 0
                    && (m.version == 1 || available.contains(&(m.name.clone(), m.version - 1)))
                    && m.imports.iter().all(|key| available.contains(key))
            })
            .unwrap();
        let mut m = pending.remove(i);
        m.imports.sort();
        m.relations.sort_by(|a, b| a.name.cmp(&b.name));
        available.insert((m.name.clone(), m.version));
        ordered.push(m);
    }
    SchemaManifest {
        format: 1,
        modules: ordered,
    }
}
fn time(mut f: impl FnMut() -> SchemaManifest) -> u128 {
    black_box(f());
    let mut samples = (0..5)
        .map(|_| {
            let start = Instant::now();
            black_box(f());
            start.elapsed().as_nanos()
        })
        .collect::<Vec<_>>();
    samples.sort_unstable();
    samples[2]
}
fn main() {
    println!("modules,previous_median_ns,graph_median_ns");
    for n in [1000, 2000, 4000, 8000] {
        let m = SchemaManifest {
            format: 1,
            modules: (0..n)
                .map(|i| {
                    Module::new(
                        &format!("n{i:05}"),
                        1,
                        if i + 1 < n {
                            vec![(format!("n{:05}", i + 1), 1)]
                        } else {
                            vec![]
                        },
                        vec![],
                    )
                })
                .collect(),
        };
        assert_eq!(previous(&m), m.canonical().unwrap());
        println!(
            "{n},{},{}",
            time(|| previous(&m)),
            time(|| m.canonical().unwrap())
        );
    }
}
