//! Snapshot reference-closure scan probe. Setup is excluded; each measured scan
//! uses a fresh authorization snapshot, so the cache must earn its benefit anew.
use std::time::Instant;
use substrate_liquid::*;
fn main() -> Result<()> {
    println!("nodes,roots,median_scan_us");
    for n in [250, 500, 1000] {
        let db = Database::memory();
        let mut relations = vec![Relation::new(
            "graph.Leaf",
            vec![Role::new("visible", DataType::Bool)],
        )];
        for i in 0..n {
            let target = if i == 0 {
                "graph.Leaf".into()
            } else {
                format!("graph.Node{}", i - 1)
            };
            relations.push(Relation::new(
                &format!("graph.Node{i}"),
                vec![Role::new("next", DataType::Ref(target))],
            ));
        }
        relations.push(Relation::new(
            "graph.Root",
            vec![Role::new(
                "next",
                DataType::Ref(format!("graph.Node{}", n - 1)),
            )],
        ));
        let names = relations.iter().map(|r| r.name.clone()).collect::<Vec<_>>();
        let mut tx = db.admin().transaction();
        tx.install(Module::new("graph", 1, vec![], relations))?;
        let mut id = tx.insert("graph.Leaf", vec![Value::Bool(true)])?;
        for i in 0..n {
            id = tx.insert(&format!("graph.Node{i}"), vec![id.into()])?;
        }
        for _ in 0..n {
            tx.insert("graph.Root", vec![id.into()])?;
        }
        for name in names {
            tx.grant("reader", &name, Grant::read_only())?;
        }
        tx.commit()?;
        let mut elapsed = vec![];
        for _ in 0..7 {
            let snap = db.session("reader").snapshot();
            let start = Instant::now();
            let rows = snap.scan("graph.Root")?;
            assert_eq!(rows.len(), n);
            elapsed.push(start.elapsed().as_secs_f64() * 1e6);
        }
        elapsed.sort_by(f64::total_cmp);
        println!("{n},{n},{:.3}", elapsed[3]);
    }
    Ok(())
}
