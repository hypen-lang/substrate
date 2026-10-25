//! Focused warm-query probes. Run release mode; times exclude population/setup.
use std::time::Instant;
use substrate_liquid::*;
fn measure(sql: &SqlSnapshot, query: &str, iterations: usize) -> (u128, usize) {
    let start = Instant::now();
    let mut rows = 0;
    for _ in 0..iterations {
        rows += std::hint::black_box(sql.query(query, &[]).unwrap())
            .rows
            .len();
    }
    (start.elapsed().as_nanos() / iterations as u128, rows)
}
fn main() -> Result<()> {
    for n in [500, 1000, 2000] {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "bench",
            1,
            vec![],
            vec![Relation::new(
                "bench.Item",
                vec![
                    Role::new("group", DataType::Int),
                    Role::new("score", DataType::Int),
                ],
            )],
        ))?;
        for i in 0..n {
            tx.insert("bench.Item", vec![0.into(), (i as i64).into()])?;
        }
        tx.commit()?;
        let sql = db.admin().snapshot().sql()?;
        for (name, query) in [
            (
                "broad_first",
                "SELECT count(*) FROM bench.Item o WHERE EXISTS (SELECT 1 FROM bench.Item i WHERE i.\"group\"=o.\"group\" AND i.id=o.id)",
            ),
            (
                "identity_first",
                "SELECT count(*) FROM bench.Item o WHERE EXISTS (SELECT 1 FROM bench.Item i WHERE i.id=o.id AND i.\"group\"=o.\"group\")",
            ),
            (
                "range_top100",
                "SELECT score FROM bench.Item WHERE score>=100 AND score<1500 ORDER BY score DESC LIMIT 100",
            ),
        ] {
            let (ns, rows) = measure(&sql, query, 3);
            println!("{name},{n},{ns},{rows}");
        }
    }
    for bytes in [0, 65536] {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "bench",
            1,
            vec![],
            vec![Relation::new(
                "bench.Blob",
                vec![Role::new("payload", DataType::Bytes)],
            )],
        ))?;
        for _ in 0..1000 {
            tx.insert("bench.Blob", vec![Value::Bytes(vec![1; bytes])])?;
        }
        tx.commit()?;
        let sql = db.admin().snapshot().sql()?;
        let (ns, rows) = measure(&sql, "SELECT count(*) FROM bench.Blob", 10);
        println!("blob_count,{bytes},{ns},{rows}");
    }
    for families in [10, 100, 1000] {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        tx.install(Module::new(
            "catalog",
            1,
            vec![],
            (0..families)
                .map(|i| Relation::new(&format!("catalog.R{i}"), vec![]))
                .collect(),
        ))?;
        tx.commit()?;
        let snapshot = db.admin().snapshot();
        for selected in [false, true] {
            let start = Instant::now();
            for _ in 0..3 {
                let sql = if selected {
                    snapshot.sql_selected(&["catalog.R0"])?
                } else {
                    snapshot.sql()?
                };
                assert_eq!(
                    sql.query("SELECT count(*) FROM catalog.R0", &[])?.rows[0][0],
                    rusqlite::types::Value::Integer(0)
                );
            }
            println!(
                "catalog_{},{families},{},3",
                if selected { "selected" } else { "all" },
                start.elapsed().as_nanos() / 3
            );
        }
    }
    Ok(())
}
