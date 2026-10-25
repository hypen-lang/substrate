//! Same authorized snapshot and output; batched tree vs per-parent SELECTs.
use rusqlite::types::Value as S;
use std::{collections::BTreeMap, hint::black_box, time::Instant};
use substrate_liquid::{nested::*, service::*, *};
fn plan(s: &Snapshot, name: &str, limit: usize) -> ReadPlan {
    let def = s.schema(name).unwrap();
    ReadPlan {
        contract: Contract {
            relation: name.into(),
            schema_id: def.id,
            fingerprint: def.fingerprint(),
        },
        filter: None,
        order: vec![],
        limit,
        include: BTreeMap::new(),
    }
}
fn fixture(n: usize) -> (Database, ReadPlan) {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(demo::core()).unwrap();
    tx.install(demo::fitness()).unwrap();
    for i in 0..n {
        let person = tx
            .insert_with_id(
                "core.Person",
                Id::from_u128(i as u128 + 1),
                vec![format!("Person {i}").as_str().into()],
            )
            .unwrap();
        for j in 0..2 {
            let w = tx.insert("fitness.Workout", vec![(30 + j).into()]).unwrap();
            tx.insert("fitness.Performed", vec![person.into(), w.into()])
                .unwrap();
        }
    }
    for relation in ["core.Person", "fitness.Performed", "fitness.Workout"] {
        tx.grant("app", relation, Grant::read_only()).unwrap();
    }
    tx.commit().unwrap();
    let s = db.session("app").snapshot();
    let mut root = plan(&s, "core.Person", n);
    let mut links = plan(&s, "fitness.Performed", 2);
    links.include.insert(
        "workout".into(),
        Include {
            direction: Direction::One,
            role: "workout".into(),
            plan: plan(&s, "fitness.Workout", 1),
        },
    );
    root.include.insert(
        "performed".into(),
        Include {
            direction: Direction::Many,
            role: "athlete".into(),
            plan: links,
        },
    );
    (db, root)
}
fn id(s: &str) -> Id {
    Id(uuid::Uuid::parse_str(s).unwrap())
}
fn naive(sql: &SqlSnapshot, n: usize) -> Vec<NestedRow> {
    let roots = sql
        .query(
            "SELECT id,name FROM core.Person ORDER BY id LIMIT ?1",
            &[S::Integer(n as i64)],
        )
        .unwrap();
    let mut result = vec![];
    for p in roots.rows {
        let (S::Text(person), S::Text(name)) = (&p[0], &p[1]) else {
            panic!()
        };
        let links=sql.query("SELECT id,athlete,workout FROM fitness.Performed WHERE athlete=?1 ORDER BY id LIMIT 2",&[S::Text(person.clone())]).unwrap();
        let mut children = vec![];
        for link in links.rows {
            let (S::Text(link_id), S::Text(athlete), S::Text(workout)) =
                (&link[0], &link[1], &link[2])
            else {
                panic!()
            };
            let row = sql
                .query(
                    "SELECT id,durationMinutes FROM fitness.Workout WHERE id=?1",
                    &[S::Text(workout.clone())],
                )
                .unwrap();
            let S::Integer(minutes) = row.rows[0][1] else {
                panic!()
            };
            let target = NestedRow {
                row: NamedRow {
                    id: id(workout),
                    fields: [(
                        "durationMinutes".into(),
                        WireValue::Int(minutes.to_string()),
                    )]
                    .into(),
                },
                related: BTreeMap::new(),
            };
            children.push(NestedRow {
                row: NamedRow {
                    id: id(link_id),
                    fields: [
                        ("athlete".into(), WireValue::Ref(athlete.clone())),
                        ("workout".into(), WireValue::Ref(workout.clone())),
                    ]
                    .into(),
                },
                related: [("workout".into(), NestedValue::One(Some(Box::new(target))))].into(),
            });
        }
        result.push(NestedRow {
            row: NamedRow {
                id: id(person),
                fields: [("name".into(), WireValue::Text(name.clone()))].into(),
            },
            related: [("performed".into(), NestedValue::Many(children))].into(),
        });
    }
    result
}
fn time(mut f: impl FnMut()) -> f64 {
    f();
    let mut samples = vec![];
    for _ in 0..7 {
        let t = Instant::now();
        for _ in 0..5 {
            f();
        }
        samples.push(t.elapsed().as_secs_f64() * 1000.0 / 5.0);
    }
    samples.sort_by(f64::total_cmp);
    samples[3]
}
fn main() {
    println!(
        "authorized=true,warm_cache=true,samples=7,iterations=5,children_per_parent=2,no_network=true"
    );
    for n in [10, 100, 500] {
        let (db, p) = fixture(n);
        let snapshot = db.session("app").snapshot();
        let sql = snapshot.sql().unwrap();
        let result = snapshot.read_nested(&p).unwrap();
        assert_eq!(
            serde_json::to_value(&result.rows).unwrap(),
            serde_json::to_value(naive(&sql, n)).unwrap()
        );
        assert_eq!(result.stats.sql_statements, 3);
        // Batched includes SQL connection/plan validation; naive reuses a connection,
        // a deliberately favorable baseline without any network round trips.
        let batch = time(|| {
            black_box(snapshot.read_nested(&p).unwrap());
        });
        let each = time(|| {
            black_box(naive(&sql, n));
        });
        println!(
            "parents={n},batched_statements=3,naive_statements={},batched_median_ms={batch:.3},naive_median_ms={each:.3},ratio_naive_over_batched={:.2},returned_nodes={}",
            1 + 3 * n,
            each / batch,
            result.stats.returned_nodes
        );
    }
}
