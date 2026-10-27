use substrate_liquid::*;

#[test]
fn selective_physical_joins_preserve_nullable_and_float_matches() {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new(
        "probe",
        1,
        vec![],
        vec![Relation::new(
            "probe.Item",
            vec![
                Role::new("optional", DataType::Int.nullable()),
                Role::new("real_value", DataType::Float),
            ],
        )],
    ))
    .unwrap();
    for (id, optional, number) in [
        (1, Value::Int(7), 1.5),
        (2, Value::Int(7), 1.5),
        (3, Value::Null, 2.0),
        (4, Value::Null, 2.0),
    ] {
        tx.insert_with_id(
            "probe.Item",
            Id::from_u128(id),
            vec![optional, Value::try_from(number).unwrap()],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let snapshot = db.admin().snapshot();
    let sql = snapshot.sql().unwrap();
    let sqlite = snapshot.sqlite_compat().unwrap();
    // Identity selection makes one join input small enough for the physical
    // index-probe optimizer, while the other input has a new scalar type.
    for (column, id, matches) in [("optional", 1, 2), ("optional", 3, 0), ("real_value", 1, 2)] {
        let query = format!(
            "SELECT b.id FROM probe.Item a JOIN probe.Item b ON a.{column}=b.{column} WHERE a.id='{}' ORDER BY b.id",
            Id::from_u128(id)
        );
        let actual = sql.query(&query, &[]).unwrap().rows;
        assert_eq!(actual, sqlite.query(&query, &[]).unwrap().rows, "{query}");
        assert_eq!(actual.len(), matches, "{query}");
    }
}
