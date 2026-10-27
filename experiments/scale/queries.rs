// Same nine SQL shapes as the earlier comparison; kept separate from fixture sizes.
pub fn workloads(n: usize) -> Vec<(&'static str, String)> {
    let id = super::person(n / 2);
    vec![
        ("01_primary_key",format!(r#"SELECT id,name FROM core."Person" WHERE id='{id}'"#)),
        ("02_indexed_equality",r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes"=45 ORDER BY id"#.into()),
        ("03_range_top_100",r#"SELECT id,"durationMinutes" FROM fitness."Workout" WHERE "durationMinutes">=40 AND "durationMinutes"<60 ORDER BY "durationMinutes",id LIMIT 100"#.into()),
        ("04_relationship_lookup",format!(r#"SELECT f.id,w.id,w."durationMinutes" FROM fitness."Performed" f JOIN fitness."Workout" w ON w.id=f.workout WHERE f.athlete='{id}' ORDER BY f.id"#)),
        ("05_cross_app_five_way",format!(r#"SELECT p.id,f.id,w."durationMinutes",c.id,d.amount FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout JOIN crm."CustomerDeal" c ON c.customer=p.id JOIN crm."Deal" d ON d.id=c.deal WHERE p.id='{id}' ORDER BY f.id,c.id"#)),
        ("06_cross_app_group_top_25",r#"SELECT p.id,COUNT(*),SUM(w."durationMinutes"),SUM(d.amount) FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout JOIN crm."CustomerDeal" c ON c.customer=p.id JOIN crm."Deal" d ON d.id=c.deal GROUP BY p.id ORDER BY SUM(d.amount) DESC,p.id LIMIT 25"#.into()),
        ("07_correlated_exists",r#"SELECT p.id,p.name FROM core."Person" p WHERE EXISTS (SELECT 1 FROM fitness."Performed" f JOIN fitness."Workout" w ON w.id=f.workout WHERE f.athlete=p.id AND w."durationMinutes">110) ORDER BY p.id LIMIT 100"#.into()),
        ("08_filtered_nested_top_1",r#"SELECT person_id,link_id,workout_id,minutes FROM (SELECT p.id AS person_id,f.id AS link_id,w.id AS workout_id,w."durationMinutes" AS minutes,ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY w."durationMinutes" DESC,f.id) AS rn FROM core."Person" p JOIN fitness."Performed" f ON f.athlete=p.id JOIN fitness."Workout" w ON w.id=f.workout WHERE w."durationMinutes">=30) ranked WHERE rn<=1 ORDER BY person_id LIMIT 100"#.into()),
        ("09_full_scan_aggregate",r#"SELECT COUNT(*),SUM("durationMinutes"),MIN("durationMinutes"),MAX("durationMinutes") FROM fitness."Workout""#.into()),
    ]
}
