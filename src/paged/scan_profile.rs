//! Opt-in diagnostic that separates redb traversal, typed decoding and reference
//! visibility costs. This is a phase profile, not an engine comparison benchmark.
use super::*;
use std::{hint::black_box, time::Instant};

fn process_cpu_us() -> Option<(f64, f64)> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage writes this correctly sized output on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return None;
        }
        // SAFETY: the successful call initialized the fields read below.
        let usage = unsafe { usage.assume_init() };
        Some((
            usage.ru_utime.tv_sec as f64 * 1e6 + usage.ru_utime.tv_usec as f64,
            usage.ru_stime.tv_sec as f64 * 1e6 + usage.ru_stime.tv_usec as f64,
        ))
    }
    #[cfg(not(unix))]
    {
        None
    }
}

#[test]
#[ignore = "opt-in release scan phase diagnostic; do not overlap benchmark runs"]
fn broad_scan_phases() {
    let rows = std::env::var("SUBSTRATE_SCAN_PROFILE_ROWS")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(100_000);
    assert!(rows > 0);
    let cache_mib = std::env::var("SUBSTRATE_SCAN_PROFILE_CACHE_MIB")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(8);
    let cache_bytes = cache_mib.checked_mul(1024 * 1024).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = PagedDatabase::open(
        dir.path().join("profile.redb"),
        PagedOptions {
            cache_bytes,
            decoded_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let mut person = Relation::new("core.Person", vec![Role::new("name", DataType::Text)]);
    person.id = Id::from_u128(1 << 120);
    let mut score = Relation::new(
        "app.Score",
        vec![
            Role::new("person", DataType::Ref("core.Person".into())),
            Role::new("score", DataType::Int),
            Role::new("bucket", DataType::Int),
        ],
    );
    score.id = Id::from_u128(2 << 120);
    let mut tx = db.admin().transaction().unwrap();
    tx.install(Module::new("core", 1, vec![], vec![person]))
        .unwrap();
    tx.install(Module::new("app", 1, vec![("core".into(), 1)], vec![score]))
        .unwrap();
    tx.commit().unwrap();
    for start in (0..rows).step_by(128) {
        let mut tx = db.admin().transaction().unwrap();
        for n in start..(start + 128).min(rows) {
            let person = Id::from_u128(n as u128 + 1);
            tx.insert_with_id(
                "core.Person",
                person,
                vec![Value::Text(format!("person-{n}"))],
            )
            .unwrap();
            tx.insert_with_id(
                "app.Score",
                Id::from_u128(n as u128 + 1 + (1 << 64)),
                vec![
                    Value::Ref(person),
                    (n as i64).into(),
                    ((n % 32) as i64).into(),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let snapshot = db.admin().snapshot().unwrap();
    let def = snapshot.def("app.Score").unwrap();
    let prefix = def.id.0.as_bytes();
    let end = prefix_end(prefix).unwrap();
    // Rotate phase ordering to avoid always giving one phase the cold cache.
    for round in 0..3 {
        for offset in 0..5 {
            let phase = (round + offset) % 5;
            let mut scratch = None;
            let mut count = 0usize;
            let cpu_start = process_cpu_us();
            let start = Instant::now();
            if phase == 4 {
                snapshot
                    .visit_authorized_rows("app.Score", &mut || Ok(()), &mut |row| {
                        black_box(row);
                        count += 1;
                        Ok(())
                    })
                    .unwrap();
            } else {
                for entry in snapshot
                    .view
                    .rows
                    .range::<&[u8]>(prefix.as_slice()..end.as_slice())
                    .unwrap()
                {
                    let (key, packed) = entry.unwrap();
                    let id = Id(uuid::Uuid::from_slice(&key.value()[16..]).unwrap());
                    match phase {
                        0 => {
                            black_box((key.value(), packed.value()));
                        }
                        1 => snapshot.validate_uncached(&def, packed.value()).unwrap(),
                        2 => {
                            black_box(snapshot.decode_uncached(&def, id, packed.value()).unwrap());
                        }
                        3 => {
                            black_box(
                                snapshot
                                    .decode_scan_row(&def, id, packed.value(), &mut scratch)
                                    .unwrap(),
                            );
                        }
                        _ => unreachable!(),
                    }
                    count += 1;
                }
            }
            let elapsed = start.elapsed();
            let cpu = cpu_start
                .zip(process_cpu_us())
                .map(|(start, end)| (end.0 - start.0, end.1 - start.1));
            assert_eq!(count, rows);
            eprintln!(
                "{}",
                serde_json::json!({
                    "round": round,
                    "phase": (["root_redb", "root_validate", "root_owned_decode", "root_reused_decode", "authorized_with_reference"][phase]),
                    "rows": rows,
                    "cache_mib": cache_mib,
                    "cursor": "standard_owned_redb",
                    "elapsed_us": elapsed.as_secs_f64() * 1e6,
                    "cpu_user_us": cpu.map(|(user, _)| user),
                    "cpu_system_us": cpu.map(|(_, system)| system),
                    "cpu_total_us": cpu.map(|(user, system)| user + system),
                })
            );
        }
    }
}
