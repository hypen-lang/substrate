//! Focused snapshot/point-read/refcount probe. Run alone for meaningful CPU results.
//! Args: ROWS SECONDS CLIENTS [MODES] [MIN_TEXT_BYTES], with comma-separated lists.
//! `-scoped` modes use expiring local credentials, without TLS transport. Clock
//! modes isolate unconditional atomic-max versus load-first clock observation.
//! Shared reads return a handle; they do not consume the full payload.
use std::{
    hint::black_box,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use substrate_liquid::{
    Database, Grant, Id, Value, Visibility,
    capability::{CapabilityScope, RelationCapability},
    demo,
};

fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // libc writes the entire rusage on success; no engine memory is accessed.
    unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        let usage = usage.assume_init();
        usage.ru_utime.tv_sec as f64
            + usage.ru_stime.tv_sec as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let rows: usize = args.get(1).map_or(10_000, |s| s.parse().unwrap());
    let seconds: f64 = args.get(2).map_or(1.0, |s| s.parse().unwrap());
    let clients: Vec<usize> = args
        .get(3)
        .map_or("1,8,32", String::as_str)
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    let modes: Vec<String> = args
        .get(4)
        .map_or(
            "arc-shared,arc-local,snapshot,retained,fresh,session",
            String::as_str,
        )
        .split(',')
        .map(str::to_owned)
        .collect();
    let payload_bytes: usize = args.get(5).map_or(0, |s| s.parse().unwrap());
    assert!(modes.iter().all(|m| matches!(
        m.as_str(),
        "arc-shared"
            | "arc-local"
            | "snapshot"
            | "retained"
            | "fresh"
            | "session"
            | "shared"
            | "retained-scoped"
            | "fresh-scoped"
            | "session-scoped"
            | "shared-scoped"
            | "clock-rmw"
            | "clock-read"
    )));
    assert!(rows > 0 && seconds.is_finite() && seconds > 0.0 && clients.iter().all(|n| *n > 0));
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install_with_visibility(demo::core(), Visibility::Private)
        .unwrap();
    tx.grant("reader", "core.Person", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    for start in (0..rows).step_by(1000) {
        let mut tx = db.admin().transaction();
        for i in start..(start + 1000).min(rows) {
            let mut text = format!("Person{i}");
            text.extend(std::iter::repeat_n(
                'x',
                payload_bytes.saturating_sub(text.len()),
            ));
            tx.insert_with_id(
                "core.Person",
                Id::from_u128(i as u128 + 1),
                vec![Value::Text(text)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let cap = db
        .issue_capability(
            Id::new(),
            "reader",
            CapabilityScope {
                relations: vec![RelationCapability {
                    relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                    grant: Grant::read_only(),
                }],
                schema_namespace: None,
                delegations: 0,
            },
        )
        .unwrap();
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let token = db.issue_credential("reader", cap.id, expires).unwrap();
    for mode in modes {
        for &clients in &clients {
            let start = Arc::new(Barrier::new(clients + 1));
            let ready = Arc::new(Barrier::new(clients + 1));
            let shared = Arc::new(1_u64);
            let clock = Arc::new(AtomicU64::new(0));
            let threads: Vec<_> = (0..clients)
                .map(|worker| {
                    let mode = mode.clone();
                    let db = db.clone();
                    let ready = ready.clone();
                    let start = start.clone();
                    let shared = shared.clone();
                    let clock = clock.clone();
                    let token = token.token.clone();
                    std::thread::spawn(move || {
                        let session = if mode.ends_with("-scoped") {
                            db.authenticate(&token).unwrap()
                        } else {
                            db.session("reader")
                        };
                        session.get("core.Person", Id::from_u128(1)).unwrap();
                        let retained = session.snapshot();
                        let local = Arc::new(1_u64);
                        let mut count = 0_u64;
                        let mut seed = worker as u64 + 1;
                        let mut samples = Vec::new();
                        ready.wait();
                        start.wait();
                        let deadline = Instant::now() + Duration::from_secs_f64(seconds);
                        while Instant::now() < deadline {
                            let sample = count.is_multiple_of(1024).then(Instant::now);
                            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let id = Id::from_u128((seed as usize % rows) as u128 + 1);
                            match mode.as_str() {
                                "arc-shared" => {
                                    black_box(shared.clone());
                                }
                                "arc-local" => {
                                    black_box(local.clone());
                                }
                                "snapshot" => {
                                    black_box(session.snapshot());
                                }
                                "retained" | "retained-scoped" => {
                                    black_box(retained.get("core.Person", id).unwrap().unwrap());
                                }
                                "fresh" | "fresh-scoped" => {
                                    black_box(
                                        session.snapshot().get("core.Person", id).unwrap().unwrap(),
                                    );
                                }
                                "session" | "session-scoped" => {
                                    black_box(session.get("core.Person", id).unwrap().unwrap());
                                }
                                "shared" | "shared-scoped" => {
                                    black_box(
                                        session.get_shared("core.Person", id).unwrap().unwrap(),
                                    );
                                }
                                "clock-rmw" | "clock-read" => {
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .map(|d| d.as_secs())
                                        .unwrap_or(u64::MAX);
                                    let last = if mode == "clock-read" {
                                        clock.load(Ordering::SeqCst)
                                    } else {
                                        0
                                    };
                                    black_box(if mode == "clock-read" && now <= last {
                                        last
                                    } else {
                                        clock.fetch_max(now, Ordering::SeqCst).max(now)
                                    });
                                }
                                _ => unreachable!(),
                            }
                            if let Some(sample) = sample {
                                samples.push(sample.elapsed().as_secs_f64() * 1e6);
                            }
                            count += 1;
                        }
                        (count, samples)
                    })
                })
                .collect();
            ready.wait();
            let cpu = cpu_seconds();
            let wall = Instant::now();
            start.wait();
            let mut count = 0;
            let mut samples = Vec::new();
            for thread in threads {
                let (n, times) = thread.join().unwrap();
                count += n;
                samples.extend(times);
            }
            let wall = wall.elapsed().as_secs_f64();
            let cpu = cpu_seconds() - cpu;
            samples.sort_by(f64::total_cmp);
            println!(
                "{}",
                serde_json::json!({"mode":mode,"rows":rows,"payload_bytes":payload_bytes,"clients":clients,"ops":count,"seconds":wall,"ops_per_second":count as f64/wall,"cpu_cores":cpu/wall,"sample_p50_us":samples.get(samples.len()/2),"sample_p99_us":samples.get(samples.len().saturating_sub(1)*99/100)})
            );
        }
    }
}
