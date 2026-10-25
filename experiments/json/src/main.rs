//! Isolated typed-parser experiment; production still uses serde_json.
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, hint::black_box, time::Instant};
use substrate_liquid::{service::*, *};

const TOKEN: &str = "json-benchmark-local-credential-not-a-secret";
const LIMIT: usize = 1024 * 1024;
const PERSON: Id = Id::from_u128(100);
const PERSON_TYPE: Id = Id::from_u128(101);

fn request(command: Command) -> Request {
    Request {
        version: 1,
        token: TOKEN.into(),
        command,
    }
}
fn profile() -> Relation {
    let mut relation = Relation::new(
        "bench.Profile",
        vec![
            Role::new("name", DataType::Text),
            Role::new("age", DataType::Int),
        ],
    )
    .check(Check::int_range("age", 0, 150));
    relation.id = PERSON_TYPE;
    relation
}
fn module(count: usize) -> Module {
    let relations = (0..count)
        .map(|n| {
            let mut relation = profile();
            relation.name = format!("bench.Profile{n}");
            relation.id = Id::from_u128(1000 + n as u128);
            relation
        })
        .collect();
    Module::new("bench", 1, vec![], relations)
}
fn insert(name: String) -> Mutation {
    Mutation::Insert {
        relation: "bench.Profile".into(),
        schema_id: PERSON_TYPE,
        fields: BTreeMap::from([
            ("name".into(), WireValue::Text(name)),
            ("age".into(), WireValue::Int("37".into())),
        ]),
        alias: None,
    }
}
fn read_request() -> Request {
    request(Command::Query {
        sql: format!("SELECT name,age FROM bench.Profile WHERE id = '{PERSON}'"),
        params: vec![],
    })
}
fn fixtures() -> Vec<(&'static str, Vec<u8>)> {
    let commands = [
        ("small_read", read_request()),
        (
            "named_write",
            request(Command::Batch {
                operations: vec![insert("Ada 🦀 \"Lovelace\"\n".into())],
            }),
        ),
        (
            "batch_100",
            request(Command::Batch {
                operations: (0..100)
                    .map(|i| insert(format!("Person {i} with Unicode 🦀 and a quote \"")))
                    .collect(),
            }),
        ),
        (
            "schema_proposal_100",
            request(Command::SchemaPropose {
                id: Id::from_u128(102),
                module: module(100),
            }),
        ),
        (
            "near_limit_text",
            request(Command::Batch {
                operations: vec![insert("x".repeat(900 * 1024))],
            }),
        ),
    ];
    commands
        .into_iter()
        .map(|(name, value)| (name, serde_json::to_vec(&value).unwrap()))
        .collect()
}
fn equivalent<T: DeserializeOwned + Serialize>(bytes: &[u8]) {
    let a: T = serde_json::from_slice(bytes).unwrap();
    let b: T = simd_json::serde::from_slice(&mut bytes.to_vec()).unwrap();
    assert_eq!(
        serde_json::to_value(a).unwrap(),
        serde_json::to_value(b).unwrap()
    );
}
fn median(v: &mut [f64]) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}
fn measure<T: DeserializeOwned>(input: &[u8], mode: usize, iterations: usize) -> f64 {
    // Ready frames model owned receive buffers; filling them is excluded for both
    // parsers. simd-json mutates escapes, so every iteration receives fresh bytes.
    let mut ready = if matches!(mode, 3 | 4 | 6) {
        (0..iterations).map(|_| input.to_vec()).collect::<Vec<_>>()
    } else {
        vec![]
    };
    let mut frames = ready.iter_mut();
    let mut buffers = simd_json::Buffers::with_max_depth(input.len(), 128);
    if mode >= 5 {
        let warm: T =
            simd_json::serde::from_slice_with_buffers(&mut input.to_vec(), &mut buffers).unwrap();
        drop(warm);
    }
    let start = Instant::now();
    for _ in 0..iterations {
        let value: T = match mode {
            0 => serde_json::from_slice(black_box(input)).unwrap(),
            1 => {
                let copy = black_box(input.to_vec());
                serde_json::from_slice(black_box(&copy)).unwrap()
            }
            2 => simd_json::serde::from_slice(black_box(&mut input.to_vec())).unwrap(),
            3 => serde_json::from_slice(black_box(frames.next().unwrap())).unwrap(),
            4 => simd_json::serde::from_slice(black_box(frames.next().unwrap())).unwrap(),
            5 => simd_json::serde::from_slice_with_buffers(
                black_box(&mut input.to_vec()),
                &mut buffers,
            )
            .unwrap(),
            6 => simd_json::serde::from_slice_with_buffers(
                black_box(frames.next().unwrap()),
                &mut buffers,
            )
            .unwrap(),
            _ => unreachable!(),
        };
        drop(black_box(value));
    }
    start.elapsed().as_secs_f64() * 1e6 / iterations as f64
}
fn parser_report<T: DeserializeOwned + Serialize>(name: &str, input: &[u8]) -> serde_json::Value {
    equivalent::<T>(input);
    let iterations = (16 * 1024 * 1024 / input.len()).clamp(16, 10_000);
    let mut samples = [vec![], vec![], vec![], vec![], vec![], vec![], vec![]];
    for mode in 0..7 {
        black_box(measure::<T>(input, mode, iterations.min(32)));
    }
    // Rotate order to reduce systematic temperature/order bias.
    for round in 0..9 {
        for step in 0..7 {
            let mode = (step + round) % 7;
            samples[mode].push(measure::<T>(input, mode, iterations));
        }
    }
    let names = [
        "serde_borrowed",
        "serde_copy_inclusive",
        "simd_copy_inclusive",
        "serde_ready_frames",
        "simd_ready_frames",
        "simd_copy_reused_buffers_depth128",
        "simd_ready_reused_buffers_depth128",
    ];
    let mut timings = serde_json::Map::new();
    for (name, values) in names.into_iter().zip(samples.iter_mut()) {
        let raw = values.clone();
        let mid = median(values);
        timings.insert(name.into(), serde_json::json!({"median_us": mid, "min_us": values[0], "max_us":values[8], "round_mean_us":raw}));
    }
    serde_json::json!({"name":name, "bytes":input.len(), "iterations_per_round":iterations, "timings":timings})
}
fn gateway() -> Gateway {
    let db = Database::memory();
    let mut tx = db.admin().transaction();
    tx.install(Module::new("bench", 1, vec![], vec![profile()]))
        .unwrap();
    tx.insert_with_id("bench.Profile", PERSON, vec!["Ada".into(), 37.into()])
        .unwrap();
    tx.grant("reader", "bench.Profile", Grant::read_only())
        .unwrap();
    tx.commit().unwrap();
    Gateway::new(
        db,
        ServerConfig {
            version: 1,
            clients: vec![ClientCredential {
                token: TOKEN.into(),
                principal: "reader".into(),
            }],
        },
    )
    .unwrap()
}
fn dispatch_report() -> serde_json::Value {
    let gateway = gateway();
    let input = serde_json::to_vec(&read_request()).unwrap();
    let mut samples = [vec![], vec![], vec![]];
    for round in 0..9 {
        for step in 0..3 {
            let mode = (step + round) % 3;
            let mut ready = if mode == 2 {
                (0..500).map(|_| input.clone()).collect::<Vec<_>>()
            } else {
                vec![]
            };
            let mut frames = ready.iter_mut();
            let start = Instant::now();
            for _ in 0..500 {
                let req: Request = match mode {
                    0 => serde_json::from_slice(&input).unwrap(),
                    1 => simd_json::serde::from_slice(&mut input.clone()).unwrap(),
                    2 => simd_json::serde::from_slice(frames.next().unwrap()).unwrap(),
                    _ => unreachable!(),
                };
                let output = gateway.handle(req);
                assert!(
                    matches!(&output, Response::Ok(Output::Query { rows, .. }) if rows.len() == 1)
                );
                drop(black_box(output));
            }
            samples[mode].push(start.elapsed().as_secs_f64() * 1e6 / 500.0);
        }
    }
    let names = ["serde_borrowed", "simd_copy_inclusive", "simd_ready_frames"];
    let results = names
        .into_iter()
        .zip(samples.iter_mut())
        .map(|(name, values)| {
            let raw = values.clone();
            let mid = median(values);
            serde_json::json!({"parser":name,"median_us":mid,"round_mean_us":raw})
        })
        .collect::<Vec<_>>();
    serde_json::json!({"name":"small_read_parse_plus_gateway", "iterations_per_round":500, "timings":results, "excludes":"network/TLS, response JSON serialization, durable writes"})
}
fn main() {
    let mut reports = fixtures()
        .into_iter()
        .map(|(name, bytes)| {
            assert!(bytes.len() <= LIMIT);
            parser_report::<Request>(name, &bytes)
        })
        .collect::<Vec<_>>();
    let metadata = serde_json::to_vec(&module(100)).unwrap();
    reports.push(parser_report::<Module>("module_metadata_100", &metadata));
    println!("{}", serde_json::to_string_pretty(&serde_json::json!({
        "serde_json":"1.0.151", "simd_json":"0.18.1", "arch":std::env::consts::ARCH,
        "os":std::env::consts::OS, "default_allocator":true, "rounds":9,
        "simd_features":"defaults: serde_impl,swar-number-parsing,runtime-detection; aarch64 NEON",
        "parsers":reports, "dispatch":dispatch_report()
    })).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parity(bytes: &[u8]) -> bool {
        let a = serde_json::from_slice::<Request>(bytes).map(|x| serde_json::to_value(x).unwrap());
        let b = simd_json::serde::from_slice::<Request>(&mut bytes.to_vec())
            .map(|x| serde_json::to_value(x).unwrap());
        assert_eq!(a.is_ok(), b.is_ok(), "{}", String::from_utf8_lossy(bytes));
        if let (Ok(a), Ok(b)) = (&a, &b) {
            assert_eq!(a, b);
        }
        a.is_ok()
    }
    #[test]
    fn actual_protocol_and_metadata_types_agree() {
        for (_, bytes) in fixtures() {
            assert!(parity(&bytes));
        }
        equivalent::<Module>(&serde_json::to_vec(&module(100)).unwrap());
    }
    #[test]
    fn unknown_duplicate_truncated_invalid_unicode_and_wrong_types_agree() {
        let valid = serde_json::to_string(&read_request()).unwrap();
        for raw in [
            valid.replacen("{", "{\"unknown\":true,", 1),
            valid.replacen("{", "{\"version\":1,", 1),
            valid.replacen("\"version\":1", "\"version\":\"1\"", 1),
            valid.replacen("\"op\":\"query\"", "\"op\":\"query\",\"extra\":0", 1),
            valid[..valid.len() - 1].into(),
            valid.clone() + " false",
            valid.replace("SELECT name,age", "\\uDC00"),
        ] {
            assert!(!parity(raw.as_bytes()));
        }
        let mut invalid = valid.into_bytes();
        invalid[3] = 0xff;
        assert!(!parity(&invalid));
    }
    #[test]
    fn lone_high_surrogate_is_a_confirmed_candidate_compatibility_blocker() {
        let valid = serde_json::to_string(&read_request()).unwrap();
        for escape in ["\\uD800", "\\uDBFF"] {
            let raw = valid.replace("SELECT name,age", escape);
            assert!(serde_json::from_slice::<Request>(raw.as_bytes()).is_err());
            let candidate: Request = simd_json::serde::from_slice(&mut raw.into_bytes()).unwrap();
            let Command::Query { sql, .. } = candidate.command else {
                panic!("query expected")
            };
            assert_eq!(
                sql.as_bytes()[0],
                0,
                "candidate turns unmatched high surrogate into NUL"
            );
        }
        assert!(parity(
            valid
                .replace("SELECT name,age", "\\uD83E\\uDD80")
                .as_bytes()
        ));
    }
    #[test]
    fn candidate_does_not_preserve_default_json_recursion_limit() {
        let mut command = serde_json::to_value(read_request().command).unwrap();
        for _ in 0..160 {
            command = serde_json::json!({"op":"using_capability","capability":Id::from_u128(1),"command":command});
        }
        let raw =
            serde_json::to_vec(&serde_json::json!({"version":1,"token":TOKEN,"command":command}))
                .unwrap();
        assert!(serde_json::from_slice::<Request>(&raw).is_err());
        assert!(simd_json::serde::from_slice::<Request>(&mut raw.clone()).is_ok());
        let mut buffers = simd_json::Buffers::with_max_depth(raw.len(), 128);
        assert!(
            simd_json::serde::from_slice_with_buffers::<Request>(&mut raw.clone(), &mut buffers)
                .is_err()
        );
    }
    #[test]
    fn bounded_reused_buffers_keep_valid_payloads_but_do_not_fix_unicode_gap() {
        for (_, mut bytes) in fixtures() {
            let a: Request = serde_json::from_slice(&bytes).unwrap();
            let mut buffers = simd_json::Buffers::with_max_depth(bytes.len(), 128);
            let b: Request =
                simd_json::serde::from_slice_with_buffers(&mut bytes, &mut buffers).unwrap();
            assert_eq!(
                serde_json::to_value(a).unwrap(),
                serde_json::to_value(b).unwrap()
            );
        }
        let valid = serde_json::to_string(&read_request()).unwrap();
        let mut bytes = valid.replace("SELECT name,age", "\\uD800").into_bytes();
        let mut buffers = simd_json::Buffers::with_max_depth(bytes.len(), 128);
        assert!(
            simd_json::serde::from_slice_with_buffers::<Request>(&mut bytes, &mut buffers).is_ok()
        );
    }
    #[test]
    fn exact_integer_strings_and_boundary_values_agree() {
        for raw in [
            "-9223372036854775808",
            "9223372036854775807",
            "9223372036854775808",
        ] {
            let req = request(Command::Query {
                sql: "SELECT ?".into(),
                params: vec![SqlWireValue::Integer(raw.into())],
            });
            assert!(parity(&serde_json::to_vec(&req).unwrap()));
            let a: Request = serde_json::from_slice(&serde_json::to_vec(&req).unwrap()).unwrap();
            let b: Request =
                simd_json::serde::from_slice(&mut serde_json::to_vec(&req).unwrap()).unwrap();
            let gateway = gateway();
            assert_eq!(
                serde_json::to_value(gateway.handle(a)).unwrap(),
                serde_json::to_value(gateway.handle(b)).unwrap()
            );
        }
        let raw = serde_json::to_string(&request(Command::Query {
            sql: "SELECT ?".into(),
            params: vec![SqlWireValue::Integer("9223372036854775807".into())],
        }))
        .unwrap();
        assert!(!parity(
            raw.replace("\"9223372036854775807\"", "9223372036854775807")
                .as_bytes()
        ));
    }
    #[test]
    fn unchanged_size_gate_precedes_either_parser() {
        fn bounded(bytes: &[u8], simd: bool) -> bool {
            bytes.len() <= LIMIT
                && if simd {
                    simd_json::serde::from_slice::<Request>(&mut bytes.to_vec()).is_ok()
                } else {
                    serde_json::from_slice::<Request>(bytes).is_ok()
                }
        }
        let bytes = serde_json::to_vec(&request(Command::Batch {
            operations: vec![insert("x".repeat(LIMIT))],
        }))
        .unwrap();
        assert!(bytes.len() > LIMIT);
        assert!(!bounded(&bytes, false));
        assert!(!bounded(&bytes, true));
    }
    #[test]
    fn unchanged_semantic_schema_validation_follows_both_decoders() {
        for (min, max) in [(0, 150), (150, 0)] {
            let mut m = module(1);
            m.relations[0].checks = vec![Check::int_range("age", min, max)];
            let bytes = serde_json::to_vec(&m).unwrap();
            let a: Module = serde_json::from_slice(&bytes).unwrap();
            let b: Module = simd_json::serde::from_slice(&mut bytes.clone()).unwrap();
            let a = SchemaManifest {
                format: 2,
                modules: vec![a],
            }
            .validate()
            .is_ok();
            let b = SchemaManifest {
                format: 2,
                modules: vec![b],
            }
            .validate()
            .is_ok();
            assert_eq!(a, b);
            assert_eq!(a, min <= max);
        }
    }
    #[test]
    fn duplicate_named_map_keys_keep_existing_semantics() {
        let raw = serde_json::to_string(&request(Command::Batch {
            operations: vec![insert("first".into())],
        }))
        .unwrap();
        let duplicate = raw.replacen(
            "\"name\":{\"Text\":\"first\"}",
            "\"name\":{\"Text\":\"ignored\"},\"name\":{\"Text\":\"first\"}",
            1,
        );
        assert_ne!(raw, duplicate);
        assert!(parity(duplicate.as_bytes()));
    }
}
