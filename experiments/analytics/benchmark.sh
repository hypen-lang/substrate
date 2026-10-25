#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
# Dependencies/build are separate from timed process invocations.
cargo build --offline --release --manifest-path experiments/analytics/Cargo.toml --target-dir target
bridge_dir=$(mktemp -d "${TMPDIR:-/tmp}/substrate-analytics.XXXXXX")
trap 'rm -rf "$bridge_dir"' EXIT
for people in 2000 20000; do
  for repetition in 1 2 3; do
    target/release/substrate-analytics-experiment "$people" "$bridge_dir/source.json"
    experiments/analytics/.venv/bin/python experiments/analytics/run.py "$bridge_dir/source.json" "docs/benchmarks/analytics-${people}-${repetition}.json"
  done
done
