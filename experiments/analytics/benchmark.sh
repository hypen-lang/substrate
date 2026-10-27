#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
# Publish one complete set. Refuse an existing destination rather than mixing
# measurements from different binaries or leaving a partial run marked latest.
output=${1:-docs/benchmarks/latest/analytics}
if [[ -e "$output" ]]; then
  echo "Benchmark output already exists: $output; choose a fresh destination." >&2
  exit 1
fi
mkdir -p "$(dirname "$output")"
result_dir=$(mktemp -d "${output}.pending.XXXXXX")
bridge_dir=$(mktemp -d "${TMPDIR:-/tmp}/substrate-analytics.XXXXXX")
trap 'rm -rf "$bridge_dir" "$result_dir"' EXIT
# Dependencies/build are separate from timed process invocations.
cargo build --offline --release --manifest-path experiments/analytics/Cargo.toml --target-dir target
for people in 2000 20000; do
  for repetition in 1 2 3; do
    target/release/substrate-analytics-experiment "$people" "$bridge_dir/source.json"
    experiments/analytics/.venv/bin/python experiments/analytics/run.py "$bridge_dir/source.json" "$result_dir/analytics-${people}-${repetition}.json"
  done
done
# rename fails if another completed run won this destination; unlike mv it
# never nests our staged directory inside another run.
python3 -c 'import os,sys; os.rename(sys.argv[1], sys.argv[2])' "$result_dir" "$output"
