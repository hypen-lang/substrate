#!/usr/bin/env python3
"""Run the existing prepared SQL ladder against prebuilt frozen variants."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("prepared", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--rows", type=int, default=2000)
parser.add_argument("--repeats", type=int, default=3)
args = parser.parse_args()
if args.output.exists():
    parser.error("output already exists; choose a fresh comparison destination")
args.output.parent.mkdir(parents=True, exist_ok=True)
assert args.repeats >= 3
env = dict(os.environ, SUBSTRATE_BENCH_ROWS=str(args.rows))
# The collection experiment never targets an external database.
env.pop("SUBSTRATE_BENCH_PG_DSN", None)
runs = []
digests = {}
for repeat in range(args.repeats):
    order = ["im", "imbl"] if repeat % 2 == 0 else ["imbl", "im"]
    for backend in order:
        binary = args.prepared / f"{backend}-sql-probe"
        result = subprocess.run([str(binary)], env=env, check=True, text=True,
                                stdout=subprocess.PIPE)
        data = json.loads(result.stdout)
        for item in data["measurements"]:
            key = item["name"]
            identity = (item["rows"], item["sha256"])
            assert digests.setdefault(key, identity) == identity, key
        runs.append({"backend": backend, "repeat": repeat, "data": data})
        args.output.write_text(json.dumps(runs, indent=2) + "\n")

summary = []
for name in digests:
    for mode in ["substrate_admin", "substrate_reader"]:
        samples = {}
        for backend in ["im", "imbl"]:
            samples[backend] = [item[mode]["median_us"] for run in runs
                                if run["backend"] == backend
                                for item in run["data"]["measurements"]
                                if item["name"] == name]
        medians = {key: statistics.median(values) for key, values in samples.items()}
        summary.append({"name": name, "mode": mode, "samples_us": samples,
                        "median_us": medians,
                        "im_over_imbl": medians["im"] / medians["imbl"]})
print(json.dumps({"rows": args.rows, "repeats": args.repeats,
                  "binary_sha256": {backend: hashlib.sha256(
                      (args.prepared / f"{backend}-sql-probe").read_bytes()).hexdigest()
                      for backend in ["im", "imbl"]},
                  "summary": summary}, indent=2))
