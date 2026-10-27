#!/usr/bin/env python3
"""Run prebuilt copies serially and check deterministic state parity."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("prepared", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--repeats", type=int, default=3)
args = parser.parse_args()
if args.output.exists():
    parser.error("output already exists; choose a fresh comparison destination")
args.output.parent.mkdir(parents=True, exist_ok=True)
assert args.repeats >= 3
records = []
for repeat in range(args.repeats):
    order = ("im", "imbl") if repeat % 2 == 0 else ("imbl", "im")
    for backend in order:
        process = subprocess.run([str(args.prepared / f"{backend}-probe"), backend],
                                 check=True, text=True, capture_output=True)
        for line in process.stdout.splitlines():
            record = json.loads(line)
            assert record["backend"] == backend
            record["repeat"] = repeat + 1
            records.append(record)

digests = {}
for record in records:
    if record["digest"] is not None:
        key = (record["rows"], record["scenario"])
        assert digests.setdefault(key, record["digest"]) == record["digest"], key
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text("".join(json.dumps(r, sort_keys=True)+"\n" for r in records))
summary = []
for size, scenario in sorted({(r["rows"], r["scenario"]) for r in records}):
    samples = {b:[r["ns_per_operation"] for r in records
                  if (r["rows"], r["scenario"], r["backend"]) == (size, scenario, b)]
               for b in ("im", "imbl")}
    medians = {b:statistics.median(v) for b,v in samples.items()}
    summary.append({"rows":size, "scenario":scenario, "im_ns":medians["im"],
                    "imbl_ns":medians["imbl"], "im_over_imbl":medians["im"]/medians["imbl"],
                    "samples":samples})
print(json.dumps({"binaries":{b:hashlib.sha256((args.prepared/f"{b}-probe").read_bytes()).hexdigest()
                              for b in ("im","imbl")}, "summary":summary}, indent=2))
