"""Experimental trusted-host columnar bridge; not an application authorization API."""
import argparse
import hashlib
import json
import platform
import statistics
import sys
import time
from pathlib import Path

start_import = time.perf_counter_ns()
import duckdb
import pyarrow as pa
IMPORT_MS = (time.perf_counter_ns() - start_import) / 1e6


def clock(fn):
    start = time.perf_counter_ns()
    result = fn()
    return result, (time.perf_counter_ns() - start) / 1e6


def canonical(rows):
    return [[str(cell) for cell in row] for row in rows]


def measure(fn, samples, iterations):
    fn()
    timings = []
    for _ in range(samples):
        start = time.perf_counter_ns()
        for _ in range(iterations):
            fn()
        timings.append((time.perf_counter_ns() - start) / 1000 / iterations)
    return {"median_us": statistics.median(timings), "min_us": min(timings), "max_us": max(timings), "samples_us": timings}


def arrow_tables(tables):
    result = {}
    for table in tables:
        names = [f["name"] for f in table["fields"]]
        arrays = [pa.array(values, type=pa.int64() if f["type"] == "int64" else pa.string())
                  for f, values in zip(table["fields"], table["columns"], strict=True)]
        result[table["name"]] = pa.Table.from_arrays(arrays, names=names)
    return result


def connect(arrow, materialize):
    # Single-thread execution matches the embedded engine fixture's CPU budget.
    con = duckdb.connect(config={"threads": "1", "memory_limit": "512MB", "enable_external_access": "false"})
    for namespace in {name.split(".")[0] for name in arrow}:
        con.execute(f'CREATE SCHEMA "{namespace}"')
    for index, (name, table) in enumerate(arrow.items()):
        namespace, relation = name.split(".")
        registered = f"source_{index}"
        con.register(registered, table)
        target = f'"{namespace}"."{relation}"'
        con.execute(f'CREATE {"TABLE" if materialize else "VIEW"} {target} AS SELECT * FROM "{registered}"')
        if materialize:
            con.unregister(registered)
            # Match the ordinary benchmark's primary and per-role indexes.
            con.execute(f'CREATE UNIQUE INDEX "id_{index}" ON {target}(id)')
            for role in table.schema.names[1:]:
                con.execute(f'CREATE INDEX "role_{index}_{role}" ON {target}("{role}")')
    con.execute("ANALYZE")
    return con


def run(path):
    raw, read_ms = clock(lambda: path.read_bytes())
    report, json_ms = clock(lambda: json.loads(raw))
    arrow, arrow_ms = clock(lambda: arrow_tables(report["tables"]))
    result = {
        "people": report["people"], "occurrences": report["occurrences"],
        "environment": {"sqlite": report["sqlite_version"], "python": sys.version.split()[0], "duckdb": duckdb.__version__, "pyarrow": pa.__version__, "platform": platform.platform(), "machine": platform.machine(), "threads": 1},
        "samples": report["samples"], "iterations": report["iterations"],
        "source_authorized_export_ms": report["authorized_export_ms"],
        "source_serialization_ms": report["serialize_columns_ms"],
        "source_fixture_ms_excluded": report["fixture_ms"],
        "source_sql_setup_ms": report["substrate_sql_setup_ms"],
        "python_import_ms": IMPORT_MS, "bridge_read_ms": read_ms, "bridge_json_decode_ms": json_ms,
        "arrow_conversion_ms": arrow_ms, "arrow_logical_bytes": sum(t.nbytes for t in arrow.values()),
        "bridge_file_bytes": len(raw), "serialized_columns_bytes": report["serialized_columns_bytes"],
        "source_revocation_checked": report["source_revocation_checked"], "modes": [],
    }
    bridge_ms = report["authorized_export_ms"] + report["serialize_columns_ms"] + read_ms + json_ms + arrow_ms
    for materialize in [False, True]:
        con, setup_ms = clock(lambda: connect(arrow, materialize))
        mode = {"name": "materialized_indexed" if materialize else "arrow_direct", "setup_ms": setup_ms,
                "bridge_plus_setup_ms": bridge_ms + setup_ms, "measurements": []}
        for i, expected in enumerate(report["measurements"]):
            name = f"q_{i}"
            con.execute(f'PREPARE {name} AS {expected["sql"]}')
            def query():
                return canonical(con.execute(f"EXECUTE {name}").fetchall())
            actual = query()
            assert actual == expected["expected"], expected["name"]
            digest = hashlib.sha256(json.dumps(actual, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
            assert digest == expected["sha256"], expected["name"]
            timing = measure(query, report["samples"], report["iterations"])
            saved_us = expected["substrate_reader"]["median_us"] - timing["median_us"]
            amortization = None if saved_us <= 0 else (bridge_ms + setup_ms) * 1000 / saved_us
            sqlite_saved_us = expected["sqlite"]["median_us"] - timing["median_us"]
            sqlite_amortization = None if sqlite_saved_us <= 0 else (bridge_ms + setup_ms) * 1000 / sqlite_saved_us
            mode["measurements"].append({"name": expected["name"], "rows": len(actual), "sha256": digest,
                "sqlite": expected["sqlite"], "substrate_reader": expected["substrate_reader"], "duckdb": timing,
                "bridge_break_even_repetitions": amortization,
                "bridge_break_even_repetitions_vs_sqlite": sqlite_amortization,
                "first_query_ms_including_bridge_setup": bridge_ms + setup_ms + timing["median_us"] / 1000})
        con.close()
        result["modes"].append(mode)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(run(args.input), indent=2) + "\n")
