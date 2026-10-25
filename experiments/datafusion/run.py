"""Prepared query comparison over identical authorized Arrow fixture exports."""
import argparse
import hashlib
import importlib.util
import json
import os
import platform
import statistics
import sys
import time
from pathlib import Path

# DataFusion's global Tokio runtime is separate from its partition setting.
# Set the worker budget before importing the extension.
os.environ["TOKIO_WORKER_THREADS"] = "1"
start_import = time.perf_counter_ns()
import datafusion as df
import duckdb
import pyarrow as pa
IMPORT_MS = (time.perf_counter_ns() - start_import) / 1e6
ENGINE_VERSION = df.SessionContext().sql('SELECT version() AS version').to_pylist()[0]['version']

spec = importlib.util.spec_from_file_location("bridge", Path(__file__).resolve().parents[1] / "analytics/run.py")
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def canonical_scalar(cell):
    # Preserve null independently of text "None", integers exactly and row bags/order.
    return None if cell is None else str(cell)


def canonical_rows(rows):
    return [[canonical_scalar(cell) for cell in row] for row in rows]


def datafusion_context(tables):
    config = (df.SessionConfig().with_target_partitions(1)
              .with_repartition_joins(False).with_repartition_aggregations(False)
              .with_repartition_windows(False).with_repartition_sorts(False)
              .with_information_schema(False))
    runtime = df.RuntimeEnvBuilder().with_disk_manager_disabled().with_greedy_memory_pool(512 * 1024 * 1024)
    ctx = df.SessionContext(config, runtime)
    for namespace in sorted({name.split('.')[0] for name in tables}):
        ctx.sql(f'CREATE SCHEMA "{namespace}"').collect()
    for name, table in tables.items():
        namespace, relation = name.split('.')
        ctx.register_record_batches(f'"{namespace}"."{relation}"', [table.to_batches(max_chunksize=8192)])
    return ctx


def prepared_datafusion(ctx, sql):
    # Physical plans contain mutable execution state, including dynamic top-k filters.
    # Cache the logical plan, but rebuild physical execution state for every call.
    frame = ctx.create_dataframe_from_logical_plan(ctx.sql(sql).optimized_logical_plan())
    def run():
        plan = frame.execution_plan()
        # Ordered queries require one output partition when concatenating batches.
        if plan.partition_count != 1:
            raise ValueError(f"expected one output partition, got {plan.partition_count}")
        rows = []
        for wrapped in ctx.execute(plan, 0):
            batch = wrapped.to_pyarrow()
            columns = [column.to_pylist() for column in batch.columns]
            rows.extend(canonical_rows(zip(*columns, strict=True)))
        return rows
    return run


def prepared_duckdb(con, sql, name):
    con.execute(f'PREPARE {name} AS {sql}')
    return lambda: canonical_rows(con.execute(f'EXECUTE {name}').fetchall())


def run(path, order):
    raw, read_ms = bridge.clock(path.read_bytes)
    source, decode_ms = bridge.clock(lambda: json.loads(raw))
    tables, arrow_ms = bridge.clock(lambda: bridge.arrow_tables(source['tables']))
    bridge_ms = source['authorized_export_ms'] + source['serialize_columns_ms'] + read_ms + decode_ms + arrow_ms
    result = {
        'people': source['people'], 'occurrences': source['occurrences'],
        'environment': {'datafusion_python': df.__version__, 'datafusion_engine': ENGINE_VERSION,
                        'duckdb': duckdb.__version__, 'pyarrow': pa.__version__,
                        'python': sys.version.split()[0], 'platform': platform.platform(),
                        'target_partitions': 1, 'tokio_worker_threads': 1, 'duckdb_threads': 1},
        'source_authorized_export_ms': source['authorized_export_ms'], 'source_serialization_ms': source['serialize_columns_ms'],
        'source_revocation_checked': source['source_revocation_checked'],
        'bridge_read_ms': read_ms, 'bridge_decode_ms': decode_ms, 'arrow_conversion_ms': arrow_ms,
        'python_import_ms': IMPORT_MS, 'bridge_ms': bridge_ms, 'modes': [],
    }
    for mode in order:
        if mode == 'datafusion_prepared':
            engine, setup_ms = bridge.clock(lambda: datafusion_context(tables))
        else:
            engine, setup_ms = bridge.clock(lambda: bridge.connect(tables, mode == 'duckdb_indexed'))
        saved = {'name': mode, 'setup_ms': setup_ms, 'bridge_plus_setup_ms': bridge_ms + setup_ms, 'measurements': []}
        for index, expected in enumerate(source['measurements']):
            query, prepare_ms = bridge.clock(lambda: prepared_datafusion(engine, expected['sql']) if mode == 'datafusion_prepared'
                                              else prepared_duckdb(engine, expected['sql'], f'q_{index}'))
            actual = query()
            if actual != expected['expected']:
                raise AssertionError((mode, expected['name'], actual[:3], expected['expected'][:3]))
            digest = hashlib.sha256(json.dumps(actual, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
            if digest != expected['sha256']:
                raise AssertionError((mode, expected['name'], 'digest mismatch'))
            timing = bridge.measure(query, source['samples'], source['iterations'])
            if query() != expected['expected']:
                raise AssertionError((mode, expected['name'], 'reused plan changed results'))
            saved['measurements'].append({'name': expected['name'], 'rows': len(actual), 'sha256': digest,
                                          'prepare_ms': prepare_ms, 'timing': timing,
                                          'substrate_reader_reference': expected['substrate_reader'],
                                          'sqlite_reference': expected['sqlite']})
        if mode != 'datafusion_prepared':
            engine.close()
        result['modes'].append(saved)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--reverse', action='store_true')
    args = parser.parse_args()
    modes = ['datafusion_prepared', 'duckdb_arrow', 'duckdb_indexed']
    if args.reverse:
        modes.reverse()
    args.output.write_text(json.dumps(run(args.input, modes), indent=2) + '\n')
