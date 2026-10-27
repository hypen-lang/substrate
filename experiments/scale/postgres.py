#!/usr/bin/env python3
"""Disposable PostgreSQL scale/control benchmark; requires PostgreSQL's CLI tools.

All results are incremental JSONL. Never connects to an existing cluster. The
server-generated fixture loader is explicitly different from Rust client inserts.
pgbench uses persistent prepared sessions, full transaction latency logs and no
automatic retries. OS caches are not flushed. See --help for resource bounds.
"""
import argparse
import ctypes
import hashlib
import json
import math
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time


TABLES = ['core."Person"', 'fitness."Workout"', 'fitness."Performed"',
          'crm."Deal"', 'crm."CustomerDeal"']


def command(args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{args[0]} exited {result.returncode}: {result.stderr[-4000:]} {result.stdout[-4000:]}")
    return result


class Connection:
    """Minimal synchronous libpq wrapper; avoids a Python package dependency."""
    def __init__(self, dsn):
        directory = command(["pg_config", "--libdir"]).stdout.strip()
        candidates = list(Path(directory).glob("libpq.dylib")) + list(Path(directory).glob("libpq.so"))
        if not candidates:
            raise RuntimeError(f"libpq not found in {directory}")
        self.lib = ctypes.CDLL(str(candidates[0]))
        pointer = ctypes.c_void_p
        for name, restype, args in [
            ("PQconnectdb", pointer, [ctypes.c_char_p]),
            ("PQstatus", ctypes.c_int, [pointer]),
            ("PQerrorMessage", ctypes.c_char_p, [pointer]),
            ("PQexec", pointer, [pointer, ctypes.c_char_p]),
            ("PQresultStatus", ctypes.c_int, [pointer]),
            ("PQresultErrorMessage", ctypes.c_char_p, [pointer]),
            ("PQntuples", ctypes.c_int, [pointer]),
            ("PQnfields", ctypes.c_int, [pointer]),
            ("PQgetvalue", ctypes.c_char_p, [pointer, ctypes.c_int, ctypes.c_int]),
            ("PQgetisnull", ctypes.c_int, [pointer, ctypes.c_int, ctypes.c_int]),
            ("PQclear", None, [pointer]), ("PQfinish", None, [pointer]),
        ]:
            fn = getattr(self.lib, name)
            fn.restype, fn.argtypes = restype, args
        self.conn = self.lib.PQconnectdb(dsn.encode())
        if not self.conn or self.lib.PQstatus(self.conn) != 0:
            error = self.lib.PQerrorMessage(self.conn).decode() if self.conn else "allocation failed"
            self.close()
            raise RuntimeError(error)

    def query(self, sql):
        result = self.lib.PQexec(self.conn, sql.encode())
        if not result:
            raise RuntimeError(self.lib.PQerrorMessage(self.conn).decode())
        try:
            if self.lib.PQresultStatus(result) not in (1, 2):
                raise RuntimeError(self.lib.PQresultErrorMessage(result).decode())
            return [[None if self.lib.PQgetisnull(result, r, c) else
                     self.lib.PQgetvalue(result, r, c).decode()
                     for c in range(self.lib.PQnfields(result))]
                    for r in range(self.lib.PQntuples(result))]
        finally:
            self.lib.PQclear(result)

    def scalar(self, sql):
        return self.query(sql)[0][0]

    def close(self):
        if getattr(self, "conn", None):
            self.lib.PQfinish(self.conn)
            self.conn = None


def size(path):
    # Apparent bytes, not sparse allocated blocks; no symlinks followed.
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file() and not p.is_symlink():
                total += p.stat().st_size
        except FileNotFoundError:
            # PostgreSQL can recycle a temporary/statistics file while sampling.
            continue
    return total


def quantiles(values):
    if not values:
        return {"samples": 0, "p50_us": None, "p95_us": None, "p99_us": None}
    values.sort()
    return {"samples": len(values), "p50_us": values[math.ceil(len(values) * .50) - 1],
            "p95_us": values[math.ceil(len(values) * .95) - 1],
            "p99_us": values[math.ceil(len(values) * .99) - 1],
            "max_us": values[-1], "mean_us": sum(values) / len(values)}


DDL = '''
CREATE SCHEMA core; CREATE SCHEMA fitness; CREATE SCHEMA crm;
CREATE FUNCTION public.bench_uuid(f bigint,n bigint) RETURNS uuid LANGUAGE SQL IMMUTABLE STRICT
AS $$ SELECT (lpad(to_hex(f),16,'0') || lpad(to_hex(n),16,'0'))::uuid $$;
CREATE TABLE core."Person"(id uuid PRIMARY KEY,name text NOT NULL);
CREATE TABLE fitness."Workout"(id uuid PRIMARY KEY,"durationMinutes" bigint NOT NULL);
CREATE TABLE fitness."Performed"(id uuid PRIMARY KEY,athlete uuid NOT NULL REFERENCES core."Person",workout uuid NOT NULL REFERENCES fitness."Workout");
CREATE TABLE crm."Deal"(id uuid PRIMARY KEY,amount bigint NOT NULL);
CREATE TABLE crm."CustomerDeal"(id uuid PRIMARY KEY,customer uuid NOT NULL REFERENCES core."Person",deal uuid NOT NULL REFERENCES crm."Deal");
CREATE INDEX person_name ON core."Person"(name);
CREATE INDEX workout_duration ON fitness."Workout"("durationMinutes");
CREATE INDEX performed_athlete ON fitness."Performed"(athlete);
CREATE INDEX performed_workout ON fitness."Performed"(workout);
CREATE INDEX deal_amount ON crm."Deal"(amount);
CREATE INDEX customer_person ON crm."CustomerDeal"(customer);
CREATE INDEX customer_deal ON crm."CustomerDeal"(deal);
'''


class Benchmark:
    def __init__(self, args, root):
        self.args, self.root = args, root
        self.data = root / "data"
        self.dsn = f"host={root} dbname=postgres connect_timeout=5"
        self.conn = None
        self.output = open(args.output, "x", buffering=1) if args.output else sys.stdout
        self.loaded = 0

    def emit(self, phase, **values):
        record = {"backend": "postgres", "phase": phase, "rows": self.args.rows,
                  "time_unix": time.time(), **values}
        self.output.write(json.dumps(record, separators=(",", ":")) + "\n")
        self.output.flush()

    def guard(self):
        free = shutil.disk_usage(self.root).free
        if free < self.args.min_free_gib * 1024**3:
            raise RuntimeError(f"disk guard: {free} bytes free below {self.args.min_free_gib} GiB")

    def start(self):
        self.guard()
        options = ["-c", "listen_addresses=", "-c", f"unix_socket_directories={self.root}",
                   "-c", "unix_socket_permissions=0700", "-c", "shared_buffers=128MB",
                   "-c", f"max_connections={max(self.args.clients) + 24}",
                   "-c", "fsync=on", "-c", "synchronous_commit=on",
                   "-c", "full_page_writes=on", "-c", "work_mem=4MB",
                   "-c", f"wal_sync_method={self.args.wal_sync_method}",
                   "-c", f"default_transaction_isolation={self.args.isolation}"]
        command(["pg_ctl", "-D", str(self.data), "-l", str(self.root / "server.log"),
                 "-o", shlex.join(options), "-w", "start"])
        self.conn = Connection(self.dsn)
        self.conn.query("SET statement_timeout='120s'")
        self.emit("server_started", postmaster_pid=int((self.data / "postmaster.pid").read_text().splitlines()[0]))

    def stop(self, mode="fast"):
        if self.conn:
            self.conn.close()
            self.conn = None
        command(["pg_ctl", "-D", str(self.data), "-m", mode, "-w", "stop"])

    def wal(self):
        return self.conn.scalar("SELECT pg_current_wal_insert_lsn()::text")

    def wal_delta(self, before):
        return int(self.conn.scalar(f"SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'{before}')::bigint"))

    def footprint(self):
        table_bytes = int(self.conn.scalar("SELECT sum(pg_total_relation_size(t)) FROM unnest(ARRAY[" +
            ",".join("'" + t + "'::regclass" for t in TABLES) + "]) t"))
        db_bytes = int(self.conn.scalar("SELECT pg_database_size(current_database())"))
        return {"table_and_index_bytes": table_bytes, "table_and_index_bytes_per_record": table_bytes / self.args.rows,
                "database_bytes": db_bytes, "cluster_apparent_bytes": size(self.data),
                "cluster_bytes_note": "live apparent-file-size estimate; rotating/disappeared files skipped, not an atomic allocation snapshot"}

    def load(self):
        full = self.args.rows // 9
        people = full + 1
        self.emit("load_start", batch_people=self.args.batch_people)
        start, before = time.perf_counter(), self.wal()
        self.conn.query("BEGIN;" + DDL + "COMMIT;")
        for begin in range(0, people, self.args.batch_people):
            self.guard()
            end, child_end = min(begin + self.args.batch_people, people), min(begin + self.args.batch_people, full)
            persons = f"SELECT i FROM generate_series({begin},{end - 1}) i"
            children = f"SELECT i,j FROM generate_series({begin},{child_end - 1}) i CROSS JOIN generate_series(0,1) j"
            self.conn.query(f'''BEGIN;
INSERT INTO core."Person" SELECT bench_uuid(0,i+1),'Person'||lpad(i::text,greatest(6,length(i::text)),'0') FROM ({persons}) p;
INSERT INTO fitness."Workout" SELECT bench_uuid(1,i*2+j+1),(i*7+j*19)%120 FROM ({children}) x;
INSERT INTO fitness."Performed" SELECT bench_uuid(2,i*2+j+1),bench_uuid(0,i+1),bench_uuid(1,i*2+j+1) FROM ({children}) x;
INSERT INTO crm."Deal" SELECT bench_uuid(3,i*2+j+1),(i+1)*100+j*17 FROM ({children}) x;
INSERT INTO crm."CustomerDeal" SELECT bench_uuid(4,i*2+j+1),bench_uuid(0,i+1),bench_uuid(3,i*2+j+1) FROM ({children}) x;
COMMIT;''')
            self.loaded = min(end * 9, self.args.rows)
            if end == people or begin % 10000 == 0:
                self.emit("load_progress", loaded_rows=self.loaded, elapsed_seconds=time.perf_counter() - start)
        elapsed = time.perf_counter() - start
        counts = [int(self.conn.scalar(f"SELECT count(*) FROM {t}")) for t in TABLES]
        assert sum(counts) == self.args.rows, counts
        probes = []
        import uuid
        for i in sorted({0, full // 2, full}):
            pid = str(uuid.UUID(int=i + 1))
            result = self.conn.query(f'SELECT id,name FROM core."Person" WHERE id=\'{pid}\'')
            assert result == [[pid, f"Person{i:06}"]], result
            probes.append(result[0])
        for i in sorted({0, full - 1}):
            for j in [0, 1]:
                wid = str(uuid.UUID(int=(1 << 64) + i * 2 + j + 1))
                result = self.conn.query(f'SELECT id,"durationMinutes" FROM fitness."Workout" WHERE id=\'{wid}\'')
                assert result == [[wid, str((i * 7 + j * 19) % 120)]], result
                probes.append(result[0])
        load_wal_bytes = self.wal_delta(before)
        self.emit("analyze_start")
        analyze = time.perf_counter()
        self.conn.query("ANALYZE")
        self.emit("load", elapsed_seconds=elapsed, records_per_second=self.args.rows / elapsed,
                  wal_bytes=load_wal_bytes, counts=dict(zip(TABLES, counts)), fixture_probes=probes,
                  analyze_seconds=time.perf_counter() - analyze,
                  load_method="server generate_series, batched atomic durable transactions; indexes/FKs active",
                  **self.footprint())

    def query_benchmarks(self):
        if not self.args.queries:
            return
        held = getattr(self.args, "held_read", False)
        if held:
            self.conn.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        try:
            self._query_benchmarks()
        finally:
            if held:
                original_error = sys.exc_info()[0] is not None
                try:
                    self.conn.query("ROLLBACK")
                except RuntimeError as error:
                    if not original_error:
                        raise
                    self.emit("query_rollback_error", error=str(error))

    def _query_benchmarks(self):
        held = getattr(self.args, "held_read", False)
        queries = json.loads(Path(self.args.queries).read_text())
        for index, item in enumerate(queries):
            self.guard()
            name, sql = item["name"], item["sql"]
            self.emit("query_start", name=name, held_snapshot=held)
            query_start = time.perf_counter()
            times = []
            try:
                self.conn.query(f"PREPARE q{index} AS {sql}")
                expected = self.conn.query(f"EXECUTE q{index}")
                for _ in range(self.args.query_samples):
                    start = time.perf_counter()
                    result = self.conn.query(f"EXECUTE q{index}")
                    times.append((time.perf_counter() - start) * 1e6)
                    assert result == expected, name
            except RuntimeError as error:
                # Default queries run in implicit transactions; the optional
                # held transaction is reset below after a statement failure.
                # A broken connection must still fail the run; never silently
                # reconnect or skip failures.
                healthy = False
                try:
                    if held:
                        # A failed statement aborts the held transaction. The
                        # fixture remains immutable throughout this query phase,
                        # so a fresh read transaction sees the same logical data.
                        self.conn.query("ROLLBACK")
                        self.conn.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    healthy = self.conn.scalar("SELECT 1") == "1"
                except RuntimeError:
                    pass
                self.emit("query_error", name=name, error=str(error),
                          elapsed_seconds=time.perf_counter() - query_start,
                          completed_samples=len(times), connection_healthy=healthy,
                          held_snapshot=held, held_snapshot_restarted=held and healthy)
                if not healthy:
                    raise
                continue
            digest = hashlib.sha256(json.dumps(expected, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
            self.emit("query", name=name, sha256=digest, result_rows=len(expected),
                      timing="libpq request + local IPC/server + decoded Python string rows; warm prepared",
                      latency=quantiles(times), samples_us=times, held_snapshot=held)

    def prepare_script(self, mode):
        path = self.root / f"{mode}.sql"
        if mode == "read":
            # One autocommit SELECT is one transaction, just like an embedded read.
            sql = f'\\set target random(1,{self.args.rows // 9 + 1})\nSELECT id,name FROM core."Person" WHERE id=bench_uuid(0,:target);\n'
        else:
            sql = f'\\set target random(1,{2 * (self.args.rows // 9)})\nBEGIN;\nUPDATE fitness."Workout" SET "durationMinutes"="durationMinutes"+1 WHERE id=bench_uuid(1,:target);\nCOMMIT;\n'
        path.write_text(sql)
        return path

    def run_pool(self, mode, clients, label):
        prefix = self.root / f"{label}-{mode}"
        output = open(str(prefix) + ".out", "w")
        proc = subprocess.Popen(["pgbench", "-n", "-M", "prepared", "--max-tries=1",
            "--random-seed=1729", "-c", str(clients), "-j", str(min(clients, 8)),
            "-T", str(self.args.seconds), "-l", "--log-prefix", str(prefix),
            "-f", str(self.prepare_script(mode)), "-d", self.dsn],
            stdout=output, stderr=subprocess.STDOUT, text=True)
        output.close()
        return proc, prefix

    def pool_result(self, proc, prefix):
        code = proc.wait(timeout=self.args.seconds + 60)
        output = Path(str(prefix) + ".out").read_text()
        if code:
            raise RuntimeError(f"pgbench exited {code}: {output[-4000:]}")
        latency, failed_logs = [], 0
        for path in self.root.glob(prefix.name + ".*"):
            if path.suffix == ".out":
                continue
            for line in path.read_text().splitlines():
                fields = line.split()
                if len(fields) < 3:
                    raise RuntimeError(f"invalid pgbench latency log: {line}")
                try:
                    latency.append(float(fields[2]))
                except ValueError:
                    failed_logs += 1
            path.unlink()
        def find(pattern, default=None):
            match = re.search(pattern, output)
            return float(match.group(1)) if match else default
        successes = int(find(r"number of transactions actually processed:\s*(\d+)", 0))
        failures = int(find(r"number of failed transactions:\s*(\d+)", failed_logs))
        tps = find(r"tps = ([\d.]+)")
        if successes != len(latency):
            raise RuntimeError(f"latency log count {len(latency)} != completed {successes}")
        return {"successes": successes, "failures": failures, "tps": tps,
                "latency": quantiles(latency), "stdout": output}

    def workload(self, mode, clients):
        self.guard()
        before_sum = int(self.conn.scalar('SELECT sum("durationMinutes") FROM fitness."Workout"'))
        self.emit("workload_start", mode=mode, clients=clients)
        before, start = self.wal(), time.perf_counter()
        label = f"{mode}-{clients}"
        pools = []
        try:
            if mode == "mixed":
                pools.append(("read", *self.run_pool("read", 4, label)))
                pools.append(("write", *self.run_pool("write", clients, label)))
            else:
                pools.append((mode, *self.run_pool(mode, clients, label)))
            for _, proc, _ in pools:
                proc.wait(timeout=self.args.seconds + 60)
            elapsed = time.perf_counter() - start
            results = {kind: self.pool_result(proc, prefix) for kind, proc, prefix in pools}
        finally:
            for _, proc, _ in pools:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
        wal = self.wal_delta(before)
        after_sum = int(self.conn.scalar('SELECT sum("durationMinutes") FROM fitness."Workout"'))
        writes = results.get("write", {}).get("successes", 0)
        assert after_sum - before_sum == writes, (before_sum, after_sum, writes)
        reads = results.get("read", {}).get("successes", 0)
        people = self.args.rows // 9 + 1
        mean_name_bytes = 12 + sum(max(0, people - 10**digits)
                                   for digits in range(6, len(str(people)))) / people
        self.emit("workload", mode=mode, clients=clients,
                  reader_clients=4 if mode == "mixed" else clients if mode == "read" else 0,
                  writer_clients=clients if mode in ("mixed", "write") else 0,
                  measured_pool_tps=sum(result["tps"] for result in results.values()),
                  elapsed_seconds_including_connection_setup=elapsed, pools=results,
                  common_wall_successful_tps=(reads + writes) / elapsed,
                  common_wall_read_tps=reads / elapsed, common_wall_write_tps=writes / elapsed,
                  exact_committed_increment_count_verified=True,
                  wal_bytes=wal, wal_bytes_per_wall_second=wal / elapsed,
                  logical_write_bytes_per_second=results.get("write", {}).get("tps", 0) * 24,
                  estimated_logical_read_bytes_per_second=results.get("read", {}).get("tps", 0) * (16 + mean_name_bytes),
                  logical_bytes_note="16-byte ID + 8-byte integer per write; read estimate uses16-byte ID + dataset-average name bytes, not sampled actual returned names; excludes protocol/index/WAL overhead",
                  mixed_note="separate pools launched consecutively; durations start after each pool connects, not a synchronized all-client barrier")

    def recovery(self):
        expected = self.conn.scalar('SELECT "durationMinutes" FROM fitness."Workout" WHERE id=bench_uuid(1,1)')
        self.conn.query("CHECKPOINT")
        self.stop("fast")
        self.emit("clean_restart_start")
        start = time.perf_counter()
        self.start()
        actual = self.conn.scalar('SELECT "durationMinutes" FROM fitness."Workout" WHERE id=bench_uuid(1,1)')
        assert actual == expected
        self.emit("clean_restart", elapsed_ms=(time.perf_counter() - start) * 1000,
                  timing="pg_ctl start polling + connect + verified point read; warm OS cache")
        self.conn.query('UPDATE fitness."Workout" SET "durationMinutes"="durationMinutes"+1 WHERE id=bench_uuid(1,1)')
        expected = str(int(expected) + 1)
        self.stop("immediate")
        self.emit("crash_restart_start")
        start = time.perf_counter()
        self.start()
        actual = self.conn.scalar('SELECT "durationMinutes" FROM fitness."Workout" WHERE id=bench_uuid(1,1)')
        assert actual == expected
        self.emit("crash_restart", elapsed_ms=(time.perf_counter() - start) * 1000,
                  committed_sentinel_preserved=True,
                  timing="pg_ctl immediate-stop/server crash + restart polling + verified point read; not power-loss test",
                  **self.footprint())

    def run(self):
        try:
            self.guard()
            command(["initdb", "-D", str(self.data), "--no-locale", "--encoding=UTF8",
                     "--auth-local=trust", "--auth-host=reject"])
            self.start()
            self.emit("metadata", version=self.conn.scalar("SELECT version()"),
                      cluster=str(self.data), postmaster_pid=int((self.data / "postmaster.pid").read_text().splitlines()[0]),
                      settings=self.conn.query("SELECT name,setting,unit FROM pg_settings WHERE name IN ('shared_buffers','work_mem','fsync','synchronous_commit','full_page_writes','max_connections','wal_sync_method','default_transaction_isolation') ORDER BY name"),
                      clients=self.args.clients, seconds=self.args.seconds,
                      note="fresh isolated cluster; UUID keys; read committed transactions; local IPC included; PostgreSQL has no Substrate visibility checks")
            self.load()
            self.query_benchmarks()
            if not self.args.skip_workloads:
                for mode in self.args.modes:
                    for clients in self.args.clients:
                        self.workload(mode, clients)
            self.recovery()
        except BaseException as error:
            self.emit("error", error=str(error), loaded_rows=self.loaded)
            raise
        finally:
            try:
                self.stop("fast")
            except Exception:
                # Also handles a server which started but whose client failed to
                # connect. Never leave a detached benchmark server behind.
                subprocess.run(["pg_ctl", "-D", str(self.data), "-m", "immediate", "-w", "stop"], capture_output=True)
            if self.output is not sys.stdout:
                self.output.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--seconds", type=int, default=3)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32, 64, 128])
    parser.add_argument("--modes", nargs="+", choices=["read", "write", "mixed"], default=["read", "write", "mixed"])
    parser.add_argument("--batch-people", type=int, default=1000)
    parser.add_argument("--output", help="new JSONL file; stdout when omitted")
    parser.add_argument("--work-dir", default="/tmp", help="parent for a fresh disposable cluster")
    parser.add_argument("--min-free-gib", type=float, default=3)
    parser.add_argument("--wal-sync-method", choices=["fsync_writethrough", "fsync", "fdatasync"],
                        default="fsync_writethrough", help="macOS full hardware flush by default; explicitly override on other platforms")
    parser.add_argument("--queries", help="JSON array of {name,sql} query workloads")
    parser.add_argument("--isolation", choices=["read committed", "serializable"], default="read committed")
    parser.add_argument("--held-read", action="store_true",
                        help="hold one repeatable-read read-only transaction across SQL query measurements")
    parser.add_argument("--query-samples", type=int, default=7)
    parser.add_argument("--skip-workloads", action="store_true")
    parser.add_argument("--keep", action="store_true", help="retain this benchmark's cluster after shutdown")
    args = parser.parse_args()
    if args.rows < 10 or args.rows % 9 != 1:
        parser.error("rows must be >=10 and equal 1 modulo 9 (100,1k,10k,100k,1m,10m all qualify)")
    if args.seconds < 1 or args.batch_people < 1 or args.query_samples < 1 or any(c < 1 or c > 256 for c in args.clients):
        parser.error("positive seconds/batch/samples and clients in 1..256 required")
    root = Path(tempfile.mkdtemp(prefix="substrate-scale-pg-", dir=args.work_dir))
    def interrupted(signum, _frame):
        raise InterruptedError(f"benchmark interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        Benchmark(args, root).run()
    finally:
        if args.keep:
            print(f"Retained benchmark cluster: {root}", file=sys.stderr)
        else:
            shutil.rmtree(root)


if __name__ == "__main__":
    main()
