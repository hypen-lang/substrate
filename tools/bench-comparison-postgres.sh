#!/bin/sh
# Create an isolated, disposable local PostgreSQL cluster; never use an existing DB.
set -eu
for program in initdb pg_ctl psql cargo; do
  command -v "$program" >/dev/null || { echo "Missing $program" >&2; exit 1; }
done
bench_dir=$(mktemp -d /tmp/substrate-bench-pg.XXXXXX)
cleanup() {
  pg_ctl -D "$bench_dir/data" -m immediate -w stop >/dev/null 2>&1 || true
  rm -rf "$bench_dir"
}
trap cleanup EXIT HUP INT TERM
chmod 700 "$bench_dir"
initdb -D "$bench_dir/data" --no-locale --encoding=UTF8 --auth-local=trust --auth-host=reject > "$bench_dir/init.log"
# Private Unix socket only: no TCP listener and no existing server/config touched.
pg_ctl -D "$bench_dir/data" -l "$bench_dir/server.log" -o "-c listen_addresses='' -c unix_socket_directories='$bench_dir' -c unix_socket_permissions=0700" -w start >/dev/null
export LC_ALL=C
export SUBSTRATE_BENCH_PG_DSN="host=$bench_dir dbname=postgres"
cargo bench --offline --bench comparison
