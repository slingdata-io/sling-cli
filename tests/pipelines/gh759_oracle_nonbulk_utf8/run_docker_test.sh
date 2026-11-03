#!/bin/bash
# Self-contained repro for GH #759: Oracle non-bulk insert corrupts non-ASCII
# text when the target database charset is not UTF-8.
#
# Starts Oracle Free 23ai (CDB AL32UTF8), adds a PDB with WE8ISO8859P1,
# runs the pipeline, then removes the container.
#
# Usage: bash run_docker_test.sh
# Requires: docker (compose v2), a built sling binary at cmd/sling/sling.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLING_CLI_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CMD_DIR="$SLING_CLI_DIR/cmd/sling"
SLING_BIN="$CMD_DIR/sling"
COMPOSE="docker compose -f $SCRIPT_DIR/docker-compose.yaml"

if [ ! -x "$SLING_BIN" ]; then
  echo "Building sling binary..."
  (cd "$CMD_DIR" && go build .)
fi

# free host port
export GH759_ORA_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')

cleanup() {
  echo "=== Tearing down Oracle container ==="
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== Starting Oracle Free container on port $GH759_ORA_PORT ==="
$COMPOSE up -d
CID=$($COMPOSE ps -q oracle)

echo "=== Waiting for Oracle to be ready ==="
for i in $(seq 1 120); do
  if docker exec "$CID" bash -c 'echo "select 42 from dual;" | sqlplus -s -l sling/sling_gh759@//localhost/FREEPDB1' 2>/dev/null | grep -q 42; then
    echo "Oracle ready."
    break
  fi
  if [ "$i" = "120" ]; then
    echo "ERROR: Oracle did not become ready in time"
    docker logs "$CID" | tail -60
    exit 1
  fi
  sleep 3
done

echo "=== Creating WE8ISO8859P1 PDB ==="
docker cp "$SCRIPT_DIR/initdb.sql" "$CID:/tmp/gh759_initdb.sql"
docker exec "$CID" bash -c 'sqlplus -s / as sysdba @/tmp/gh759_initdb.sql' | grep -E "ORA-|SP2-" && exit 1
docker exec "$CID" bash -c "echo \"select value from nls_database_parameters where parameter = 'NLS_CHARACTERSET';\" | sqlplus -s -l sling/sling_gh759@//localhost/GH759WE8" | grep -q WE8ISO8859P1
echo "GH759WE8 charset: WE8ISO8859P1"

export ORACLE_GH759="oracle://sling:sling_gh759@127.0.0.1:$GH759_ORA_PORT/FREEPDB1?service_name=FREEPDB1"
export ORACLE_GH759_WE8="oracle://sling:sling_gh759@127.0.0.1:$GH759_ORA_PORT/GH759WE8?service_name=GH759WE8"

echo "=== Running GH759 pipeline ==="
cd "$SLING_CLI_DIR"
"$SLING_BIN" run -d -p "$SCRIPT_DIR/p.gh759_oracle_nonbulk_utf8.yaml"
