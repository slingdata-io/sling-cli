#!/bin/bash
# CDC key detection on a Postgres source. Starts its own Postgres 16 with
# wal_level=logical, runs the pipeline as a role with only REPLICATION and
# SELECT, then checks that a table with no key fails fast.
#
# Usage: bash run_docker_test.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLING_CLI_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CMD_DIR="$SLING_CLI_DIR/cmd/sling"
SLING_BIN="$CMD_DIR/sling"
CONTAINER=sling-cdc-pk-pg

if [ ! -x "$SLING_BIN" ]; then
  echo "Building sling binary..."
  (cd "$CMD_DIR" && go build .)
fi

cleanup() {
  docker compose -f "$SCRIPT_DIR/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$SCRIPT_DIR/docker-compose.yaml" up -d --wait

for i in $(seq 1 60); do
  if docker exec $CONTAINER psql -U postgres -d cdc_pk_db -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'cdc_reader'" 2>/dev/null | grep -q 1; then
    break
  fi
  if [ "$i" = "60" ]; then
    echo "ERROR: Postgres did not become ready in time"
    docker logs $CONTAINER | tail -60
    exit 1
  fi
  sleep 1
done

export POSTGRES_CDC_PK='postgresql://postgres:sling_cdc_test@127.0.0.1:55434/cdc_pk_db?sslmode=disable'
export POSTGRES_CDC_PK_RO='postgresql://cdc_reader:cdc_reader_pw@127.0.0.1:55434/cdc_pk_db?sslmode=disable'
export SLING_STATE='POSTGRES_CDC_PK/public'

"$SLING_BIN" run -d -p "$SCRIPT_DIR/p.cdc_pg_pk.yaml"

set +e
out=$("$SLING_BIN" run -d -r "$SCRIPT_DIR/r.cdc_pg_no_key.yaml" 2>&1)
rc=$?
set -e
echo "$out" | tail -20
if [ $rc -eq 0 ]; then
  echo "ERROR: CDC on src.no_key must fail"
  exit 1
fi
if ! echo "$out" | grep -q 'change-capture mode needs a primary key for src.no_key'; then
  echo "ERROR: expected the no-key error for src.no_key"
  exit 1
fi
if echo "$out" | grep -qi 'created table'; then
  echo "ERROR: sling made a target table before the key check"
  exit 1
fi
echo "CDC PG no-key test PASSED"
