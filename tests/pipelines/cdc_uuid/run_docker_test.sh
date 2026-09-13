#!/bin/bash
# Self-contained reproduction for the Postgres CDC UUID Arrow panic.
#
# Spins up a Postgres 16 container with wal_level=logical, exposes it as the
# POSTGRES_CDC_UUID Sling connection, runs the pipeline, then tears the
# container down. Source and target are both the container, so the repro needs
# no external warehouse (the panic is on the source read side anyway).
#
# Usage: bash run_docker_test.sh
# Requires:
#   - docker (with compose v2)
#   - a built sling binary at sling-cli/cmd/sling/sling (or it will build one)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLING_CLI_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CMD_DIR="$SLING_CLI_DIR/cmd/sling"
SLING_BIN="$CMD_DIR/sling"

if [ ! -x "$SLING_BIN" ]; then
  echo "Building sling binary..."
  (cd "$CMD_DIR" && go build .)
fi

cleanup() {
  echo "=== Tearing down Postgres container ==="
  docker compose -f "$SCRIPT_DIR/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== Starting Postgres container (wal_level=logical) ==="
docker compose -f "$SCRIPT_DIR/docker-compose.yaml" up -d --wait

echo "=== Waiting for Postgres to be ready ==="
for i in $(seq 1 60); do
  # pg_isready can report ready during init, so run a real query instead
  if docker exec sling-cdc-uuid-pg psql -U postgres -d cdc_uuid_db -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "Postgres ready."
    break
  fi
  if [ "$i" = "60" ]; then
    echo "ERROR: Postgres did not become ready in time"
    docker logs sling-cdc-uuid-pg | tail -60
    exit 1
  fi
  sleep 1
done

# Verify logical decoding is on (CDC needs a replication slot)
wal_level=$(docker exec sling-cdc-uuid-pg psql -U postgres -d cdc_uuid_db -tAc "SHOW wal_level" 2>/dev/null || echo "")
echo "wal_level: $wal_level"

# Expose the container as the POSTGRES_CDC_UUID Sling connection for this run
export POSTGRES_CDC_UUID='postgresql://postgres:sling_cdc_test@127.0.0.1:55433/cdc_uuid_db?sslmode=disable'

# Sling CDC requires SLING_STATE — keep it in the same container.
export SLING_STATE='POSTGRES_CDC_UUID/public'

echo "=== Running CDC UUID pipeline ==="
"$SLING_BIN" run -d -p "$SCRIPT_DIR/p.cdc_uuid.yaml"

echo "=== CDC UUID reproduction completed ==="
