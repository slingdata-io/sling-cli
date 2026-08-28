#!/bin/bash
# Self-contained MySQL + MariaDB geometry reproduction test.
# Spins up mysql:8.0 and mariadb:11 via docker compose, exposes them as the
# MYSQL/MARIADB Sling connections, runs the pipeline, then tears the
# containers down.
#
# Usage: bash run_docker_test.sh
# Requires: Docker (with compose v2), a built sling binary at cmd/sling/sling
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
  echo "=== Tearing down MySQL & MariaDB containers ==="
  docker compose -f "$SCRIPT_DIR/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== Starting MySQL & MariaDB containers ==="
docker compose -f "$SCRIPT_DIR/docker-compose.yaml" up -d --wait

echo "=== Waiting for stable SQL connections from host ==="
stable=0
for i in $(seq 1 60); do
  mysql_ok=$(docker exec sling-test-geom-mysql mysqladmin ping -h localhost -pmysql >/dev/null 2>&1 && echo 1 || echo 0)
  mariadb_ok=$(docker exec sling-test-geom-mariadb healthcheck.sh --connect --innodb_initialized >/dev/null 2>&1 && echo 1 || echo 0)
  if [ "$mysql_ok" = "1" ] && [ "$mariadb_ok" = "1" ]; then
    stable=$((stable + 1))
    if [ "$stable" -ge 3 ]; then
      echo "MySQL & MariaDB stable."
      break
    fi
  else
    stable=0
  fi
  if [ "$i" = "60" ]; then
    echo "ERROR: MySQL or MariaDB did not become stable in time"
    docker logs sling-test-geom-mysql 2>&1 | tail -20
    docker logs sling-test-geom-mariadb 2>&1 | tail -20
    exit 1
  fi
  sleep 1
done

# Expose containers as Sling connections for this run only.
# LOCAL_CRS is the local conn with the geometry_crs prop (p.52).
export MYSQL='mysql://root:mysql@127.0.0.1:33306/geom_test'
export MARIADB='mariadb://root:mariadb@127.0.0.1:33307/geom_test'
export LOCAL_CRS='{"type":"file","geometry_crs":"EPSG:4326"}'

echo "=== Running MySQL & MariaDB geometry parquet test ==="
"$SLING_BIN" run -d -p "$SCRIPT_DIR/p.52.mysql_mariadb_geometry_parquet.yaml"

echo "=== MySQL & MariaDB reproduction test completed ==="
