#!/bin/bash
# Continuous mode test: MySQL (binlog) to Postgres, state in a Postgres schema.
# Steps: run and change the source, status on SIGUSR1, a second session is
# refused, graceful stop on SIGTERM, resume with no repeated rows, then a kill
# between the target write and the state commit.
#
# Usage: bash run_docker_test.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLING_CLI_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CMD_DIR="$SLING_CLI_DIR/cmd/sling"
SLING_BIN="$CMD_DIR/sling"
REPL="$SCRIPT_DIR/r.cdc_continuous.yaml"
MYSQL_C=sling-cdc-cont-mysql
PG_C=sling-cdc-cont-pg
WORK="$(mktemp -d)"

if [ ! -x "$SLING_BIN" ]; then
  echo "Building sling binary..."
  (cd "$CMD_DIR" && go build .)
fi

PIDS=""
cleanup() {
  for p in $PIDS; do kill -9 "$p" >/dev/null 2>&1 || true; done
  docker compose -f "$SCRIPT_DIR/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
  /bin/rm -f "$WORK"/*.log
  rmdir "$WORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() {
  echo "ERROR: $1"
  for f in "$WORK"/*.log; do
    echo "=== $f"
    sed 's/\x1b\[[0-9;]*m//g' "$f" | tail -25
  done
  exit 1
}

docker compose -f "$SCRIPT_DIR/docker-compose.yaml" up -d --wait

export MYSQL_CDC_CONT='mysql://sling_repl:sling_repl_pass@127.0.0.1:3398/cdc_cont_db?allowNativePasswords=true&parseTime=true'
export POSTGRES_CDC_CONT='postgresql://postgres:sling_cdc_test@127.0.0.1:55435/cdc_cont_db?sslmode=disable'
export SLING_STATE='POSTGRES_CDC_CONT/cdc_cont'
export SLING_LOGGING=NO_COLOR

mysql_exec() {
  docker exec "$MYSQL_C" mysql -uroot -psling_cdc_test -N -B cdc_cont_db -e "$1" 2>/dev/null
}

pg_query() {
  docker exec "$PG_C" psql -U postgres -d cdc_cont_db -At -F $'\t' -c "$1"
}

# same rows in the source and the target, and no duplicate key in the target
tables_match() {
  local t
  for t in orders items; do
    [ "$(mysql_exec "SELECT id, note FROM $t ORDER BY id")" = "$(pg_query "SELECT id, note FROM cdc_cont.$t ORDER BY id")" ] || return 1
    [ "$(pg_query "SELECT count(*) - count(DISTINCT id) FROM cdc_cont.$t")" = "0" ] || return 1
  done
}

wait_match() {
  for _ in $(seq 1 60); do
    tables_match && return 0
    sleep 1
  done
  fail "$1: target does not match the source"
}

wait_log() { # file pattern seconds
  for _ in $(seq 1 "$3"); do
    grep -q "$2" "$1" && return 0
    sleep 1
  done
  fail "log $(basename "$1") has no '$2' after $3 s"
}

# wait_exit pid seconds: sets EXIT_CODE, or fails when the process does not exit
wait_exit() {
  for _ in $(seq 1 "$2"); do
    if ! kill -0 "$1" 2>/dev/null; then
      set +e; wait "$1"; EXIT_CODE=$?; set -e
      return 0
    fi
    sleep 1
  done
  fail "process $1 did not exit after $2 s"
}

start_sling() { # logfile [VAR=value...]
  local log=$1; shift
  env "$@" "$SLING_BIN" run -r "$REPL" --continuous > "$log" 2>&1 &
  PID=$!
  PIDS="$PIDS $PID"
}

# sum_cycles file: totals of the cycle lines as "I U D"
sum_cycles() {
  sed 's/\x1b\[[0-9;]*m//g' "$1" | grep '\[cdc\] cycle #' |
    sed -E 's/.*\| \+([0-9]+) ~([0-9]+) -([0-9]+) \|.*/\1 \2 \3/' |
    awk '{i+=$1; u+=$2; d+=$3} END {print i+0, u+0, d+0}'
}

INS=0; UPD=0; DEL=0

# 1. start the first session (initial snapshot, then cycles)
start_sling "$WORK/run1.log"
P1=$PID
wait_log "$WORK/run1.log" "continuous run started" 120
grep -q "continuous run started: 1 cdc group (2 streams), interval 5s" "$WORK/run1.log" || fail "start line"

# 2. change the source
mysql_exec "
  INSERT INTO orders VALUES (4, 'o4'), (5, 'o5');
  UPDATE orders SET note = 'o1u' WHERE id = 1;
  DELETE FROM orders WHERE id = 3;
  INSERT INTO items VALUES (3, 'i3');
  UPDATE items SET note = 'i2u' WHERE id = 2;"
INS=$((INS + 3)); UPD=$((UPD + 2)); DEL=$((DEL + 1))

# 3. cycle line and target
wait_log "$WORK/run1.log" "cycle #" 60
wait_match "session 1"
echo "target matches the source after session 1"

# 4. status table on SIGUSR1
kill -USR1 "$P1"
wait_log "$WORK/run1.log" "continuous status" 15

# 5. a second session and a one-shot run are refused
set +e
"$SLING_BIN" run -r "$REPL" --continuous > "$WORK/held.log" 2>&1
rc=$?
"$SLING_BIN" run -r "$REPL" > "$WORK/oneshot.log" 2>&1
rc_one=$?
set -e
[ $rc -eq 1 ] || fail "second session: expected exit 1, got $rc"
grep -q "held by another continuous session" "$WORK/held.log" || fail "second session: no lease message"
[ $rc_one -ne 0 ] || fail "one-shot run during a session must fail"
grep -q "held by a continuous session" "$WORK/oneshot.log" || fail "one-shot run: no lease message"
echo "lease check PASSED"

# 6. graceful stop
kill -TERM "$P1"
wait_exit "$P1" 60
[ "$EXIT_CODE" -eq 0 ] || fail "SIGTERM: expected exit 0, got $EXIT_CODE"
grep -q "stop received" "$WORK/run1.log" || fail "SIGTERM: no stop message"
grep -q "state saved" "$WORK/run1.log" || fail "SIGTERM: no 'state saved'"
grep "state saved" "$WORK/run1.log" | sed 's/\x1b\[[0-9;]*m//g'
read -r i u d <<< "$(sum_cycles "$WORK/run1.log")"
[ "$i $u $d" = "3 2 1" ] || fail "session 1 cycle lines: expected '3 2 1', got '$i $u $d'"

# 7. change the source while stopped, then resume: no row repeats
mysql_exec "
  INSERT INTO orders VALUES (6, 'o6');
  UPDATE items SET note = 'i1u' WHERE id = 1;
  DELETE FROM items WHERE id = 2;"
INS=$((INS + 1)); UPD=$((UPD + 1)); DEL=$((DEL + 1))

start_sling "$WORK/run2.log" SLING_CONTINUOUS_MAX_CYCLES=3
wait_exit "$PID" 120
[ "$EXIT_CODE" -eq 0 ] || fail "resume: expected exit 0, got $EXIT_CODE"
read -r i u d <<< "$(sum_cycles "$WORK/run2.log")"
[ "$i $u $d" = "1 1 1" ] || fail "resume cycle lines: expected '1 1 1', got '$i $u $d'"
tables_match || fail "resume: target does not match the source"
echo "resume PASSED"

# 8. kill between the target write and the state commit
mysql_exec "
  INSERT INTO orders VALUES (7, 'o7'), (8, 'o8');
  UPDATE orders SET note = 'o2u' WHERE id = 2;
  DELETE FROM items WHERE id = 3;"
INS=$((INS + 2)); UPD=$((UPD + 1)); DEL=$((DEL + 1))

start_sling "$WORK/run3.log" SLING_TEST_CDC_KILL_AFTER_WRITE=1
wait_exit "$PID" 120
[ "$EXIT_CODE" -eq 137 ] || fail "kill hook: expected exit 137, got $EXIT_CODE"

start_sling "$WORK/run4.log" SLING_CONTINUOUS_MAX_CYCLES=2
wait_exit "$PID" 120
[ "$EXIT_CODE" -eq 0 ] || fail "after kill: expected exit 0, got $EXIT_CODE"
wait_match "after kill"
read -r i u d <<< "$(sum_cycles "$WORK/run4.log")"
[ "$i $u $d" = "2 1 1" ] || fail "after kill cycle lines: expected '2 1 1', got '$i $u $d'"
echo "kill test PASSED"

echo "CDC continuous test: inserts=$INS updates=$UPD deletes=$DEL"
echo "CDC continuous test PASSED"
