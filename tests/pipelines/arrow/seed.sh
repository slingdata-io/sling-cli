#!/usr/bin/env bash
# Arrow lane CLI suite: seeds {SCHEMA}.arrow_src on {SOURCE} with seed.yaml.
#
# The suite cases run in parallel and read the same source table. A lock lets
# one seed run at a time, since the seeds also share /tmp/arrow_src.csv and
# since two connections can point to the same table (POSTGRES, POSTGRES_ADBC).
# When the table has its 10000 rows already, the seed does not load it again.
#
# env (from the suite entry): SOURCE, SCHEMA
set -e

lock=/tmp/sling_arrow_seed.lock
for _ in $(seq 1 900); do
  mkdir "$lock" 2>/dev/null && break
  # a lock older than 15 minutes is from a killed run
  if [ -n "$(find "$lock" -maxdepth 0 -mmin +15 2>/dev/null)" ]; then
    rmdir "$lock" 2>/dev/null || true
  fi
  sleep 1
done
[ -d "$lock" ] || { echo "arrow lane seed: lock not acquired" >&2; exit 1; }
trap 'rmdir "$lock" 2>/dev/null || true' EXIT

cnt=$(SLING_ROW_CNT= sling conns exec "$SOURCE" "select count(*) as cnt from $SCHEMA.arrow_src" 2>/dev/null | grep -oE '\b10000\b' | head -1 || true)
# Postgres creates decimal(18,4) as a plain numeric. The lane declines a
# decimal with no precision (SLING-125), so c_dec gets its precision here.
# This also changes a table from an older seed.
set_dec_precision() {
  case "$SOURCE" in POSTGRES*) ;; *) return 0 ;; esac
  q="select 'no_precision' as res from information_schema.columns where table_schema = '$SCHEMA' and table_name = 'arrow_src' and column_name = 'c_dec' and numeric_precision is null"
  if SLING_ROW_CNT= sling conns exec "$SOURCE" "$q" 2>/dev/null | grep -q no_precision; then
    SLING_ROW_CNT= sling conns exec "$SOURCE" "alter table $SCHEMA.arrow_src alter column c_dec type numeric(18,4)"
  fi
}

if [ "$cnt" = "10000" ]; then
  set_dec_precision
  echo "arrow lane seed complete: 10000 rows in $SCHEMA.arrow_src (already seeded)"
  exit 0
fi

SLING_ROW_CNT=10000 sling run -p tests/pipelines/arrow/seed.yaml
set_dec_precision
