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
if [ "$cnt" = "10000" ]; then
  echo "arrow lane seed complete: 10000 rows in $SCHEMA.arrow_src (already seeded)"
  exit 0
fi

SLING_ROW_CNT=10000 sling run -p tests/pipelines/arrow/seed.yaml
