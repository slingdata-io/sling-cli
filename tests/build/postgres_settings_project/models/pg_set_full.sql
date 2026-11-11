/**
mode: full-refresh
unlogged: true
settings:
  table:
    fillfactor: 70
**/
SELECT id, region, d FROM {{ ref('pg_set_plain') }}
