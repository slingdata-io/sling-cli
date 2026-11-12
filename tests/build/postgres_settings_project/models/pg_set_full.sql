/**
mode: full-refresh
unlogged: true
settings:
  table:
    fillfactor: 70
  query:
    work_mem: 64MB
hooks:
  end:
    - type: query
      connection: '{target.name}'
      query: |
        DROP TABLE IF EXISTS sling_build_pg.pg_hook_probe;
        CREATE TABLE sling_build_pg.pg_hook_probe AS SELECT current_setting('work_mem') AS wm
**/
SELECT id, region, d, current_setting('work_mem') AS wm FROM {{ ref('pg_set_plain') }}
