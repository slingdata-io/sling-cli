/**
mode: incremental
unique_key: id
update_key: id
unlogged: true
settings:
  table:
    fillfactor: 70
  query:
    work_mem: 64MB
**/
SELECT id, region, d, current_setting('work_mem') AS wm FROM {{ ref('pg_set_plain') }}
{% if is_incremental() %}
WHERE id > (SELECT max(id) FROM {{ this }})
{% endif %}
