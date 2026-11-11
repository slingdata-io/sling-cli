/**
mode: incremental
unique_key: id
update_key: id
unlogged: true
settings:
  table:
    fillfactor: 70
**/
SELECT id, region, d FROM {{ ref('pg_set_plain') }}
{% if is_incremental() %}
WHERE id > (SELECT max(id) FROM {{ this }})
{% endif %}
