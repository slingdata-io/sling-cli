/**
mode: incremental
unique_key: id
update_key: created_at
merge_strategy: delete+insert
range:
  start: '2024-01-01'
  advance: 7d
**/
SELECT id, name, created_at::date AS created_at
FROM {{ ref('stg_orders') }}
WHERE {incremental_where_cond}
