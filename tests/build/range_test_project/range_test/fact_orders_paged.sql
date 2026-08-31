/**
mode: incremental
unique_key: id
update_key: created_at
merge_strategy: delete+insert
range:
  advance: 7d
**/
SELECT id, name, created_at::date AS created_at
FROM {{ ref('stg_orders') }}
WHERE {incremental_where_cond}
