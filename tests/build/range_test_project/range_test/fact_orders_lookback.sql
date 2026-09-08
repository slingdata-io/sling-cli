/**
mode: incremental
unique_key: id
update_key: created_at
merge_strategy: delete+insert
range:
  lookback: 2d
**/
SELECT id, name, created_at::date AS created_at
FROM {{ ref('stg_orders') }}
WHERE {{ incremental_where_cond() }}
