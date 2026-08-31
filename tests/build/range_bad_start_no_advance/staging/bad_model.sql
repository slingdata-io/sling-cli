/**
mode: incremental
unique_key: id
update_key: created_at
range:
  start: '2024-01-01'
**/
SELECT 1 AS id, '2024-01-01'::date AS created_at
WHERE {{ incremental_where_cond() }}
