SELECT
    1 AS id,
    'order_a' AS name,
    '2024-01-01'::timestamp AS updated_at
WHERE {incremental_where_cond}
