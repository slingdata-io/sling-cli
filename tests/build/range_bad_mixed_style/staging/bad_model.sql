/**
mode: incremental
unique_key: id
update_key: created_at
**/
SELECT 1 AS id, '2024-01-01'::date AS created_at
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
AND {{ incremental_where_cond() }}
