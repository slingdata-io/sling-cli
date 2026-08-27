/**
mode: full-refresh
**/
SELECT id, name, run_label
FROM {{ ref('stg_orders') }}
