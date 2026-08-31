/**
mode: full-refresh
**/
SELECT 1 AS id, 'order_1' AS name, '{{ run_label }}' AS run_label
UNION ALL
SELECT 2, 'order_2', '{{ run_label }}'
