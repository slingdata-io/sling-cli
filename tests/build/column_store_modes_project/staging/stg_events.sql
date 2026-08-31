SELECT 1 AS id, 'a' AS name, CAST('2024-01-01' AS DATE) AS created_at
UNION ALL
SELECT 2, 'b', CAST('2024-01-02' AS DATE)
{% if extra %}
UNION ALL
SELECT 3, 'c', CAST('2024-01-03' AS DATE)
{% endif %}
