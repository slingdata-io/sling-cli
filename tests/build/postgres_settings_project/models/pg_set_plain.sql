SELECT g AS id, 'eu' AS region, CURRENT_DATE AS d
FROM generate_series(1, {% if extra %}3{% else %}2{% endif %}) g
