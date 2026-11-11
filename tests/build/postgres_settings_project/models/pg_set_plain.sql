SELECT g AS id, 'eu' AS region, CURRENT_DATE AS d, current_setting('work_mem') AS wm
FROM generate_series(1, {% if extra %}3{% else %}2{% endif %}) g
