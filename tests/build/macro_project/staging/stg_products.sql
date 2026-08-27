SELECT
    1 as id,
    {{ clean_string("'  Widget A  '") }} as name,
    {{ null_if_empty("'active'") }} as status,
    1500 as price_cents
UNION ALL
SELECT
    2,
    {{ clean_string("'  Widget B  '") }},
    {{ null_if_empty("''") }},
    2500
