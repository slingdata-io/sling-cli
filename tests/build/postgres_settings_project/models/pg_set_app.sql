/**
mode: append
**/
SELECT id, region, d FROM {{ ref('pg_set_plain') }}
