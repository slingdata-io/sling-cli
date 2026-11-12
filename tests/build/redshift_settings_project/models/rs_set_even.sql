/**
dist: even
**/
SELECT id, region, d FROM {{ ref('rs_set_plain') }}
