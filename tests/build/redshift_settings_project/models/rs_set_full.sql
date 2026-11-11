/**
dist: id
sort: [d, region]
**/
SELECT id, region, d FROM {{ ref('rs_set_plain') }}
