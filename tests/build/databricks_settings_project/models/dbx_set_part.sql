/**
partition_by: d
**/
SELECT id, region, d FROM {{ ref('dbx_set_plain') }}
