/**
settings:
  table:
    compression: ZSTD
    replication_num: 1
**/
SELECT id, region, d FROM {{ ref('sr_set_plain') }}
