/**
settings:
  table:
    compression: ZSTD
    replication_num: 1
  query:
    query_timeout: 600
**/
SELECT id, region, d, @@query_timeout AS qt FROM {{ ref('sr_set_plain') }}
