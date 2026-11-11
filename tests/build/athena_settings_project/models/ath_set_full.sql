/**
settings:
  table:
    format: PARQUET
    partitioning: [d]
    table_type: ICEBERG
**/
SELECT id, region, d FROM {{ ref('ath_set_plain') }}
