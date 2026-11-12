/**
partition_by: d
cluster_by: [region, id]
settings:
  table:
    description: orders
    labels:
      team: data
    partition_expiration_days: 30
**/
SELECT id, region, d FROM {{ ref('bq_set_plain') }}
