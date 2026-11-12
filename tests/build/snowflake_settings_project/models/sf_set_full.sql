/**
transient: true
cluster_by: [region, d]
settings:
  table:
    comment: orders
    data_retention_time_in_days: 0
  query:
    query_tag: sling_build_225
**/
SELECT id, region, d FROM {{ ref('sf_set_plain') }}
