/**
cluster_by: region
settings:
  table:
    delta.autoOptimize.optimizeWrite: true
**/
SELECT id, region, d FROM {{ ref('dbx_set_plain') }}
