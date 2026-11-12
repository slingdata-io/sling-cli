/**
mode: full-refresh
order_by: (event_day, id)
partition_by: toYYYYMM(event_day)
primary_key: event_day
ttl: event_day + INTERVAL 100 YEAR
settings:
  table:
    index_granularity: 1024
    storage_policy: default
**/
SELECT id, event_day, ts FROM {{ ref('ch_set_plain') }}
