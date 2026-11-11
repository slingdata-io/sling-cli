/**
mode: incremental
unique_key: id
update_key: ts
order_by: (event_day, id)
partition_by: toYYYYMM(event_day)
settings:
  query:
    mutations_sync: 2
    max_threads: 3
hooks:
  start:
    - type: query
      connection: '{target.name}'
      query: SELECT 'sling_settings_hook_probe' AS p
**/
SELECT id, event_day, ts FROM {{ ref('ch_set_plain') }}
{% if is_incremental() %}
WHERE ts > (SELECT max(ts) FROM {{ this }})
{% endif %}
