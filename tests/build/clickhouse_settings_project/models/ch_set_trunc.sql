/**
mode: truncate
settings:
  query:
    max_threads: 2
**/
SELECT id, event_day, ts FROM {{ ref('ch_set_plain') }}
