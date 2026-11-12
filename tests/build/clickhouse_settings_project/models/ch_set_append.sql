{{ config(mode='append', settings={'query': {'max_insert_threads': 2}}) }}
SELECT id, event_day, ts FROM {{ ref('ch_set_plain') }}
