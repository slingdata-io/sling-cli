{%- config(mode='incremental', unique_key='id', merge_strategy='delete+insert', update_key='created_at') -%}

SELECT
    id,
    name,
    created_at
FROM {{ ref('stg_events') }}
{% if is_incremental() %}
WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
