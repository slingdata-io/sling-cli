{%- config(mode='incremental', unique_key='id', merge_strategy='delete+insert', update_key='created_at') -%}

SELECT
    id,
    name,
    created_at
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
