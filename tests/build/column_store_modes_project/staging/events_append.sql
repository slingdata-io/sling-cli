{%- config(mode='append') -%}

SELECT
    id,
    name,
    created_at
FROM {{ ref('stg_events') }}
