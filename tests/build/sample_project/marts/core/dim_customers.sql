{%- config(mode='view') -%}

SELECT
    id,
    name,
    status
FROM {{ ref('stg_customers') }}
