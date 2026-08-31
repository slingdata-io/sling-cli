/**
mode: view
tags:
  - finance
**/
SELECT
    count(*) as total_orders
FROM {{ ref('fct_orders') }}
