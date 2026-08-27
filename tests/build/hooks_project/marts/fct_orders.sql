/**
mode: full-refresh
hooks:
  start:
    - type: log
      message: "building fct_orders from stg_orders"
    - type: query
      connection: POSTGRES
      query: "SELECT 1"
  end:
    - type: log
      message: "fct_orders build complete"
**/
SELECT
    id,
    name
FROM {{ ref('stg_orders') }}
