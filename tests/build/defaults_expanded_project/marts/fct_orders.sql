/**
mode: full-refresh
tags: [marts]
hooks:
  start:
    - type: log
      message: "fct_orders frontmatter start hook"
**/
SELECT
    id,
    name
FROM {{ ref('stg_orders') }}
