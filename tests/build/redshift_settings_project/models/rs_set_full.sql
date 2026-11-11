/**
dist: id
sort: [d, region]
settings:
  query:
    statement_timeout: 600000
**/
SELECT id, region, d, current_setting('statement_timeout') AS st FROM {{ ref('rs_set_plain') }}
