/**
hooks:
  start:
    - type: log
      message: "building stg_orders model"
      level: warn
  end:
    - type: log
      message: "finished stg_orders model"
      level: warn
**/
SELECT 1 as id, 'order_1' as name
UNION ALL
SELECT 2, 'order_2'
