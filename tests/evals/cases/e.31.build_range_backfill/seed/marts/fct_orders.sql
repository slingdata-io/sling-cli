/**
mode: incremental
unique_key: order_key
update_key: order_date
range:
  start: 1992-01-01
  end: 1992-12-31
  advance: 1mo
  lookback: 3d
**/
select
  o_orderkey as order_key,
  o_orderdate::date as order_date,
  o_totalprice as total_price
from orders
where {{ incremental_where_cond() }}
