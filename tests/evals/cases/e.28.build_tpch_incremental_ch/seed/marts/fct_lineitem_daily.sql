/**
mode: incremental
unique_key: [order_key, line_number]
update_key: ship_date
**/
select
  l_orderkey as order_key,
  l_linenumber as line_number,
  l_shipdate as ship_date,
  l_extendedprice * (1 - l_discount) as net_revenue
from eval_tpch.lineitem
