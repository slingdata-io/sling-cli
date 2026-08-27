/** mode: view **/
select
  o_orderkey as order_key,
  o_custkey as cust_key,
  o_orderdate::date as order_date,
  o_totalprice as total_price
from orders
