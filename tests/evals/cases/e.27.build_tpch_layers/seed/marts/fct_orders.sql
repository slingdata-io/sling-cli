/** mode: table **/
select
  order_key,
  cust_key,
  order_date,
  sum(net_revenue) as order_total
from {{ ref('int_order_items') }}
group by 1, 2, 3
