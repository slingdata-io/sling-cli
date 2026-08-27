select
  l.order_key,
  l.line_number,
  o.cust_key,
  o.order_date,
  l.extended_price * (1 - l.discount) as net_revenue
from {{ ref('stg_lineitem') }} l
join {{ ref('stg_orders') }} o on o.order_key = l.order_key
