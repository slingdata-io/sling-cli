/**
mode: table
tests:
  - unique: order_id
  - not_null: [order_id]
**/
select
  order_id,
  customer_id,
  lower(status) as status,
  loaded_at
from (
  select
    *,
    row_number() over (partition by order_id order by loaded_at desc) as rn
  from eval_ecom.raw_orders
) t
where rn = 1
