/** mode: view **/
select
  l_orderkey as order_key,
  l_linenumber as line_number,
  l_extendedprice as extended_price,
  l_discount as discount,
  l_shipdate::date as ship_date
from lineitem
