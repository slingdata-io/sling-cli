/** mode: table **/
select
  c.cust_key,
  c.name,
  n.n_name as nation_name,
  coalesce(sum(o.total_price), 0) as ltv
from {{ ref('stg_customers') }} c
left join nation n on n.n_nationkey = c.nation_key
left join {{ ref('stg_orders') }} o on o.cust_key = c.cust_key
group by 1, 2, 3
