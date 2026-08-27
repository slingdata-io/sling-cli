select
  c.c_custkey as cust_key,
  coalesce(sum(o.o_totalprice), 0) as ltv
from customer c
left join orders o on o.o_custkey = c.c_custkey
group by 1
order by ltv desc, cust_key
limit 10
