select o.id, o.amount, c.name
from public.orders o
join public.customers c on c.id = o.customer_id
where o.amount > 0
