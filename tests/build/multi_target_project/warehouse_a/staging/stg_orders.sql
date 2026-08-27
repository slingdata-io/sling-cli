SELECT
  g.id,
  'order_' || g.id::text as name,
  now() - (random() * interval '365 days') as created_at,
  round((random() * 1000)::numeric, 2) as amount
FROM generate_series(1, 500000) as g(id)
