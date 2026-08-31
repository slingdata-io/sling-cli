CREATE TABLE IF NOT EXISTS public.orders (
  id bigint primary key,
  customer_id bigint,
  updated_at timestamp,
  amount numeric
);
INSERT INTO public.orders (id, customer_id, updated_at, amount) VALUES
  (1, 10, '2024-01-01 00:00:00', 9.50),
  (2, 11, '2024-01-02 00:00:00', 12.00);
