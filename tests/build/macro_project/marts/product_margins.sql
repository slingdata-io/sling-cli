/**
mode: full-refresh
**/
SELECT
    id,
    name,
    {{ cents_to_dollars('price_cents') }} as price_dollars,
    {{ safe_divide('price_cents', '100') }} as price_ratio
FROM {{ ref('stg_products') }}
