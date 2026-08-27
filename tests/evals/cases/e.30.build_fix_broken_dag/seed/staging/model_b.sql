SELECT * FROM {{ ref('stg_missing') }}
UNION ALL
SELECT * FROM {{ ref('model_a') }}
