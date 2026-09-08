/**
database: FIN_DB
**/
select customer_id from {{ ref('dim_customers') }}
