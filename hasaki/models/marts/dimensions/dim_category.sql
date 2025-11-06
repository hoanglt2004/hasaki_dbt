{{ config(materialized='table') }}

select distinct
    dense_rank() over (order by category_name) as category_id,
    category_name
from {{ ref('stg_product_api') }}
where category_name is not null

