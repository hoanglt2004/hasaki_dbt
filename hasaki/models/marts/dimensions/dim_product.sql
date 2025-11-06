{{ config(materialized='table') }}

select distinct
    p.product_id,
    p.product_name,
    p.brand_id,
    c.category_id
from {{ ref('stg_product_api') }} p
left join {{ ref('dim_category') }} c
  on p.category_name = c.category_name

