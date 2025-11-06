{{ config(materialized='table') }}

select distinct
    brand_id,
    brand_name
from {{ ref('stg_product_api') }}
where brand_id is not null

