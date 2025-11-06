{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['review_id', 'product_id']
) }}

with latest as (

    select distinct on (review_id, product_id)
        review_id,
        product_id,
        user_id,
        rating_star,
        review_content,
        created_at,
        captured_at
    from {{ ref('stg_reviews_api') }}
    order by review_id, product_id, captured_at desc

)

select
   *
from latest
