{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
    product_id::int as product_id,
    (data->>'name') as product_name,
    (data->'brand'->>'id')::int as brand_id,
    (data->'brand'->>'name') as brand_name,
    (data->'seo'->>'product:category') as category_name,
    regexp_replace(data->>'final_price', '[^0-9]', '', 'g')::numeric as final_price,
    regexp_replace(data->>'market_price', '[^0-9]', '', 'g')::numeric as market_price,
    (data->'bought')::int as bought,
    (data->'branch'->>'total')::int as total_branches,
    (data->'branch'->>'in_stock')::int as branches_in_stock,
    (data->'branch'->>'stock_available')::int as stock_available,
    (data->'rating'->>'avg_rate')::float as avg_rating,
    (data->'rating'->>'total')::int as total_reviews,
    (data->'discount_market_percent')::float *(-1) as discount_percent,
    NULLIF(data->'deal_data'->>'coming', '')::timestamp AS deal_start,
    NULLIF(data->'deal_data'->>'expire', '')::timestamp AS deal_expire,
    session_id,
    created_at as captured_at
from raw.product_api

{% if is_incremental() %}
  where created_at > (
      select coalesce(max(created_at), '1900-01-01'::timestamp)
      from {{ this }} as prev
  )
{% endif %}