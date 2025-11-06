{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['review_id', 'product_id']
) }}

select
    (review->>'id')::int as review_id,
    (data->>'id')::int as product_id,
    (review->>'user_id')::int as user_id,
    (review->'rating'->>'star')::int as rating_star,
    NULLIF(
    CASE 
        WHEN trim(review->>'content') = '' THEN NULL
        WHEN lower(trim(review->>'content')) IN ('null', 'undefined', 'none') THEN NULL
        WHEN trim(review->>'content') ~ '^[\.\,\!\?\-\_]+$' THEN NULL  -- chỉ toàn ký tự đặc biệt
        ELSE trim(review->>'content')
    END,
    '') AS review_content,
    review->>'user_fullname' as user_name,
    to_timestamp((review->>'created_at')::bigint) as created_at,
    session_id,
    captured_at
from raw.product_api,
    jsonb_array_elements(data->'short_rating_data'->'reviews') as review
where 
    CASE 
        WHEN trim(review->>'content') = '' THEN NULL
        WHEN lower(trim(review->>'content')) IN ('null', 'undefined', 'none') THEN NULL
        WHEN trim(review->>'content') ~ '^[\.\,\!\?\-\_]+$' THEN NULL
        ELSE trim(review->>'content')
    END is not null

{% if is_incremental() %}
and (review->>'id')::int > (select coalesce(max(review_id),0) from {{ this }})
{% endif %}
