{{ config(materialized='incremental') }}

select
    product_id,
    final_price,
    market_price,
    bought,
    discount_percent,
    session_id
from {{ ref('stg_product_api') }}

{% if is_incremental() %}
where session_id not in (select session_id from {{ this }})
{% endif %}
