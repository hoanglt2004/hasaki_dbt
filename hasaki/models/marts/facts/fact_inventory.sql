{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

with src as (
    select
        product_id,
        session_id,
        total_branches,
        branches_in_stock,
        stock_available,
        captured_at
    from {{ ref('stg_product_api') }}
)

select
    *
from src

{% if is_incremental() %}
  where captured_at > (select max(captured_at) from {{ this }})
{% endif %}
