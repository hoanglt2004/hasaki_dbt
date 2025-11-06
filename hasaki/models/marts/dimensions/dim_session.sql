{{ config(materialized='table') }}

select distinct
    session_id,
    captured_at as crawl_date,
    extract(day from captured_at)::int as day,
    extract(month from captured_at)::int as month,
    extract(year from captured_at)::int as year
from {{ ref('stg_product_api') }}
