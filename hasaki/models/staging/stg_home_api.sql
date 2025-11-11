{{ config(materialized='view') }}

select
    session_id,
    created_at as captured_at,
    extract(day from created_at) as day,
    extract(month from created_at) as month,
    extract(year from created_at) as year
from raw.home_api
