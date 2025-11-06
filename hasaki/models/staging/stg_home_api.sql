{{ config(materialized='view') }}

select
    session_id,
    captured_at,
    extract(day from captured_at) as day,
    extract(month from captured_at) as month,
    extract(year from captured_at) as year
from raw.home_api
