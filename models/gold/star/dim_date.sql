{{ config(materialized='table') }}

with series as (
  select generate_series(
    date '2019-01-01',      
    date '2030-12-31',      -- for a future buffer
    interval '1 day'
  )::date as scraped_date
)
select
  to_char(scraped_date, 'YYYYMMDD')::int as date_id,
  scraped_date,
  date_trunc('month', scraped_date)::date as month_start
from series
order by scraped_date