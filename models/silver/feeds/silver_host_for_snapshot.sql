{{ config(materialized='view') }}

with base as (
  select
    host_id,
    coalesce(nullif(trim(host_name), ''), 'unknown')                  as host_name,
    host_is_superhost,
    coalesce(nullif(trim(lower(host_neighbourhood)), ''), 'unknown')  as host_neighbourhood,
    host_since,
    scraped_date::date                                                as scraped_date,
    date_trunc('month', scraped_date::date)::date                     as month_start
  from {{ ref('silver_host') }}
  where host_id is not null
),
ranked as (
  select
    *,
    row_number() over (
      partition by host_id, month_start
      order by scraped_date
    ) as rn
  from base
)
select
  host_id,
  host_name,
  host_is_superhost,
  host_neighbourhood,
  host_since,
  scraped_date
from ranked
where rn = 1