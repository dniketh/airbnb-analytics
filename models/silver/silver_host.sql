{{ config(materialized='table') }}

with base as (
  select * from {{ ref('silver_airbnb_clean') }}
),
dedup as (
  select distinct on (host_id)
    host_id,
    coalesce(nullif(trim(host_name), ''), 'unknown') as host_name,
    host_is_superhost,
    coalesce(nullif(trim(host_neighbourhood), ''), 'unknown') as host_neighbourhood,
    host_since_raw as host_since,
    scraped_date::timestamp as scraped_date
  from base
  where host_id is not null
  order by host_id, scraped_date desc
)
select * from dedup