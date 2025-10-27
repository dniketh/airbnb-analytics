{{ config(materialized='table') }}

with src as (
  select
    host_id,
    coalesce(nullif(trim(host_name), ''), 'unknown')                  as host_name,
    (host_is_superhost::text in ('t'))::boolean as host_is_superhost,
    coalesce(nullif(trim(lower(host_neighbourhood)), ''), 'unknown')  as host_neighbourhood,
    host_since                                                        as host_since,
    scraped_date::date                                                as scraped_date
  from {{ ref('silver_airbnb_clean') }}
  where host_id is not null
),
dedup as ( --for making sure to have  no duplicates on the same scraped_date
  select
    host_id,
    scraped_date,
    max(host_is_superhost::int)::boolean as host_is_superhost,         
    max(host_name)                       as host_name,                
    max(host_neighbourhood)               as host_neighbourhood,        
    max(host_since)                       as host_since
  from src
  group by host_id, scraped_date
)
select *
from dedup
order by host_id, scraped_date