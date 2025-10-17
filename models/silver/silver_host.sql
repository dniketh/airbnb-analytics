{{ config(materialized='view') }}

-- One row per host per scrape (historical feed for snapshots)
select distinct
  host_id,
  coalesce(nullif(trim(host_name), ''), 'unknown')                  as host_name,
  host_is_superhost,
  coalesce(nullif(trim(lower(host_neighbourhood)), ''), 'unknown')  as host_neighbourhood,
  host_since_raw                                                    as host_since,
  scraped_date::timestamp                                           as scraped_date
from {{ ref('silver_airbnb_clean') }}
where host_id is not null