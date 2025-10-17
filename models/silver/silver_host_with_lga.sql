{{ config(materialized='table') }}

with host as (
  select
    host_id,
    coalesce(nullif(trim(host_name), ''), 'unknown')                         as host_name,
    host_is_superhost,
    -- normalize for join
    coalesce(nullif(trim(lower(host_neighbourhood)), ''), 'unknown')         as host_neighbourhood,
    -- ensure a timestamp for snapshots/temporal joins later
    (scraped_date)::timestamp                                                as scraped_date
  from {{ ref('silver_host') }}
),

map as (
  -- deterministic suburb→LGA mapping (from your combined table)
  select
    suburb_name,         -- already normalized to lower case in silver_lga_combined
    lga_code,
    lga_name
  from {{ ref('silver_lga_combined') }}
)

select
  h.host_id,
  h.host_name,
  h.host_is_superhost,
  h.host_neighbourhood,
  m.lga_code,
  m.lga_name,
  h.scraped_date as scraped_date,
  (m.lga_code is not null) as is_mapped  
from host as h
left join map m
  on m.suburb_name = h.host_neighbourhood