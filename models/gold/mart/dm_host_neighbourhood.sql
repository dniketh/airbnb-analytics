-- models/gold/dm_host_neighbourhood.sql
{{ config(materialized='view') }}

with base as (
  -- Start from fact for pricing/availability/revenue-by-listing
  select
    f.listing_id,
    f.host_id,
    f.price,
    f.has_availability,
    f.availability_30,
    f.recorded_date,
    f.month_start,
    case when f.has_availability
         then greatest(0, 30 - f.availability_30) * f.price
         else 0
    end::numeric(14,2) as est_revenue_active
  from {{ ref('fact_listings') }} f
),
-- Time-valid host join to fetch host_neighbourhood at that date
host_scd as (
  select
    b.*,
    lower(trim(h.host_neighbourhood))::text as host_neighbourhood
  from base b
  left join {{ ref('dim_host') }} h
    on  h.host_id = b.host_id
    and b.recorded_date >= h.record_start_date
    and b.recorded_date <  coalesce(h.record_end_date, '9999-12-31'::timestamp)
),
-- Map host_neighbourhood -> LGA (first via suburb map, then fallback via LGA name)
lga_mapped as (
  select
    hs.*,
    -- prefer suburb match from silver_lga; fallback to 1-row-per-LGA table
    coalesce(sl.lga_code::text, lc.lga_code::text) as host_neighbourhood_lga_code
  from host_scd hs
  left join {{ ref('silver_lga') }} sl
    on sl.suburb_name = hs.host_neighbourhood             -- suburb-level map
  left join {{ ref('silver_lga_code_clean') }} lc
    on lower(trim(lc.lga_name)) = hs.host_neighbourhood   -- LGA-name fallback
),
with_lga_name as (
  select
    lm.*,
    dl.lga_name as host_neighbourhood_lga
  from lga_mapped lm
  left join {{ ref('dim_lga') }} dl
    on dl.lga_code = lm.host_neighbourhood_lga_code
)

select
  month_start,
  host_neighbourhood_lga,          -- display name (from dim_lga)
  host_neighbourhood_lga_code,     -- stable key

  -- Number of distinct hosts (per brief)
  count(distinct host_id)                                             as distinct_hosts,

  -- Average Estimated revenue per active listings (per brief)
  round(avg(est_revenue_active) filter (where has_availability), 2)   as avg_est_revenue_per_active_listing,

  -- Estimated revenue per host = total_est_revenue_active / distinct_hosts (per brief)
  round(
    sum(est_revenue_active) / nullif(count(distinct host_id), 0), 2
  )                                                                   as est_revenue_per_host

from with_lga_name
group by 1,2,3
order by host_neighbourhood_lga, month_start