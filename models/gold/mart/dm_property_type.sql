-- models/gold/dm_property_type.sql
{{ config(materialized='view') }}

with base as (
  -- start from the fact
  select
    f.listing_id,
    f.host_id,
    f.price,
    f.has_availability,
    f.availability_30,
    f.review_scores_rating,
    f.recorded_date,
    f.month_start
  from {{ ref('fact_listings') }} f
),

-- time-valid SCD2 join to bring in listing attributes
property_scd as (
  select
    b.*,
    dl.property_type,
    dl.room_type,
    dl.accommodates
  from base b
  left join {{ ref('dim_property') }} dl
    on  dl.listing_id = b.listing_id
    and b.recorded_date >= dl.record_start_date
    and b.recorded_date <  coalesce(dl.record_end_date, '9999-12-31'::timestamp)
),

-- time-valid SCD2 join to bring in host superhost flag
host_scd as (
  select
    l.*,
    dh.host_is_superhost
  from property_scd l
  left join {{ ref('dim_host') }} dh
    on  dh.host_id = l.host_id
    and l.recorded_date >= dh.record_start_date
    and l.recorded_date <  coalesce(dh.record_end_date, '9999-12-31'::timestamp)
),

-- per-row derivations per the brief
derived as (
  select
    month_start,
    property_type,
    room_type,
    accommodates,
    listing_id,
    host_id,
    price,
    has_availability,
    availability_30,
    review_scores_rating,
    coalesce(host_is_superhost,false) as host_is_superhost,

    -- Number of stays (only for active listings) = 30 - availability_30 (clamped at 0)
    case when has_availability then greatest(0, 30 - availability_30) else 0 end as stays_active,

    -- Estimated revenue per active listing = stays * price
    case when has_availability then (greatest(0, 30 - availability_30) * price) else 0 end::numeric(14,2)
      as est_revenue_active
  from host_scd
)

select
  month_start,
  property_type,
  room_type,
  accommodates,

  -- total listings
  count(*)                                                     as listings_total,

  -- active / inactive listings
  count(*) filter (where has_availability)                     as listings_active,
  count(*) filter (where not has_availability)                 as listings_inactive,

  -- Active listings rate = active / total * 100
  round(
    100.0 * count(*) filter (where has_availability)
    / nullif(count(*), 0), 2
  )                                                            as active_listings_rate_pct,

  -- Distinct hosts
  count(distinct host_id)                                      as distinct_hosts,

  -- Superhost rate (assignment definition): distinct superhost hosts / distinct hosts * 100
  round(
    100.0 * count(distinct host_id) filter (where host_is_superhost)
    / nullif(count(distinct host_id), 0), 2
  )                                                            as superhost_rate_pct,

  -- Price stats over ACTIVE listings
  min(price)  filter (where has_availability)                  as min_price_active,
  max(price)  filter (where has_availability)                  as max_price_active,
  round(avg(price) filter (where has_availability), 2)         as avg_price_active,
  round((
  percentile_cont(0.5) within group (order by price)
  filter (where has_availability)
)::numeric, 2)                                        as median_price_active,

  -- Average review score over ACTIVE listings
  round(avg(review_scores_rating) filter (where has_availability), 2)
                                                               as avg_review_score_active,

  -- Total Number of stays (sum across active listings)
  sum(stays_active)                                            as total_stays_active,

  -- Average Estimated revenue per active listings
  round(avg(est_revenue_active) filter (where has_availability), 2)
                                                               as avg_est_revenue_per_active_listing,

  -- Month-over-month previous counts (for % change)
  lag(count(*))                           over (
    partition by property_type, room_type, accommodates
    order by month_start
  )                                                            as prev_total_listings,

  lag(count(*) filter (where has_availability)) over (
    partition by property_type, room_type, accommodates
    order by month_start
  )                                                            as prev_active_listings,

  lag(count(*) filter (where not has_availability)) over (
    partition by property_type, room_type, accommodates
    order by month_start
  )                                                            as prev_inactive_listings,

  -- % change MoM for active listings
  round((
    100.0 * (
      count(*) filter (where has_availability)
      - lag(count(*) filter (where has_availability)) over (
          partition by property_type, room_type, accommodates
          order by month_start
        )
    )
    / nullif(
        lag(count(*) filter (where has_availability)) over (
          partition by property_type, room_type, accommodates
          order by month_start
        ), 0
      ))::numeric
  , 2)                                                         as pct_change_active_listings_mom,

  -- % change MoM for inactive listings
  round((
    100.0 * (
      count(*) filter (where not has_availability)
      - lag(count(*) filter (where not has_availability)) over (
          partition by property_type, room_type, accommodates
          order by month_start
        )
    )
    / nullif(
        lag(count(*) filter (where not has_availability)) over (
          partition by property_type, room_type, accommodates
          order by month_start
        ), 0
      ))::numeric
  , 2)                                                         as pct_change_inactive_listings_mom

from derived
group by 1,2,3,4
order by property_type, room_type, accommodates, month_start