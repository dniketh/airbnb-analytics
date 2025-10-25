{{ config(materialized='view') }}

with base as (
  select
    f.listing_id,
    f.host_id,
    f.price,
    f.has_availability::boolean                 as has_availability,
    f.availability_30::int                      as availability_30,
    f.review_scores_rating::numeric             as review_scores_rating,
    d.scraped_date::date                        as recorded_date,   -- <- from dim_date via date_id
    d.month_start::date                         as month_start
  from {{ ref('fact_listings') }} f
  left join {{ ref('dim_date') }} d
    on d.date_id = f.date_id
),
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
    and b.recorded_date <  coalesce(dl.record_end_date, '9999-12-31'::date)
),
host_joined as (
  select
    l.*,
    coalesce(dh.host_is_superhost, false) as host_is_superhost
  from property_scd l
  left join {{ ref('dim_host') }} dh
    on  dh.host_id = l.host_id
    and l.recorded_date >= dh.record_start_date
    and l.recorded_date <  coalesce(dh.record_end_date, '9999-12-31'::date)
),
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
    host_is_superhost,
    case when has_availability then greatest(0, 30 - availability_30) else 0 end                                       as stays_active,
    case when has_availability then (greatest(0, 30 - availability_30) * price) else 0 end::numeric(14,2)               as est_revenue_active
  from host_joined
),
agg as (
  select
    month_start,
    property_type,
    room_type,
    accommodates,

    count(distinct listing_id)                                                      as listings_total,
    count(distinct listing_id) filter (where has_availability)                      as listings_active,
    count(distinct listing_id) filter (where not has_availability)                  as listings_inactive,

    count(distinct host_id)                                                         as distinct_hosts,
    count(distinct host_id) filter (where host_is_superhost)                        as superhost_hosts,

    min(price)  filter (where has_availability)                                     as min_price_active,
    max(price)  filter (where has_availability)                                     as max_price_active,
    round( (avg(price) filter (where has_availability))::numeric, 2)                as avg_price_active,
    round( (percentile_cont(0.5) within group (order by price)
              filter (where has_availability))::numeric, 2)                         as median_price_active,

    round( (avg(review_scores_rating) filter (where has_availability))::numeric, 2) as avg_review_score_active,

    sum(stays_active)                                                               as total_stays_active,
    round( (avg(est_revenue_active) filter (where has_availability))::numeric, 2)   as avg_est_revenue_per_active_listing
  from derived
  group by 1,2,3,4
)
select
  month_start,
  property_type,
  room_type,
  accommodates,

  round( (100.0 * listings_active / nullif(listings_total, 0))::numeric, 2)         as active_listings_rate_pct,

  distinct_hosts,
  round( (100.0 * superhost_hosts / nullif(distinct_hosts, 0))::numeric, 2)        as superhost_rate_pct,
  total_stays_active,
  min_price_active,
  max_price_active,
  avg_price_active,
  median_price_active,
  avg_review_score_active,
  avg_est_revenue_per_active_listing,
  -- % change MoM (active)
  round((
    100.0 * (
      listings_active
      - lag(listings_active) over (partition by property_type, room_type, accommodates order by month_start)
    ) / nullif(
      lag(listings_active) over (partition by property_type, room_type, accommodates order by month_start), 0
    )
  )::numeric, 2)                                                                     as pct_change_active_listings_mom,

  -- % change MoM (inactive)
  round((
    100.0 * (
      listings_inactive
      - lag(listings_inactive) over (partition by property_type, room_type, accommodates order by month_start)
    ) / nullif(
      lag(listings_inactive) over (partition by property_type, room_type, accommodates order by month_start), 0
    )
  )::numeric, 2)                                                                     as pct_change_inactive_listings_mom

from agg
order by property_type, room_type, accommodates, month_start