{{ config(materialized='view') }}

with base as (
  select
    f.listing_id,
    f.host_id,
    f.suburb_id,
    f.lga_code,
    f.price,
    f.has_availability,
    f.availability_30,
    f.number_of_reviews,
    f.review_scores_rating,
    f.recorded_date,
    f.month_start
  from {{ ref('fact_listings') }} f
),
host_scd as (
  select
    b.*,
    h.host_is_superhost
  from base b
  left join {{ ref('dim_host') }} h
    on  h.host_id = b.host_id
    and b.recorded_date >= h.record_start_date
    and b.recorded_date <  coalesce(h.record_end_date, '9999-12-31'::timestamp)
),
with_suburb as (
  select
    s.suburb_name,
    t.*
  from host_scd t
  left join {{ ref('dim_suburb') }} s
    on s.suburb_id = t.suburb_id
),
derived as (
  select
    suburb_name,
    month_start,
    listing_id,
    host_id,
    price,
    has_availability,
    availability_30,
    review_scores_rating,
    coalesce(host_is_superhost, false) as host_is_superhost,
    case when has_availability then greatest(0, 30 - availability_30) else 0 end as stays_active,
    case when has_availability then (greatest(0, 30 - availability_30) * price) else 0 end::numeric(14,2) as est_revenue_active
  from with_suburb
)

select
  month_start,
  suburb_name,

  -- totals
  count(*)                                                as listings_total,
  count(*) filter (where has_availability)                as listings_active,
  count(*) filter (where not has_availability)            as listings_inactive,

  -- rates
  round( (100.0 * count(*) filter (where has_availability)
         / nullif(count(*), 0))::numeric, 2)              as active_listing_rate_pct,

  -- hosts & superhost rate (host-level)
  count(distinct host_id)                                 as distinct_hosts,
  round( (100.0 * count(distinct host_id) filter (where host_is_superhost)
         / nullif(count(distinct host_id), 0))::numeric, 2) as superhost_rate_pct,

  -- price stats over active
  min(price)  filter (where has_availability)             as min_price_active,
  max(price)  filter (where has_availability)             as max_price_active,
  round(avg(price) filter (where has_availability), 2)    as avg_price_active,
  round((
  percentile_cont(0.5) within group (order by price)
  filter (where has_availability)
)::numeric, 2)                                        as median_price_active,

  -- reviews over active
  round(avg(review_scores_rating) filter (where has_availability), 2)
                                                        as avg_review_score_active,

  -- stays & revenue
  sum(stays_active)                                       as total_stays_active,
  round(sum(est_revenue_active), 2)                       as total_est_revenue_active,
  round(sum(est_revenue_active) / nullif(count(distinct host_id), 0), 2)
                                                        as est_revenue_per_host,

  -- MoM helpers (previous month counts)
  lag(count(*) filter (where has_availability))
    over (partition by suburb_name order by month_start)  as prev_active_listings,
  lag(count(*) filter (where not has_availability))
    over (partition by suburb_name order by month_start)  as prev_inactive_listings,

  -- MoM % change: ACTIVE
  round((
    100.0 * (
      count(*) filter (where has_availability)
      - lag(count(*) filter (where has_availability))
          over (partition by suburb_name order by month_start)
    )
    / nullif(
        lag(count(*) filter (where has_availability))
          over (partition by suburb_name order by month_start), 0
      )
  )::numeric, 2)                                          as pct_change_active_listings,

  -- MoM % change: INACTIVE
  round((
    100.0 * (
      count(*) filter (where not has_availability)
      - lag(count(*) filter (where not has_availability))
          over (partition by suburb_name order by month_start)
    )
    / nullif(
        lag(count(*) filter (where not has_availability))
          over (partition by suburb_name order by month_start), 0
      )
  )::numeric, 2)                                          as pct_change_inactive_listings

from derived
group by 1,2
order by month_start, suburb_name