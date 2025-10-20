{{ config(materialized='view') }}

-- 1) Base facts
with base as (
  select
    f.listing_id,
    f.host_id,
    f.suburb_id,
    f.date_id,
    f.price,
    f.has_availability,
    f.availability_30,
    f.review_scores_rating,
    f.number_of_reviews
  from {{ ref('fact_listings') }} f
),
-- 2) Date attributes (for SCD2 joins + month grouping)
with_date as (
  select
    b.*,
    d.scraped_date,
    d.month_start
  from base b
  left join {{ ref('dim_date') }} d
    on b.date_id = d.date_id
),
-- 3) Time-valid SCD2 join to HOST for every scrape row
host_joined as (
  select
    wd.*,
    coalesce(h.host_is_superhost, false) as host_is_superhost
  from with_date wd
  left join {{ ref('dim_host') }} h
    on  h.host_id = wd.host_id
    and wd.scraped_date::timestamp >= h.record_start_date
    and wd.scraped_date::timestamp <  coalesce(h.record_end_date,'9999-12-31'::timestamp)
),
-- 4) Suburb name (listing_neighbourhood)
with_suburb as (
  select
    s.suburb_name as listing_neighbourhood,
    hj.*
  from host_joined hj
  left join {{ ref('dim_suburb') }} s
    on s.suburb_id = hj.suburb_id
),
-- 5) Per-row derivations (keep ALL scrapes within the month)
derived as (
  select
    listing_neighbourhood,
    month_start,
    listing_id,
    host_id,
    price::numeric(14,2)                 as price,
    has_availability::boolean            as has_availability,
    availability_30::int                 as availability_30,
    review_scores_rating::numeric(6,2)   as review_scores_rating,
    host_is_superhost,
    case when has_availability then greatest(0, 30 - availability_30) else 0 end                                        as stays_active,
    case when has_availability then (greatest(0, 30 - availability_30) * price) else 0 end::numeric(14,2)               as est_revenue_active
  from with_suburb
),
-- 6) Collapse ALL scrapes in the month → ONE row per (listing, month, neighbourhood)
per_listing_month as (
  select
    listing_neighbourhood,
    month_start,
    listing_id,
    host_id,

    -- Active in month if ANY scrape was active
    bool_or(has_availability)                                                     as was_active_in_month,

    -- Host superhost in month if ANY scrape had superhost=true
    bool_or(host_is_superhost)                                                    as is_superhost_in_month,

    -- Per-listing monthly avg price over ACTIVE scrapes
    avg(price) filter (where has_availability)::numeric(14,2)                     as price_month_avg_active,

    -- Per-listing monthly avg review score over ACTIVE scrapes
    avg(review_scores_rating) filter (where has_availability)::numeric(6,2)       as review_score_month_avg_active,

    -- Additive within month (sum across scrapes)
    sum(stays_active)                                                             as stays_active_sum,
    sum(est_revenue_active)::numeric(14,2)                                        as est_revenue_active_sum
  from derived
  group by listing_neighbourhood, month_start, listing_id, host_id
)
-- 7) Final aggregation per neighbourhood & month
select
  month_start,
  listing_neighbourhood,

  -- totals (distinct listings via per_listing_month grain)
  count(distinct listing_id)                                                      as listings_total,
  count(distinct listing_id) filter (where was_active_in_month)                   as listings_active,
  count(distinct listing_id) filter (where not was_active_in_month)               as listings_inactive,

  -- Active listings rate
  round((
    100.0 * count(distinct listing_id) filter (where was_active_in_month)
    / nullif(count(distinct listing_id), 0)
  )::numeric, 2)                                                                  as active_listing_rate_pct,

  -- Distinct hosts & superhost rate (host-level, month rollup)
  count(distinct host_id)                                                         as distinct_hosts,
  round((
    100.0 * count(distinct host_id) filter (where is_superhost_in_month)
    / nullif(count(distinct host_id), 0)
  )::numeric, 2)                                                                  as superhost_rate_pct,

  -- Price stats over ACTIVE listings (use listing-level monthly averages)
  min(price_month_avg_active)                                                     as min_price_active,
  max(price_month_avg_active)                                                     as max_price_active,
  round(avg(price_month_avg_active), 2)::numeric(14,2)                            as avg_price_active,
  round((
    (percentile_cont(0.5) within group (order by price_month_avg_active))::numeric
  ), 2)::numeric(14,2)                                                            as median_price_active,

  -- Average review score over ACTIVE listings (listing-level monthly averages)
  round(avg(review_score_month_avg_active), 2)::numeric(6,2)                      as avg_review_score_active,

  -- Stays & revenue totals + average per active listing
  sum(stays_active_sum)                                                           as total_stays_active,
  round(sum(est_revenue_active_sum), 2)::numeric(14,2)                            as total_est_revenue_active,
  round((
    (sum(est_revenue_active_sum)
     / nullif(count(distinct listing_id) filter (where was_active_in_month), 0)
    )::numeric
  ), 2)::numeric(14,2)                                                            as avg_est_revenue_per_active_listing,

  -- MoM helpers (based on active/inactive listing counts)
  lag(count(distinct listing_id) filter (where was_active_in_month))
    over (partition by listing_neighbourhood order by month_start)
    as prev_active_listings,

  lag(count(distinct listing_id) filter (where not was_active_in_month))
    over (partition by listing_neighbourhood order by month_start)
    as prev_inactive_listings,

  -- % change active
  round((
    100.0 * (
      count(distinct listing_id) filter (where was_active_in_month)
      - lag(count(distinct listing_id) filter (where was_active_in_month))
          over (partition by listing_neighbourhood order by month_start)
    )
    / nullif(
        lag(count(distinct listing_id) filter (where was_active_in_month))
          over (partition by listing_neighbourhood order by month_start),
        0
      )
  )::numeric, 2)::numeric(6,2)                                                    as pct_change_active_listings,

  -- % change inactive
  round((
    100.0 * (
      count(distinct listing_id) filter (where not was_active_in_month)
      - lag(count(distinct listing_id) filter (where not was_active_in_month))
          over (partition by listing_neighbourhood order by month_start)
    )
    / nullif(
        lag(count(distinct listing_id) filter (where not was_active_in_month))
          over (partition by listing_neighbourhood order by month_start),
        0
      )
  )::numeric, 2)::numeric(6,2)                                                    as pct_change_inactive_listings

from per_listing_month
group by month_start, listing_neighbourhood
order by listing_neighbourhood, month_start