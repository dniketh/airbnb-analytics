{{ config(materialized='view') }}

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
with_date as (
  select
    b.*,
    d.scraped_date,
    d.month_start
  from base b
  left join {{ ref('dim_date') }} d
    on b.date_id = d.date_id
),
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
with_suburb as (
  select
    s.suburb_name as listing_neighbourhood,
    hj.*
  from host_joined hj
  left join {{ ref('dim_suburb') }} s
    on s.suburb_id = hj.suburb_id
),
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
per_listing_month as ( -- this is done in case same listing id shows more than once in the same month (handled in silver by taking only latest, but just a fall back)
  select
    listing_neighbourhood,
    month_start,
    listing_id,
    host_id,

    bool_or(has_availability)                                                     as was_active_in_month,

    bool_or(host_is_superhost)                                                    as is_superhost_in_month,

    avg(price) filter (where has_availability)::numeric(14,2)                     as price_month_avg_active, -- this would be just the price if no listing occurs twice in the same month

    avg(review_scores_rating) filter (where has_availability)::numeric(6,2)       as review_score_month_avg_active, -- same here
    sum(stays_active)                                                             as stays_active_sum,

    sum(est_revenue_active)::numeric(14,2)                                        as est_revenue_active_sum
  from derived
  group by listing_neighbourhood, month_start, listing_id, host_id
)
select
  month_start,
  listing_neighbourhood,
  round((
    100.0 * count(distinct listing_id) filter (where was_active_in_month)
    / nullif(count(distinct listing_id), 0)
  )::numeric, 2)                                                                  as active_listing_rate_pct,

  count(distinct host_id)                                                         as distinct_hosts,
  round((
    100.0 * count(distinct host_id) filter (where is_superhost_in_month)
    / nullif(count(distinct host_id), 0)
  )::numeric, 2)                                                                  as superhost_rate_pct,

  min(price_month_avg_active)                                                     as min_price_active,
  max(price_month_avg_active)                                                     as max_price_active,
  round(avg(price_month_avg_active), 2)::numeric(14,2)                            as avg_price_active,
  round((
    (percentile_cont(0.5) within group (order by price_month_avg_active))::numeric
  ), 2)::numeric(14,2)                                                            as median_price_active,

  round(avg(review_score_month_avg_active), 2)::numeric(6,2)                      as avg_review_score_active,
  sum(stays_active_sum)                                                           as total_stays_active,
  round((
    (sum(est_revenue_active_sum)
     / nullif(count(distinct listing_id) filter (where was_active_in_month), 0)
    )::numeric
  ), 2)::numeric(14,2)                                                            as avg_est_revenue_per_active_listing,

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