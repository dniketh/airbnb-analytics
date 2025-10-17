{{ config(materialized='table') }}

with src as (
  select
    listing_id,
    host_id,

    -- suburb for FK derivation
    lower(trim(listing_neighbourhood))::text as suburb_name,

    -- metrics (cast & normalize)
    price::numeric(12,2)                      as price,
    (has_availability in ('t','true','1'))    as has_availability,
    availability_30::int                      as availability_30,
    number_of_reviews::int                    as number_of_reviews,
    review_scores_rating::numeric(5,2)        as review_scores_rating,
    review_scores_accuracy::numeric(5,2)      as review_scores_accuracy,
    review_scores_cleanliness::numeric(5,2)   as review_scores_cleanliness,
    review_scores_checkin::numeric(5,2)       as review_scores_checkin,
    review_scores_communication::numeric(5,2) as review_scores_communication,
    review_scores_value::numeric(5,2)         as review_scores_value,

    scraped_date::timestamp                   as scraped_date
  from {{ ref('silver_listing') }}
  where listing_id is not null
),
with_lga as (
  select
    s.*,
    coalesce(sl_sub.lga_code, lc.lga_code::text) as lga_code
  from src s
  -- 1) normal: neighbourhood is a suburb
  left join {{ ref('silver_lga') }} sl_sub
    on sl_sub.suburb_name = s.suburb_name
  -- 2) fallback: neighbourhood is actually an LGA name
  left join {{ ref('silver_lga_code_clean') }} lc
    on lower(trim(lc.lga_name)) = s.suburb_name
),
final as (
  select
    listing_id,
    host_id,

    -- FK to dim_suburb (keep dims independent)
    {{ dbt_utils.generate_surrogate_key(['suburb_name']) }} as suburb_id,

    -- FK to dim_lga (we’re using lga_code as PK in the dim)
    coalesce(lga_code, 'unknown')                  as lga_code,

    -- metrics
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value,

    -- time
    scraped_date as recorded_date,
    date_trunc('month', scraped_date)::date        as month_start,

    -- optional derived convenience metrics (ok to compute here or in DM)
    case when has_availability then greatest(0, 30 - availability_30) else 0 end as stays_active,
    case when has_availability then (greatest(0, 30 - availability_30) * price)::numeric(14,2) else 0 end
      as est_revenue_active
  from with_lga
)
select * from final