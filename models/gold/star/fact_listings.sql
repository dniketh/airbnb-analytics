{{ config(materialized='table') }}

with src as (
  select
    listing_id::bigint                        as listing_id,
    host_id::bigint                           as host_id,
    lower(trim(listing_neighbourhood))        as listing_neighbourhood,
    price::numeric                            as price,
    has_availability::boolean                 as has_availability,
    availability_30::int                      as availability_30,
    number_of_reviews::int                    as number_of_reviews,
    review_scores_rating::numeric             as review_scores_rating,
    scraped_date::date                        as recorded_date,
    to_char(scraped_date, 'YYYYMMDD')::int    as date_id,         -- FK → dim_date.date_id
    date_trunc('month', scraped_date)::date   as month_start
  from {{ ref('silver_airbnb_clean') }}
  where listing_id is not null
),

with_suburb as (
  select
    s.*,
    ds.suburb_id
  from src s
  left join {{ ref('dim_suburb') }} ds
    on ds.suburb_name = s.listing_neighbourhood
)

select
  listing_id,
  host_id,
  suburb_id,
  date_id,
  price,
  has_availability,
  availability_30,
  number_of_reviews,
  review_scores_rating
from with_suburb