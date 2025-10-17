{{ config(materialized='table') }}

with l as (
  select
    listing_id,
    host_id,
    property_type,
    room_type,
    accommodates,

    -- normalized join key
    coalesce(nullif(trim(lower(listing_neighbourhood)), ''), 'unknown') as suburb_name,

    -- metrics (raw per scrape)
    price::numeric(12,2)                      as price,
    has_availability,
    availability_30::int                      as availability_30,
    number_of_reviews::int                    as number_of_reviews,
    review_scores_rating::numeric(5,2)        as review_scores_rating,
    review_scores_accuracy::numeric(5,2)      as review_scores_accuracy,
    review_scores_cleanliness::numeric(5,2)   as review_scores_cleanliness,
    review_scores_checkin::numeric(5,2)       as review_scores_checkin,
    review_scores_communication::numeric(5,2) as review_scores_communication,
    review_scores_value::numeric(5,2)         as review_scores_value,

    scraped_date::timestamp as scraped_date   -- exact scrape time (per snapshot)
  from {{ ref('silver_listing') }}
),

-- suburb → LGA map
map as (
  select
    suburb_name,
    lga_code,
    lga_name
  from {{ ref('silver_lga_combined') }}
)

select
  l.listing_id,
  l.host_id,
  l.property_type,
  l.room_type,
  l.accommodates,
  l.suburb_name,
  m.lga_code,
  m.lga_name,
  l.price,
  l.has_availability,
  l.availability_30,
  l.number_of_reviews,
  l.review_scores_rating,
  l.review_scores_accuracy,
  l.review_scores_cleanliness,
  l.review_scores_checkin,
  l.review_scores_communication,
  l.review_scores_value,
  l.scraped_date
from l
left join map m
  on m.suburb_name = l.suburb_name