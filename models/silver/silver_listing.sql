{{ config(materialized='table') }}

with src as (
  select * from {{ ref('silver_airbnb_clean') }}
)

select
  -- keys / ids
  listing_id,
  host_id,
  -- proprty attributes
  property_type,
  room_type,
  accommodates,
  -- suburb 
  listing_neighbourhood,

  -- metrics (raw per-scrape)
  price::numeric(12,2)                      as price,
  has_availability                          as has_availability,
  availability_30::int                      as availability_30,
  number_of_reviews::int                    as number_of_reviews,
  review_scores_rating::numeric(5,2)        as review_scores_rating,
  review_scores_accuracy::numeric(5,2)      as review_scores_accuracy,
  review_scores_cleanliness::numeric(5,2)   as review_scores_cleanliness,
  review_scores_checkin::numeric(5,2)       as review_scores_checkin,
  review_scores_communication::numeric(5,2) as review_scores_communication,
  review_scores_value::numeric(5,2)         as review_scores_value,

  -- time columns
  scraped_date::timestamp                        as scraped_date,      -- original date 
  date_trunc('month', scraped_date)::date   as month              -- convenience for rollups
from src
where listing_id is not null