{{ config(materialized='table') }}

with base as (
  select * from {{ ref('silver_airbnb_clean') }}
),
dedup as (
    select distinct on (listing_id, date_trunc('month', scraped_date))
  listing_id,
  host_id,
  property_type,
  room_type,
  accommodates,
  listing_neighbourhood,
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
  scraped_date::timestamp as scraped_date,  -- ← cast added here
  date_trunc('month', scraped_date)::date as month
from base
where listing_id is not null
order by listing_id, date_trunc('month', scraped_date), scraped_date desc
)
select * from dedup