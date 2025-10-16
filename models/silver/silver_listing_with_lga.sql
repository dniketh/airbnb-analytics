{{ config(materialized='table') }}

with l as (
  select * from {{ ref('silver_listing') }}
),
map as (
  select suburb_name, lga_code
  from {{ ref('silver_lga_suburb_clean') }}
),
lga as (
  select lga_code, lga_name
  from {{ ref('silver_lga') }}
)
select
  l.listing_id,
  l.host_id,
  l.property_type,
  l.room_type,
  l.accommodates,
  l.listing_neighbourhood as suburb,
  lg.lga_code,
  lg.lga_name,
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
  l.month,
  l.scraped_date
from l
left join map m  on m.suburb_name = l.listing_neighbourhood
left join lga lg on lg.lga_code    = m.lga_code