{{ config(materialized='table') }}

select
  listing_id,
  host_id,
  scraped_date::timestamp as record_ts,
  scraped_date::date as record_date,
  price,
  availability_30,
  (30 - availability_30) as estimated_stays,
  (30 - availability_30) * price as estimated_revenue,
  has_availability,
  number_of_reviews,
  review_scores_rating
from {{ ref('silver_listing') }}