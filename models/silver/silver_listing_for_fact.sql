{{ config(materialized='table') }}

with src as (
  select
    listing_id,
    host_id,
    scrape_id,
    scraped_date::date as scraped_date,
    host_name,
    host_is_superhost,
    host_neighbourhood,
    host_since,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price::numeric(12,2)                       as price,
    has_availability::boolean                  as has_availability,
    availability_30::int                       as availability_30,
    number_of_reviews::int                     as number_of_reviews,
    review_scores_rating::numeric(5,2)         as review_scores_rating,
    review_scores_accuracy::numeric(5,2)       as review_scores_accuracy,
    review_scores_cleanliness::numeric(5,2)    as review_scores_cleanliness,
    review_scores_checkin::numeric(5,2)        as review_scores_checkin,
    review_scores_communication::numeric(5,2)  as review_scores_communication,
    review_scores_value::numeric(5,2)          as review_scores_value,
    source_file,
    source_month::date as source_month
  from {{ ref('silver_airbnb_clean') }}
  where listing_id is not null
),
scoped as (
  select *
  from src
  where date_trunc('month', scraped_date)::date = source_month -- to remove rows that has scraped_date not matching with the loaded month
),
stamped as (
  select
    s.*,
    date_trunc('month', s.scraped_date)::date as month_start
  from scoped s
),
--    latest record is taken using scraped_date 
latest_per_month as (
  select *
  from (
    select
      st.*,
      row_number() over (
        partition by st.listing_id, st.month_start
        order by st.scraped_date desc
      ) as rn
    from stamped st
  ) x
  where rn = 1
)
select
  listing_id,
  host_id,
  scrape_id,
  scraped_date,
  month_start,
  host_name,
  host_is_superhost,
  host_neighbourhood,
  host_since,
  listing_neighbourhood,
  property_type,
  room_type,
  accommodates,
  price,
  has_availability,
  availability_30,
  number_of_reviews,
  review_scores_rating,
  review_scores_accuracy,
  review_scores_cleanliness,
  review_scores_checkin,
  review_scores_communication,
  review_scores_value
from latest_per_month