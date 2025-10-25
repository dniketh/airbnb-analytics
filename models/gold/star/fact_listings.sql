{{ config(materialized='table') }}

with src as (
  select
    listing_id::bigint                 as listing_id,
    host_id::bigint                    as host_id,
    scraped_date::date                 as scraped_date,       -- used only to derive date_id
    lower(trim(listing_neighbourhood)) as listing_neighbourhood,
    price::numeric(12,2)               as price,
    has_availability::boolean          as has_availability,
    availability_30::int               as availability_30,
    number_of_reviews::int             as number_of_reviews,
    review_scores_rating::numeric(5,2) as review_scores_rating
  from {{ ref('silver_listing_for_fact') }}   -- <- use your silver model name here
  where listing_id is not null
),
with_date as (
  select
    s.*,
    d.date_id  -- FK to dim_date
  from src s
  left join {{ ref('dim_date') }} d
    on d.scraped_date = s.scraped_date
),
with_suburb as (
  select
    wd.*,
    ds.suburb_id
  from with_date wd
  left join {{ ref('dim_suburb') }} ds
    on ds.suburb_name = wd.listing_neighbourhood
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