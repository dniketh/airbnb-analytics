{{ config(materialized='view') }}

with src as (
  select * from {{ source('bronze','airbnb_raw') }}
)
select
  cast(listing_id as bigint)                        as listing_id,
  cast(host_id as bigint)                           as host_id,
  cast(scrape_id as bigint)                         as scrape_id,
  cast(scraped_date as date)                        as scraped_date,

  nullif(trim(host_name), '')                       as host_name,
  case lower(host_is_superhost)
    when 't' then true when 'f' then false else null end
                                                    as host_is_superhost,
  coalesce(nullif(trim(lower(host_neighbourhood)), ''), 'unknown') as host_neighbourhood,
  nullif(trim(host_since), '')                      as host_since_raw,

  coalesce(nullif(trim(lower(listing_neighbourhood)), ''), 'unknown') as listing_neighbourhood,
  coalesce(nullif(trim(lower(property_type)), ''), 'unknown')         as property_type,
  coalesce(nullif(trim(lower(room_type)), ''), 'unknown')             as room_type,
  cast(accommodates as int)                                          as accommodates,

  cast(price as numeric(12,2))                      as price,
  case lower(has_availability)
    when 't' then true when 'f' then false else null end
                                                    as has_availability,
  cast(availability_30 as int)                      as availability_30,
  cast(number_of_reviews as int)                    as number_of_reviews,
  cast(review_scores_rating as numeric(5,2))        as review_scores_rating,
  cast(review_scores_accuracy as numeric(5,2))      as review_scores_accuracy,
  cast(review_scores_cleanliness as numeric(5,2))   as review_scores_cleanliness,
  cast(review_scores_checkin as numeric(5,2))       as review_scores_checkin,
  cast(review_scores_communication as numeric(5,2)) as review_scores_communication,
  cast(review_scores_value as numeric(5,2))         as review_scores_value
from src
where listing_id is not null or host_id is not null