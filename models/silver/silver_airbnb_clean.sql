{{ config(materialized='view') }}

with src as (
  select
    -- raw fields
    listing_id, host_id, scrape_id, scraped_date, host_name, host_is_superhost,
    host_neighbourhood, host_since, listing_neighbourhood, property_type,
    room_type, accommodates, price, has_availability, availability_30,
    number_of_reviews, review_scores_rating, review_scores_accuracy,
    review_scores_cleanliness, review_scores_checkin,
    review_scores_communication, review_scores_value,
    -- provenance
    source_file,
    source_month
  from {{ source('bronze','airbnb_raw') }}
),

clean as (
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
    nullif(trim(host_since), '')                      as host_since,

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
    cast(review_scores_value as numeric(5,2))         as review_scores_value,

    -- pass through provenance
    source_file,
    source_month
  from src
  where listing_id is not null or host_id is not null
)

select *
from clean
where
  -- keep rows whose scraped_date falls into the month implied by the file name
  date_trunc('month', scraped_date)::date = source_month

  -- (optional) keep legacy rows that don’t have provenance populated (if any)
  or source_month is null
;