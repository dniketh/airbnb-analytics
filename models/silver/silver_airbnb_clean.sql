{{ config(materialized='table') }}

with src as (
  select
    -- raw fields
    listing_id, host_id, scrape_id, scraped_date, host_name, host_is_superhost,
    host_neighbourhood, host_since, listing_neighbourhood, property_type,
    room_type, accommodates, price, has_availability, availability_30,
    number_of_reviews, review_scores_rating, review_scores_accuracy,
    review_scores_cleanliness, review_scores_checkin,
    review_scores_communication, review_scores_value,
    source_file,
    source_month
  from {{ ref('airbnb') }}
),

clean as (
  select
    -- ids & dates
    nullif(trim(listing_id), '')::bigint           as listing_id,
    nullif(trim(host_id), '')::bigint              as host_id,
    nullif(trim(scrape_id), '')::bigint           as scrape_id,
    cast(scraped_date as date)                     as scraped_date,

    -- host attrs
    nullif(trim(host_name), '')                    as host_name,
    case lower(host_is_superhost)
      when 't' then true when 'f' then false else null end
                                                   as host_is_superhost,
    coalesce(nullif(trim(lower(host_neighbourhood)), ''), 'unknown') as host_neighbourhood,
    nullif(trim(host_since), '')                   as host_since,

    -- listing attrs
    coalesce(nullif(trim(lower(listing_neighbourhood)), ''), 'unknown') as listing_neighbourhood,
    coalesce(nullif(trim(lower(property_type)), ''), 'unknown')         as property_type,
    coalesce(nullif(trim(lower(room_type)), ''), 'unknown')             as room_type,
    nullif(trim(accommodates), '')::int           as accommodates,

    -- metrics
    nullif(trim(price), '')::numeric(12,2)        as price,
    case lower(has_availability)
      when 't' then true when 'f' then false else null end
                                                   as has_availability,
    nullif(trim(availability_30), '')::int        as availability_30,
    nullif(trim(number_of_reviews), '')::int      as number_of_reviews,
    nullif(trim(review_scores_rating), '')::numeric(5,2)        as review_scores_rating,
    nullif(trim(review_scores_accuracy), '')::numeric(5,2)      as review_scores_accuracy,
    nullif(trim(review_scores_cleanliness), '')::numeric(5,2)   as review_scores_cleanliness,
    nullif(trim(review_scores_checkin), '')::numeric(5,2)       as review_scores_checkin,
    nullif(trim(review_scores_communication), '')::numeric(5,2) as review_scores_communication,
    nullif(trim(review_scores_value), '')::numeric(5,2)         as review_scores_value,

    -- provenance
    source_file,
    source_month
  from src
  -- require at least one id to be present (and not just empty text)
  where nullif(trim(listing_id), '') is not null
     or nullif(trim(host_id), '')    is not null
)

select *
from clean
where
    date_trunc('month', scraped_date)::date = source_month