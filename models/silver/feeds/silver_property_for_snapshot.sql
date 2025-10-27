{{ config(materialized='view') }}
-- feeder for propery snapshot
with base as (
  select
    listing_id,
    host_id,
    coalesce(nullif(trim(lower(property_type)), ''), 'unknown')       as property_type,
    coalesce(nullif(trim(lower(room_type)), ''), 'unknown')           as room_type,
    accommodates::int                                                 as accommodates,
    coalesce(nullif(trim(lower(listing_neighbourhood)), ''), 'unknown') as listing_neighbourhood,
    scraped_date::date                                           ,
    date_trunc('month', scraped_date)::date                           as month_start
  from {{ ref('silver_listing') }}
  where listing_id is not null
),--making sure latest record in that month is given to snapshot
ranked as (
  select
    *,
    row_number() over (
      partition by listing_id, month_start
      order by scraped_date desc
    ) as rn
  from base
)
select
  listing_id,
  host_id,
  property_type,
  room_type,
  accommodates,
  listing_neighbourhood,
  scraped_date
from ranked
where rn = 1