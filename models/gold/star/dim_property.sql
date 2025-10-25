{{ config(materialized='table') }}

with s as (
  select
    listing_id,
    property_type,
    room_type,
    accommodates,
    dbt_valid_from::date as snap_date   -- from snapshot scraped_date (date)
  from {{ ref('property_snapshot') }}
),
prop_scd as (
  select
    listing_id,
    property_type,
    room_type,
    accommodates,
    date_trunc('month', snap_date)::timestamp as record_start_date,
    (date_trunc('month', snap_date) + interval '1 month')::timestamp as record_end_date
  from s
),
-- Ensure only one row per (listing_id, record_start_date) i.e only record (latest if there is multiple) of property details in each load
dedup as (
  select distinct on (listing_id, record_start_date)
    listing_id,
    property_type,
    room_type,
    accommodates,
    record_start_date,
    record_end_date
  from prop_scd
  order by listing_id, record_start_date, property_type, room_type
)
select *
from dedup
order by listing_id, record_start_date