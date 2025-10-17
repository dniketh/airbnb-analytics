{{ config(materialized='table') }}

select
  listing_id,
  host_id,
  property_type,
  room_type,
  accommodates,
  dbt_valid_from as record_start_date,
  dbt_valid_to   as record_end_date,
  (dbt_valid_to is null) as is_current
from {{ ref('listing_core_snapshot') }}