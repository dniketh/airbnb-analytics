{{ config(materialized='table') }}

select
  listing_id,
  suburb,
  lga_code,
  lga_name,
  dbt_valid_from as record_start_date,
  dbt_valid_to   as record_end_date,
  (dbt_valid_to is null) as is_current
from {{ ref('listing_location_snapshot') }}