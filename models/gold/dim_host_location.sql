{{ config(materialized='table') }}

select
  host_id,
  suburb,
  lga_code,
  lga_name,
  dbt_valid_from as record_start_date,
  dbt_valid_to   as record_end_date,
  (dbt_valid_to is null) as is_current
from {{ ref('host_location_snapshot') }}