{{ config(materialized='table') }}

select
  host_id,
  host_name,
  host_is_superhost,
  host_neighbourhood,
  host_since,
  dbt_valid_from as record_start_date,
  dbt_valid_to   as record_end_date,
  (dbt_valid_to is null) as is_current
from {{ ref('host_snapshot') }}