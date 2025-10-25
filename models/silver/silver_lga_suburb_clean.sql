{{ config(materialized='table') }}

with raw as (
  select
    lower(trim(nullif(lga_name, '')))      as lga_name_raw,
    lower(trim(nullif(suburb_name, '')))   as suburb_name_raw
  from bronze.lga_suburb
),
filtered as (
  select
    lga_name_raw,
    suburb_name_raw
  from raw
  where lga_name_raw    is not null
    and suburb_name_raw is not null
)
select distinct
  lga_name_raw    as lga_name,
  suburb_name_raw as suburb_name
from filtered
order by lga_name, suburb_name