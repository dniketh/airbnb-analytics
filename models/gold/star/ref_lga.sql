{{ config(materialized='table') }}

select distinct
  lga_code::text                  as lga_code,
  lower(trim(lga_name))::text     as lga_name,
  lower(trim(suburb_name))::text  as lga_suburb
from {{ ref('silver_lga') }}
where lga_code is not null
  and suburb_name is not null