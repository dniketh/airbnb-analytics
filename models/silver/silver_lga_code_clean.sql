{{ config(materialized='table') }}

with src as (
  select
    lga_code,
    lga_name
  from {{ ref('lga_code') }}
),
--just for verification purposes - not really a need 
clean as ( 
  select
    nullif(trim(lower(lga_code)), '') as lga_code,   
    nullif(trim(lower(lga_name)), '') as lga_name
  from src
  where nullif(trim(lga_code), '') is not null
    and nullif(trim(lga_name), '') is not null
)
select distinct
  lga_code,
  lga_name
from clean