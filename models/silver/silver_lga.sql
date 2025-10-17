{{ config(materialized='table') }}

with raw as (
  select
    coalesce(nullif(trim(lower(lga_name)), ''), 'unknown')    as lga_name_norm,
    coalesce(nullif(trim(lower(suburb_name)), ''), 'unknown') as suburb_name
  from {{ source('bronze','lga_suburb') }}
),

codes as (
  select
    lga_code,                      
    lower(lga_name) as lga_name_norm
  from {{ ref('silver_lga_code_clean') }}
),

joined as (
  select
    r.suburb_name,
    c.lga_code,
    c.lga_name_norm as lga_name
  from raw r
  left join codes c
    on c.lga_name_norm = r.lga_name_norm
)

select distinct suburb_name, lga_code, lga_name
from joined