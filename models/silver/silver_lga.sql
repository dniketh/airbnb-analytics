{{ config(materialized='table') }}

with raw as (
  select
    lga_name    as lga_name_norm,   -- already lower/trimmed in silver_lga_suburb_clean
    suburb_name                      -- already lower/trimmed
  from {{ ref('silver_lga_suburb_clean') }}
),

-- Cleaned LGA code lookup (name -> code)
codes as (
  select
    lga_code,
    lower(lga_name) as lga_name_norm
  from {{ ref('silver_lga_code_clean') }}
),

-- Attach LGA code to each suburb via cleaned LGA name
joined as (
  select
    r.suburb_name,
    c.lga_code,
    c.lga_name_norm as lga_name
  from raw r
  left join codes c
    on c.lga_name_norm = r.lga_name_norm
)

-- One row per suburb with its LGA code and the cleaned LGA name
select distinct
  suburb_name,
  lga_code,
  lga_name
from joined
order by suburb_name, lga_name