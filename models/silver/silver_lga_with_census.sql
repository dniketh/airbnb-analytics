{{ config(materialized='table') }}

with lga_codes as (
  select lga_code, lga_name
  from {{ ref('silver_lga_code_clean') }}
),
g01 as (
  select
    lga_code,
    population_total,
    age_0_14_p,
    age_15_24_p,
    age_25_44_p,
    age_45_64_p,
    age_65_plus_p
  from {{ ref('silver_census_g01_clean') }}
),
g02 as (
  select
    lga_code,
    median_age_persons,
    average_household_size,
    median_mortgage_repay_monthly,
    median_rent_weekly,
    median_personal_income_weekly
  from {{ ref('silver_census_g02_clean') }}
)
select
  c.lga_code,
  c.lga_name,
  g02.median_age_persons,
  g02.average_household_size,
  g02.median_mortgage_repay_monthly,
  g02.median_rent_weekly,
  g02.median_personal_income_weekly,
  g01.population_total,
  g01.age_0_14_p,
  g01.age_15_24_p,
  g01.age_25_44_p,
  g01.age_45_64_p,
  g01.age_65_plus_p
from lga_codes c
left join g02 on g02.lga_code = c.lga_code
left join g01 on g01.lga_code = c.lga_code