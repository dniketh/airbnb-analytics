{{ config(materialized='table') }}

select
  lga_code,
  lga_name,
  median_age_persons,
  average_household_size,
  median_mortgage_repay_monthly,
  median_rent_weekly,
  median_personal_income_weekly,
  population_total,
  age_0_14_p,
  age_15_24_p,
  age_25_44_p,
  age_45_64_p,
  age_65_plus_p
from {{ ref('silver_lga_with_census') }}