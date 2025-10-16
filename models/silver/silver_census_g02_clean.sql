{{ config(materialized='table') }}

with src as (
  select * from {{ source('bronze','census_g02') }}
)
select
  nullif(trim(lower(lga_code_2016)), '')             as lga_code,

  cast(median_age_persons as numeric(5,2))           as median_age_persons,
  cast(average_household_size as numeric(5,2))       as average_household_size,
  cast(median_mortgage_repay_monthly as numeric(10,2)) as median_mortgage_repay_monthly,
  cast(median_rent_weekly as numeric(10,2))          as median_rent_weekly,
  cast(median_tot_prsnl_inc_weekly as numeric(10,2)) as median_personal_income_weekly
from src
where nullif(trim(lga_code_2016), '') is not null