{{ config(materialized='table') }}

with src as (
  select * from {{ ref('census_g01') }}
),
clean as (
  select
    nullif(trim(lower(src.lga_code_2016)), '')                         as lga_code,

    cast(tot_p_p as bigint)                                        as population_total,
    cast(tot_p_m as bigint)                                        as population_male,
    cast(tot_p_f as bigint)                                        as population_female,

    coalesce(cast(age_0_4_yr_p  as bigint), 0)                     as age_0_4_p,
    coalesce(cast(age_5_14_yr_p as bigint), 0)                     as age_5_14_p,
    coalesce(cast(age_15_19_yr_p as bigint), 0)                    as age_15_19_p,
    coalesce(cast(age_20_24_yr_p as bigint), 0)                    as age_20_24_p,
    coalesce(cast(age_25_34_yr_p as bigint), 0)                    as age_25_34_p,
    coalesce(cast(age_35_44_yr_p as bigint), 0)                    as age_35_44_p,
    coalesce(cast(age_45_54_yr_p as bigint), 0)                    as age_45_54_p,
    coalesce(cast(age_55_64_yr_p as bigint), 0)                    as age_55_64_p,
    coalesce(cast(age_65_74_yr_p as bigint), 0)                    as age_65_74_p,
    coalesce(cast(age_75_84_yr_p as bigint), 0)                    as age_75_84_p,
    coalesce(cast(age_85ov_p    as bigint), 0)                     as age_85_plus_p
  from src
)
select
  lga_code,
  population_total,
  population_male,
  population_female,
  (age_0_4_p + age_5_14_p)                          as age_0_14_p, --bigger age buckets for easier analyis if needed.
  (age_15_19_p + age_20_24_p)                       as age_15_24_p,
  (age_25_34_p + age_35_44_p)                       as age_25_44_p,
  (age_45_54_p + age_55_64_p)                       as age_45_64_p,
  (age_65_74_p + age_75_84_p + age_85_plus_p)       as age_65_plus_p
from clean
where lga_code is not null