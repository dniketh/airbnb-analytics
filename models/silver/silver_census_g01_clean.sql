{{ config(materialized='table') }}

with src as (
  select * from {{ ref('census_g01') }}
),
clean as (
  select
    lpad(regexp_replace(lga_code_2016::text, '\D', '', 'g'), 5, '0') as lga_code,

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
select * from clean
where lga_code is not null