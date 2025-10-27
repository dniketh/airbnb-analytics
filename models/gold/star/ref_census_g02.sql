{{ config(materialized='table') }}
select *
from {{ ref('silver_census_g02_clean') }}