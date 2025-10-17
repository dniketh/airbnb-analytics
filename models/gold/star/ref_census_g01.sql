{{ config(materialized='view') }}
select *
from {{ ref('silver_census_g01_clean') }}