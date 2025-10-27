{{ config(
    alias='census_g02',
    materialized='table'
) }}

select *
from {{ source('bronze', 'census_g02') }}