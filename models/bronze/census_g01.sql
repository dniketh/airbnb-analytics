{{ config(
    alias='census_g01',
    materialized='table'
) }}

select *
from {{ source('bronze', 'census_g01') }}