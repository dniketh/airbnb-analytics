{{ config(
    alias='airbnb',
    materialized='table'
) }}

select *
from {{ source('bronze', 'airbnb_raw') }}