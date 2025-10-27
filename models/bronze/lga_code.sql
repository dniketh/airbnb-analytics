{{ config(
    alias='lga_code',
    materialized='table'
) }}

select *
from {{ source('bronze', 'lga_code') }}