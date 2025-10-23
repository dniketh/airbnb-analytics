{{ config(
    alias='lga_suburb',
    materialized='table'
) }}

select *
from {{ source('bronze', 'lga_suburb') }}