{{ config(materialized='table') }}

select
  lga_code::text                as lga_code,   -- PK used by facts & marts
  lower(trim(lga_name))::text   as lga_name
from {{ ref('silver_lga_code_clean') }}
where lga_code is not null
group by 1,2
order by lga_name