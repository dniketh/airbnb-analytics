{{ config(materialized='table') }}
-- listing neighbourhood as a dimension
with src as (
  select distinct lower(trim(listing_neighbourhood))::text as suburb_name
  from {{ ref('silver_listing') }}
  where listing_neighbourhood is not null
)
select
  {{ dbt_utils.generate_surrogate_key(['suburb_name']) }} as suburb_id,
  suburb_name
from src