{% snapshot host_location_snapshot %}
{{
  config(
    target_schema = 'silver_snapshots',
    unique_key    = 'host_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}
with h as (
  select host_id, host_neighbourhood, scraped_date
  from {{ ref('silver_host') }}
),
map as (
  select suburb_name, lga_code, lga_name
  from {{ ref('silver_lga_suburb_clean') }}
)
select
  h.host_id,
  h.host_neighbourhood as suburb,
  m.lga_code,
  m.lga_name,
  h.scraped_date
from h
left join map m
  on m.suburb_name = h.host_neighbourhood
{% endsnapshot %}