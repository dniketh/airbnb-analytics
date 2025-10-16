{% snapshot listing_location_snapshot %}
{{
  config(
    target_schema = 'silver_snapshots',
    unique_key    = 'listing_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}
with base as (
  select
    listing_id,
    host_id,
    suburb,
    lga_code,
    lga_name,
    month,
    scraped_date,
    row_number() over (
      partition by listing_id, month
      order by lga_code nulls last, lga_name
    ) as rn
  from {{ ref('silver_listing_with_lga') }}
)
select
  listing_id,
  host_id,
  suburb,
  lga_code,
  lga_name,
  month,
  scraped_date
from base
where rn = 1
{% endsnapshot %}