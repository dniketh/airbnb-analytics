{% snapshot listing_location_snapshot %}
{{
  config(
    target_schema = 'silver_snapshots',
    unique_key    = 'listing_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}
select
  listing_id,
  host_id,
  suburb_name,
  lga_code,
  lga_name,
  scraped_date
from {{ ref('silver_listing_with_lga') }}
{% endsnapshot %}