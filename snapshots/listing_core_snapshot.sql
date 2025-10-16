{% snapshot listing_core_snapshot %}
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
  property_type,
  room_type,
  accommodates,
  month,
  scraped_date
from {{ ref('silver_listing') }}
{% endsnapshot %}