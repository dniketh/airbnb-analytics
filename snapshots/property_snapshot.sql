{% snapshot property_snapshot %}
{{
  config(
    target_schema = 'silver',
    unique_key    = 'listing_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}
select
  listing_id,
  property_type,
  room_type,
  accommodates,
  scraped_date
from {{ ref('silver_property_for_snapshot') }}
{% endsnapshot %}