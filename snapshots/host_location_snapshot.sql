{% snapshot host_location_snapshot %}
{{
  config(
    target_schema = 'silver_snapshots',
    unique_key    = 'host_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}
select
  host_id,
  host_neighbourhood as suburb,
  lga_code,
  lga_name,
  scraped_date
from {{ ref('silver_host_with_lga') }}
{% endsnapshot %}