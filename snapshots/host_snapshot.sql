{% snapshot host_snapshot %}
{{
  config(
    target_schema = 'silver',
    unique_key    = 'host_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}
select
  host_id,
  host_name,
  host_is_superhost,
  host_neighbourhood,
  host_since,
  scraped_date
from {{ ref('silver_host') }}
{% endsnapshot %}