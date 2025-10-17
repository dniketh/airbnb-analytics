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
  price,
  has_availability,
  availability_30,
  number_of_reviews,
  review_scores_rating,
  review_scores_accuracy,
  review_scores_cleanliness,
  review_scores_checkin,
  review_scores_communication,
  review_scores_value,
  scraped_date
from {{ ref('silver_listing') }}            -- your per-scrape history
{% endsnapshot %}