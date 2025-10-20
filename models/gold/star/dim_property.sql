with snaps as (
  select
    listing_id,
    property_type, room_type, accommodates,
    dbt_valid_from::timestamp as record_start_date,
    dbt_valid_to::timestamp   as raw_record_end_date
  from silver.property_snapshot
),
ordered as (
  select s.*,
         lead(record_start_date) over (partition by listing_id order by record_start_date) as next_start
  from snaps s
)
select
  listing_id, property_type, room_type, accommodates,
  record_start_date,
  coalesce(next_start, timestamp '9999-12-31 00:00:00') as record_end_date
from ordered