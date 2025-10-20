{{ config(materialized='view') }}

with s as (
  select
    host_id,
    host_name,
    host_is_superhost,
    host_neighbourhood,
    host_since,
    dbt_valid_from::timestamp as record_start_date,
    dbt_valid_to::timestamp   as raw_record_end_date
  from {{ ref('host_snapshot') }}
),

ordered as (
  select
    s.*,
    lead(record_start_date) over (
      partition by host_id order by record_start_date
    ) as next_start
  from s
)

select
  host_id,
  host_name,
  host_is_superhost,
  host_neighbourhood,
  host_since,
  record_start_date,
  coalesce(next_start, timestamp '9999-12-31 00:00:00') as record_end_date,
  (next_start is null) as is_current
from ordered
order by host_id, record_start_date