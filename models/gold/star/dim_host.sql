{{ config(materialized='table') }}

with s as (
  select
    host_id,
    host_name,
    host_is_superhost,
    host_neighbourhood,
    host_since,
    dbt_valid_from::date as snapshot_date
  from {{ ref('host_snapshot') }}
),
month_windows as (
  select
    host_id,
    host_name,
    host_is_superhost,
    host_neighbourhood,
    host_since,
    date_trunc('month', snapshot_date)::timestamp                         as record_start_date,
    (date_trunc('month', snapshot_date) + interval '1 month')::timestamp  as record_end_date
  from s
),
dedup_same_month as (
  -- If (for any reason) multiple snapshot rows landed in the same month, (to simplify joins and we are only doing monthly data marts)
  select distinct on (host_id, record_start_date)
    host_id,
    host_name,
    host_is_superhost,
    host_neighbourhood,
    host_since,
    record_start_date,
    record_end_date
  from month_windows
  order by host_id, record_start_date
)
select *
from dedup_same_month
order by host_id, record_start_date