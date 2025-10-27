{{ config(materialized='view') }}


with base as (
  select
    f.listing_id,
    f.host_id,
    f.date_id,
    f.price,
    f.has_availability,
    f.availability_30
  from {{ ref('fact_listings') }} f
),
with_date as (
  select
    b.*,
    d.scraped_date::date as scraped_date,
    d.month_start::date  as month_start
  from base b
  left join {{ ref('dim_date') }} d
    on b.date_id = d.date_id
),
host_joined as (
  select
    wd.*,
    h.host_neighbourhood,
    lower(trim(h.host_neighbourhood)) as host_neigh_norm, -- incase join fails because of name capitalization
    coalesce(h.host_is_superhost, false) as host_is_superhost
  from with_date wd
  left join {{ ref('dim_host') }} h
    on  h.host_id = wd.host_id
    and wd.scraped_date::timestamp >= h.record_start_date
    and wd.scraped_date::timestamp <  coalesce(h.record_end_date, timestamp '9999-12-31')
),
--- this is added because we have overseas and unknown (added in silver layer)  in host neighbourhood value
overrides as (
  select 'unknown'  as key_norm, null::text as lga_code, 'Unknown'  as lga_name
  union all
  select 'overseas', null::text,            'Overseas'
),
-- To map host_neighbourhood -> main LGA (lga suburb join first to get main LGA, if that fails see if we can get LGA name directly, else use override )
mapped_lga as (
  select
    hj.*,
    r_sub.lga_code as code_by_suburb,
    r_sub.lga_name as name_by_suburb,
    r_name.lga_code as code_by_name,
    r_name.lga_name as name_by_name,
    o.lga_code      as code_by_override,
    o.lga_name      as name_by_override,
    coalesce(r_sub.lga_code, r_name.lga_code, o.lga_code)::text as host_neighbourhood_lga_code,
    coalesce(r_sub.lga_name, r_name.lga_name, o.lga_name)       as host_neighbourhood_lga
  from host_joined hj
  left join {{ ref('ref_lga') }} r_sub
    on hj.host_neigh_norm = lower(trim(r_sub.lga_suburb))
  left join {{ ref('ref_lga') }} r_name
    on r_sub.lga_code is null
   and hj.host_neigh_norm = lower(trim(r_name.lga_name))
  left join overrides o
    on hj.host_neigh_norm = o.key_norm
),
derived as (
  select
    host_neighbourhood_lga,
    host_neighbourhood_lga_code,
    month_start,
    listing_id,
    host_id,
    has_availability::boolean                 as has_availability,
    availability_30::int                      as availability_30,
    price::numeric(14,2)                      as price,
    case when has_availability
         then greatest(0, 30 - availability_30) * price
         else 0 end::numeric(14,2)           as est_revenue_active
  from mapped_lga
),
per_listing_month as (
  select
    host_neighbourhood_lga,
    host_neighbourhood_lga_code,
    month_start,
    listing_id,
    host_id,
    bool_or(has_availability)                              as was_active_in_month,
    sum(est_revenue_active)::numeric(14,2)                 as est_revenue_active_sum -- to get the revenue for each listing here so that we can use in the below select query
  from derived
  group by host_neighbourhood_lga, host_neighbourhood_lga_code, month_start, listing_id, host_id
)
select
  month_start,
  host_neighbourhood_lga,
  host_neighbourhood_lga_code,
  count(distinct host_id) as distinct_hosts,

  round((
    (sum(est_revenue_active_sum) / nullif(count(distinct listing_id) filter (where was_active_in_month), 0))::numeric
  ), 2)::numeric(14,2)  as est_revenue_per_active_listing,

  round((
    (sum(est_revenue_active_sum) / nullif(count(distinct host_id), 0))::numeric
  ), 2)::numeric(14,2)  as est_revenue_per_host

from per_listing_month
group by month_start, host_neighbourhood_lga, host_neighbourhood_lga_code
order by host_neighbourhood_lga, month_start