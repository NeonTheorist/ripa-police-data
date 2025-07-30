{{ config(materialized = 'table') }}

-- Base rows from the intermediate model
with base as (
  select
    stop_id,
    coalesce(perceived_race, 'UNKNOWN') as perceived_race,
    nullif(trim(stop_reason), '') as stop_reason,
    nullif(trim(search_basis), '') as search_basis,
    lower(coalesce(stop_result, '')) as stop_result
  from {{ ref('int_search_outcomes') }}
),

-- One row per stop × race × reason where a search took place
searched as (
  select distinct
    cast(stop_id as string) as stop_id,
    perceived_race,
    coalesce(stop_reason, 'UNKNOWN') as stop_reason
  from base
  where search_basis is not null
),

-- One row per stop × race × reason where contraband/evidence was found
found as (
  select distinct
    cast(stop_id as string) as stop_id,
    perceived_race,
    coalesce(stop_reason, 'UNKNOWN') as stop_reason
  from base
  where lower(stop_result) like '%contraband%' or lower(stop_result) like '%evidence%'
),

agg as (
  select
    s.perceived_race,
    s.stop_reason,
    count(distinct s.stop_id) as searches,
    count(distinct f.stop_id) as finds,
    safe_divide(count(distinct f.stop_id), nullif(count(distinct s.stop_id), 0)) as yield_rate
  from searched s
  left join found f
    on s.stop_id = f.stop_id
   and s.perceived_race = f.perceived_race
   and s.stop_reason = f.stop_reason
  group by 1,2
)

select * from agg
order by perceived_race, stop_reason;
