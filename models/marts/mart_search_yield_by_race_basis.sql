{{ config(materialized = 'table') }}

with base as (
  select
    stop_id,
    coalesce(perceived_race, 'UNKNOWN') as perceived_race,
    nullif(trim(search_basis), '') as search_basis,
    lower(coalesce(stop_result, '')) as stop_result
  from {{ ref('int_search_outcomes') }}
),

searched as (
  select distinct
    cast(stop_id as string) as stop_id,
    perceived_race,
    coalesce(search_basis, 'UNKNOWN') as search_basis
  from base
  where search_basis is not null
),

found as (
  select distinct
    cast(stop_id as string) as stop_id,
    perceived_race,
    coalesce(search_basis, 'UNKNOWN') as search_basis
  from base
  where lower(stop_result) like '%contraband%' or lower(stop_result) like '%evidence%'
),

agg as (
  select
    s.perceived_race,
    s.search_basis,
    count(distinct s.stop_id) as searches,
    count(distinct f.stop_id) as finds,
    safe_divide(count(distinct f.stop_id), nullif(count(distinct s.stop_id), 0)) as yield_rate
  from searched s
  left join found f
    on s.stop_id = f.stop_id
   and s.perceived_race = f.perceived_race
   and s.search_basis = f.search_basis
  group by 1,2
)

select * from agg
order by perceived_race, search_basis;
