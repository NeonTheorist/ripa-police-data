{{ config(materialized = 'view') }}

with source as (

    select
        stop_id,
        person_id,
        search_basis
    from {{ ref('stg_ripa_search_basis') }}

),

cleaned as (

    select
        stop_id,
        person_id,
        nullif(trim(search_basis), '') as search_basis_clean
    from source

),

aggregated as (

    select
        stop_id,
        person_id,
        max(case when search_basis_clean is not null then 1 else 0 end) as had_search,
        count(distinct search_basis_clean) as distinct_search_basis_count,
        string_agg(distinct search_basis_clean, ' | ' order by search_basis_clean) as search_basis_list
    from cleaned
    group by 1, 2

)

select *
from aggregated