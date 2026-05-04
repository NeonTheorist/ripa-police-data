{{ config(materialized = 'view') }}

with source as (

    select
        stop_id,
        person_id,
        contraband_type
    from {{ ref('stg_ripa_contraband_evid') }}

),

cleaned as (

    select
        stop_id,
        person_id,
        nullif(trim(contraband_type), '') as contraband_type_clean
    from source

),

aggregated as (

    select
        stop_id,
        person_id,
        max(case when contraband_type_clean is not null then 1 else 0 end) as had_find,
        count(distinct contraband_type_clean) as distinct_contraband_type_count,
        string_agg(distinct contraband_type_clean, ' | ' order by contraband_type_clean) as contraband_type_list
    from cleaned
    group by 1, 2

)

select *
from aggregated