{{ config(materialized = 'view') }}

with source as (

    select
        stop_id,
        person_id,
        perceived_race
    from {{ ref('stg_ripa_race') }}

),

cleaned as (

    select
        stop_id,
        person_id,
        nullif(trim(perceived_race), '') as perceived_race_clean
    from source

),

aggregated as (

    select
        stop_id,
        person_id,
        count(distinct perceived_race_clean) as distinct_perceived_race_count,
        string_agg(distinct perceived_race_clean, ' | ' order by perceived_race_clean) as perceived_race_list
    from cleaned
    group by 1, 2

),

final as (

    select
        stop_id,
        person_id,
        case
            when distinct_perceived_race_count = 0 then null
            when distinct_perceived_race_count = 1 then perceived_race_list
            else 'Multiple/ambiguous race'
        end as perceived_race,
        distinct_perceived_race_count,
        perceived_race_list
    from aggregated

)

select *
from final