{{ config(materialized = 'view') }}

with source as (

    select
        stop_id,
        person_id,
        stop_reason
    from {{ ref('stg_ripa_stop_reason') }}

),

cleaned as (

    select
        stop_id,
        person_id,
        nullif(trim(stop_reason), '') as stop_reason_clean
    from source

),

aggregated as (

    select
        stop_id,
        person_id,
        count(distinct stop_reason_clean) as distinct_stop_reason_count,
        string_agg(distinct stop_reason_clean, ' | ' order by stop_reason_clean) as stop_reason_list
    from cleaned
    group by 1, 2

),

final as (

    select
        stop_id,
        person_id,
        case
            when distinct_stop_reason_count = 0 then null
            when distinct_stop_reason_count = 1 then stop_reason_list
            else 'Multiple stop reasons'
        end as stop_reason,
        distinct_stop_reason_count,
        stop_reason_list
    from aggregated

)

select *
from final