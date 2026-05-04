{{ config(materialized = 'table') }}

with base as (

    select
        stop_month,
        perceived_race,
        had_search,
        had_find
    from {{ ref('fct_ripa_person_stop') }}
    where stop_month is not null
      and perceived_race is not null

),

final as (

    select
        stop_month,
        perceived_race,
        count(*) as total_stopped_persons,
        countif(had_search = 1) as searched_persons,
        countif(had_search = 1 and had_find = 1) as searched_and_found_persons,
        countif(had_search = 0 and had_find = 1) as find_without_search_persons,

        safe_divide(
            countif(had_search = 1),
            count(*)
        ) as search_rate,

        safe_divide(
            countif(had_search = 1 and had_find = 1),
            nullif(countif(had_search = 1), 0)
        ) as yield_rate

    from base
    group by 1, 2

)

select *
from final
order by stop_month, perceived_race