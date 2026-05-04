{{ config(materialized = 'table') }}

with stops as (

    select
        stop_id,
        person_id,
        agency,
        city_name,
        stop_date as stop_date_raw,
        perceived_gender,
        safe_cast(perceived_age as int64) as perceived_age
    from {{ ref('stg_ripa_stops') }}

),

stops_clean as (

    select
        stop_id,
        person_id,
        agency,
        city_name,

        case
            when safe_cast(stop_date_raw as date) is null then null
            when extract(year from safe_cast(stop_date_raw as date)) < 1000
                then date_add(safe_cast(stop_date_raw as date), interval 2000 year)
            else safe_cast(stop_date_raw as date)
        end as stop_date,

        perceived_gender,
        perceived_age,

        case
            when perceived_age is null then null
            when perceived_age < 18 then 'Under 18'
            when perceived_age between 18 and 24 then '18-24'
            when perceived_age between 25 and 34 then '25-34'
            when perceived_age between 35 and 44 then '35-44'
            when perceived_age between 45 and 54 then '45-54'
            when perceived_age between 55 and 64 then '55-64'
            else '65+'
        end as age_bucket

    from stops

),

final as (

    select
        s.stop_id,
        s.person_id as pid,
        s.stop_date,
        date_trunc(s.stop_date, month) as stop_month,
        r.perceived_race,
        coalesce(sf.had_search, 0) as had_search,
        coalesce(ff.had_find, 0) as had_find,
        s.perceived_gender,
        s.perceived_age,
        s.age_bucket,
        s.agency,
        s.city_name,
        sr.stop_reason
    from stops_clean s
    left join {{ ref('int_search_flag') }} sf
        on s.stop_id = sf.stop_id
       and s.person_id = sf.person_id
    left join {{ ref('int_find_flag') }} ff
        on s.stop_id = ff.stop_id
       and s.person_id = ff.person_id
    left join {{ ref('int_stop_reason_summary') }} sr
        on s.stop_id = sr.stop_id
       and s.person_id = sr.person_id
    left join {{ ref('int_race_summary') }} r
        on s.stop_id = r.stop_id
       and s.person_id = r.person_id

)

select *
from final