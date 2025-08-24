{{ config(materialized = 'view') }}

with stops as (
    select
        stop_id,
        ori,
        agency,
        years_experience,
        stop_date,
        stop_time,
        stop_duration_minutes,
        stop_in_response_to_cfs,
        stop_during_welfare_check,
        stoptype,
        officer_assignment_key,
        assignment,
        intersection,
        block,
        landmark,
        street,
        highway_exit,
        is_school,
        school_name,
        city_name,
        beat,
        beat_name,
        person_id,
        is_student,
        limited_english_flag,
        perceived_age,
        perceived_gender,
        non_binary_flag,
        unhoused_flag,
        perceived_sexual_orientation,
        passenger_flag,
        inside_residence_flag
    from {{ ref('stg_ripa_stops') }}
),

search_basis as (
    select
        stop_id,
        person_id,
        search_basis,
        search_basis_explanation,
        inserted_at
    from {{ ref('stg_ripa_search_basis') }}
),

stop_result as (
    select
        stop_id,
        person_id,
        stop_result_code,
        stop_result,
        violation_code,
        violation_description,
        inserted_at
    from {{ ref('stg_ripa_stop_result') }}
),

stop_reason as (
    select
        stop_id,
        person_id,
        stop_reason,
        stop_reason_code,
        stop_reason_code_text,
        stop_reason_detail,
        stop_reason_explanation,
        inserted_at
    from {{ ref('stg_ripa_stop_reason') }}
),

race as (
    select
        stop_id,
        person_id,
        perceived_race,
        inserted_at
    from {{ ref('stg_ripa_race') }}
),

joined as (
    select
        s.stop_id,
        s.stop_date,
        s.stop_time,
        s.agency,
        s.city_name,
        s.perceived_age,
        s.perceived_gender,
        r.perceived_race,
        sb.search_basis,
        sb.search_basis_explanation,
        sr.stop_result,
        sr.violation_code,
        sr.violation_description,
        rr.stop_reason,
        rr.stop_reason_code,
        rr.stop_reason_code_text,
        rr.stop_reason_detail,
        rr.stop_reason_explanation
    from stops s
    left join race r on s.stop_id = r.stop_id
    left join search_basis sb on s.stop_id = sb.stop_id
    left join stop_result sr on s.stop_id = sr.stop_id
    left join stop_reason rr on s.stop_id = rr.stop_id
)

select *
from joined
