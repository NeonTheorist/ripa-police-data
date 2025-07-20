{{ config(
    materialized='view',
    enabled=true
) }}

-- models/staging/ripa/stg_ripa_stops.sql

with source as (

select * from {{ source('raw', 'ripa_stops_datasd_raw') }}


),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        ori as agency_ori,
        agency,
        expYears as officer_years_experience,
        stopdate,
        stoptime,
        stopduration,
        stopInResponseToCFS as stop_response_to_cfs,
        stopmadeduringwelfarecheck as stop_during_welfare_check,
        stoptype,
        officerAssignmentkey as officer_assignment_key,
        assignment as officer_assignment_desc,
        intersection,
        block,
        landmark,
        street,
        highwayExit,
        isschool as is_school,
        schoolName as school_name,
        cityname as city,
        beat,
        beatName as beat_name,
        isstudent as is_student,
        perceivedLimitedEnglish as perceived_limited_english,
        perceivedAge as perceived_age,
        perceivedGender as perceived_gender,
        NonBinaryPerson as is_non_binary,
        perceivedUnhoused as perceived_unhoused,
        perceivedSexualOrientation as perceived_sexual_orientation,
        passengerInVehicle as is_passenger,
        insideResidence as inside_residence

    from source

)

select * from renamed
