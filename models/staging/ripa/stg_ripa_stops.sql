{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_stops_datasd') }}

),

renamed as (

    select
        id as stop_id,
        ori,
        agency,
        expYears as years_experience,
        stopdate as stop_date,
        stoptime as stop_time,
        stopduration as stop_duration_minutes,
        stopInResponseToCFS as stop_in_response_to_cfs,
        stopmadeduringwelfarecheck as stop_during_welfare_check,
        stoptype,
        officerAssignmentkey as officer_assignment_key,
        assignment,
        intersection,
        block,
        landmark,
        street,
        highwayExit as highway_exit,
        isschool as is_school,
        schoolName as school_name,
        cityname as city_name,
        beat,
        beatName as beat_name,
        PID as person_id,
        isstudent as is_student,
        perceivedLimitedEnglish as limited_english_flag,
        perceivedAge as perceived_age,
        perceivedGender as perceived_gender,
        NonBinaryPerson as non_binary_flag,
        perceivedUnhoused as unhoused_flag,
        perceivedSexualOrientation as perceived_sexual_orientation,
        passengerInVehicle as passenger_flag,
        insideResidence as inside_residence_flag
    from source

)

select * 
from renamed
