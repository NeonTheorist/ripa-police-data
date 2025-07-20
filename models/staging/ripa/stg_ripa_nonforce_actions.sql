{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'ripa_nonforce_actions_datasd_raw') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        nonforceactiontaken as non_force_action_taken,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
