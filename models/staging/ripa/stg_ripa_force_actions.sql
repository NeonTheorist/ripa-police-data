{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_force_actions_datasd') }}

),

renamed as (

    select
        --id as stop_id,
        --regexp_replace(cast(id as string), '[^0-9]', '') as stop_id,
        cast(left(id, (length(id) - 1)) as int) as stop_id, 
        pid as person_id,
        forceactiontaken as force_action_taken,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
