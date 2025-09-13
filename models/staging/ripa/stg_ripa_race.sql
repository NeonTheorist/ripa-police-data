{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_race_datasd') }}

),

renamed as (

    select
        id as stop_id,
        --regexp_replace(cast(id as string), '[^0-9]', '') as stop_id,
        pid as person_id,
        case
            when race in ('Hispanic/Latine(x)', 'Hispanic/Latino(a)')
                then 'Hispanic/Latino(a)'
            else race
        end as perceived_race,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
