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
        pid as person_id,
        race as perceived_race,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
