{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_disability_datasd') }}

),

renamed as (

    select
        id as stop_id,
        --regexp_replace(cast(id as string), '[^0-9]', '') as stop_id,
        pid as person_id,
        uid as stop_uid,
        disability as perceived_disability,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
