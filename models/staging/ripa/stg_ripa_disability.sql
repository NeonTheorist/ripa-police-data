{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'ripa_disability_datasd_raw') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        uid as stop_uid,
        disability as perceived_disability,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
