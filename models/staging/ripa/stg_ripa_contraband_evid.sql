{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_contraband_evid_datasd') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        uid as stop_uid,
        contraband as contraband_type,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
