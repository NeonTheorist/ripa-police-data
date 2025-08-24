{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_prop_seize_basis_datasd') }}

),

renamed as (

    select
        id as stop_id,
        --regexp_replace(cast(id as string), '[^0-9]', '') as stop_id,
        pid as person_id,
        basisforpropertyseizure as basis_for_property_seizure,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
