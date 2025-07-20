{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'ripa_prop_seize_basis_datasd_raw') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        basisforpropertyseizure as basis_for_property_seizure,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
