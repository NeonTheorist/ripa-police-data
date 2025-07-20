{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'ripa_prop_seize_type_datasd_raw') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        typeofpropertyseized as type_of_property_seized,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed

