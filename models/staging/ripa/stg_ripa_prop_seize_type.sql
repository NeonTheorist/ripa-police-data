{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_prop_seize_type_datasd') }}

),

renamed as (

    select
        id as stop_id,
        --regexp_replace(cast(id as string), '[^0-9]', '') as stop_id,
        pid as person_id,
        typeofpropertyseized as type_of_property_seized,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed

