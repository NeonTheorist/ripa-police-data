{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'ripa_search_basis_datasd_raw') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        basisforsearch as search_basis,
        basisforsearchexplanation as search_basis_explanation,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
