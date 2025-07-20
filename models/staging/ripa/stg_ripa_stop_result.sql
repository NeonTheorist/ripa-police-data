{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'ripa_stop_result_datasd_raw') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        resultkey as stop_result_code,
        result as stop_result,
        code as violation_code,
        resulttext as violation_description,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
