{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_stop_result_datasd') }}

),

renamed as (

    select
        id as stop_id,
        --regexp_replace(cast(id as string), '[^0-9]', '') as stop_id,
        --cast(left(id, (length(id) - 1)) as int) as stop_id,
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
