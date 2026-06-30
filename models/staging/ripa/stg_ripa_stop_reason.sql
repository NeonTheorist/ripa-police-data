{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('raw', 'raw_ripa_stop_reason_datasd') }}

),

renamed as (

    select
        id as stop_id,
        pid as person_id,
        reasonforstop as stop_reason,
        reasonforstopcode as stop_reason_code,
        reasonforstopcodetext as stop_reason_code_text,
        reasonforstopdetail as stop_reason_detail,
        reasonforstopexplanation as stop_reason_explanation,
        insertdatetime as inserted_at
    from source

)

select * 
from renamed
