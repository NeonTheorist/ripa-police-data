{{ config(
    materialized='view',
    enabled=true
) }}

-- Minimal working model

select
    1 as id,
    'test' as note
