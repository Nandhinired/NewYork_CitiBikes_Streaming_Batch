{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select 
        language,
        name,
        operator,
        phone_number,
        short_name,
        system_id,
        timezone,
        url,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', last_updated)  as last_updated,
        ttl   
    from {{ source('citi_bikes_stream', 'systemInfo_external_table') }}
)
select *
from source
