{{
    config(
        materialized='incremental'
    )
}}


with source as (
    select 
        language,
        name as system_name,
        operator,
        phone_number,
        system_id,
        timezone,
        url as system_url,
        last_updated as system_time     
    from {{ ref('stg_systemInfo') }}
),
unique_source as (
    select *,
            row_number() over(partition by system_name,system_time) as row_number
    from source
)
select   
    {{ dbt_utils.generate_surrogate_key(['system_id', 'system_time']) }} as systemKey, *
except
       (row_number),
from unique_source
where row_number = 1
