{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select        
        Year,
        Month,
        Day,
        Temperature_Max,
        Temperature_Avg,
        Temperature_Min,
        DewPoint_Max,
        DewPoint_Avg,
        DewPoint_Min,
        Humidity_Max,
        Humidity_Avg,
        Humidity_Min,	
        Wind_Max,
        Wind_Avg,
        Wind_Min,	
        condition
    from {{ source('citi_bikes_batch', 'climate_external_table') }}
),
unique_source as (
    select *,
            row_number() over(partition by Year,Month,Day,condition) as row_number
    from source
)
select * 
except
       (row_number),
from unique_source
where row_number = 1




