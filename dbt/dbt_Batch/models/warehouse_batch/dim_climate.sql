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
    from {{ ref('stg_climate') }} 
)
select  {{ dbt_utils.generate_surrogate_key(['Year', 'Month', 'Day']) }} as climate_id,
* 
from source

