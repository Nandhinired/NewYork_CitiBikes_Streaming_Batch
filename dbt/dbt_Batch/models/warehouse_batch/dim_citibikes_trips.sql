{{
    config(
        materialized='incremental',  
        partition_by={
        "field": "started_at",
        "data_type": "timestamp",
        "granularity": "month"
      }
    )
}}

with source as (
    select        
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual
    from {{ ref('stg_citibikes_trips') }} 
)
select  {{ dbt_utils.generate_surrogate_key(['ride_id']) }} AS trip_id,
* 
from source