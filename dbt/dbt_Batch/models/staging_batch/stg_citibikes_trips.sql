{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select        
        ride_id,
        rideable_type,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', started_at)  as started_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ended_at)  as ended_at,    
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual
    from {{ source('citi_bikes_batch', 'citibikes_trips_external_table') }}
),
unique_source as (
    select *,
            row_number() over(partition by ride_id,started_at,ended_at) as row_number
    from source
)
select * 
except
       (row_number),
from unique_source
where row_number = 1


