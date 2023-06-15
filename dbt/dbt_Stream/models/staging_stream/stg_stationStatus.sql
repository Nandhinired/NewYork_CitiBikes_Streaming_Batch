{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select 
        COALESCE(eightd_has_available_keys, false) AS eightd_has_available_keys,
        is_installed,
        is_renting,
        is_returning,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', last_reported)  as last_reported,
        legacy_id,
        COALESCE(num_bike_spots_available, 0) AS num_bike_spots_available,
        num_bikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,
        num_ebikes_available,
        COALESCE(num_scooter_spots_available, 0) AS num_scooter_spots_available,
        num_scooters_available,
        num_scooters_unavailable,
        station_id,
        COALESCE(station_status, 'NA') AS station_status,
        TIMESTAMP_SECONDS(last_updated)  as last_updated,
        ttl

    from {{ source('citi_bikes_stream', 'stationStatus_external_table') }}
)
select  
   *
from source
