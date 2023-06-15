{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select 
        COALESCE(address, 'NA') AS address,
        capacity,
        COALESCE(client_station_id, 'NA') AS client_station_id,
        COALESCE(dockless_bikes_parking_zone_capacity, 0) AS dockless_bikes_parking_zone_capacity,
        eightd_has_key_dispenser,
        electric_bike_surcharge_waiver,
        external_id,
        has_kiosk,
        lat,
        legacy_id,
        lon,
        station_name,
        COALESCE(rack_model, 'NA') AS rack_model,
        COALESCE(region_code, 'NA') AS region_code,
        CAST(region_id AS INTEGER) AS region_id,
        short_name,
        station_id,
        station_type,
        COALESCE(target_bike_capacity, 0) AS target_bike_capacity,
        COALESCE(target_scooter_capacity, 0) AS target_scooter_capacity,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', last_updated)  as last_updated,
        ttl,
        bikes_availability,
        COALESCE(description, 'NA') AS description,
        docks_availability,
        id,
        name,
        off_dock_bikes_count,
        off_dock_remaining_bike_capacity,
        service_type,
        android,
        ios,
        case
            when rental_methods like '%KEY%' then true
            else false
        end as rental_method_key_flag,
        case
            when rental_methods like '%CREDITCARD%' then true
            else false
        end as rental_method_credit_flag
    from {{ source('citi_bikes_stream', 'stationInfo_external_table') }}
)
select 
    station_id,
    station_name,
    lat,
    lon,
    last_updated,
    max(address) as address,
    max(capacity) as capacity,
    max(client_station_id) as client_station_id,
    max(dockless_bikes_parking_zone_capacity) as dockless_bikes_parking_zone_capacity,
    max(eightd_has_key_dispenser) as eightd_has_available_keys,
    max(electric_bike_surcharge_waiver) as electric_bike_surcharge_waiver,
    max(external_id) as external_id,
    max(has_kiosk) as has_kiosk,
    max(legacy_id) as legacy_id,
    max(rack_model) as rack_model,
    max(region_code) as region_code,
    max(region_id) as region_id,
    max(short_name) as short_name,
    max(station_type) as station_type,
    max(target_bike_capacity) as target_bike_capacity,
    max(target_scooter_capacity) as target_scooter_capacity,
    max(ttl) as ttl,
    max(bikes_availability) as bikes_availability,
    max(description) as description,
    max(docks_availability) as docks_availability,
    max(id) as id,
    max(name) as name,
    max(off_dock_bikes_count) as off_dock_bikes_count,
    max(off_dock_remaining_bike_capacity) as off_dock_remaining_bike_capacity,
    max(service_type) as service_type,
    max(android) as android,
    max(ios) as ios,
    max(rental_method_key_flag) as rental_method_key_flag,
    max(rental_method_credit_flag) as rental_method_credit_flag  
from source     
group by station_id,station_name,lat,lon,last_updated


