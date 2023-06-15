{{
    config(
        materialized='incremental'
    )
}}



with station_status as (
    select  
        LEAD(last_updated, 1) OVER(PARTITION BY station_id ORDER BY last_updated) as row_expiry_date, *
    from {{ ref('stg_stationStatus') }} 
),
station_info as (
    select  
        LEAD(last_updated, 1) OVER(PARTITION BY station_id ORDER BY last_updated) as row_expiry_date, *
    from {{ ref('stg_stationInfo') }} 
),
station_status_latest as (
    select *
    from station_status
    where row_expiry_date is NULL
),
station_info_latest as (
    select *
    from station_info
    where row_expiry_date is NULL
)
select
    {{ dbt_utils.generate_surrogate_key(['info.station_id', 'info.last_updated']) }} as stationKey,
    info.station_id,
    info.station_name,
    info.lat,
    info.lon,
    info.last_updated,
    info.address,  
    info.client_station_id,
    info.dockless_bikes_parking_zone_capacity,
    info.electric_bike_surcharge_waiver,
    info.external_id,
    info.region_id,
    info.station_type,
    info.service_type,
    info.android,
    info.ios,
    info.rental_method_key_flag,
    info.rental_method_credit_flag, 
    status.eightd_has_available_keys,
    status.station_status
from station_info_latest info
left join station_status_latest status
on info.station_id = status.station_id 



