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
    where row_expiry_date is NULL and station_id is not NULL
),
station_info_latest as (
    select *
    from station_info
    where row_expiry_date is NULL and station_id is not NULL
)
select
    dst.stationKey,
    dd.dateKey,
    status.is_installed,
    status.is_renting,
    status.is_returning,
    status.num_bike_spots_available,
    status.num_bikes_available,
    status.num_bikes_disabled,  
    status.num_docks_available,
    status.num_docks_disabled,
    status.num_ebikes_available,
    status.num_scooter_spots_available,
    status.num_scooters_available,
    status.num_scooters_unavailable,
    status.last_reported,
    info.capacity,
    info.station_id
from station_info_latest info
left join station_status_latest status
on status.station_id = info.station_id
left join {{ ref('dim_station') }} dst
on info.station_id = dst.station_id
left join {{ ref('dim_datetime') }} dd 
on dd.date = date_trunc(status.last_reported, HOUR)











