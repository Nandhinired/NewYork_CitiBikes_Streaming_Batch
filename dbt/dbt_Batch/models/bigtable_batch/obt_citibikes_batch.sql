{{ config(
  materialized = 'table',
  partition_by={
    "field": "datestamp",
    "data_type": "timestamp",
    "granularity": "month"
  }
  ) }}

SELECT
    fact.trip_id AS trip_id,
    fact.distance_meters AS distance_travelled,
    fact.climate_id AS climate_id,   
    fact.dateKey AS dateKey,
    fact.datestamp AS datestamp, 

    climate.condition as weather,
    climate.Temperature_Avg as average_temperature,
    
    CONCAT(CAST(trips.start_lat AS STRING), ',', CAST(trips.start_lng AS STRING)) as start_location,
    CONCAT(CAST(trips.end_lat AS STRING), ',', CAST(trips.end_lng AS STRING)) as end_location,
    trips.start_station_id as start_stationid,
    trips.start_station_name as start_stationame,
    trips.end_station_id as end_stationid,
    trips.end_station_name as end_station_name,
    trips.rideable_type as bike_type,
    trips.member_casual as customer_type,
    trips.started_at as start_time,
    trips.ended_at as end_time,
 
    dt.weekendFlag as weekendFlag
    
from {{ ref('fact_trips_climate') }} fact 
left join {{ ref('dim_citibikes_trips') }} trips
    on fact.trip_id = trips.trip_id
left join {{ ref('dim_climate') }} climate 
    on fact.climate_id = climate.climate_id
left join {{ ref('dim_batch_datetime') }} dt 
    on fact.dateKey = dt.dateKey
