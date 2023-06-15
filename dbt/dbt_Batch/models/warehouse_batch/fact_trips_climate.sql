{{ config(
  materialized = 'table',
  partition_by={
    "field": "datestamp",
    "data_type": "timestamp",
    "granularity": "month"
  }
  ) }}

SELECT 
    bikes.trip_id AS trip_id,
    TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_duration_min,
     2 * ASIN(SQRT(IFNULL(
        POW(SIN((end_lat - start_lat) * 3.14 / 360), 2) +
        COS(start_lat * 3.14 / 180) * COS(end_lat * 3.14 / 180) * POW(
          SIN((end_lng - start_lng) * 3.14 / 360), 2),0) /
      IFNULL( 1 - (POW(SIN((end_lat - start_lat) * 3.14 / 360), 2  ) +
          COS(start_lat * 3.14 / 180) * COS(end_lat * 3.14 / 180) * POW(
            SIN((end_lng - start_lng) * 3.14 / 360), 2  )),1)) ) * 6371000 AS distance_meters,
    climate.climate_id AS climate_id,   
    dt.dateKey AS dateKey,
    dt.date AS datestamp 
 FROM {{ ref('dim_citibikes_trips') }} bikes
  LEFT JOIN {{ ref('dim_climate') }} climate
    ON EXTRACT(YEAR FROM bikes.started_at) = climate.Year AND EXTRACT(MONTH FROM bikes.started_at) = climate.Month
       AND EXTRACT(DAY FROM bikes.started_at) = climate.Day
  LEFT JOIN {{ ref('dim_batch_datetime') }} dt
    ON dt.date = date_trunc(bikes.started_at, DAY)

   

