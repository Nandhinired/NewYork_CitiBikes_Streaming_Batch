{{
    config(
    materialized='incremental',
    partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "month"
  }
    )
}}

SELECT
    fact.stationKey as stationKey,
    fact.last_reported as timestamp,
    fact.dateKey as dateKey,
    fact.capacity as stationCapacity,
    fact.num_bikes_available as numBikesAvailable,
    fact.station_id as stationId,
    fact.num_bikes_available/fact.capacity as bikesAvailabilityRatio,
    
    CONCAT(CAST(st.lat AS STRING), ',', CAST(st.lon AS STRING)) as location,
    st.station_name as stationName,
    st.station_type as stationType,
    st.service_type as serviceType,
    st.android as androidLink,
    st.ios as iosLink,
   
    dt.date as dateHour,
    dt.dayOfMonth as dayOfMonth,
    dt.dayOfWeek as dayOfWeek,
    dt.weekendFlag as  weekendFlag
    
from {{ ref('fact_bikes_stream') }} fact 
left join {{ ref('dim_station') }} st 
    on fact.stationKey = st.stationKey
left join {{ ref('dim_datetime') }} dt 
    on fact.dateKey = dt.dateKey
