{{ config(
    materialized = 'table'
) }}

WITH trips AS (
    SELECT *
    FROM {{ ref('stg_yellow_taxi_trips') }}
)

SELECT
    -- Primary key
    concat_ws('-', 
        cast(pickup_location_id as varchar), 
        cast(dropoff_location_id as varchar),
        to_char(pickup_datetime, 'YYYYMMDDHH24MISS')
    ) AS trip_id,
    
    -- Timestamps
    pickup_datetime,
    dropoff_datetime,
    trip_month,
    
    -- Trip details
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    
    -- Locations
    pickup_location_id,
    dropoff_location_id,
    
    -- Payment details
    payment_type,
    payment_type_desc,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    
    -- Rate info
    ratecodeid,
    rate_code_desc,
    
    -- Flags
    store_and_fwd_flag,
    
    -- Calculated fields
    (tip_amount / NULLIF(fare_amount, 0)) * 100 AS tip_percentage,
    (total_amount / NULLIF(trip_distance, 0)) AS cost_per_mile,
    (trip_distance / NULLIF(trip_duration_minutes, 0) * 60) AS avg_speed_mph
    
FROM trips