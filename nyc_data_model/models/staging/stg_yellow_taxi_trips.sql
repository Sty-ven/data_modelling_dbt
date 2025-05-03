{{ config(
    materialized = 'view'
) }}

WITH source AS (
    SELECT
        VENDORID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PULOCATIONID,
        DOLOCATIONID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        DATA_MONTH
    FROM {{ source('raw', 'yellow_taxi_trips') }}
),

cleaned AS (
    SELECT
        VENDORID,
        -- Convert string timestamps to TIMESTAMP format
        TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME) AS pickup_datetime,
        TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME) AS dropoff_datetime,
        COALESCE(PASSENGER_COUNT, 0) AS passenger_count,
        TRIP_DISTANCE,
        RATECODEID,
        STORE_AND_FWD_FLAG,
        PULOCATIONID AS pickup_location_id,
        DOLOCATIONID AS dropoff_location_id,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE,
        TRY_TO_DATE(DATA_MONTH) AS trip_month
    FROM source
)

SELECT *,
    -- Add calculated fields
    DATEDIFF(minute, pickup_datetime, dropoff_datetime) AS trip_duration_minutes,
    CASE
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
        ELSE 'Other'
    END AS payment_type_desc,
    CASE
        WHEN RATECODEID = 1 THEN 'Standard rate'
        WHEN RATECODEID = 2 THEN 'JFK'
        WHEN RATECODEID = 3 THEN 'Newark'
        WHEN RATECODEID = 4 THEN 'Nassau or Westchester'
        WHEN RATECODEID = 5 THEN 'Negotiated fare'
        WHEN RATECODEID = 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_code_desc
FROM cleaned
WHERE pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND pickup_location_id IS NOT NULL
  AND dropoff_location_id IS NOT NULL