
  
    

        create or replace transient table nyc_taxi_db.processed.prc_taxi_trips
         as
        (

WITH source_data AS (
    SELECT 
        -- Convert datetime columns from microseconds to TIMESTAMP_NTZ
        TO_TIMESTAMP_NTZ(TPEP_PICKUP_DATETIME / 1000000) AS PICKUP_DATETIME,
        TO_TIMESTAMP_NTZ(TPEP_DROPOFF_DATETIME / 1000000) AS DROPOFF_DATETIME,
        
        trip_distance,
        total_amount,
        tolls_amount,
        tip_amount,
        store_and_fwd_flag,
        payment_type,
        passenger_count,
        mta_tax,
        improvement_surcharge,
        fare_amount,
        extra,
        congestion_surcharge,
        airport_fee,
        VendorID,
        RatecodeID,
        PULocationID,
        DOLocationID

    FROM nyc_taxi_db.raw.nyc_taxi_trips 
    WHERE 
        -- Remove records with NULL values in any column
        trip_distance IS NOT NULL
        AND TPEP_PICKUP_DATETIME IS NOT NULL
        AND TPEP_DROPOFF_DATETIME IS NOT NULL
        AND total_amount IS NOT NULL
        AND payment_type IS NOT NULL
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL
        AND RatecodeID IS NOT NULL
        AND store_and_fwd_flag IS NOT NULL
        AND VendorID IS NOT NULL
        AND AIRPORT_FEE IS NOT NULL
        AND PASSENGER_COUNT IS NOT NULL
        AND FARE_AMOUNT > 0
)
,
validated_data AS (
    SELECT 
        *,
        -- Validate rate_code_id (should be between 1-6)
        CASE 
            WHEN RatecodeID BETWEEN 1 AND 6 THEN RatecodeID 
            ELSE NULL 
        END AS valid_ratecodeid,

        -- Validate payment_type (should be between 0-5)
        CASE 
            WHEN payment_type BETWEEN 0 AND 5 THEN payment_type 
            ELSE NULL 
        END AS valid_payment_type,

        -- Validate store_and_fwd_flag (should be Y or N)
        CASE 
            WHEN store_and_fwd_flag IN ('Y', 'N') THEN store_and_fwd_flag 
            ELSE NULL 
        END AS valid_store_and_fwd_flag,

        --Add trip duration
        TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) AS trip_duration_min,

    FROM source_data
)
-- Remove invalid records where validation failed
SELECT 
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    trip_duration_min,
    total_amount,
    tolls_amount,
    tip_amount,
    valid_store_and_fwd_flag AS store_and_fwd_flag,
    valid_payment_type AS payment_type,
    passenger_count,
    mta_tax,
    improvement_surcharge,
    fare_amount,
    extra,
    congestion_surcharge,
    airport_fee,
    VendorID,
    valid_ratecodeid AS RatecodeID,
    PULocationID,
    DOLocationID
FROM validated_data
WHERE 
    valid_ratecodeid IS NOT NULL 
    AND valid_payment_type IS NOT NULL 
    AND valid_store_and_fwd_flag IS NOT NULL
    --AND 1<trip_duration_min<1440
    AND PULOCATIONID!=DOLOCATIONID
    AND pickup_datetime < dropoff_datetime
        );
      
  