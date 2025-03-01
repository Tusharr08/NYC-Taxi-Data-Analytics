

WITH trips AS (
    SELECT DISTINCT 
        pickup_datetime,
        dropoff_datetime,
        trip_distance,
        trip_duration_min,
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
    FROM nyc_taxi_db.processed.prc_taxi_trips
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY pickup_datetime, dropoff_datetime) AS trip_id,  -- Generate sequential trip_id
    *
FROM trips