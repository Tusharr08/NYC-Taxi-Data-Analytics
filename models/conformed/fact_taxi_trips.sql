{{ config(materialized='table', unique_key = 'trip_id') }}

SELECT 
    -- Generate trip_id with proper concatenation
    trip_id,
    trip_distance,
    pickup_datetime,
    dl.location_id AS pickup_location_id,  
    CONCAT(COALESCE(dl.zone, ''), ' ', COALESCE(dl.borough, '')) AS pickup_location,
    dt.time_id AS pickup_time_id,
    dt.day_of_week as day_of_week,
    dt.hour as hour_of_day,

    dropoff_datetime,
    dl2.location_id AS dropoff_location_id,  
    CONCAT(COALESCE(dl2.zone, ''), ' ', COALESCE(dl2.borough, '')) AS dropoff_location,

    trip_duration_min,
    (trip_distance / NULLIF(TIMESTAMPDIFF(SECOND, pickup_datetime, dropoff_datetime), 0)) * 3600 AS avg_speed_kmh,
    
    total_amount,
    tolls_amount,
    tip_amount,
    spt.payment_type AS payment_type,
    passenger_count,
    fare_amount,
    congestion_surcharge,

    CASE 
        WHEN pulocationid IN (132, 138) OR dolocationid IN (132, 138) THEN 'Yes'
        ELSE 'No'
    END AS is_airport_trip,
    airport_fee,

    CASE 
        WHEN fare_amount / NULLIF(trip_distance, 0) > 5 THEN 'Yes'
        ELSE 'No'
    END AS is_surge_pricing,
    vendor_id,
    sv.vendor_name AS vendor_name,
    src.rate_code_description AS rate_code

FROM {{ ref('dim_taxi_trips')}} as dtt 
LEFT JOIN {{ ref('vendors')}} sv ON dtt.vendorid = sv.vendor_id 
LEFT JOIN {{ ref('rate_codes')}} src ON dtt.ratecodeid = src.rate_code_id
LEFT JOIN {{ ref('payments')}} spt ON dtt.payment_type = spt.payment_id
LEFT JOIN {{ ref('dim_location')}} dl ON dtt.pulocationid = dl.location_id
LEFT JOIN {{ ref('dim_location')}} dl2 ON dtt.dolocationid = dl2.location_id
LEFT JOIN {{ ref('dim_time')}} dt ON dtt.pickup_datetime = dt.timestamp
