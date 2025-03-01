{{ config( materialized= 'view')}}

SELECT 
    dt.year,
    dt.month,
    ROUND(SUM(t.fare_amount), 2) AS total_earnings,
    COUNT(t.trip_id) AS total_trips
FROM {{ ref('fact_taxi_trips')}} t
JOIN {{ ref('dim_time')}} dt ON t.pickup_time_id = dt.time_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month