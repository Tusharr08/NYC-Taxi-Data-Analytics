{{ config( materialized= 'view')}}

SELECT 
    dt.hour as hour,
    dt.day_of_week as day_of_week,
    COUNT(*) AS trip_count
FROM {{ ref('fact_taxi_trips')}}  t
JOIN {{ ref('dim_time')}}  dt ON t.pickup_time_id = dt.time_id
GROUP BY dt.hour, dt.day_of_week
ORDER BY dt.hour, dt.day_of_week