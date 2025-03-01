{{ config( materialized= 'view')}}

SELECT 
    dt.hour as hour_of_day, 
    p.borough as pickup_zone, 
    COUNT(t.trip_id) AS total_trips
FROM {{ ref('fact_taxi_trips') }} t
JOIN {{ ref('dim_time') }} dt ON t.pickup_time_id = dt.time_id
JOIN {{ ref('dim_location') }} p ON t.pickup_location_id = p.location_id
GROUP BY dt.hour, p.borough
ORDER BY total_trips DESC
