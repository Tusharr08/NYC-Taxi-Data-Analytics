{{ config( materialized= 'view')}}

Select * from (
SELECT 
    l1.zone AS pickup_zone,
    l2.zone AS dropoff_zone,
    ROUND(AVG(trip_duration_min), 2) AS avg_trip_duration_mins, 
    COUNT(*) AS trip_count
FROM {{ ref('fact_taxi_trips')}} t
JOIN {{ ref('dim_location')}} l1 ON t.pickup_location_id = l1.location_id
JOIN {{ ref('dim_location')}} l2 ON t.dropoff_location_id = l2.location_id
GROUP BY l1.zone, l2.zone
ORDER BY trip_count DESC
) s where dropoff_zone is not null and pickup_zone is not null and dropoff_zone!='N/A' and pickup_zone!='N/A'