{{ config( materialized= 'view')}}

SELECT 
    dl_pickup.zone AS pickup_location,
    dl_dropoff.zone AS dropoff_location,
    COUNT(ft.trip_id) AS trip_count
FROM {{ ref('fact_taxi_trips')}} ft
JOIN {{ ref('dim_location')}} dl_pickup 
    ON ft.pickup_location_id = dl_pickup.location_id
JOIN {{ ref('dim_location')}} dl_dropoff 
    ON ft.dropoff_location_id = dl_dropoff.location_id
GROUP BY dl_pickup.zone, dl_dropoff.zone
ORDER BY trip_count DESC
limit 20