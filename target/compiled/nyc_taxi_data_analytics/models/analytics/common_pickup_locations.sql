

SELECT 
    dl_pickup.zone AS pickup_location,
    dl_dropoff.zone AS dropoff_location,
    COUNT(ft.trip_id) AS trip_count
FROM nyc_taxi_db.conformed.fact_taxi_trips ft
JOIN nyc_taxi_db.conformed.dim_location dl_pickup 
    ON ft.pickup_location_id = dl_pickup.location_id
JOIN nyc_taxi_db.conformed.dim_location dl_dropoff 
    ON ft.dropoff_location_id = dl_dropoff.location_id
GROUP BY dl_pickup.zone, dl_dropoff.zone
ORDER BY trip_count DESC
limit 20