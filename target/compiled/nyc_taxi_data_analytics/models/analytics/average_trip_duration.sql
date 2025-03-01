

Select * from (
SELECT 
    dl.borough AS pickup_borough,
    ROUND(AVG(ft.trip_duration_min), 2) AS avg_trip_duration,
    COUNT(ft.trip_id) AS total_trips
FROM nyc_taxi_db.conformed.fact_taxi_trips ft
JOIN nyc_taxi_db.conformed.dim_location dl ON ft.pickup_location_id = dl.location_id
GROUP BY dl.borough
ORDER BY avg_trip_duration DESC
) s where pickup_borough!='N/A'