

SELECT 
    dt.year,
    dt.month,
    ROUND(SUM(t.fare_amount), 2) AS total_earnings,
    COUNT(t.trip_id) AS total_trips
FROM nyc_taxi_db.conformed.fact_taxi_trips t
JOIN nyc_taxi_db.conformed.dim_time dt ON t.pickup_time_id = dt.time_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month