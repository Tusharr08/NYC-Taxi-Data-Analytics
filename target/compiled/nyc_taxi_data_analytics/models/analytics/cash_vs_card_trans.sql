

SELECT 
    payment_type ,
    COUNT(ft.trip_id) AS total_trips,
    ROUND(100.0 * COUNT(ft.trip_id) / SUM(COUNT(ft.trip_id)) OVER(), 2) AS percentage
FROM nyc_taxi_db.conformed.fact_taxi_trips ft
GROUP BY payment_type
ORDER BY total_trips DESC