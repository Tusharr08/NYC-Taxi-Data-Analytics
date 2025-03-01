

SELECT 
    spt.payment_type as payment_category,
    COUNT(ft.trip_id) AS total_trips,
    ROUND(100.0 * COUNT(ft.trip_id) / SUM(COUNT(ft.trip_id)) OVER(), 2) AS percentage
FROM nyc_taxi_db.conformed.fact_taxi_trips ft
JOIN nyc_taxi_db.seeds.payments spt 
    ON ft.payment_type = spt.payment_type
GROUP BY spt.payment_type
ORDER BY total_trips DESC