

SELECT 
    vendor_name,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM nyc_taxi_db.conformed.fact_taxi_trips 
GROUP BY vendor_name
ORDER BY total_revenue DESC