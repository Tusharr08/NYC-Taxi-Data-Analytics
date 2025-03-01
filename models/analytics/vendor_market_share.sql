{{ config( materialized= 'view')}}

SELECT 
    vendor_name,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM {{ ref('fact_taxi_trips')}} 
GROUP BY vendor_name
ORDER BY total_revenue DESC