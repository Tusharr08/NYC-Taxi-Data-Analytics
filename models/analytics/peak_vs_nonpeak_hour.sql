{{ config( materialized= 'view')}}

SELECT 
    CASE 
        WHEN dt.hour BETWEEN 7 AND 9 OR dt.hour BETWEEN 17 AND 19 THEN 'Peak'
        ELSE 'Non-Peak'
    END AS time_period,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare,
    SUM(t.fare_amount) AS total_revenue
FROM {{ ref('fact_taxi_trips')}} t
JOIN {{ ref('dim_time')}}  dt ON t.pickup_time_id = dt.time_id
GROUP BY time_period
order by total_revenue