{{ config( materialized= 'view')}}

SELECT 
    dt.hour AS hour_of_day, 
    t.payment_type, 
    ROUND(AVG(t.tip_amount), 2) AS avg_tip
FROM {{ ref('fact_taxi_trips') }} t
JOIN {{ ref('dim_time') }} dt ON t.pickup_time_id = dt.time_id
WHERE t.tip_amount > 0
GROUP BY dt.hour, t.payment_type
ORDER BY avg_tip DESC
