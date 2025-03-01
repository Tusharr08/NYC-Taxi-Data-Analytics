{{ config( materialized= 'view')}}

SELECT 
    dl.borough AS pickup_borough,
    dl.zone as pickup_zone,
    ROUND(AVG(ft.fare_amount), 2) AS avg_fare,
    ROUND(AVG(ft.trip_distance), 2) AS avg_distance
FROM {{ ref('fact_taxi_trips')}}  ft
JOIN {{ ref('dim_location')}}  dl ON ft.pickup_location_id = dl.location_id
GROUP BY dl.borough, dl.zone
ORDER BY avg_fare DESC