
  create or replace   view nyc_taxi_db.analytics.fare_amount_by_borough
  
   as (
    

SELECT 
    dl.borough AS pickup_borough,
    dl.zone as pickup_zone,
    ROUND(AVG(ft.fare_amount), 2) AS avg_fare,
    ROUND(AVG(ft.trip_distance), 2) AS avg_distance
FROM nyc_taxi_db.conformed.fact_taxi_trips  ft
JOIN nyc_taxi_db.conformed.dim_location  dl ON ft.pickup_location_id = dl.location_id
GROUP BY dl.borough, dl.zone
ORDER BY avg_fare DESC
  );

