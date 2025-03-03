
  create or replace   view nyc_taxi_db.analytics.short_vs_long_trips
  
   as (
    

SELECT 
    CASE 
        WHEN trip_distance < 5 THEN 'Short Trip'
        ELSE 'Long Trip' 
    END AS trip_category,
    COUNT(trip_id) AS total_trips,
    ROUND(100.0 * COUNT(trip_id) / SUM(COUNT(trip_id)) OVER(), 2) AS percentage
FROM nyc_taxi_db.conformed.fact_taxi_trips
GROUP BY trip_category
ORDER BY total_trips DESC
  );

