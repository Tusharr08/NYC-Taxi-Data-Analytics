
  create or replace   view nyc_taxi_db.analytics.average_fare_amount
  
   as (
    

SELECT 
    ROUND(AVG(fare_amount), 2) AS avg_fare_per_trip, 
    COUNT(trip_id) AS total_trips 
FROM nyc_taxi_db.conformed.fact_taxi_trips
  );

