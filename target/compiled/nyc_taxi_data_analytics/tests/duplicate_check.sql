SELECT trip_id, COUNT(*)
FROM nyc_taxi_db.conformed.dim_taxi_trips
GROUP BY trip_id
HAVING COUNT(*) > 1