SELECT 
    (SELECT COUNT(*) FROM nyc_taxi_db.conformed.fact_taxi_trips) AS fact_count,
    (SELECT COUNT(*) FROM nyc_taxi_db.conformed.dim_taxi_trips) AS dim_count
WHERE 
    (SELECT COUNT(*) FROM nyc_taxi_db.conformed.fact_taxi_trips) 
    != (SELECT COUNT(*) FROM nyc_taxi_db.conformed.dim_taxi_trips)