{{ config(materialized='view') }}

SELECT 
    ROUND(AVG(fare_amount), 2) AS avg_fare_per_trip, 
    COUNT(trip_id) AS total_trips 
FROM {{ ref('fact_taxi_trips') }}