SELECT trip_id, COUNT(*)
FROM {{ ref('dim_taxi_trips')}}
GROUP BY trip_id
HAVING COUNT(*) > 1

