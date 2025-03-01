{{ config( materialized= 'view')}}


WITH trip_gaps AS (
    SELECT 
        TRIP_ID,
        PICKUP_DATETIME, 
        DROPOFF_DATETIME,
        hour_of_day,
        LAG(DROPOFF_DATETIME) OVER (ORDER BY PICKUP_DATETIME) AS prev_trip_end
    FROM {{ ref('fact_taxi_trips')}}
)
SELECT 
    HOUR_OF_DAY,
    abs(ROUND(AVG(DATEDIFF(MINUTE, prev_trip_end, PICKUP_DATETIME)), 2)) AS avg_waiting_time_mins
FROM trip_gaps
WHERE prev_trip_end IS NOT NULL
GROUP BY HOUR_OF_DAY
ORDER BY HOUR_OF_DAY