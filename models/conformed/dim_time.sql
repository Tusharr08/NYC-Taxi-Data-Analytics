{{ config(materialized='table') }}

SELECT DISTINCT
    DATE_PART('epoch', pickup_datetime)::INT AS time_id,
    pickup_datetime AS timestamp,
    DATE_PART('hour', pickup_datetime)::INT AS hour,
    DATE_PART('day', pickup_datetime)::INT AS day,
    MONTHNAME(pickup_datetime) AS month,
    DATE_PART('year', pickup_datetime)::INT AS year,
    DAYNAME(pickup_datetime) AS day_of_week
FROM {{ ref('prc_taxi_trips') }}
