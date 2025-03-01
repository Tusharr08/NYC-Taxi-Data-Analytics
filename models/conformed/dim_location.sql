{{ config(materialized='table') }}

SELECT 
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone
FROM {{ ref('taxi_zone_lookup') }}