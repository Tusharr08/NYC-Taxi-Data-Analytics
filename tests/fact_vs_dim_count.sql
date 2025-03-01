SELECT 
    (SELECT COUNT(*) FROM {{ ref('fact_taxi_trips') }}) AS fact_count,
    (SELECT COUNT(*) FROM {{ ref('dim_taxi_trips') }}) AS dim_count
WHERE 
    (SELECT COUNT(*) FROM {{ ref('fact_taxi_trips') }}) 
    != (SELECT COUNT(*) FROM {{ ref('dim_taxi_trips') }})
