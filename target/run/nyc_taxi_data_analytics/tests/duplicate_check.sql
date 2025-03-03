select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT trip_id, COUNT(*)
FROM nyc_taxi_db.conformed.dim_taxi_trips
GROUP BY trip_id
HAVING COUNT(*) > 1
      
    ) dbt_internal_test