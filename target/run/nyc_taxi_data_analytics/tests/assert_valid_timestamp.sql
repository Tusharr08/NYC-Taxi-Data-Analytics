select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      Select * 
from nyc_taxi_db.processed.prc_taxi_trips
where pickup_datetime > dropoff_datetime
      
    ) dbt_internal_test