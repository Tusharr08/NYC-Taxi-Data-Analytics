select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pickup_datetime
from nyc_taxi_db.conformed.fact_taxi_trips
where pickup_datetime is null



      
    ) dbt_internal_test