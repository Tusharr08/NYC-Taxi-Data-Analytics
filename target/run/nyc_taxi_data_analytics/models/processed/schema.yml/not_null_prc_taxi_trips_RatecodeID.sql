select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select RatecodeID
from nyc_taxi_db.processed.prc_taxi_trips
where RatecodeID is null



      
    ) dbt_internal_test