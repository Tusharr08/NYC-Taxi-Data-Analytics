select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select PULocationID
from nyc_taxi_db.processed.prc_taxi_trips
where PULocationID is null



      
    ) dbt_internal_test