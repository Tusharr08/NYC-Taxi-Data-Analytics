select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select store_and_fwd_flag
from nyc_taxi_db.processed.prc_taxi_trips
where store_and_fwd_flag is null



      
    ) dbt_internal_test