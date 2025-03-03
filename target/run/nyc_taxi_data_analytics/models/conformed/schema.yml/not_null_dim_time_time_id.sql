select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select time_id
from nyc_taxi_db.conformed.dim_time
where time_id is null



      
    ) dbt_internal_test