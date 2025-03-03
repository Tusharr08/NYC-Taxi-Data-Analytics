select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select zone
from nyc_taxi_db.conformed.dim_location
where zone is null



      
    ) dbt_internal_test