select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select borough
from nyc_taxi_db.conformed.dim_location
where borough is null



      
    ) dbt_internal_test